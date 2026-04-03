package daemon

import (
	"context"
	"testing"
)

func TestPRReviewPollingNudgesWorkerOncePerNewBlockingReviewBatch(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"CHANGES_REQUESTED","body":"Handle the nil case too."}]}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update.\n"
	secondNudge := "New blocking PR review feedback on #42:\n- bob: Handle the nil case too.\n\nAddress the feedback in the PR review and push an update.\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "first review nudge", func() bool {
		return deps.amux.countKey("pane-1", firstNudge) == 1 && active.lastReviewCount.Load() == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}) == 2
	})
	if got, want := deps.amux.countKey("pane-1", firstNudge), 1; got != want {
		t.Fatalf("first review nudge count = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second review nudge", func() bool {
		return deps.amux.countKey("pane-1", secondNudge) == 1 && active.lastReviewCount.Load() == 2
	})

	if got, want := active.lastReviewCount.Load(), int64(2); got != want {
		t.Fatalf("assignment.lastReviewCount = %d, want %d", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		firstNudge,
		secondNudge,
	})
	if got, want := deps.events.countType(EventWorkerNudgedReview), 2; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestPRReviewPollingAdvancesCountWithoutNudgingForNonBlockingReviews(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"APPROVED","body":"Looks good after that."}]}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update.\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial blocking review nudge", func() bool {
		return deps.amux.countKey("pane-1", firstNudge) == 1 && active.lastReviewCount.Load() == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "non-blocking review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}) == 2 &&
			active.lastReviewCount.Load() == 2
	})
	if got, want := active.lastReviewCount.Load(), int64(2); got != want {
		t.Fatalf("assignment.lastReviewCount = %d, want %d", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		firstNudge,
	})
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestPRReviewPollingIgnoresEmptyReviewPayload(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update.\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial review nudge", func() bool {
		return deps.amux.countKey("pane-1", firstNudge) == 1 && active.lastReviewCount.Load() == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "empty review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}) == 2
	})

	if got, want := active.lastReviewCount.Load(), int64(1); got != want {
		t.Fatalf("assignment.lastReviewCount = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}
