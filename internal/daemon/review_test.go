package daemon

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestPRReviewPollingNudgesWorkerOncePerNewBlockingReviewBatch(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"CHANGES_REQUESTED","body":"Handle the nil case too."}]}`, nil)

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
	makeWorkerIdleForReviewNudge(deps)

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update."
	secondNudge := "New blocking PR review feedback on #42:\n- bob: Handle the nil case too.\n\nAddress the feedback in the PR review and push an update."
	firstNudgeSent := firstNudge + "\n"
	secondNudgeSent := secondNudge + "\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "first review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && deps.amux.countKey("pane-1", firstNudgeSent) == 1 && worker.LastReviewCount == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 2
	})
	if got, want := deps.amux.countKey("pane-1", firstNudgeSent), 1; got != want {
		t.Fatalf("first review nudge count = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && deps.amux.countKey("pane-1", secondNudgeSent) == 1 && worker.LastReviewCount == 2
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after review polling")
	}
	if got, want := worker.LastReviewCount, 2; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		firstNudgeSent,
		secondNudgeSent,
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
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
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"APPROVED","body":"Looks good after that."}]}`, nil)

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
	makeWorkerIdleForReviewNudge(deps)

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update."
	firstNudgeSent := firstNudge + "\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial blocking review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && deps.amux.countKey("pane-1", firstNudgeSent) == 1 && worker.LastReviewCount == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "non-blocking review poll processed", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 2 &&
			worker.LastReviewCount == 2
	})
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after non-blocking reviews")
	}
	if got, want := worker.LastReviewCount, 2; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		firstNudgeSent,
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
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
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

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
	makeWorkerIdleForReviewNudge(deps)

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update."
	firstNudgeSent := firstNudge + "\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && deps.amux.countKey("pane-1", firstNudgeSent) == 1 && worker.LastReviewCount == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "empty review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 2
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after empty review payload")
	}
	if got, want := worker.LastReviewCount, 1; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestPRReviewPollingNudgesWorkerForGitHubActionsIssueCommentsWithoutLGTM(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"Potential bug: stale local branch in prepareAdoptedClone."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"Potential bug: stale local branch in prepareAdoptedClone."},{"author":{"login":"cweill"},"body":"Thanks, taking a look."}]}`, nil)

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
	makeWorkerIdleForReviewNudge(deps)

	firstNudge := "New blocking PR review feedback on #42:\n- github-actions: Potential bug: stale local branch in prepareAdoptedClone.\n\nAddress the feedback in the PR review and push an update."
	firstNudgeSent := firstNudge + "\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "issue comment review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", firstNudgeSent) == 1 &&
			worker.LastReviewCount == 0 &&
			worker.LastIssueCommentCount == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "non-bot comment poll processed", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 2 &&
			worker.LastIssueCommentCount == 2
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after issue comment polling")
	}
	if got, want := worker.LastIssueCommentCount, 2; got != want {
		t.Fatalf("worker.LastIssueCommentCount = %d, want %d", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		firstNudgeSent,
	})
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestPRReviewPollingResumesFromPersistedWorkerStateAfterRestart(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	firstCaptureTicker := newFakeTicker()
	firstPollTicker := newFakeTicker()
	secondCaptureTicker := newFakeTicker()
	secondPollTicker := newFakeTicker()
	deps.tickers.enqueue(firstCaptureTicker, firstPollTicker, secondCaptureTicker, secondPollTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"CHANGES_REQUESTED","body":"Handle the nil case too."}]}`, nil)

	first := deps.newDaemon(t)
	ctx := context.Background()
	if err := first.Start(ctx); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}

	if err := first.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update."
	secondNudge := "New blocking PR review feedback on #42:\n- bob: Handle the nil case too.\n\nAddress the feedback in the PR review and push an update."
	firstNudgeSent := firstNudge + "\n"
	secondNudgeSent := secondNudge + "\n"

	firstPollTicker.tick(deps.clock.Now())
	waitFor(t, "first persisted review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastReviewCount == 1 && deps.amux.countKey("pane-1", firstNudgeSent) == 1
	})

	if err := first.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}

	second := deps.newDaemon(t)
	if err := second.Start(ctx); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = second.Stop(context.Background())
	})

	secondPollTicker.tick(deps.clock.Now())
	waitFor(t, "restart review poll", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) >= 2
	})
	if got, want := deps.amux.countKey("pane-1", firstNudgeSent), 1; got != want {
		t.Fatalf("first review nudge count after restart = %d, want %d", got, want)
	}

	secondPollTicker.tick(deps.clock.Now())
	waitFor(t, "second persisted review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastReviewCount == 2 && deps.amux.countKey("pane-1", secondNudgeSent) == 1
	})
}

func TestPRReviewPollingResumesIssueCommentCursorAfterRestart(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	firstCaptureTicker := newFakeTicker()
	firstPollTicker := newFakeTicker()
	secondCaptureTicker := newFakeTicker()
	secondPollTicker := newFakeTicker()
	deps.tickers.enqueue(firstCaptureTicker, firstPollTicker, secondCaptureTicker, secondPollTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"### PR Review\n\n### Blocking Issues\n\n**1. Add regression coverage for issue comments**"}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"### PR Review\n\n### Blocking Issues\n\n**1. Add regression coverage for issue comments**"}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"### PR Review\n\n### Blocking Issues\n\n**1. Add regression coverage for issue comments**"},{"author":{"login":"github-actions"},"body":"### PR Review\n\n### Blocking Issue\n\n**1. Persist the issue comment cursor across restarts**"}]}`, nil)

	first := deps.newDaemon(t)
	ctx := context.Background()
	if err := first.Start(ctx); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}

	if err := first.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	firstNudge := "New blocking PR review feedback on #42:\n- github-actions: Add regression coverage for issue comments\n\nAddress the feedback in the PR review and push an update."
	secondNudge := "New blocking PR review feedback on #42:\n- github-actions: Persist the issue comment cursor across restarts\n\nAddress the feedback in the PR review and push an update."
	firstNudgeSent := firstNudge + "\n"
	secondNudgeSent := secondNudge + "\n"

	firstPollTicker.tick(deps.clock.Now())
	waitFor(t, "first persisted issue comment nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastIssueCommentCount == 1 && deps.amux.countKey("pane-1", firstNudgeSent) == 1
	})

	if err := first.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}

	second := deps.newDaemon(t)
	if err := second.Start(ctx); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = second.Stop(context.Background())
	})

	secondPollTicker.tick(deps.clock.Now())
	waitFor(t, "restart issue comment poll", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) >= 2
	})
	if got, want := deps.amux.countKey("pane-1", firstNudgeSent), 1; got != want {
		t.Fatalf("first issue comment nudge count after restart = %d, want %d", got, want)
	}

	secondPollTicker.tick(deps.clock.Now())
	waitFor(t, "second persisted issue comment nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastIssueCommentCount == 2 && deps.amux.countKey("pane-1", secondNudgeSent) == 1
	})
}

func TestPRReviewPollingDefersBlockingReviewNudgeUntilWorkerIsIdle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	payload := `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, payload, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, payload, nil)

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

	nudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "busy worker review poll", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 1
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after deferred review poll")
	}
	if got, want := worker.LastReviewCount, 0; got != want {
		t.Fatalf("worker.LastReviewCount after deferred poll = %d, want %d", got, want)
	}
	if got := deps.amux.countKey("pane-1", nudgeSent); got != 0 {
		t.Fatalf("review nudge count while worker active = %d, want 0", got)
	}

	makeWorkerIdleForReviewNudge(deps)
	prTicker.tick(deps.clock.Now())
	waitFor(t, "deferred review nudge after idle", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastReviewCount == 1 && deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func TestPRReviewPollingDefersBlockingReviewNudgeWhenFreshCaptureShowsActivity(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	payload := `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, payload, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, payload, nil)
	deps.amux.captureSequence("pane-1", []string{"still coding"})

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

	makeWorkerIdleForReviewNudge(deps)
	nudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "fresh capture review deferral", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCapture == "still coding"
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after fresh capture deferral")
	}
	if got, want := worker.LastReviewCount, 0; got != want {
		t.Fatalf("worker.LastReviewCount after fresh capture deferral = %d, want %d", got, want)
	}
	if got := deps.amux.countKey("pane-1", nudgeSent); got != 0 {
		t.Fatalf("review nudge count after fresh capture deferral = %d, want 0", got)
	}

	makeWorkerIdleForReviewNudge(deps)
	prTicker.tick(deps.clock.Now())
	waitFor(t, "fresh capture review nudge after idle", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastReviewCount == 1 && deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func makeWorkerIdleForReviewNudge(deps *testDeps) {
	deps.clock.Advance((2 * defaultCaptureInterval) + time.Second)
}
