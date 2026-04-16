package daemon

import (
	"context"
	"errors"
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
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"CHANGES_REQUESTED","body":"Handle the nil case too."}]}`)

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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "first review poll cycle completion")
	waitFor(t, "first review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && deps.amux.countKey("pane-1", firstNudgeSent) == 1 && worker.LastReviewCount == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "second review poll cycle completion")
	waitFor(t, "second review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 2
	})
	if got, want := deps.amux.countKey("pane-1", firstNudgeSent), 1; got != want {
		t.Fatalf("first review nudge count = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "third review poll cycle completion")
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
		wrappedCodexPrompt("Implement daemon core") + "\n",
		firstNudgeSent,
		secondNudgeSent,
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
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

func TestPRReviewPollingNudgesWorkerWithInlineReviewCommentLocation(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[]}`, nil)
	deps.commands.queue("gh", []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}, `[
		{
			"user":{"login":"alice"},
			"path":"internal/daemon/review.go",
			"line":174,
			"body":"Include reviewer details in the worker nudge.",
			"created_at":"2026-04-02T09:05:00Z"
		}
	]`, nil)

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

	nudge := "New blocking PR review feedback on #42:\n- alice on internal/daemon/review.go:174: Include reviewer details in the worker nudge.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "inline review comment nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", nudgeSent) == 1 &&
			worker.LastReviewCount == 0 &&
			worker.LastInlineReviewCommentCount == 1
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{
		wrappedCodexPrompt("Implement daemon core") + "\n",
		nudgeSent,
	})
}

func TestPRReviewPollingDoesNotDuplicateSeenReviewsWhenOlderInlineCommentArrivesLater(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"bob"},"state":"CHANGES_REQUESTED","body":"Please add tests.","submittedAt":"2026-04-02T09:05:00Z"}],"comments":[]}`, nil)
	deps.commands.queue("gh", []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"bob"},"state":"CHANGES_REQUESTED","body":"Please add tests.","submittedAt":"2026-04-02T09:05:00Z"}],"comments":[]}`, nil)
	deps.commands.queue("gh", []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}, `[
		{
			"user":{"login":"alice"},
			"path":"internal/daemon/review.go",
			"line":174,
			"body":"Include reviewer details in the worker nudge.",
			"created_at":"2026-04-02T09:04:00Z"
		}
	]`, nil)

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

	firstNudge := "New blocking PR review feedback on #42:\n- bob: Please add tests.\n\nAddress the feedback in the PR review and push an update."
	secondNudge := "New blocking PR review feedback on #42:\n- alice on internal/daemon/review.go:174: Include reviewer details in the worker nudge.\n\nAddress the feedback in the PR review and push an update."
	firstNudgeSent := firstNudge + "\n"
	secondNudgeSent := secondNudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "initial review poll cycle completion")
	waitFor(t, "initial review nudge", func() bool {
		return deps.amux.countKey("pane-1", firstNudgeSent) == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "late inline comment poll cycle completion")
	waitFor(t, "late inline comment nudge", func() bool {
		return deps.amux.countKey("pane-1", secondNudgeSent) == 1
	})

	if got, want := deps.amux.countKey("pane-1", firstNudgeSent), 1; got != want {
		t.Fatalf("first review nudge count = %d, want %d", got, want)
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
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"APPROVED","body":"Looks good after that."}]}`)

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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "initial blocking review poll cycle completion")
	waitFor(t, "initial blocking review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && deps.amux.countKey("pane-1", firstNudgeSent) == 1 && worker.LastReviewCount == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "non-blocking review poll cycle completion")
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
		wrappedCodexPrompt("Implement daemon core") + "\n",
		firstNudgeSent,
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestPRReviewPollingEmitsApprovedEventOncePerApprovalTransition(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "APPROVED", []prReview{
		testReview("alice", "APPROVED", "Looks good."),
	}, nil))
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "APPROVED", []prReview{
		testReview("alice", "APPROVED", "Looks good."),
	}, []prComment{
		testIssueComment("github-actions", "CI finished successfully."),
	}))
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "APPROVED", "Looks good."),
	}, []prComment{
		testIssueComment("github-actions", "CI finished successfully."),
	}))
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "APPROVED", []prReview{
		testReview("alice", "APPROVED", "Looks good."),
	}, []prComment{
		testIssueComment("github-actions", "CI finished successfully."),
	}))

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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "approval review poll cycle completion")
	waitFor(t, "approval event emitted", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastReviewCount == 1 &&
			worker.ReviewApproved &&
			deps.events.countType(EventReviewApproved) == 1 &&
			stateEventCountByType(deps.state, EventReviewApproved) == 1
	})

	event, ok := deps.events.lastEventOfType(EventReviewApproved)
	if !ok {
		t.Fatal("missing approved review event")
	}
	if got, want := event.Message, "pull request approved"; got != want {
		t.Fatalf("approved event message = %q, want %q", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "approval review poll after non-blocking comment")
	waitFor(t, "approval poll processed with new comment", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 2 &&
			worker.LastIssueCommentCount == 1 &&
			worker.ReviewApproved
	})

	if got, want := deps.events.countType(EventReviewApproved), 1; got != want {
		t.Fatalf("approved review event count = %d, want %d", got, want)
	}
	if got, want := stateEventCountByType(deps.state, EventReviewApproved), 1; got != want {
		t.Fatalf("approved review state event count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 0; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "approval state cleared poll cycle completion")
	waitFor(t, "approval state cleared", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 3 &&
			!worker.ReviewApproved
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "approval re-emitted poll cycle completion")
	waitFor(t, "approval re-emitted", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 4 &&
			worker.ReviewApproved &&
			deps.events.countType(EventReviewApproved) == 2 &&
			stateEventCountByType(deps.state, EventReviewApproved) == 2
	})
	deps.amux.requireSentKeys(t, "pane-1", []string{wrappedCodexPrompt("Implement daemon core") + "\n"})
}

func TestPRReviewPollingIgnoresEmptyReviewPayload(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`)
	queuePRReviewPayload(deps, 42, ``)

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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "initial review poll cycle completion")
	waitFor(t, "initial review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && deps.amux.countKey("pane-1", firstNudgeSent) == 1 && worker.LastReviewCount == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "empty review poll cycle completion")
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
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"Potential bug: stale local branch in prepareAdoptedClone."}]}`)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"Potential bug: stale local branch in prepareAdoptedClone."},{"author":{"login":"cweill"},"body":"Thanks, taking a look."}]}`)

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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "issue comment review poll cycle completion")
	waitFor(t, "issue comment review nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", firstNudgeSent) == 1 &&
			worker.LastReviewCount == 0 &&
			worker.LastIssueCommentCount == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "non-bot comment review poll cycle completion")
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
		wrappedCodexPrompt("Implement daemon core") + "\n",
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
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"CHANGES_REQUESTED","body":"Handle the nil case too."}]}`)

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

	tickAndWaitForHeartbeat(t, first, deps, firstPollTicker, adaptivePRFastPollInterval, "first persisted review poll cycle completion")
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

	tickAndWaitForHeartbeat(t, second, deps, secondPollTicker, adaptivePRFastPollInterval, "restart review poll cycle completion")
	waitFor(t, "restart review poll", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) >= 2
	})
	if got, want := deps.amux.countKey("pane-1", firstNudgeSent), 1; got != want {
		t.Fatalf("first review nudge count after restart = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, second, deps, secondPollTicker, adaptivePRFastPollInterval, "second persisted review poll cycle completion")
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
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"### PR Review\n\n### Blocking Issues\n\n**1. Add regression coverage for issue comments**"}]}`)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"### PR Review\n\n### Blocking Issues\n\n**1. Add regression coverage for issue comments**"}]}`)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[{"author":{"login":"github-actions"},"body":"### PR Review\n\n### Blocking Issues\n\n**1. Add regression coverage for issue comments**"},{"author":{"login":"github-actions"},"body":"### PR Review\n\n### Blocking Issue\n\n**1. Persist the issue comment cursor across restarts**"}]}`)

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

	tickAndWaitForHeartbeat(t, first, deps, firstPollTicker, adaptivePRFastPollInterval, "first persisted issue comment poll cycle completion")
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

	tickAndWaitForHeartbeat(t, second, deps, secondPollTicker, adaptivePRFastPollInterval, "restart issue comment poll cycle completion")
	waitFor(t, "restart issue comment poll", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) >= 2
	})
	if got, want := deps.amux.countKey("pane-1", firstNudgeSent), 1; got != want {
		t.Fatalf("first issue comment nudge count after restart = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, second, deps, secondPollTicker, adaptivePRFastPollInterval, "second persisted issue comment poll cycle completion")
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
	queuePRReviewPayload(deps, 42, payload)
	queuePRReviewPayload(deps, 42, payload)

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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "busy worker review poll cycle completion")
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
	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "idle worker review poll cycle completion")
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
	queuePRReviewPayload(deps, 42, payload)
	queuePRReviewPayload(deps, 42, payload)
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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "fresh capture review poll cycle completion")
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
	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "post-deferral review poll cycle completion")
	waitFor(t, "fresh capture review nudge after idle", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastReviewCount == 1 && deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func TestPRReviewPollingRetriesIssueCommentNudgeAfterBusyDeferral(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, ``, nil)
	payload := marshalReviewPayload(t, "", nil, []prComment{
		testIssueComment("github-actions", "Potential bug: add regression coverage."),
	})
	queuePRReviewPayload(deps, 42, payload)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, ``, nil)
	queuePRReviewPayload(deps, 42, payload)

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

	nudge := "New blocking PR review feedback on #42:\n- github-actions: Potential bug: add regression coverage.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "busy worker issue comment poll cycle completion")
	waitFor(t, "busy worker issue comment poll", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 1 &&
			worker.LastCIState == ciStatePending
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after deferred issue comment poll")
	}
	if got, want := worker.LastIssueCommentCount, 0; got != want {
		t.Fatalf("worker.LastIssueCommentCount after deferred poll = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 0; got != want {
		t.Fatalf("worker.ReviewNudgeCount after deferred poll = %d, want %d", got, want)
	}
	if got := deps.amux.countKey("pane-1", nudgeSent); got != 0 {
		t.Fatalf("issue comment nudge count while worker active = %d, want 0", got)
	}

	makeWorkerIdleForReviewNudge(deps)
	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "idle worker issue comment poll cycle completion")
	waitFor(t, "deferred issue comment nudge after idle", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastIssueCommentCount == 1 &&
			worker.ReviewNudgeCount == 1 &&
			deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func TestPRReviewPollingLogsGitHubRateLimitWarnings(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, "HTTP 429: API rate limit exceeded\nRetry-After: 120\n", errors.New("gh: HTTP 429"))

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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "review poll rate limit event", func() bool {
		return deps.events.countType(EventPRRateLimited) == 1
	})

	event, ok := deps.events.lastEventOfType(EventPRRateLimited)
	if !ok {
		t.Fatal("missing GitHub rate limit event")
	}
	if got, want := event.Message, "github: rate limited until 09:02"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}), 1; got != want {
		t.Fatalf("review poll count = %d, want %d", got, want)
	}
}

func makeWorkerIdleForReviewNudge(deps *testDeps) {
	deps.clock.Advance((2 * defaultCaptureInterval) + time.Second)
}
