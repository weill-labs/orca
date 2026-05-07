package daemon

import (
	"context"
	"fmt"
	"testing"
)

func TestPRReviewPollingDedupesUnresolvedReviewThreads(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1156", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1156", "--json", "number"}, `[{"number":42}]`, nil)
	reviewPayload := reviewThreadTriggerPayload()
	queuePRReviewPayload(deps, 42, reviewPayload)
	queuePRReviewThreadsPayload(deps, 42, reviewThreadsPayload(reviewThreadNode("thread-1", false, "internal/daemon/review.go", 42, "comment-1", "alice", "Please handle this.")))
	queuePRReviewPayload(deps, 42, reviewPayload)
	queuePRReviewThreadsPayload(deps, 42, reviewThreadsPayload(reviewThreadNode("thread-1", false, "internal/daemon/review.go", 42, "comment-1", "alice", "Please handle this.")))

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1156", "Implement review feedback polling", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	nudge := "New blocking PR review feedback on #42:\n- alice on internal/daemon/review.go:42: Please handle this.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "first review thread poll cycle completion")
	waitFor(t, "first review thread nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastIssueCommentWatermark != "" && deps.amux.countKey("pane-1", nudgeSent) == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "dedupe review thread poll cycle completion")
	waitFor(t, "dedupe review thread poll processed", func() bool {
		return deps.commands.countCalls("gh", prReviewThreadsGraphQLArgs(42)) == 2
	})
	if got, want := deps.amux.countKey("pane-1", nudgeSent), 1; got != want {
		t.Fatalf("review thread nudge count = %d, want %d", got, want)
	}
}

func TestPRReviewPollingDefersReviewThreadNudgeUntilWorkerIsIdle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1156", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1156", "--json", "number"}, `[{"number":42}]`, nil)
	reviewPayload := reviewThreadTriggerPayload()
	threadPayload := reviewThreadsPayload(reviewThreadNode("thread-1", false, "internal/daemon/review.go", 42, "comment-1", "alice", "Please handle this."))
	queuePRReviewPayload(deps, 42, reviewPayload)
	queuePRReviewThreadsPayload(deps, 42, threadPayload)
	queuePRReviewPayload(deps, 42, reviewPayload)
	queuePRReviewThreadsPayload(deps, 42, threadPayload)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1156", "Implement review feedback polling", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	nudge := "New blocking PR review feedback on #42:\n- alice on internal/daemon/review.go:42: Please handle this.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "busy review thread poll cycle completion")
	waitFor(t, "busy review thread poll processed", func() bool {
		return deps.commands.countCalls("gh", prReviewThreadsGraphQLArgs(42)) == 1
	})
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after busy review thread poll")
	}
	if got := worker.LastIssueCommentWatermark; got != "" {
		t.Fatalf("worker.LastIssueCommentWatermark after deferral = %q, want empty", got)
	}
	if got := deps.amux.countKey("pane-1", nudgeSent); got != 0 {
		t.Fatalf("review thread nudge count while worker active = %d, want 0", got)
	}

	makeWorkerIdleForReviewNudge(deps)
	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "idle review thread poll cycle completion")
	waitFor(t, "deferred review thread nudge after idle", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastIssueCommentWatermark != "" && deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func TestPRReviewPollingStopsOnPRCloseBeforeReviewThreads(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1156", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"state":"CLOSED","mergedAt":null,"closedAt":"2026-04-16T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-1156"))

	if got, want := update.CompletionStatus, TaskStatusFailed; got != want {
		t.Fatalf("update.CompletionStatus = %q, want %q", got, want)
	}
	if got := deps.commands.countCalls("gh", prReviewThreadsGraphQLArgs(42)); got != 0 {
		t.Fatalf("review thread lookup count after closed PR = %d, want 0", got)
	}
	if got := deps.events.countType(EventWorkerNudgedReview); got != 0 {
		t.Fatalf("review nudge event count after closed PR = %d, want 0", got)
	}
}

func TestPRReviewPollingNudgesForHumanThreadAndBotIssueComment(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1156", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1156", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "", nil, []prComment{
		testIssueComment("coderabbitai[bot]", "Potential bug: cover the review-thread retry path."),
	}))
	queuePRReviewThreadsPayload(deps, 42, reviewThreadsPayload(reviewThreadNode("thread-1", false, "internal/daemon/review.go", 42, "comment-1", "alice", "Please handle this.")))

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1156", "Implement review feedback polling", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	nudge := "New blocking PR review feedback on #42:\n- coderabbitai[bot]: Potential bug: cover the review-thread retry path.\n- alice on internal/daemon/review.go:42: Please handle this.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "human and bot review feedback poll cycle completion")
	waitFor(t, "human thread and bot issue comment nudge", func() bool {
		return deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func TestPRReviewPollingTreatsUnresolvedThreadsAsBlockingAfterApproval(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1156", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1156", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "APPROVED", []prReview{
		testReview("alice", "APPROVED", "Looks good after this thread is handled."),
	}, nil))
	queuePRReviewThreadsPayload(deps, 42, reviewThreadsPayload(reviewThreadNode("thread-1", false, "internal/daemon/review.go", 42, "comment-1", "alice", "Please handle this before merge.")))

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1156", "Implement review feedback polling", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	nudge := "New blocking PR review feedback on #42:\n- alice on internal/daemon/review.go:42: Please handle this before merge.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "approved unresolved review thread poll cycle completion")
	waitFor(t, "approved unresolved review thread nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			!worker.ReviewApproved &&
			worker.LastIssueCommentWatermark != "" &&
			deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func queuePRReviewThreadsPayload(deps *testDeps, prNumber int, payload string) {
	deps.commands.queue("gh", prReviewThreadsGraphQLArgs(prNumber), payload, nil)
}

func reviewThreadTriggerPayload() string {
	return `{"reviewDecision":"","reviews":[{"author":{"login":"alice"},"state":"COMMENTED","body":"Leaving an inline thread."}],"comments":[]}`
}

func reviewThreadsPayload(nodes string) string {
	return `{"data":{"repository":{"pullRequest":{"reviewThreads":{"nodes":[` + nodes + `]}}}}}`
}

func reviewThreadNode(id string, resolved bool, path string, line int, commentID string, author string, body string) string {
	return fmt.Sprintf(
		`{"id":%q,"isResolved":%t,"path":%q,"line":%d,"comments":{"nodes":[{"id":%q,"body":%q,"createdAt":"2026-04-02T09:05:00Z","author":{"login":%q}}]}}`,
		id,
		resolved,
		path,
		line,
		commentID,
		body,
		author,
	)
}
