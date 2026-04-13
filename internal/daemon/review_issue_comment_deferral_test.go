package daemon

import (
	"context"
	"testing"
	"time"
)

func TestPRReviewPollingRetriesIssueCommentNudgeAfterFreshCaptureDeferral(t *testing.T) {
	t.Parallel()

	payload := marshalReviewPayload(t, "", nil, []prComment{
		testIssueComment("github-actions[bot]", "### PR Review\n\n### Blocking Issues\n\n**1. Preserve issue comment watermarks until the nudge lands**"),
		testIssueComment("cweill", "Checking the failure locally."),
	})
	d, deps, prTicker := startFreshCaptureIssueCommentPollTest(t, payload, "still coding after review poll")

	makeWorkerIdleForReviewNudge(deps)
	nudge := "New blocking PR review feedback on #42:\n- github-actions[bot]: Preserve issue comment watermarks until the nudge lands\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "fresh capture issue comment poll cycle completion")
	waitFor(t, "fresh capture issue comment deferral", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 1 &&
			worker.LastCapture == "still coding after review poll"
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after fresh capture issue comment deferral")
	}
	if got, want := worker.LastIssueCommentCount, 0; got != want {
		t.Fatalf("worker.LastIssueCommentCount after fresh capture deferral = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 0; got != want {
		t.Fatalf("worker.ReviewNudgeCount after fresh capture deferral = %d, want %d", got, want)
	}
	if got := deps.amux.countKey("pane-1", nudgeSent); got != 0 {
		t.Fatalf("issue comment nudge count after fresh capture deferral = %d, want 0", got)
	}

	makeWorkerIdleForReviewNudge(deps)
	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "post-fresh-capture issue comment poll cycle completion")
	waitFor(t, "fresh capture issue comment nudge after idle", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastIssueCommentCount == 2 &&
			worker.ReviewNudgeCount == 1 &&
			deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func TestPRReviewPollingDoesNotResetReviewNudgeCountBeforeDeferredIssueCommentNudgeLands(t *testing.T) {
	t.Parallel()

	payload := marshalReviewPayload(t, "", nil, []prComment{
		testIssueComment("github-actions[bot]", "### PR Review\n\n### Blocking Issues\n\n**1. Retry the fresh-capture issue comment nudge**"),
		testIssueComment("cweill", "Looking at it."),
	})
	d, deps, prTicker := startFreshCaptureIssueCommentPollTest(t, payload, "still coding after fresh capture")

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after assignment")
	}
	worker.ReviewNudgeCount = 2
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	makeWorkerIdleForReviewNudge(deps)
	nudge := "New blocking PR review feedback on #42:\n- github-actions[bot]: Retry the fresh-capture issue comment nudge\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "fresh capture review nudge reset poll cycle completion")
	waitFor(t, "fresh capture review nudge reset deferral", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 1 &&
			worker.LastCapture == "still coding after fresh capture"
	})

	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after fresh capture nudge reset deferral")
	}
	if got, want := worker.LastIssueCommentCount, 0; got != want {
		t.Fatalf("worker.LastIssueCommentCount after fresh capture nudge reset deferral = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 2; got != want {
		t.Fatalf("worker.ReviewNudgeCount after fresh capture nudge reset deferral = %d, want %d", got, want)
	}
	if got := deps.amux.countKey("pane-1", nudgeSent); got != 0 {
		t.Fatalf("issue comment nudge count after fresh capture nudge reset deferral = %d, want 0", got)
	}

	makeWorkerIdleForReviewNudge(deps)
	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "fresh capture review nudge reset retry poll cycle completion")
	waitFor(t, "fresh capture review nudge reset retry", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastIssueCommentCount == 2 &&
			worker.ReviewNudgeCount == 1 &&
			deps.amux.countKey("pane-1", nudgeSent) == 1
	})
}

func startFreshCaptureIssueCommentPollTest(t *testing.T, payload, captureOutput string) (*Daemon, *testDeps, *fakeTicker) {
	t.Helper()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, payload)
	queuePRReviewPayload(deps, 42, payload)
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		paneCaptureFromOutput(captureOutput),
		paneCaptureFromOutput(captureOutput),
	})

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

	return d, deps, prTicker
}
