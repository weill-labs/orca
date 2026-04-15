package daemon

import (
	"context"
	"testing"
)

func TestPRReviewPollingNudgesForEditedBlockingIssueCommentWithoutCountChange(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)

	progress := testIssueComment("github-actions", "### PR Review: LAB-1273\n\nReviewing...\n\n- [x] Gather context\n- [ ] Read changed files\n- [ ] Post final review\n\n[View job run](https://github.com/weill-labs/orca/actions/runs/1)")
	progress.ID = "IC_progress"

	blocking := progress
	blocking.Body = "Potential bug: review comment edits should still reach the worker."

	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "", nil, []prComment{progress}))
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "", nil, []prComment{blocking}))
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "", nil, []prComment{blocking}))

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

	nudge := "New blocking PR review feedback on #42:\n- github-actions: Potential bug: review comment edits should still reach the worker.\n\nAddress the feedback in the PR review and push an update."
	nudgeSent := nudge + "\n"

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "initial progress issue comment poll cycle completion")
	waitFor(t, "progress issue comment watermark advance", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 1 &&
			worker.LastIssueCommentCount == 1
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after progress issue comment poll")
	}
	if got, want := worker.ReviewNudgeCount, 0; got != want {
		t.Fatalf("worker.ReviewNudgeCount after progress poll = %d, want %d", got, want)
	}
	if got := deps.amux.countKey("pane-1", nudgeSent); got != 0 {
		t.Fatalf("issue comment nudge count after progress poll = %d, want 0", got)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "edited blocking issue comment poll cycle completion")
	waitFor(t, "edited blocking issue comment nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", nudgeSent) == 1 &&
			worker.LastIssueCommentCount == 1 &&
			worker.ReviewNudgeCount == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "repeat edited blocking issue comment poll cycle completion")
	waitFor(t, "repeat edited blocking issue comment poll", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 3
	})

	if got, want := deps.amux.countKey("pane-1", nudgeSent), 1; got != want {
		t.Fatalf("issue comment nudge count after repeated edited poll = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}
