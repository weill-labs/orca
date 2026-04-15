package daemon

import (
	"context"
	"testing"
)

func TestPRReviewPollingLogsAndEmitsTraceForQueuedReviewNudge(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	logs := &fakeLogSink{}
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
	}, nil))

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = logs.Printf
	})
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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "queued review trace poll completion")
	waitFor(t, "queued review trace", func() bool {
		return deps.events.countType(EventReviewPollTrace) == 1
	})

	want := "review poll trace: issue=LAB-689 pr_number=42 prev=reviews:0/inline:0/comments:0 curr=reviews:1/inline:0/comments:0 blocking_count=1 idle_result=idle action=queue_review_nudge persisted=false"
	assertLatestReviewPollTrace(t, deps, logs, 1, want)
}

func TestPRReviewPollingLogsAndEmitsTraceForBusyDeferral(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	logs := &fakeLogSink{}
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
	}, nil))

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = logs.Printf
	})
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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "busy review trace poll completion")
	waitFor(t, "busy review trace", func() bool {
		return deps.events.countType(EventReviewPollTrace) == 1
	})

	want := "review poll trace: issue=LAB-689 pr_number=42 prev=reviews:0/inline:0/comments:0 curr=reviews:1/inline:0/comments:0 blocking_count=1 idle_result=recent_output action=defer_review_nudge persisted=false"
	assertLatestReviewPollTrace(t, deps, logs, 1, want)
}

func TestPRReviewPollingLogsAndEmitsTraceForPersistedNonBlockingFeedback(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	logs := &fakeLogSink{}
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
	}, nil))
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
		testReview("bob", "APPROVED", "Looks good after that."),
	}, nil))

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = logs.Printf
	})
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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "initial review trace poll completion")
	waitFor(t, "initial queued review trace", func() bool {
		return deps.events.countType(EventReviewPollTrace) == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "non-blocking review trace poll completion")
	waitFor(t, "persisted non-blocking review trace", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastReviewCount == 2 &&
			deps.events.countType(EventReviewPollTrace) == 2
	})

	want := "review poll trace: issue=LAB-689 pr_number=42 prev=reviews:1/inline:0/comments:0 curr=reviews:2/inline:0/comments:0 blocking_count=0 idle_result=not_checked action=persist_non_blocking_feedback persisted=true"
	assertLatestReviewPollTrace(t, deps, logs, 2, want)
}

func assertLatestReviewPollTrace(t *testing.T, deps *testDeps, logs *fakeLogSink, wantCount int, want string) {
	t.Helper()

	if got, wantCount := deps.events.countType(EventReviewPollTrace), wantCount; got != wantCount {
		t.Fatalf("trace event sink count = %d, want %d", got, wantCount)
	}
	event, ok := deps.events.lastEventOfType(EventReviewPollTrace)
	if !ok {
		t.Fatal("missing review poll trace event in event sink")
	}
	if got, want := event.Message, want; got != want {
		t.Fatalf("event sink trace message = %q, want %q", got, want)
	}

	if got, wantCount := stateEventCountByType(deps.state, EventReviewPollTrace), wantCount; got != wantCount {
		t.Fatalf("trace db event count = %d, want %d", got, wantCount)
	}
	event, ok = lastStateEventOfType(deps.state, EventReviewPollTrace)
	if !ok {
		t.Fatal("missing review poll trace event in state store")
	}
	if got, want := event.Message, want; got != want {
		t.Fatalf("state trace message = %q, want %q", got, want)
	}

	messages := logs.messages()
	if got, wantCount := len(messages), wantCount; got != wantCount {
		t.Fatalf("trace log count = %d, want %d", got, wantCount)
	}
	if got, want := messages[len(messages)-1], want; got != want {
		t.Fatalf("last trace log = %q, want %q", got, want)
	}
}

func stateEventCountByType(state *fakeState, eventType string) int {
	state.mu.Lock()
	defer state.mu.Unlock()

	count := 0
	for _, event := range state.events {
		if event.Type == eventType {
			count++
		}
	}
	return count
}

func lastStateEventOfType(state *fakeState, eventType string) (Event, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()

	for i := len(state.events) - 1; i >= 0; i-- {
		if state.events[i].Type == eventType {
			return state.events[i], true
		}
	}
	return Event{}, false
}
