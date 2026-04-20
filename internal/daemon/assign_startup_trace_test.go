package daemon

import (
	"context"
	"reflect"
	"testing"
)

func TestAssignEmitsStartupTransitionDiagnostics(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1419", "Trace the startup transition", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	got := deps.events.eventsByType(EventWorkerStartupTransition)
	wantMessages := []string{
		"attempt 1/3: " + assignStartupStepHandshakeReady,
		"attempt 1/3: " + assignStartupStepWaitGate,
		"attempt 1/3: " + assignStartupStepGateAcquired,
		"attempt 1/3: " + assignStartupStepPromptSubmitted,
		"attempt 1/3: " + assignStartupStepPromptConfirmed,
		"attempt 1/3: " + assignStartupStepGateReleased,
	}
	if len(got) != len(wantMessages) {
		t.Fatalf("startup transition events = %d, want %d", len(got), len(wantMessages))
	}

	messages := make([]string, 0, len(got))
	for i, event := range got {
		messages = append(messages, event.Message)
		if gotIssue, wantIssue := event.Issue, "LAB-1419"; gotIssue != wantIssue {
			t.Fatalf("event[%d].Issue = %q, want %q", i, gotIssue, wantIssue)
		}
		if gotPane, wantPane := event.PaneID, "pane-1"; gotPane != wantPane {
			t.Fatalf("event[%d].PaneID = %q, want %q", i, gotPane, wantPane)
		}
		if gotRetry, wantRetry := event.Retry, 1; gotRetry != wantRetry {
			t.Fatalf("event[%d].Retry = %d, want %d", i, gotRetry, wantRetry)
		}
	}
	if !reflect.DeepEqual(messages, wantMessages) {
		t.Fatalf("startup transition messages = %#v, want %#v", messages, wantMessages)
	}
}
