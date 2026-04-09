package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func TestExitedEventEscalatesWorkerImmediately(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-922", "pane-1")
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content:        []string{"shell prompt"},
		CurrentCommand: "bash",
		Exited:         true,
	}})
	deps.amux.enqueueEventSequence(fakeAmuxEventSequence{
		events: []amuxapi.Event{{Type: "exited", PaneName: "pane-1"}},
	})

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "worker escalation from exited event", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.Health == WorkerHealthEscalated
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after exited event")
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got, want := deps.amux.captureCount("pane-1"), 1; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerEscalated), 1; got != want {
		t.Fatalf("worker escalated events = %d, want %d", got, want)
	}
}

func TestExitedEventListenerReconnectsAfterStreamError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-922", "pane-1")
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content:        []string{"shell prompt"},
		CurrentCommand: "bash",
		Exited:         true,
	}})
	deps.amux.enqueueEventSequence(fakeAmuxEventSequence{err: errors.New("stream dropped")})
	deps.amux.enqueueEventSequence(fakeAmuxEventSequence{
		events: []amuxapi.Event{{Type: "exited", PaneName: "pane-1"}},
	})

	var sleeps []time.Duration
	deps.sleep = func(_ context.Context, delay time.Duration) error {
		sleeps = append(sleeps, delay)
		return nil
	}

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "event stream reconnect", func() bool {
		return deps.amux.eventsCallCount() >= 2
	})
	waitFor(t, "worker escalation after reconnect", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.Health == WorkerHealthEscalated
	})

	if got, want := len(sleeps), 1; got != want {
		t.Fatalf("len(sleeps) = %d, want %d", got, want)
	}
	if sleeps[0] <= 0 {
		t.Fatalf("sleep[0] = %s, want positive backoff", sleeps[0])
	}
}
