package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func TestExitedEventRestartsWorkerImmediately(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-922", "pane-1")
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		{
			Content:        []string{"shell prompt"},
			CurrentCommand: "bash",
		},
		{
			Content:        []string{"shell prompt"},
			CurrentCommand: "bash",
			Exited:         true,
		},
	})
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

	waitFor(t, "worker restart from exited event", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.Health == WorkerHealthHealthy && worker.RestartCount == 1
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after exited event")
	}
	if got, want := worker.Health, WorkerHealthHealthy; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got, want := worker.RestartCount, 1; got != want {
		t.Fatalf("worker.RestartCount = %d, want %d", got, want)
	}
	if got, want := deps.amux.captureCount("pane-1"), 3; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", "codex --yolo\n"), 1; got != want {
		t.Fatalf("restart send count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerEscalated), 0; got != want {
		t.Fatalf("worker escalated events = %d, want %d", got, want)
	}
}

func TestExitedEventListenerReconnectsAfterStreamError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-922", "pane-1")
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		{
			Content:        []string{"shell prompt"},
			CurrentCommand: "bash",
		},
		{
			Content:        []string{"shell prompt"},
			CurrentCommand: "bash",
			Exited:         true,
		},
	})
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
	waitFor(t, "worker restart after reconnect", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.Health == WorkerHealthHealthy && worker.RestartCount == 1
	})

	if got, want := len(sleeps), 1; got != want {
		t.Fatalf("len(sleeps) = %d, want %d", got, want)
	}
	if sleeps[0] <= 0 {
		t.Fatalf("sleep[0] = %s, want positive backoff", sleeps[0])
	}
	if got, want := deps.amux.countKey("pane-1", "codex --yolo\n"), 1; got != want {
		t.Fatalf("restart send count = %d, want %d", got, want)
	}
}

func TestCheckTaskExitedEventPreservesLastCaptureOnCaptureError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedActiveAssignment(t, deps, "LAB-922", "pane-1")
	deps.amux.capturePaneErr = errors.New("capture failed")

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("seeded worker missing")
	}
	worker.LastCapture = "last useful output"
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d := deps.newDaemon(t)
	active, err := deps.state.ActiveAssignmentByIssue(context.Background(), d.project, "LAB-922")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}

	update := d.checkTaskExitedEvent(context.Background(), active)

	if got, want := update.Active.Worker.LastCapture, "last useful output"; got != want {
		t.Fatalf("update.Active.Worker.LastCapture = %q, want %q", got, want)
	}
	if got, want := update.Active.Worker.Health, WorkerHealthHealthy; got != want {
		t.Fatalf("update.Active.Worker.Health = %q, want %q", got, want)
	}
	if update.WorkerChanged {
		t.Fatal("update.WorkerChanged = true, want false before queued restart runs")
	}
	if update.TaskChanged {
		t.Fatal("update.TaskChanged = true, want false before queued restart runs")
	}
	if got, want := len(update.Events), 0; got != want {
		t.Fatalf("len(update.Events) = %d, want %d", got, want)
	}
	if !update.hasNudges() {
		t.Fatal("update.hasNudges() = false, want true")
	}
	if got, want := deps.amux.captureCount("pane-1"), 1; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}

	update.runNudges(context.Background(), d)
	if got, want := update.Active.Worker.LastCapture, "last useful output"; got != want {
		t.Fatalf("update.Active.Worker.LastCapture after restart = %q, want %q", got, want)
	}
	if got, want := update.Active.Worker.Health, WorkerHealthHealthy; got != want {
		t.Fatalf("update.Active.Worker.Health after restart = %q, want %q", got, want)
	}
	if got, want := update.Active.Worker.RestartCount, 1; got != want {
		t.Fatalf("update.Active.Worker.RestartCount after restart = %d, want %d", got, want)
	}
	if got, want := update.Active.Worker.FirstCrashAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("update.Active.Worker.FirstCrashAt after restart = %v, want %v", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", "codex --yolo\n"), 1; got != want {
		t.Fatalf("restart send count = %d, want %d", got, want)
	}
}

func TestConsumeExitedEventStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cancel    bool
		event     amuxapi.Event
		streamErr error
		want      bool
	}{
		{
			name:  "returns true when stream closes cleanly",
			event: amuxapi.Event{Type: "exited"},
			want:  true,
		},
		{
			name:      "returns true when stream reports an error",
			streamErr: errors.New("stream dropped"),
			want:      true,
		},
		{
			name:   "returns false when context is canceled",
			cancel: true,
			want:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			d := deps.newDaemon(t)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.cancel {
				cancel()
			}

			eventsCh := make(chan amuxapi.Event, 1)
			errCh := make(chan error, 1)
			if tt.event != (amuxapi.Event{}) {
				eventsCh <- tt.event
			}
			if tt.streamErr != nil {
				errCh <- tt.streamErr
			}
			close(eventsCh)
			close(errCh)

			if got := d.consumeExitedEventStream(ctx, eventsCh, errCh); got != tt.want {
				t.Fatalf("consumeExitedEventStream() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestSubscribeExitedEvents(t *testing.T) {
	t.Parallel()

	t.Run("uses exited filter when context is live", func(t *testing.T) {
		t.Parallel()

		mock := &amuxapi.MockClient{}
		d := &Daemon{amux: mock}

		eventsCh, errCh := d.subscribeExitedEvents(context.Background())

		if eventsCh == nil {
			t.Fatal("eventsCh = nil, want channel")
		}
		if errCh == nil {
			t.Fatal("errCh = nil, want channel")
		}
		if got, want := len(mock.EventsCalls), 1; got != want {
			t.Fatalf("len(mock.EventsCalls) = %d, want %d", got, want)
		}
		if got, want := mock.EventsCalls[0], (amuxapi.EventsRequest{
			Filter:      []string{"exited"},
			NoReconnect: true,
		}); len(got.Filter) != len(want.Filter) || got.Filter[0] != want.Filter[0] || got.NoReconnect != want.NoReconnect {
			t.Fatalf("Events() request = %#v, want %#v", got, want)
		}
	})

	t.Run("returns closed channels when context is canceled", func(t *testing.T) {
		t.Parallel()

		mock := &amuxapi.MockClient{}
		d := &Daemon{amux: mock}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		eventsCh, errCh := d.subscribeExitedEvents(ctx)
		if got := len(mock.EventsCalls); got != 0 {
			t.Fatalf("len(mock.EventsCalls) = %d, want 0", got)
		}

		if _, ok := <-eventsCh; ok {
			t.Fatal("eventsCh open, want closed")
		}
		if _, ok := <-errCh; ok {
			t.Fatal("errCh open, want closed")
		}
	})
}

func TestActiveAssignmentByPane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		paneID    string
		mutate    func(t *testing.T, deps *testDeps)
		wantOK    bool
		wantIssue string
	}{
		{
			name:      "returns active assignment for tracked pane",
			paneID:    "pane-1",
			wantOK:    true,
			wantIssue: "LAB-922",
		},
		{
			name:   "returns false when worker is missing",
			paneID: "missing-pane",
			wantOK: false,
		},
		{
			name:   "returns false when worker has no issue",
			paneID: "pane-1",
			mutate: func(t *testing.T, deps *testDeps) {
				t.Helper()

				worker, ok := deps.state.worker("pane-1")
				if !ok {
					t.Fatal("seeded worker missing")
				}
				worker.Issue = ""
				if err := deps.state.PutWorker(context.Background(), worker); err != nil {
					t.Fatalf("PutWorker() error = %v", err)
				}
			},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			seedActiveAssignment(t, deps, "LAB-922", "pane-1")
			if tt.mutate != nil {
				tt.mutate(t, deps)
			}

			d := deps.newDaemon(t)
			active, ok := d.activeAssignmentByPane(context.Background(), tt.paneID)
			if ok != tt.wantOK {
				t.Fatalf("activeAssignmentByPane() ok = %t, want %t", ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if got, want := active.Task.Issue, tt.wantIssue; got != want {
				t.Fatalf("active.Task.Issue = %q, want %q", got, want)
			}
		})
	}
}
