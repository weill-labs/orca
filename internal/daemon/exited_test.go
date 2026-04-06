package daemon

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestExitedPaneDetectionEscalatesWorker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		initialHealth    string
		initialNudge     int
		initialCapture   string
		snapshot         PaneCapture
		wantLastCapture  string
		wantNoRecoveries bool
	}{
		{
			name:           "same output still escalates exited pane",
			initialHealth:  WorkerHealthHealthy,
			initialCapture: "shell prompt",
			snapshot: PaneCapture{
				Content:        []string{"shell prompt"},
				CurrentCommand: "bash",
				Exited:         true,
			},
			wantLastCapture: "shell prompt",
		},
		{
			name:           "changed output does not recover exited pane",
			initialHealth:  WorkerHealthStuck,
			initialNudge:   1,
			initialCapture: "old output",
			snapshot: PaneCapture{
				Content:        []string{"final shell output"},
				CurrentCommand: "bash",
				Exited:         true,
			},
			wantLastCapture:  "final shell output",
			wantNoRecoveries: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			captureTicker := newFakeTicker()
			pollTicker := newFakeTicker()
			deps.tickers.enqueue(captureTicker, pollTicker)
			seedActiveAssignment(t, deps, "LAB-739", "pane-1")

			worker, ok := deps.state.worker("pane-1")
			if !ok {
				t.Fatal("seeded worker missing")
			}
			worker.Health = tt.initialHealth
			worker.NudgeCount = tt.initialNudge
			worker.LastCapture = tt.initialCapture
			if err := deps.state.PutWorker(context.Background(), worker); err != nil {
				t.Fatalf("PutWorker() error = %v", err)
			}

			deps.amux.capturePaneSequence("pane-1", []PaneCapture{tt.snapshot})

			d := deps.newDaemon(t)
			ctx := context.Background()
			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			captureTicker.tick(deps.clock.Now())
			waitFor(t, "worker escalation after exited pane capture", func() bool {
				worker, ok := deps.state.worker("pane-1")
				return ok && worker.Health == WorkerHealthEscalated
			})

			worker, ok = deps.state.worker("pane-1")
			if !ok {
				t.Fatal("worker missing after exited pane detection")
			}
			if got, want := worker.Health, WorkerHealthEscalated; got != want {
				t.Fatalf("worker.Health = %q, want %q", got, want)
			}
			if got, want := worker.LastCapture, tt.wantLastCapture; got != want {
				t.Fatalf("worker.LastCapture = %q, want %q", got, want)
			}
			if got := deps.amux.countKey("pane-1", "\n"); got != 0 {
				t.Fatalf("nudge count = %d, want 0", got)
			}
			if got, want := deps.events.countType(EventWorkerEscalated), 1; got != want {
				t.Fatalf("worker escalated events = %d, want %d", got, want)
			}
			if tt.wantNoRecoveries {
				if got := deps.events.countType(EventWorkerRecovered); got != 0 {
					t.Fatalf("worker recovered events = %d, want 0", got)
				}
			}

			event, ok := deps.events.lastEventOfType(EventWorkerEscalated)
			if !ok {
				t.Fatal("worker escalation event missing")
			}
			if !strings.Contains(event.Message, "pane exited") {
				t.Fatalf("event.Message = %q, want to contain %q", event.Message, "pane exited")
			}

			task, ok := deps.state.task("LAB-739")
			if !ok {
				t.Fatal("task missing after exited pane detection")
			}
			if got, want := task.Status, TaskStatusActive; got != want {
				t.Fatalf("task.Status = %q, want %q", got, want)
			}
		})
	}
}

func TestExitedPaneDetectionWaitsForPersistentExitedState(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	seedActiveAssignment(t, deps, "LAB-739", "pane-1")

	recentExit := deps.clock.Now().Add(-5 * time.Second).Format(time.RFC3339)
	staleExit := deps.clock.Now().Add(-31 * time.Second).Format(time.RFC3339)
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		{
			Content:        []string{"shell prompt"},
			CurrentCommand: "bash",
			Exited:         true,
			ExitedSince:    recentExit,
		},
		{
			Content:        []string{"shell prompt"},
			CurrentCommand: "bash",
			Exited:         true,
			ExitedSince:    staleExit,
		},
	})

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "first exited capture", func() bool {
		return deps.amux.captureCount("pane-1") == 1
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after first exited capture")
	}
	if got, want := worker.Health, WorkerHealthHealthy; got != want {
		t.Fatalf("worker.Health after recent exited capture = %q, want %q", got, want)
	}
	if got := deps.events.countType(EventWorkerEscalated); got != 0 {
		t.Fatalf("worker escalated events after recent exited capture = %d, want 0", got)
	}

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "persistent exited escalation", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.Health == WorkerHealthEscalated
	})
}
