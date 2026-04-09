package daemon

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

type sleepRecorder struct {
	mu        sync.Mutex
	durations []time.Duration
}

func (r *sleepRecorder) Sleep(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.durations = append(r.durations, delay)
	return nil
}

func (r *sleepRecorder) snapshot() []time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Duration(nil), r.durations...)
}

func TestExitedPaneDetectionRestartsWithExponentialBackoff(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	sleep := &sleepRecorder{}
	deps.sleep = sleep.Sleep
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedActiveAssignment(t, deps, "LAB-739", "pane-1")

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	firstCrashAt := deps.clock.Now()
	staleExit := firstCrashAt.Add(-31 * time.Second).Format(time.RFC3339)
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		{Content: []string{"shell prompt"}, CurrentCommand: "bash", Exited: true, ExitedSince: staleExit},
		{Content: []string{"shell prompt"}, CurrentCommand: "bash", Exited: true, ExitedSince: staleExit},
		{Content: []string{"shell prompt"}, CurrentCommand: "bash", Exited: true, ExitedSince: staleExit},
		{Content: []string{"shell prompt"}, CurrentCommand: "bash", Exited: true, ExitedSince: staleExit},
	})

	captureTicker.tick(firstCrashAt)
	waitFor(t, "first restart after exited pane", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.RestartCount == 1 && deps.amux.countKey("pane-1", "codex --yolo\n") == 1
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after first restart")
	}
	if got, want := worker.Health, WorkerHealthHealthy; got != want {
		t.Fatalf("worker.Health after first restart = %q, want %q", got, want)
	}
	if got, want := worker.FirstCrashAt, firstCrashAt; !got.Equal(want) {
		t.Fatalf("worker.FirstCrashAt after first restart = %v, want %v", got, want)
	}
	if got := sleep.snapshot(); len(got) != 0 {
		t.Fatalf("sleep calls after first restart = %#v, want none", got)
	}

	deps.clock.Advance(time.Minute)
	captureTicker.tick(deps.clock.Now())
	waitFor(t, "second restart after exited pane", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.RestartCount == 2 && deps.amux.countKey("pane-1", "codex --yolo\n") == 2
	})
	if got, want := sleep.snapshot(), []time.Duration{10 * time.Second}; !equalDurations(got, want) {
		t.Fatalf("sleep calls after second restart = %#v, want %#v", got, want)
	}

	deps.clock.Advance(time.Minute)
	captureTicker.tick(deps.clock.Now())
	waitFor(t, "third restart after exited pane", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.RestartCount == 3 && deps.amux.countKey("pane-1", "codex --yolo\n") == 3
	})
	if got, want := sleep.snapshot(), []time.Duration{10 * time.Second, 60 * time.Second}; !equalDurations(got, want) {
		t.Fatalf("sleep calls after third restart = %#v, want %#v", got, want)
	}

	deps.clock.Advance(time.Minute)
	captureTicker.tick(deps.clock.Now())
	waitFor(t, "fourth crash escalates worker", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.Health == WorkerHealthEscalated
	})

	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after escalation")
	}
	if got, want := worker.RestartCount, 3; got != want {
		t.Fatalf("worker.RestartCount after escalation = %d, want %d", got, want)
	}
	if got, want := worker.FirstCrashAt, firstCrashAt; !got.Equal(want) {
		t.Fatalf("worker.FirstCrashAt after escalation = %v, want %v", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", "codex --yolo\n"), 3; got != want {
		t.Fatalf("restart send count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerEscalated), 1; got != want {
		t.Fatalf("worker escalated events = %d, want %d", got, want)
	}
	if got := deps.events.countType(EventWorkerRecovered); got != 0 {
		t.Fatalf("worker recovered events = %d, want 0", got)
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
}

func TestExitedPaneDetectionResetsCrashWindowAfterHealthyRun(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	sleep := &sleepRecorder{}
	deps.sleep = sleep.Sleep
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedActiveAssignment(t, deps, "LAB-739", "pane-1")

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("seeded worker missing")
	}
	worker.Health = WorkerHealthHealthy
	worker.LastCapture = "still running"
	worker.RestartCount = 2
	worker.FirstCrashAt = deps.clock.Now().Add(-6 * time.Minute)
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		paneCaptureFromOutput("still running"),
		{
			Content:        []string{"shell prompt"},
			CurrentCommand: "bash",
			Exited:         true,
			ExitedSince:    deps.clock.Now().Add(-31 * time.Second).Format(time.RFC3339),
		},
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "healthy worker reset after restart window", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.RestartCount == 0 && worker.FirstCrashAt.IsZero()
	})

	deps.clock.Advance(time.Second)
	secondCrashAt := deps.clock.Now()
	captureTicker.tick(secondCrashAt)
	waitFor(t, "restart after reset crash window", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.RestartCount == 1 && deps.amux.countKey("pane-1", "codex --yolo\n") == 1
	})

	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after restart window reset")
	}
	if got, want := worker.FirstCrashAt, secondCrashAt; !got.Equal(want) {
		t.Fatalf("worker.FirstCrashAt after reset = %v, want %v", got, want)
	}
	if got := sleep.snapshot(); len(got) != 0 {
		t.Fatalf("sleep calls after reset crash = %#v, want none", got)
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
	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

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

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "first exited capture", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCapture == "shell prompt" && deps.amux.countKey("pane-1", "codex --yolo\n") == 0
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after first exited capture")
	}
	if got, want := worker.Health, WorkerHealthHealthy; got != want {
		t.Fatalf("worker.Health after recent exited capture = %q, want %q", got, want)
	}
	if got := deps.amux.countKey("pane-1", "codex --yolo\n"); got != 0 {
		t.Fatalf("restart send count after recent exited capture = %d, want 0", got)
	}

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "persistent exited restart", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.RestartCount == 1 && deps.amux.countKey("pane-1", "codex --yolo\n") == 1
	})
}

func TestShouldEscalateExitedPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	now := deps.clock.Now()

	tests := []struct {
		name     string
		snapshot PaneCapture
		want     bool
	}{
		{
			name: "missing exited timestamp falls back to escalate",
			snapshot: PaneCapture{
				Exited: true,
			},
			want: true,
		},
		{
			name: "invalid exited timestamp falls back to escalate",
			snapshot: PaneCapture{
				Exited:      true,
				ExitedSince: "not-a-timestamp",
			},
			want: true,
		},
		{
			name: "future exited timestamp does not escalate",
			snapshot: PaneCapture{
				Exited:      true,
				ExitedSince: now.Add(time.Minute).Format(time.RFC3339),
			},
			want: false,
		},
		{
			name: "recent exited timestamp does not escalate",
			snapshot: PaneCapture{
				Exited:      true,
				ExitedSince: now.Add(-5 * time.Second).Format(time.RFC3339),
			},
			want: false,
		},
		{
			name: "stale exited timestamp escalates",
			snapshot: PaneCapture{
				Exited:      true,
				ExitedSince: now.Add(-31 * time.Second).Format(time.RFC3339),
			},
			want: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got, want := d.shouldEscalateExitedPane(tt.snapshot, now), tt.want; got != want {
				t.Fatalf("shouldEscalateExitedPane() = %v, want %v", got, want)
			}
		})
	}
}

func equalDurations(got, want []time.Duration) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
