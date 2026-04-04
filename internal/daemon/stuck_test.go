package daemon

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestStuckDetectionMatchesTextPatternsThenEscalates(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		StuckTextPatterns: []string{"permission prompt"},
		StuckTimeout:      time.Hour,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   2,
	}
	deps.amux.captureSequence("pane-1", []string{
		"permission prompt",
		"permission prompt",
		"permission prompt",
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

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "first nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 1
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "second nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 2
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "escalation event", func() bool {
		return deps.events.countType(EventWorkerEscalated) == 1
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 2; got != want {
		t.Fatalf("nudge count = %d, want %d", got, want)
	}
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventWorkerNudged, EventWorkerEscalated)
}

func TestStuckDetectionUsesIdleTimeoutAndRecoversOnOutputChange(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.config.profiles["codex"] = AgentProfile{
		Name:            "codex",
		StartCommand:    "codex --yolo",
		StuckTimeout:    5 * time.Minute,
		NudgeCommand:    "Enter",
		MaxNudgeRetries: 1,
	}
	deps.amux.captureSequence("pane-1", []string{
		"working",
		"working",
		"working",
		"working again",
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

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "initial monitored capture", func() bool {
		return deps.amux.captureCount("pane-1") == 2
	})
	if got := deps.amux.countKey("pane-1", "\n"); got != 0 {
		t.Fatalf("unexpected nudge count after initial activity = %d", got)
	}

	deps.clock.Advance(6 * time.Minute)
	captureTicker.tick(deps.clock.Now())
	waitFor(t, "idle timeout nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 1
	})

	deps.clock.Advance(1 * time.Minute)
	captureTicker.tick(deps.clock.Now())
	waitFor(t, "worker recovery event", func() bool {
		return deps.events.countType(EventWorkerRecovered) == 1
	})

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventWorkerNudged, EventWorkerRecovered)
}

func TestStuckDetectionCapturesDiagnosticsBeforeForceKill(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		goBased           bool
		historySequence   []PaneCapture
		wantLogName       string
		wantSignalCalls   []signalCall
		wantSleepCalls    []time.Duration
		wantHistoryCalls  int
		wantLogSubstrings []string
	}{
		{
			name:    "go worker sends sigquit before kill",
			goBased: true,
			historySequence: []PaneCapture{
				{
					Content:        []string{"stuck output"},
					CWD:            "/tmp/clone-01",
					CurrentCommand: "codex",
					ChildPIDs:      []int{4242},
				},
				{
					Content:        []string{"goroutine dump"},
					CWD:            "/tmp/clone-01",
					CurrentCommand: "codex",
					ChildPIDs:      []int{4242},
				},
			},
			wantLogName:      "20260402T090000Z-goroutine-dump-LAB-710.log",
			wantSignalCalls:  []signalCall{{PID: 4242, Signal: syscall.SIGQUIT}},
			wantSleepCalls:   []time.Duration{5 * time.Second},
			wantHistoryCalls: 2,
			wantLogSubstrings: []string{
				"stuck output",
				"sigquit_pid: 4242",
				"goroutine dump",
			},
		},
		{
			name:    "non go worker skips sigquit and still saves pane output",
			goBased: false,
			historySequence: []PaneCapture{
				{
					Content:        []string{"stuck output only"},
					CWD:            "/tmp/clone-01",
					CurrentCommand: "claude",
				},
			},
			wantLogName:      "20260402T090000Z-pane-output-LAB-710.log",
			wantHistoryCalls: 1,
			wantLogSubstrings: []string{
				"stuck output only",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			captureTicker := newFakeTicker()
			prTicker := newFakeTicker()
			deps.tickers.enqueue(captureTicker, prTicker)
			deps.config.profiles["codex"] = AgentProfile{
				Name:              "codex",
				StartCommand:      "codex --yolo",
				StuckTextPatterns: []string{"permission prompt"},
				StuckTimeout:      time.Hour,
				GoBased:           tt.goBased,
				NudgeCommand:      "Enter",
				MaxNudgeRetries:   0,
			}
			deps.amux.captureSequence("pane-1", []string{"permission prompt"})
			deps.amux.captureHistorySequence("pane-1", tt.historySequence)

			d := deps.newDaemon(t)
			ctx := context.Background()
			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			if err := d.Assign(ctx, "LAB-710", "Capture diagnostics before kill", "codex"); err != nil {
				t.Fatalf("Assign() error = %v", err)
			}
			deps.commands.reset()
			deps.amux.killCalls = nil

			captureTicker.tick(deps.clock.Now())
			waitFor(t, "task failure after stuck diagnostics", func() bool {
				task, ok := deps.state.task("LAB-710")
				return ok && task.Status == TaskStatusFailed
			})

			task, _ := deps.state.task("LAB-710")
			if got, want := task.Status, TaskStatusFailed; got != want {
				t.Fatalf("task.Status = %q, want %q", got, want)
			}
			if _, ok := deps.state.worker("pane-1"); ok {
				t.Fatal("worker still present after force kill")
			}

			if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("kill calls = %#v, want %#v", got, want)
			}
			if got := deps.amux.captureHistoryCount("pane-1"); got != tt.wantHistoryCalls {
				t.Fatalf("capture history count = %d, want %d", got, tt.wantHistoryCalls)
			}
			if got, want := deps.signalCalls(), tt.wantSignalCalls; !reflect.DeepEqual(got, want) {
				t.Fatalf("signal calls = %#v, want %#v", got, want)
			}
			if got, want := deps.sleepCalls(), tt.wantSleepCalls; !reflect.DeepEqual(got, want) {
				t.Fatalf("sleep calls = %#v, want %#v", got, want)
			}
			if got, want := deps.pool.releasedClones(), []Clone{{
				Name:          deps.pool.clone.Name,
				Path:          deps.pool.clone.Path,
				CurrentBranch: "LAB-710",
				AssignedTask:  "LAB-710",
			}}; !reflect.DeepEqual(got, want) {
				t.Fatalf("released clones = %#v, want %#v", got, want)
			}
			if got := deps.commands.callsByName("git"); len(got) != 0 {
				t.Fatalf("cleanup git calls = %#v, want none", got)
			}

			logPath := filepath.Join(deps.postmortemDir, tt.wantLogName)
			data, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatalf("ReadFile(%q) error = %v", logPath, err)
			}
			for _, want := range tt.wantLogSubstrings {
				if !strings.Contains(string(data), want) {
					t.Fatalf("postmortem log missing %q in %q", want, string(data))
				}
			}

			deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventWorkerEscalated, EventTaskFailed)
		})
	}
}

func TestStopInterruptsStuckWorkerDiagnosticsWait(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		StuckTextPatterns: []string{"permission prompt"},
		StuckTimeout:      time.Hour,
		GoBased:           true,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   0,
	}
	deps.amux.captureSequence("pane-1", []string{"permission prompt"})
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content:        []string{"stuck output"},
		CWD:            "/tmp/clone-01",
		CurrentCommand: "codex",
		ChildPIDs:      []int{4242},
	}})

	d := deps.newDaemon(t)
	sleepStarted := make(chan struct{})
	d.sleep = func(ctx context.Context, delay time.Duration) error {
		close(sleepStarted)
		<-ctx.Done()
		return ctx.Err()
	}

	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	if err := d.Assign(ctx, "LAB-710", "Capture diagnostics before kill", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	captureTicker.tick(deps.clock.Now())
	select {
	case <-sleepStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stuck diagnostics sleep")
	}

	stopDone := make(chan error, 1)
	go func() {
		stopDone <- d.Stop(context.Background())
	}()

	select {
	case err := <-stopDone:
		if err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Stop() blocked while stuck diagnostics wait was in progress")
	}

	if got := deps.amux.captureHistoryCount("pane-1"); got != 1 {
		t.Fatalf("capture history count = %d, want 1", got)
	}

	task, ok := deps.state.task("LAB-710")
	if !ok {
		t.Fatal("task missing after stop")
	}
	if got, want := task.Status, TaskStatusFailed; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}

	event, ok := deps.events.lastEventOfType(EventTaskFailed)
	if !ok {
		t.Fatalf("lastEventOfType(%q) = false, want true", EventTaskFailed)
	}
	if strings.Contains(event.Message, "diagnostics error") {
		t.Fatalf("event.Message = %q, want no diagnostics error after stop cancellation", event.Message)
	}
}
