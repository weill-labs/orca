package daemon

import (
	"context"
	"reflect"
	"strings"
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

func TestStuckDetectionEscalatesWithoutCleanupOrKill(t *testing.T) {
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

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-710", "Leave escalated worker running", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	deps.commands.reset()
	deps.amux.killCalls = nil

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "worker escalation", func() bool {
		active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-710")
		return err == nil && active.Worker.Health == WorkerHealthEscalated
	})

	task, ok := deps.state.task("LAB-710")
	if !ok {
		t.Fatal("task missing after stuck escalation")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}

	active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-710")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if got, want := active.Worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-710",
		"status":         "escalated",
		"task":           "LAB-710",
		"tracked_issues": `[{"id":"LAB-710","status":"active"}]`,
	})
	if got, want := deps.amux.killCalls, []string(nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want none", got)
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	if got := deps.commands.callsByName("git"); len(got) != 0 {
		t.Fatalf("cleanup git calls = %#v, want none", got)
	}
	if got := deps.events.countType(EventTaskFailed); got != 0 {
		t.Fatalf("task failed event count = %d, want 0", got)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventWorkerEscalated)
}

func TestStuckDetectionEscalationCapturesDiagnosticsWithoutCleanupOrKill(t *testing.T) {
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
	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-710", "Leave escalated worker running", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	deps.commands.reset()
	deps.amux.killCalls = nil

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "worker escalation", func() bool {
		return deps.events.countType(EventWorkerEscalated) == 1
	})

	task, ok := deps.state.task("LAB-710")
	if !ok {
		t.Fatal("task missing after stuck escalation")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}

	active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-710")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if got, want := active.Worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got := deps.amux.captureHistoryCount("pane-1"); got != 0 {
		t.Fatalf("capture history count = %d, want 0", got)
	}
	if got, want := deps.amux.killCalls, []string(nil); !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want none", got)
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	if got := deps.commands.callsByName("git"); len(got) != 0 {
		t.Fatalf("cleanup git calls = %#v, want none", got)
	}
	if got := deps.events.countType(EventTaskFailed); got != 0 {
		t.Fatalf("task failed event count = %d, want 0", got)
	}

	event, ok := deps.events.lastEventOfType(EventWorkerEscalated)
	if !ok {
		t.Fatal("worker escalation event missing")
	}
	if strings.Contains(event.Message, "diagnostics") {
		t.Fatalf("event.Message = %q, want no diagnostics suffix", event.Message)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventWorkerEscalated)
}

func TestCancelKillsPaneAfterStuckEscalation(t *testing.T) {
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
		MaxNudgeRetries:   0,
	}
	deps.amux.captureSequence("pane-1", []string{"permission prompt"})

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-710", "Cancel escalated worker", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "worker escalation", func() bool {
		active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-710")
		return err == nil && active.Worker.Health == WorkerHealthEscalated
	})

	deps.commands.reset()
	deps.amux.killCalls = nil

	if err := d.Cancel(ctx, "LAB-710"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	waitFor(t, "task cancellation", func() bool {
		task, ok := deps.state.task("LAB-710")
		return ok && task.Status == TaskStatusCancelled
	})

	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker still present after cancellation")
	}
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventWorkerEscalated, EventWorkerPostmortem, EventTaskCancelled)
}
