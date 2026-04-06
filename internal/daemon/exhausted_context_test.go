package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStuckDetectionAutoReassignsEscalatedLowContextCodexWorker(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		PostmortemEnabled: true,
		StuckTextPatterns: []string{"permission prompt"},
		StuckTimeout:      time.Hour,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   0,
	}
	deps.amux.spawnResults = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		{Content: []string{"permission prompt"}, CurrentCommand: "codex"},
		{Content: []string{"9% context remaining", "›"}, CurrentCommand: "codex"},
	})
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-819", "--state", "open", "--json", "number"}, `[]`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-819", "Implement exhausted context recovery", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	task, ok := deps.state.task("LAB-819")
	if !ok {
		t.Fatal("task missing after initial assignment")
	}
	task.PRNumber = 42
	deps.state.putTaskForTest(task)

	deps.commands.reset()
	deps.commands.queue("git", []string{"rev-list", "--count", "origin/main..HEAD"}, "3\n", nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-819", "--state", "open", "--json", "number"}, `[{"number":42}]`, nil)

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "worker escalation", func() bool {
		active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-819")
		return err == nil && active.Worker.Health == WorkerHealthEscalated
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "replacement assignment", func() bool {
		task, ok := deps.state.task("LAB-819")
		return ok && task.Status == TaskStatusActive && task.PaneID == "pane-2"
	})

	task, ok = deps.state.task("LAB-819")
	if !ok {
		t.Fatal("task missing after auto reassign")
	}
	if got, want := task.PaneID, "pane-2"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("old worker still present after auto reassign")
	}
	if _, ok := deps.state.worker("pane-2"); !ok {
		t.Fatal("replacement worker missing after auto reassign")
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement exhausted context recovery",
		"Enter",
		"$postmortem",
		"Enter",
	})
	if got := deps.events.countType(EventTaskAssigned); got != 2 {
		t.Fatalf("task assigned event count = %d, want 2", got)
	}
	if got := deps.events.countType(EventTaskCancelled); got != 1 {
		t.Fatalf("task cancelled event count = %d, want 1", got)
	}

	deps.amux.mu.Lock()
	reassignmentPrompt := deps.amux.sentKeys["pane-2"][0]
	deps.amux.mu.Unlock()
	for _, want := range []string{
		"Implement exhausted context recovery",
		"Previous worker summary:",
		"PR: #42",
		"Branch: LAB-819",
		"Commit count: 3",
	} {
		if !strings.Contains(reassignmentPrompt, want) {
			t.Fatalf("replacement prompt = %q, want substring %q", reassignmentPrompt, want)
		}
	}

	deps.events.requireTypes(t,
		EventDaemonStarted,
		EventTaskAssigned,
		EventWorkerEscalated,
		EventWorkerPostmortem,
		EventTaskCancelled,
	)
}

func TestStuckDetectionDoesNotAutoReassignWithoutLowContextPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		PostmortemEnabled: true,
		StuckTextPatterns: []string{"permission prompt"},
		StuckTimeout:      time.Hour,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   0,
	}
	deps.amux.spawnResults = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		{Content: []string{"permission prompt"}, CurrentCommand: "codex"},
		{Content: []string{"18% context remaining", "›"}, CurrentCommand: "codex"},
	})
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-820", "--state", "open", "--json", "number"}, `[]`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-820", "Keep the escalated worker running", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "worker escalation", func() bool {
		active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-820")
		return err == nil && active.Worker.Health == WorkerHealthEscalated
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "capture processing", func() bool {
		return deps.amux.captureCount("pane-1") >= 2
	})

	task, ok := deps.state.task("LAB-820")
	if !ok {
		t.Fatal("task missing after capture processing")
	}
	if got, want := task.PaneID, "pane-1"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	if got := deps.events.countType(EventTaskAssigned); got != 1 {
		t.Fatalf("task assigned event count = %d, want 1", got)
	}
	if got := deps.events.countType(EventTaskCancelled); got != 0 {
		t.Fatalf("task cancelled event count = %d, want 0", got)
	}
	if got := deps.events.countType(EventWorkerPostmortem); got != 0 {
		t.Fatalf("postmortem event count = %d, want 0", got)
	}
}

func TestExhaustedContextAutoReassignReason(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	profile := AgentProfile{Name: "codex"}

	tests := []struct {
		name     string
		profile  AgentProfile
		health   string
		snapshot PaneCapture
		want     string
		wantOK   bool
	}{
		{
			name:     "matches idle codex prompt below threshold",
			profile:  profile,
			health:   WorkerHealthEscalated,
			snapshot: PaneCapture{Content: []string{"9% context remaining", "›"}},
			want:     "idle codex prompt with 9% context remaining",
			wantOK:   true,
		},
		{
			name:     "requires escalated worker",
			profile:  profile,
			health:   WorkerHealthHealthy,
			snapshot: PaneCapture{Content: []string{"9% context remaining", "›"}},
			wantOK:   false,
		},
		{
			name:     "requires idle agent prompt",
			profile:  profile,
			health:   WorkerHealthEscalated,
			snapshot: PaneCapture{Content: []string{"9% context remaining", "still working"}},
			wantOK:   false,
		},
		{
			name:     "requires remaining context below threshold",
			profile:  profile,
			health:   WorkerHealthEscalated,
			snapshot: PaneCapture{Content: []string{"15% context remaining", "›"}},
			wantOK:   false,
		},
		{
			name:     "ignores other agent profiles",
			profile:  AgentProfile{Name: "claude"},
			health:   WorkerHealthEscalated,
			snapshot: PaneCapture{Content: []string{"9% context remaining", "›"}},
			wantOK:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reason, ok := d.exhaustedContextAutoReassignReason(ActiveAssignment{
				Worker: Worker{Health: tt.health},
			}, tt.profile, tt.snapshot)
			if got, want := ok, tt.wantOK; got != want {
				t.Fatalf("exhaustedContextAutoReassignReason() ok = %v, want %v", got, want)
			}
			if got, want := reason, tt.want; got != want {
				t.Fatalf("exhaustedContextAutoReassignReason() reason = %q, want %q", got, want)
			}
		})
	}
}

func TestExhaustedContextProgressSummaryUsesFallbacks(t *testing.T) {
	t.Parallel()

	got := exhaustedContextProgressSummary("", 0, "")
	for _, want := range []string{
		"PR: none",
		"Branch: unknown",
		"Commit count: unavailable",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("exhaustedContextProgressSummary() = %q, want substring %q", got, want)
		}
	}
}

func TestAutoReassignEscalatedWorkerEmitsCompletionFailureWhenCancelFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.killErr = errors.New("kill failed")
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-821", "--state", "open", "--json", "number"}, `[]`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-821", "Recover after exhausted context", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-821")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}

	deps.commands.reset()
	d.autoReassignEscalatedWorker(ctx, active, "idle codex prompt with 9% context remaining")

	if got := deps.events.countType(EventTaskCompletionFailed); got != 1 {
		t.Fatalf("task completion failed event count = %d, want 1", got)
	}
	if got := deps.events.countType(EventTaskAssigned); got != 1 {
		t.Fatalf("task assigned event count = %d, want 1", got)
	}
	if got := deps.events.countType(EventTaskCancelled); got != 1 {
		t.Fatalf("task cancelled event count = %d, want 1", got)
	}
	if got := deps.amux.spawnCount(); got != 1 {
		t.Fatalf("spawn count = %d, want 1", got)
	}

	event, ok := deps.events.lastEventOfType(EventTaskCompletionFailed)
	if !ok {
		t.Fatalf("lastEventOfType(%q) = false, want true", EventTaskCompletionFailed)
	}
	if !strings.Contains(event.Message, "auto reassign cancellation failed: kill failed") {
		t.Fatalf("event.Message = %q, want auto reassign cancellation context", event.Message)
	}
}
