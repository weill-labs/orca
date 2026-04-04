package daemon

import (
	"context"
	"reflect"
	"testing"
)

func TestDaemonStartFailsMissingPaneAssignmentsAndReleasesClone(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedActiveAssignment(t, deps, "LAB-721", "pane-1")

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "startup reconciliation", func() bool {
		task, ok := deps.state.task("LAB-721")
		if !ok || task.Status != TaskStatusFailed {
			return false
		}
		_, workerOK := deps.state.worker("pane-1")
		return !workerOK
	})

	task, ok := deps.state.task("LAB-721")
	if !ok {
		t.Fatal("task missing after startup reconciliation")
	}
	if got, want := task.Status, TaskStatusFailed; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker retained after missing-pane reconciliation")
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-721",
		AssignedTask:  "LAB-721",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskFailed)
	event, ok := deps.events.lastEventOfType(EventTaskFailed)
	if !ok {
		t.Fatal("task failure event missing")
	}
	if got, want := event.Message, "worker pane missing on daemon startup"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

func TestDaemonStartResumesMonitoringLiveAssignments(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedActiveAssignment(t, deps, "LAB-722", "pane-1")
	deps.amux.listPanes = []Pane{{ID: "pane-1", Name: "worker-1"}}
	deps.amux.captureSequence("pane-1", []string{"updated output"})

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "capture resume", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCapture == "updated output"
	})

	task, ok := deps.state.task("LAB-722")
	if !ok {
		t.Fatal("task missing after startup")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := deps.amux.captureCount("pane-1"), 1; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
}

func TestDaemonStartMissingPaneRecoveryIsIdempotentAcrossRestart(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedActiveAssignment(t, deps, "LAB-723", "pane-1")

	first := deps.newDaemon(t)
	ctx := context.Background()
	if err := first.Start(ctx); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	waitFor(t, "first startup reconciliation", func() bool {
		task, ok := deps.state.task("LAB-723")
		if !ok {
			return false
		}
		return task.Status == TaskStatusFailed
	})
	if err := first.Stop(ctx); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}

	second := deps.newDaemon(t)
	if err := second.Start(ctx); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = second.Stop(context.Background())
	})

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-723",
		AssignedTask:  "LAB-723",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.events.countType(EventTaskFailed), 1; got != want {
		t.Fatalf("task failed events = %d, want %d", got, want)
	}
}

func seedActiveAssignment(t *testing.T, deps *testDeps, issue, paneID string) {
	t.Helper()

	now := deps.clock.Now()
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        issue,
		Status:       TaskStatusActive,
		Prompt:       "Implement startup recovery",
		PaneID:       paneID,
		PaneName:     paneID,
		CloneName:    deps.pool.clone.Name,
		ClonePath:    deps.pool.clone.Path,
		Branch:       issue,
		AgentProfile: "codex",
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        "/tmp/project",
		PaneID:         paneID,
		PaneName:       paneID,
		Issue:          issue,
		ClonePath:      deps.pool.clone.Path,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastActivityAt: now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	deps.pool.mu.Lock()
	if deps.pool.acquired == nil {
		deps.pool.acquired = make(map[string]bool)
	}
	deps.pool.acquired[deps.pool.clone.Path] = true
	deps.pool.mu.Unlock()
}
