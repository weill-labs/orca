package daemon

import (
	"context"
	"reflect"
	"testing"
)

func TestReconcileMissingPRNumbersBackfillsOpenPRNumber(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1259", "pane-1", "worker-01", 0)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1259", "--state", "all", "--json", "number,state"}, `[{"number":42,"state":"OPEN"}]`, nil)

	d := deps.newDaemon(t)
	d.reconcileMissingPRNumbers(context.Background())

	task, ok := deps.state.task("LAB-1259")
	if !ok {
		t.Fatal("LAB-1259 task missing after reconciliation")
	}
	if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("pane-1 worker missing after reconciliation")
	}
	if got, want := worker.LastPRNumber, 42; got != want {
		t.Fatalf("worker.LastPRNumber = %d, want %d", got, want)
	}
	if worker.LastPushAt.IsZero() {
		t.Fatal("worker.LastPushAt = zero, want reconciliation timestamp")
	}

	deps.amux.requireSentKeys(t, "pane-1", nil)
	if got := len(deps.amux.waitIdleCalls); got != 0 {
		t.Fatalf("wait idle call count = %d, want 0", got)
	}
	if got := deps.events.countType(EventPRDetected); got != 1 {
		t.Fatalf("pr.detected event count = %d, want 1", got)
	}
	if got := deps.events.countType(EventPRMerged); got != 0 {
		t.Fatalf("pr.merged event count = %d, want 0", got)
	}
}

func TestReconcileMissingPRNumbersBackfillsMergedPRNumberAndCompletes(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1260", "pane-1", "worker-01", 0)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1260", "--state", "all", "--json", "number,state"}, `[{"number":77,"state":"MERGED"}]`, nil)

	d := deps.newDaemon(t)
	d.reconcileMissingPRNumbers(context.Background())

	task, ok := deps.state.task("LAB-1260")
	if !ok {
		t.Fatal("LAB-1260 task missing after reconciliation")
	}
	if got, want := task.PRNumber, 77; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker still present after merged PR reconciliation")
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: "LAB-1260", State: IssueStateDone}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}

	deps.events.requireTypes(t, EventPRDetected, EventPRMerged, EventWorkerPostmortem, EventTaskCompleted)
	deps.amux.requireSentKeys(t, "pane-1", []string{
		mergedWrapUpPrompt,
		postmortemCommand,
		"Enter",
	})
}

func TestReconcileMissingPRNumbersSkipsTrackedTasks(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1261", "pane-1", "worker-01", 42)

	d := deps.newDaemon(t)
	d.reconcileMissingPRNumbers(context.Background())

	if got := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-1261", "--state", "all", "--json", "number,state"}); got != 0 {
		t.Fatalf("lookupOpenOrMergedPRNumber call count = %d, want 0", got)
	}
	if got := deps.events.countType(EventPRDetected); got != 0 {
		t.Fatalf("pr.detected event count = %d, want 0", got)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func seedReconcileAssignment(t *testing.T, deps *testDeps, issue, paneID, workerID string, prNumber int) {
	t.Helper()

	now := deps.clock.Now()
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        issue,
		Status:       TaskStatusActive,
		Prompt:       "Monitor active task",
		WorkerID:     workerID,
		PaneID:       paneID,
		PaneName:     paneID,
		CloneName:    "clone-" + issue,
		ClonePath:    "/tmp/" + issue,
		Branch:       issue,
		AgentProfile: "codex",
		PRNumber:     prNumber,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        "/tmp/project",
		WorkerID:       workerID,
		PaneID:         paneID,
		PaneName:       paneID,
		Issue:          issue,
		ClonePath:      "/tmp/" + issue,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastCapture:    defaultCodexReadyOutput(),
		LastActivityAt: now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
}
