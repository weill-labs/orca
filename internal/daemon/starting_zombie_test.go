package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCancelClearsStartingTaskWithoutLivePaneAndAllowsReassign(t *testing.T) {
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

	issue := "LAB-1747"
	staleAt := deps.clock.Now().Add(-startingTaskDriftThreshold - time.Minute)
	seedStartingAssignment(t, deps, issue, "pane-1747", "worker-1747", staleAt)
	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("starting task missing after seed")
	}
	task.Project = ""
	task.CloneName = ""
	task.AgentProfile = "missing-profile"
	deps.state.putTaskForTest(task)
	worker, ok := deps.state.worker("worker-1747")
	if !ok {
		t.Fatal("worker-1747 missing after seed")
	}
	worker.Project = ""
	if err := deps.state.PutWorker(ctx, worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	deps.amux.paneExists = map[string]bool{"pane-1747": false}

	if err := d.Cancel(ctx, issue); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if _, ok := deps.state.task(issue); ok {
		t.Fatal("starting task still stored after cancellation")
	}
	worker, ok = deps.state.worker("worker-1747")
	if !ok {
		t.Fatal("worker-1747 missing after cancellation")
	}
	if worker.PaneID != "" || worker.Issue != "" || worker.ClonePath != "" {
		t.Fatalf("worker after cancel = %#v, want released worker claim", worker)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: issue,
		AssignedTask:  issue,
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if len(deps.amux.killCalls) != 0 {
		t.Fatalf("kill calls = %#v, want none for missing starting pane", deps.amux.killCalls)
	}
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskCancelled)

	if err := d.Assign(ctx, issue, "Implement LAB-1747 recovery", "codex"); err != nil {
		t.Fatalf("Assign() after clearing starting task error = %v", err)
	}
	task, ok = deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after reassignment")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status after reassignment = %q, want %q", got, want)
	}
}

func TestCancelKillsLiveStartingPaneAndClearsTask(t *testing.T) {
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

	issue := "LAB-1747"
	seedStartingAssignment(t, deps, issue, "pane-1747", "worker-1747", deps.clock.Now())
	deps.amux.paneExists = map[string]bool{"pane-1747": true}

	if err := d.Cancel(ctx, issue); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1747"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if _, ok := deps.state.task(issue); ok {
		t.Fatal("starting task still stored after cancellation")
	}
	worker, ok := deps.state.worker("worker-1747")
	if !ok {
		t.Fatal("worker-1747 missing after cancellation")
	}
	if worker.PaneID != "" || worker.Issue != "" || worker.ClonePath != "" {
		t.Fatalf("worker after cancel = %#v, want released worker claim", worker)
	}
	if got := len(deps.pool.releasedClones()); got != 1 {
		t.Fatalf("released clone count = %d, want 1", got)
	}
	if got := deps.events.countType(EventTaskCancelled); got != 1 {
		t.Fatalf("task.cancelled event count = %d, want 1", got)
	}
}

func TestCancelLiveStartingPaneKillFailureKeepsAssignment(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.killErr = errors.New("kill failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	issue := "LAB-1747"
	seedStartingAssignment(t, deps, issue, "pane-1747", "worker-1747", deps.clock.Now())
	deps.amux.paneExists = map[string]bool{"pane-1747": true}

	err := d.Cancel(ctx, issue)
	if err == nil || !strings.Contains(err.Error(), "kill starting pane pane-1747") {
		t.Fatalf("Cancel() error = %v, want kill starting pane failure", err)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1747"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if _, ok := deps.state.task(issue); !ok {
		t.Fatal("starting task missing after kill failure")
	}
	worker, ok := deps.state.worker("worker-1747")
	if !ok {
		t.Fatal("worker-1747 missing after kill failure")
	}
	if worker.PaneID != "pane-1747" || worker.Issue != issue || worker.ClonePath == "" {
		t.Fatalf("worker after kill failure = %#v, want preserved worker claim", worker)
	}
	if got := len(deps.pool.releasedClones()); got != 0 {
		t.Fatalf("released clone count = %d, want 0", got)
	}
	if got := deps.events.countType(EventTaskCancelled); got != 0 {
		t.Fatalf("task.cancelled event count = %d, want 0", got)
	}
}

func TestCancelStartingPaneCheckFailureKeepsAssignment(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.paneExistsErr = errors.New("pane lookup failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	issue := "LAB-1747"
	seedStartingAssignment(t, deps, issue, "pane-1747", "worker-1747", deps.clock.Now())

	err := d.Cancel(ctx, issue)
	if err == nil || !strings.Contains(err.Error(), "check pane pane-1747") {
		t.Fatalf("Cancel() error = %v, want pane lookup failure", err)
	}
	if got := len(deps.amux.killCalls); got != 0 {
		t.Fatalf("kill call count = %d, want 0", got)
	}
	if _, ok := deps.state.task(issue); !ok {
		t.Fatal("starting task missing after pane lookup failure")
	}
	worker, ok := deps.state.worker("worker-1747")
	if !ok {
		t.Fatal("worker-1747 missing after pane lookup failure")
	}
	if worker.PaneID != "pane-1747" || worker.Issue != issue || worker.ClonePath == "" {
		t.Fatalf("worker after pane lookup failure = %#v, want preserved worker claim", worker)
	}
	if got := len(deps.pool.releasedClones()); got != 0 {
		t.Fatalf("released clone count = %d, want 0", got)
	}
}

func TestFailedAssignStartingZombieCanBeCancelledAndReassigned(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitIdleErr = errors.New("wait idle failed")
	deps.state.restoreTaskErr = errors.New("restore failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	issue := "LAB-1747"
	err := d.Assign(ctx, issue, "Implement LAB-1747 recovery", "codex")
	if err == nil || !strings.Contains(err.Error(), "agent handshake") {
		t.Fatalf("Assign() error = %v, want agent handshake failure", err)
	}
	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("failed assignment did not leave expected starting task")
	}
	if got, want := task.Status, TaskStatusStarting; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if exists, err := deps.amux.PaneExists(ctx, task.PaneID); err != nil {
		t.Fatalf("PaneExists() error = %v", err)
	} else if exists {
		t.Fatal("PaneExists() = true, want rollback to remove failed assignment pane")
	}

	deps.state.restoreTaskErr = nil
	deps.amux.waitIdleErr = nil
	if err := d.Cancel(ctx, issue); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if _, ok := deps.state.task(issue); ok {
		t.Fatal("starting task still stored after cancellation")
	}
	if err := d.Assign(ctx, issue, "Retry LAB-1747 recovery", "codex"); err != nil {
		t.Fatalf("Assign() after cancelling failed starting task error = %v", err)
	}
}

func TestResumeStartingTaskReturnsRecoveryGuidance(t *testing.T) {
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

	seedStartingAssignment(t, deps, "LAB-1747", "pane-1747", "worker-1747", deps.clock.Now())

	err := d.Resume(ctx, "LAB-1747", "")
	if err == nil {
		t.Fatal("Resume() succeeded, want starting-state guidance")
	}
	for _, want := range []string{"still starting", "orca cancel LAB-1747", "orca reconcile --fix"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Resume() error = %v, want %q", err, want)
		}
	}
}

func TestReconcileFlagsAndFixesStaleStartingTaskWithoutPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	issue := "LAB-1747"
	staleAt := deps.clock.Now().Add(-startingTaskDriftThreshold - time.Minute)
	seedStartingAssignment(t, deps, issue, "pane-1747", "worker-1747", staleAt)
	deps.amux.paneExists = map[string]bool{"pane-1747": false}

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if got, want := findingKindsByIssue(result.Findings), map[string]string{issue: ReconcileStartingZombie}; !reflect.DeepEqual(got, want) {
		t.Fatalf("finding kinds = %#v, want %#v", got, want)
	}
	finding := result.Findings[0]
	if finding.Action != reconcileActionReported || !strings.Contains(finding.Message, "cancel and clear") {
		t.Fatalf("finding = %#v, want reported cancel-and-clear guidance", finding)
	}
	if got := len(deps.commands.callsByName("gh")); got != 0 {
		t.Fatalf("gh call count = %d, want 0 for starting drift", got)
	}

	result, err = deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{
		Project: "/tmp/project",
		Fix:     true,
	})
	if err != nil {
		t.Fatalf("Reconcile(--fix) error = %v", err)
	}
	if got, want := result.Fixed, 1; got != want {
		t.Fatalf("result.Fixed = %d, want %d", got, want)
	}
	if got, want := result.Findings[0].Action, reconcileActionFixed; got != want {
		t.Fatalf("fixed finding action = %q, want %q", got, want)
	}
	if _, ok := deps.state.task(issue); ok {
		t.Fatal("starting task still stored after reconcile --fix")
	}
	worker, ok := deps.state.worker("worker-1747")
	if !ok {
		t.Fatal("worker-1747 missing after reconcile --fix")
	}
	if worker.PaneID != "" || worker.Issue != "" || worker.ClonePath != "" {
		t.Fatalf("worker after reconcile --fix = %#v, want released worker claim", worker)
	}
	if got := len(deps.pool.releasedClones()); got != 1 {
		t.Fatalf("released clone count = %d, want 1", got)
	}
	if got := deps.events.countType(EventTaskCancelled); got != 1 {
		t.Fatalf("task.cancelled event count = %d, want 1", got)
	}
}

func TestReconcileSkipsFreshStartingTaskWithoutPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedStartingAssignment(t, deps, "LAB-1747", "", "worker-1747", deps.clock.Now())

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(result.Findings) != 0 {
		t.Fatalf("findings = %#v, want none before starting threshold", result.Findings)
	}
}

func TestReconcileFlagsZeroTimestampStartingTaskWithoutPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	issue := "LAB-1747"
	seedStartingAssignment(t, deps, issue, "pane-1747", "worker-1747", deps.clock.Now())
	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("starting task missing after seed")
	}
	task.CreatedAt = time.Time{}
	task.UpdatedAt = time.Time{}
	deps.state.putTaskForTest(task)
	deps.amux.paneExists = map[string]bool{"pane-1747": false}

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if got, want := findingKindsByIssue(result.Findings), map[string]string{issue: ReconcileStartingZombie}; !reflect.DeepEqual(got, want) {
		t.Fatalf("finding kinds = %#v, want %#v", got, want)
	}
}

func seedStartingAssignment(t *testing.T, deps *testDeps, issue, paneID, workerID string, updatedAt time.Time) {
	t.Helper()

	paneName := workerPaneName(issue, workerID)
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        issue,
		Status:       TaskStatusStarting,
		State:        TaskStateAssigned,
		Prompt:       "Implement " + issue,
		WorkerID:     workerID,
		PaneID:       paneID,
		PaneName:     paneName,
		CloneName:    deps.pool.clone.Name,
		ClonePath:    deps.pool.clone.Path,
		Branch:       issue,
		AgentProfile: "codex",
		CreatedAt:    updatedAt,
		UpdatedAt:    updatedAt,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     workerID,
		PaneID:       paneID,
		PaneName:     paneName,
		Issue:        issue,
		ClonePath:    deps.pool.clone.Path,
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
		CreatedAt:    updatedAt,
		LastSeenAt:   updatedAt,
		UpdatedAt:    updatedAt,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
}
