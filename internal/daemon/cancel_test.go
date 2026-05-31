package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestCancelKillsAgentCleansCloneAndFreesResources(t *testing.T) {
	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
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
	deps.commands.reset()
	deps.amux.killCalls = nil

	if err := d.Cancel(ctx, "LAB-689"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	waitFor(t, "task cancellation", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusCancelled
	})

	task, _ := deps.state.task("LAB-689")
	if got, want := task.Status, TaskStatusCancelled; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker still present after cancellation")
	}

	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-689",
		AssignedTask:  "LAB-689",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}

	if got := deps.commands.callsByName("git"); len(got) != 0 {
		t.Fatalf("git calls = %#v, want none during daemon cleanup", got)
	}
	if got := deps.events.lastMessage(EventWorkerPostmortem); !strings.Contains(got, "sent") {
		t.Fatalf("postmortem event message = %q, want sent status", got)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventWorkerPostmortem, EventTaskCancelled)
}

func TestCancelSendsPostmortemBeforeCleanup(t *testing.T) {
	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)

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

	deps.commands.reset()
	deps.amux.waitIdleCalls = nil
	deps.amux.killCalls = nil

	if err := d.Cancel(ctx, "LAB-689"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	waitFor(t, "task cancellation", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusCancelled
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{wrappedCodexPrompt("LAB-689", "Implement daemon core"), "Enter", "$postmortem", "Enter"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got := deps.commands.callsByName("git"); len(got) != 0 {
		t.Fatalf("git calls = %#v, want none during daemon cleanup", got)
	}
	if got := deps.events.lastMessage(EventWorkerPostmortem); !strings.Contains(got, "sent") {
		t.Fatalf("postmortem event message = %q, want sent status", got)
	}
}

func TestCancelSkipsPostmortemWhenProfileDisablesIt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.config.profiles["claude"] = AgentProfile{
		Name:            "claude",
		StartCommand:    "claude --dangerously-skip-permissions",
		StuckTimeout:    5 * time.Minute,
		NudgeCommand:    "Enter",
		MaxNudgeRetries: 2,
	}

	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-690", "Implement daemon core", "claude"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	deps.commands.reset()
	deps.amux.waitIdleCalls = nil
	deps.amux.killCalls = nil

	if err := d.Cancel(ctx, "LAB-690"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	waitFor(t, "task cancellation", func() bool {
		task, ok := deps.state.task("LAB-690")
		return ok && task.Status == TaskStatusCancelled
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{"Implement daemon core", "Enter"})
	if got := deps.amux.waitIdleCalls; len(got) != 0 {
		t.Fatalf("waitIdle calls = %#v, want none", got)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got := deps.commands.callsByName("git"); len(got) != 0 {
		t.Fatalf("git calls = %#v, want none during daemon cleanup", got)
	}
	if got := deps.events.lastMessage(EventWorkerPostmortem); !strings.Contains(got, "skipped") {
		t.Fatalf("postmortem event message = %q, want skipped status", got)
	}
}

func TestCancelCleansOrphanWorkerWhenTaskMissing(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	worker := seedOrphanWorker(t, deps, "LAB-1852", "worker-orphan", "pane-orphan", deps.pool.clone.Path)
	d := deps.newDaemon(t)
	d.started.Store(true)

	if err := d.Cancel(context.Background(), "LAB-1852"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	if _, ok := deps.state.worker(worker.WorkerID); ok {
		t.Fatal("orphan worker still present after cancel")
	}
	workers, err := deps.state.ListWorkers(context.Background(), "/tmp/project")
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	for _, worker := range workers {
		if worker.Issue == "LAB-1852" {
			t.Fatalf("worker list still contains LAB-1852: %#v", workers)
		}
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-1852",
		AssignedTask:  "LAB-1852",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	deps.events.requireTypes(t, EventTaskCancelled)
}

func TestCancelCleansOrphanWorkerWhenTaskIsTerminal(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	now := deps.clock.Now()
	deps.state.putTaskForTest(Task{
		Project:   "/tmp/project",
		Issue:     "LAB-1853",
		Status:    TaskStatusDone,
		State:     TaskStateDone,
		Branch:    "feature/lab-1853",
		CreatedAt: now,
		UpdatedAt: now,
	})
	worker := seedOrphanWorker(t, deps, "LAB-1853", "worker-terminal", "pane-terminal", deps.pool.clone.Path)
	d := deps.newDaemon(t)
	d.started.Store(true)

	if err := d.Cancel(context.Background(), "LAB-1853"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	if _, ok := deps.state.worker(worker.WorkerID); ok {
		t.Fatal("orphan worker still present after cancel")
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "feature/lab-1853",
		AssignedTask:  "feature/lab-1853",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	deps.events.requireTypes(t, EventTaskCancelled)
}

func TestCancelStaleActiveTaskSkipsMissingPaneWaits(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	now := deps.clock.Now()
	deps.amux.paneExists = map[string]bool{"pane-stale": false}
	deps.amux.waitIdleErr = errors.New("wait idle should not run for missing pane")
	deps.amux.killErr = errors.New("kill should not run for missing pane")
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1999",
		Status:       TaskStatusActive,
		State:        TaskStateAssigned,
		PaneID:       "pane-stale",
		PaneName:     "w-LAB-1999",
		WorkerID:     "worker-stale",
		CloneName:    deps.pool.clone.Name,
		ClonePath:    deps.pool.clone.Path,
		Branch:       "LAB-1999",
		AgentProfile: "codex",
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-stale",
		PaneID:       "pane-stale",
		PaneName:     "w-LAB-1999",
		Issue:        "LAB-1999",
		ClonePath:    deps.pool.clone.Path,
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
		CreatedAt:    now,
		LastSeenAt:   now,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	d := deps.newDaemon(t)
	d.started.Store(true)

	if err := d.Cancel(context.Background(), "LAB-1999"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	task, ok := deps.state.task("LAB-1999")
	if !ok {
		t.Fatal("LAB-1999 task missing")
	}
	if got, want := task.Status, TaskStatusCancelled; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got := deps.amux.waitIdleCalls; len(got) != 0 {
		t.Fatalf("wait idle calls = %#v, want none", got)
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none", got)
	}
	if _, ok := deps.state.worker("worker-stale"); ok {
		t.Fatal("worker-stale still present after cancellation")
	}
	deps.events.requireTypes(t, EventWorkerPostmortem, EventTaskCancelled)
}

func TestCancelDoesNotKillPaneWhenAnotherTaskBlocks(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	now := deps.clock.Now()

	// Two workers share the same pane. Worker A has an active task
	// (LAB-1860). Worker B is an orphan (LAB-1861 has no task row).
	// Canceling B must not kill the pane because A is still using it.
	const sharedPane = "pane-shared"

	deps.state.putTaskForTest(Task{
		Project:   "/tmp/project",
		Issue:     "LAB-1860",
		PaneID:    sharedPane,
		Status:    TaskStatusActive,
		State:     TaskStateAssigned,
		Branch:    "LAB-1860",
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        "/tmp/project",
		WorkerID:       "worker-active",
		PaneID:         sharedPane,
		PaneName:       workerPaneName("LAB-1860", "worker-active"),
		Issue:          "LAB-1860",
		ClonePath:      deps.pool.clone.Path,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastActivityAt: now,
		CreatedAt:      now,
		LastSeenAt:     now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PutWorker() active = %v", err)
	}

	orphan := seedOrphanWorker(t, deps, "LAB-1861", "worker-orphan", sharedPane, deps.pool.clone.Path)

	d := deps.newDaemon(t)
	d.started.Store(true)

	if err := d.Cancel(context.Background(), "LAB-1861"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none — pane shared with active task", got)
	}
	if _, ok := deps.state.worker(orphan.WorkerID); ok {
		t.Fatal("orphan worker row still present after cancel")
	}
	if got := deps.pool.releasedClones(); len(got) != 1 || got[0].Path != deps.pool.clone.Path {
		t.Fatalf("released clones = %#v, want orphan's clone released", got)
	}
}

func TestCancelCleansOrphanWorkerWithNoClonePath(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	worker := seedOrphanWorker(t, deps, "LAB-1862", "worker-noclone", "pane-noclone", "")
	d := deps.newDaemon(t)
	d.started.Store(true)

	if err := d.Cancel(context.Background(), "LAB-1862"); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	if _, ok := deps.state.worker(worker.WorkerID); ok {
		t.Fatal("orphan worker row still present after cancel")
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none — worker had no clone path", got)
	}
	deps.events.requireTypes(t, EventTaskCancelled)
}

func TestCancelKeepsOrphanWorkerWhenCleanupFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	worker := seedOrphanWorker(t, deps, "LAB-1854", "worker-release-fails", "pane-release-fails", deps.pool.clone.Path)
	deps.pool.releaseErr = errors.New("release failed")
	d := deps.newDaemon(t)
	d.started.Store(true)

	if err := d.Cancel(context.Background(), "LAB-1854"); err == nil {
		t.Fatal("Cancel() error = nil, want cleanup failure")
	}

	if _, ok := deps.state.worker(worker.WorkerID); !ok {
		t.Fatal("orphan worker was deleted after cleanup failure")
	}
}

func TestCancelRPCReturnsSuccessWhenTaskRowMissingAfterOrphanCleanup(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedOrphanWorker(t, deps, "LAB-1852", "worker-orphan", "pane-orphan", deps.pool.clone.Path)
	d := deps.newDaemon(t)
	d.started.Store(true)

	response := dispatchRPCRequest(context.Background(), rpcRequest{
		JSONRPC: jsonRPCVersion,
		ID:      json.RawMessage(`1`),
		Method:  "cancel",
		Params:  json.RawMessage(`{"issue":"LAB-1852"}`),
	}, d, missingTaskStatusStore{fakeDaemonStateStore{}}, "", "/tmp/project")
	if response.Error != nil {
		t.Fatalf("cancel rpc error = %#v", response.Error)
	}
	result, ok := response.Result.(TaskActionResult)
	if !ok {
		t.Fatalf("cancel rpc result = %#v, want TaskActionResult", response.Result)
	}
	if got, want := result.Issue, "LAB-1852"; got != want {
		t.Fatalf("result.Issue = %q, want %q", got, want)
	}
	if got, want := result.Status, TaskStatusCancelled; got != want {
		t.Fatalf("result.Status = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("worker-orphan"); ok {
		t.Fatal("orphan worker still present after cancel rpc")
	}
}

type missingTaskStatusStore struct {
	fakeDaemonStateStore
}

func (missingTaskStatusStore) TaskStatus(context.Context, string, string) (state.TaskStatus, error) {
	return state.TaskStatus{}, state.ErrNotFound
}

func seedOrphanWorker(t *testing.T, deps *testDeps, issue, workerID, paneID, clonePath string) Worker {
	t.Helper()

	now := deps.clock.Now()
	worker := Worker{
		Project:        "/tmp/project",
		WorkerID:       workerID,
		PaneID:         paneID,
		PaneName:       workerPaneName(issue, workerID),
		Issue:          issue,
		ClonePath:      clonePath,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastActivityAt: now,
		CreatedAt:      now,
		LastSeenAt:     now,
		UpdatedAt:      now,
	}
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	return worker
}
