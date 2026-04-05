package daemon

import (
	"context"
	"reflect"
	"testing"
)

type daemonResumer interface {
	Resume(context.Context, string, string) error
}

func TestResumeRestartsExistingWorkerInPlace(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-757", "pane-1")
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("seeded worker missing")
	}
	worker.Health = WorkerHealthEscalated
	worker.NudgeCount = 2
	worker.LastCapture = "shell prompt"
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
	deps.amux.mu.Lock()
	deps.amux.paneExistsCalls = nil
	deps.amux.captureCalls = nil
	deps.amux.mu.Unlock()

	resumer, ok := any(d).(daemonResumer)
	if !ok {
		t.Fatal("Daemon does not implement Resume")
	}

	if err := resumer.Resume(ctx, "LAB-757", ""); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	if got, want := deps.amux.paneExistsCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pane exists calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"codex --yolo\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.captureCount("pane-1"), 1; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}

	task, ok := deps.state.task("LAB-757")
	if !ok {
		t.Fatal("task missing after resume")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
}

func TestResumeRestartsExistingWorkerInPlaceAndSendsPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-757", "pane-1")

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	deps.amux.mu.Lock()
	deps.amux.paneExistsCalls = nil
	deps.amux.mu.Unlock()

	resumer, ok := any(d).(daemonResumer)
	if !ok {
		t.Fatal("Daemon does not implement Resume")
	}

	if err := resumer.Resume(ctx, "LAB-757", "Continue from the latest review comments"); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	if got, want := deps.amux.paneExistsCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pane exists calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"codex --yolo\n", "Continue from the latest review comments\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestResumeRespawnsCancelledTaskInExistingClone(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-757", "pane-9")

	task, ok := deps.state.task("LAB-757")
	if !ok {
		t.Fatal("seeded task missing")
	}
	task.Status = TaskStatusCancelled
	task.UpdatedAt = deps.clock.Now()
	if err := deps.state.PutTask(context.Background(), task); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}
	if err := deps.state.DeleteWorker(context.Background(), task.Project, task.PaneID); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}

	deps.amux.spawnPane = Pane{ID: "pane-2", Name: "worker-LAB-757"}
	deps.amux.paneExists = map[string]bool{"pane-9": false}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	deps.commands.reset()

	resumer, ok := any(d).(daemonResumer)
	if !ok {
		t.Fatal("Daemon does not implement Resume")
	}

	if err := resumer.Resume(ctx, "LAB-757", "Resume work after the cancellation"); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	if got, want := deps.amux.paneExistsCalls, []string{"pane-9"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pane exists calls = %#v, want %#v", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 1; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
	spawn := deps.amux.spawnRequests[0]
	if got, want := spawn.CWD, deps.pool.clone.Path; got != want {
		t.Fatalf("spawn cwd = %q, want %q", got, want)
	}
	if got, want := spawn.Command, "codex --yolo"; got != want {
		t.Fatalf("spawn command = %q, want %q", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-2", []string{"Resume work after the cancellation\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-2", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-2", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got := deps.commands.callsByName("git"); len(got) != 0 {
		t.Fatalf("git calls = %#v, want none", got)
	}

	resumedTask, ok := deps.state.task("LAB-757")
	if !ok {
		t.Fatal("task missing after resume")
	}
	if got, want := resumedTask.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := resumedTask.PaneID, "pane-2"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	if got, want := resumedTask.Prompt, "Resume work after the cancellation"; got != want {
		t.Fatalf("task.Prompt = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker("pane-2")
	if !ok {
		t.Fatal("worker missing after respawn")
	}
	if got, want := worker.Issue, "LAB-757"; got != want {
		t.Fatalf("worker.Issue = %q, want %q", got, want)
	}
	if got, want := worker.ClonePath, deps.pool.clone.Path; got != want {
		t.Fatalf("worker.ClonePath = %q, want %q", got, want)
	}

	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: "LAB-757", State: IssueStateInProgress}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue status updates = %#v, want %#v", got, want)
	}
}
