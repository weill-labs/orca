package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
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
	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after resume")
	}
	if got, want := worker.Health, WorkerHealthHealthy; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got, want := worker.NudgeCount, 0; got != want {
		t.Fatalf("worker.NudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.LastCapture, ""; got != want {
		t.Fatalf("worker.LastCapture = %q, want %q", got, want)
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
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout, Settle: 2 * time.Second},
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

	deps.amux.spawnPane = Pane{ID: "pane-2", Name: "w-LAB-757"}
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
	if got, want := spawn.Name, "w-LAB-757"; got != want {
		t.Fatalf("spawn name = %q, want %q", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-2", []string{"Resume work after the cancellation\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-2", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-2", Timeout: defaultAgentHandshakeTimeout, Settle: 2 * time.Second},
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
	if got, want := resumedTask.PaneName, "w-LAB-757"; got != want {
		t.Fatalf("task.PaneName = %q, want %q", got, want)
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
	if got, want := worker.PaneName, "w-LAB-757"; got != want {
		t.Fatalf("worker.PaneName = %q, want %q", got, want)
	}

	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: "LAB-757", State: IssueStateInProgress}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue status updates = %#v, want %#v", got, want)
	}
}

func TestResumeNormalizesLegacyNumericPaneRefBeforePaneChecks(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-757", "7")

	task, ok := deps.state.task("LAB-757")
	if !ok {
		t.Fatal("seeded task missing")
	}
	task.Status = TaskStatusCancelled
	task.UpdatedAt = deps.clock.Now()
	if err := deps.state.PutTask(context.Background(), task); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}

	deps.amux.paneExists = map[string]bool{"7": true}

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
	deps.amux.waitIdleCalls = nil
	deps.amux.mu.Unlock()

	resumer, ok := any(d).(daemonResumer)
	if !ok {
		t.Fatal("Daemon does not implement Resume")
	}

	if err := resumer.Resume(ctx, "LAB-757", ""); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	if got, want := deps.amux.paneExistsCalls, []string{"7"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pane exists calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "7", []string{"codex --yolo\n", "Implement startup recovery\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "7", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "7", Timeout: defaultAgentHandshakeTimeout, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}

	resumedTask, ok := deps.state.task("LAB-757")
	if !ok {
		t.Fatal("task missing after resume")
	}
	if got, want := resumedTask.PaneID, "7"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	worker, ok := deps.state.worker(resumedTask.WorkerID)
	if !ok {
		t.Fatal("worker missing after resume")
	}
	if got, want := worker.PaneID, "7"; got != want {
		t.Fatalf("worker.PaneID = %q, want %q", got, want)
	}
}

func TestResumeReturnsNormalizationErrorWithoutWorker(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-757", "7")

	task, ok := deps.state.task("LAB-757")
	if !ok {
		t.Fatal("seeded task missing")
	}
	task.Status = TaskStatusCancelled
	task.PaneName = ""
	task.UpdatedAt = deps.clock.Now()
	if err := deps.state.PutTask(context.Background(), task); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}
	if err := deps.state.DeleteWorker(context.Background(), task.Project, task.PaneID); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}

	state := &resumeStateStub{
		fakeState:  deps.state,
		putTaskErr: errors.New("put task failed"),
	}
	d := newResumeCoverageDaemon(t, deps, state, deps.amux)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	resumer, ok := any(d).(daemonResumer)
	if !ok {
		t.Fatal("Daemon does not implement Resume")
	}

	err := resumer.Resume(ctx, "LAB-757", "")
	if err == nil || !strings.Contains(err.Error(), "normalize task pane ref: put task failed") {
		t.Fatalf("Resume() error = %v, want normalization task error", err)
	}
	if got := len(deps.amux.paneExistsCalls); got != 0 {
		t.Fatalf("pane exists calls = %d, want 0 after normalization failure", got)
	}
}

func TestResumeRejectsInvalidStatesAndMissingTasks(t *testing.T) {
	t.Run("not started", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		d := deps.newDaemon(t)

		err := d.Resume(context.Background(), "LAB-757", "")
		if !errors.Is(err, ErrNotStarted) {
			t.Fatalf("Resume() error = %v, want ErrNotStarted", err)
		}
	})

	t.Run("missing task", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
		d := deps.newDaemon(t)
		if err := d.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		t.Cleanup(func() { _ = d.Stop(context.Background()) })

		err := d.Resume(context.Background(), "LAB-999", "")
		if !errors.Is(err, ErrTaskNotFound) {
			t.Fatalf("Resume() error = %v, want ErrTaskNotFound", err)
		}
	})

	t.Run("done task", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
		now := deps.clock.Now()
		deps.state.putTaskForTest(Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			Status:       TaskStatusDone,
			AgentProfile: "codex",
			CreatedAt:    now,
			UpdatedAt:    now,
		})

		d := deps.newDaemon(t)
		if err := d.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		t.Cleanup(func() { _ = d.Stop(context.Background()) })

		err := d.Resume(context.Background(), "LAB-757", "")
		if err == nil || !strings.Contains(err.Error(), "cannot be resumed") {
			t.Fatalf("Resume() error = %v, want cannot be resumed", err)
		}
	})

	t.Run("missing profile", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
		now := deps.clock.Now()
		deps.state.putTaskForTest(Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			Status:       TaskStatusCancelled,
			AgentProfile: "missing",
			ClonePath:    deps.pool.clone.Path,
			CreatedAt:    now,
			UpdatedAt:    now,
		})

		d := deps.newDaemon(t)
		if err := d.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		t.Cleanup(func() { _ = d.Stop(context.Background()) })

		err := d.Resume(context.Background(), "LAB-757", "")
		if err == nil || !strings.Contains(err.Error(), `load agent profile "missing"`) {
			t.Fatalf("Resume() error = %v, want missing profile", err)
		}
	})
}

func TestResumeHandlesWorkerLookupAndPaneErrors(t *testing.T) {
	t.Run("worker lookup error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
		seedActiveAssignment(t, deps, "LAB-757", "pane-1")

		state := &resumeStateStub{
			fakeState:     deps.state,
			workerByIDErr: errors.New("state unavailable"),
		}
		d := newResumeCoverageDaemon(t, deps, state, deps.amux)
		if err := d.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		t.Cleanup(func() { _ = d.Stop(context.Background()) })

		err := d.Resume(context.Background(), "LAB-757", "")
		if err == nil || !strings.Contains(err.Error(), "load worker worker-01") {
			t.Fatalf("Resume() error = %v, want worker lookup context", err)
		}
	})

	t.Run("pane exists error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
		seedActiveAssignment(t, deps, "LAB-757", "pane-1")
		deps.amux.paneExistsErr = errors.New("amux unavailable")

		d := deps.newDaemon(t)
		if err := d.Start(context.Background()); err != nil {
			t.Fatalf("Start() error = %v", err)
		}
		t.Cleanup(func() { _ = d.Stop(context.Background()) })

		err := d.Resume(context.Background(), "LAB-757", "")
		if err == nil || !strings.Contains(err.Error(), "check pane pane-1") {
			t.Fatalf("Resume() error = %v, want pane lookup context", err)
		}
	})
}

func TestResumeResumesCancelledTaskInLivePaneUsingSavedPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-757", "pane-1")

	task, ok := deps.state.task("LAB-757")
	if !ok {
		t.Fatal("seeded task missing")
	}
	task.Status = TaskStatusCancelled
	if err := deps.state.PutTask(context.Background(), task); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}
	if err := deps.state.DeleteWorker(context.Background(), task.Project, task.PaneID); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() { _ = d.Stop(context.Background()) })

	if err := d.Resume(context.Background(), "LAB-757", ""); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{"codex --yolo\n", "Implement startup recovery\n"})
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: "LAB-757", State: IssueStateInProgress}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue status updates = %#v, want %#v", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); !ok {
		t.Fatal("worker missing after live-pane resume")
	}
}

func TestResumeWorkerHandlesNoPaneAndLookupErrors(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	worker, ok, err := d.resumeWorker(context.Background(), Task{})
	if err != nil {
		t.Fatalf("resumeWorker() error = %v", err)
	}
	if ok || worker != (Worker{}) {
		t.Fatalf("resumeWorker() = (%#v, %t), want zero worker and false", worker, ok)
	}

	state := &resumeStateStub{
		fakeState:       deps.state,
		workerByPaneErr: errors.New("lookup failed"),
	}
	d = newResumeCoverageDaemon(t, deps, state, deps.amux)
	_, _, err = d.resumeWorker(context.Background(), Task{PaneID: "pane-1"})
	if err == nil || !strings.Contains(err.Error(), "load worker pane-1") {
		t.Fatalf("resumeWorker() error = %v, want worker lookup context", err)
	}
}

func TestResumeWorkerFallsBackToPaneLookupForCanonicalPaneNames(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	now := deps.clock.Now()
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-01",
		PaneID:       "pane-1",
		PaneName:     "w-LAB-757",
		Issue:        "LAB-757",
		AgentProfile: "codex",
		UpdatedAt:    now,
		LastSeenAt:   now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d := deps.newDaemon(t)
	worker, ok, err := d.resumeWorker(context.Background(), Task{
		Project:  "/tmp/project",
		Issue:    "LAB-757",
		PaneID:   "pane-1",
		PaneName: "w-LAB-757",
	})
	if err != nil {
		t.Fatalf("resumeWorker() error = %v", err)
	}
	if !ok {
		t.Fatal("resumeWorker() = ok false, want true")
	}
	if got, want := worker.WorkerID, "worker-01"; got != want {
		t.Fatalf("worker.WorkerID = %q, want %q", got, want)
	}
}

func TestResumeExistingPaneErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("missing pane id", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		d := deps.newDaemon(t)

		err := d.resumeExistingPane(context.Background(), Task{Issue: "LAB-757"}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "no worker pane") {
			t.Fatalf("resumeExistingPane() error = %v, want missing pane", err)
		}
	})

	t.Run("restart agent error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.amux.sendKeysErr = errors.New("restart failed")
		d := deps.newDaemon(t)

		err := d.resumeExistingPane(context.Background(), Task{
			Issue:        "LAB-757",
			PaneID:       "pane-1",
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, true, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "restart agent in pane pane-1") {
			t.Fatalf("resumeExistingPane() error = %v, want restart error", err)
		}
	})

	t.Run("set metadata error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		amux := &resumeAmuxStub{
			fakeAmux:       deps.amux,
			setMetadataErr: errors.New("metadata failed"),
		}
		d := newResumeCoverageDaemon(t, deps, deps.state, amux)

		err := d.resumeExistingPane(context.Background(), Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			PaneID:       "pane-1",
			PaneName:     "pane-1",
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, true, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "set pane metadata") {
			t.Fatalf("resumeExistingPane() error = %v, want metadata error", err)
		}
	})

	t.Run("build metadata error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		state := &resumeStateStub{
			fakeState:      deps.state,
			tasksByPaneErr: errors.New("tasks unavailable"),
		}
		d := newResumeCoverageDaemon(t, deps, state, deps.amux)

		err := d.resumeExistingPane(context.Background(), Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			PaneID:       "pane-1",
			PaneName:     "pane-1",
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, true, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "build pane metadata") {
			t.Fatalf("resumeExistingPane() error = %v, want metadata build error", err)
		}
	})

	t.Run("send prompt error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.amux.sendKeysResults = []error{nil, errors.New("prompt failed")}
		d := deps.newDaemon(t)

		err := d.resumeExistingPane(context.Background(), Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			PaneID:       "pane-1",
			PaneName:     "pane-1",
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, true, deps.config.profiles["codex"], "continue")
		if err == nil || !strings.Contains(err.Error(), "send prompt") {
			t.Fatalf("resumeExistingPane() error = %v, want prompt error", err)
		}
	})

	t.Run("issue status error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.issueTracker.errors = map[string]error{IssueStateInProgress: errors.New("linear unavailable")}
		d := deps.newDaemon(t)

		err := d.resumeExistingPane(context.Background(), Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			Status:       TaskStatusCancelled,
			PaneID:       "pane-1",
			PaneName:     "pane-1",
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, true, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "set issue status") {
			t.Fatalf("resumeExistingPane() error = %v, want issue status error", err)
		}
	})
}

func TestResumeWithFreshPaneErrorPaths(t *testing.T) {
	t.Parallel()

	t.Run("missing clone path", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		d := deps.newDaemon(t)

		err := d.resumeWithFreshPane(context.Background(), Task{Issue: "LAB-757"}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "no clone path") {
			t.Fatalf("resumeWithFreshPane() error = %v, want clone path error", err)
		}
	})

	t.Run("spawn error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		amux := &resumeAmuxStub{
			fakeAmux: deps.amux,
			spawnErr: errors.New("spawn failed"),
		}
		d := newResumeCoverageDaemon(t, deps, deps.state, amux)

		err := d.resumeWithFreshPane(context.Background(), Task{
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "spawn pane") {
			t.Fatalf("resumeWithFreshPane() error = %v, want spawn error", err)
		}
	})

	t.Run("metadata build error kills spawned pane", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		state := &resumeStateStub{
			fakeState:      deps.state,
			tasksByPaneErr: errors.New("tasks unavailable"),
		}
		d := newResumeCoverageDaemon(t, deps, state, deps.amux)

		err := d.resumeWithFreshPane(context.Background(), Task{
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "build pane metadata") {
			t.Fatalf("resumeWithFreshPane() error = %v, want metadata build error", err)
		}
		if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("killCalls = %#v, want %#v", got, want)
		}
	})

	t.Run("handshake error kills spawned pane", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.amux.waitIdleErr = errors.New("idle timeout")
		d := deps.newDaemon(t)

		err := d.resumeWithFreshPane(context.Background(), Task{
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "agent handshake") {
			t.Fatalf("resumeWithFreshPane() error = %v, want handshake error", err)
		}
		if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("killCalls = %#v, want %#v", got, want)
		}
	})

	t.Run("set metadata error kills spawned pane", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		amux := &resumeAmuxStub{
			fakeAmux:       deps.amux,
			setMetadataErr: errors.New("metadata failed"),
		}
		d := newResumeCoverageDaemon(t, deps, deps.state, amux)

		err := d.resumeWithFreshPane(context.Background(), Task{
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "set pane metadata") {
			t.Fatalf("resumeWithFreshPane() error = %v, want metadata error", err)
		}
		if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("killCalls = %#v, want %#v", got, want)
		}
	})

	t.Run("send prompt error kills spawned pane", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.amux.sendKeysErr = errors.New("prompt failed")
		d := deps.newDaemon(t)

		err := d.resumeWithFreshPane(context.Background(), Task{
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-757",
			Prompt:       "Resume from saved prompt",
			AgentProfile: "codex",
		}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "send prompt") {
			t.Fatalf("resumeWithFreshPane() error = %v, want prompt error", err)
		}
		if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("killCalls = %#v, want %#v", got, want)
		}
	})

	t.Run("saved prompt fallback and issue status error kill spawned pane", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		deps.issueTracker.errors = map[string]error{IssueStateInProgress: errors.New("linear unavailable")}
		d := deps.newDaemon(t)

		err := d.resumeWithFreshPane(context.Background(), Task{
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-757",
			Prompt:       "Resume from saved prompt",
			AgentProfile: "codex",
		}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "set issue status") {
			t.Fatalf("resumeWithFreshPane() error = %v, want issue status error", err)
		}
		deps.amux.requireSentKeys(t, "pane-1", []string{"Resume from saved prompt\n"})
		if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("killCalls = %#v, want %#v", got, want)
		}
	})

	t.Run("store resumed task error kills spawned pane", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		state := &resumeStateStub{
			fakeState:    deps.state,
			putWorkerErr: errors.New("put worker failed"),
		}
		d := newResumeCoverageDaemon(t, deps, state, deps.amux)

		err := d.resumeWithFreshPane(context.Background(), Task{
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-757",
			AgentProfile: "codex",
		}, Worker{}, false, deps.config.profiles["codex"], "")
		if err == nil || !strings.Contains(err.Error(), "store worker after resume") {
			t.Fatalf("resumeWithFreshPane() error = %v, want store worker error", err)
		}
		if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("killCalls = %#v, want %#v", got, want)
		}
	})
}

func TestStoreResumedTaskCoversFallbacksAndWriteErrors(t *testing.T) {
	t.Parallel()

	t.Run("fills pane name clone name and worker health", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		d := deps.newDaemon(t)
		oldWorker := Worker{Project: "/tmp/project", WorkerID: "worker-01", PaneID: "old-pane", PaneName: "worker-01", AgentProfile: "codex"}
		if err := deps.state.PutWorker(context.Background(), oldWorker); err != nil {
			t.Fatalf("PutWorker() error = %v", err)
		}

		task := Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			Status:       TaskStatusCancelled,
			WorkerID:     "worker-01",
			PaneID:       "old-pane",
			PaneName:     "w-LAB-757",
			ClonePath:    filepath.Join(t.TempDir(), "clone-01"),
			AgentProfile: "codex",
		}
		worker := Worker{
			Project:      "/tmp/project",
			WorkerID:     "worker-01",
			PaneID:       "old-pane",
			PaneName:     "w-LAB-757",
			AgentProfile: "codex",
		}
		pane := Pane{ID: "pane-2"}

		if err := d.storeResumedTask(context.Background(), task, worker, true, pane); err != nil {
			t.Fatalf("storeResumedTask() error = %v", err)
		}

		resumedTask, ok := deps.state.task("LAB-757")
		if !ok {
			t.Fatal("task missing after storeResumedTask")
		}
		if got, want := resumedTask.PaneName, "w-LAB-757"; got != want {
			t.Fatalf("task.PaneName = %q, want %q", got, want)
		}
		if got, want := resumedTask.CloneName, "clone-01"; got != want {
			t.Fatalf("task.CloneName = %q, want %q", got, want)
		}
		resumedWorker, ok := deps.state.worker("worker-01")
		if !ok {
			t.Fatal("worker missing after storeResumedTask")
		}
		if got, want := resumedWorker.PaneID, "pane-2"; got != want {
			t.Fatalf("worker.PaneID = %q, want %q", got, want)
		}
		if got, want := resumedWorker.PaneName, "w-LAB-757"; got != want {
			t.Fatalf("worker.PaneName = %q, want %q", got, want)
		}
		if got, want := resumedWorker.Health, WorkerHealthHealthy; got != want {
			t.Fatalf("worker.Health = %q, want %q", got, want)
		}
	})

	t.Run("store worker error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		state := &resumeStateStub{
			fakeState:    deps.state,
			putWorkerErr: errors.New("put worker failed"),
		}
		d := newResumeCoverageDaemon(t, deps, state, deps.amux)

		err := d.storeResumedTask(context.Background(), Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			AgentProfile: "codex",
		}, Worker{}, false, Pane{ID: "pane-2"})
		if err == nil || !strings.Contains(err.Error(), "store worker after resume") {
			t.Fatalf("storeResumedTask() error = %v, want put worker error", err)
		}
	})

	t.Run("store task error", func(t *testing.T) {
		t.Parallel()

		deps := newTestDeps(t)
		state := &resumeStateStub{
			fakeState:  deps.state,
			putTaskErr: errors.New("put task failed"),
		}
		d := newResumeCoverageDaemon(t, deps, state, deps.amux)

		err := d.storeResumedTask(context.Background(), Task{
			Project:      "/tmp/project",
			Issue:        "LAB-757",
			ClonePath:    deps.pool.clone.Path,
			AgentProfile: "codex",
		}, Worker{}, false, Pane{ID: "pane-2"})
		if err == nil || !strings.Contains(err.Error(), "store task after resume") {
			t.Fatalf("storeResumedTask() error = %v, want put task error", err)
		}
	})
}

type resumeStateStub struct {
	*fakeState
	taskByIssueErr  error
	workerByIDErr   error
	workerByPaneErr error
	tasksByPaneErr  error
	putWorkerErr    error
	putTaskErr      error
	deleteWorkerErr error
}

func (s *resumeStateStub) TaskByIssue(ctx context.Context, project, issue string) (Task, error) {
	if s.taskByIssueErr != nil {
		return Task{}, s.taskByIssueErr
	}
	return s.fakeState.TaskByIssue(ctx, project, issue)
}

func (s *resumeStateStub) WorkerByID(ctx context.Context, project, workerID string) (Worker, error) {
	if s.workerByIDErr != nil {
		return Worker{}, s.workerByIDErr
	}
	return s.fakeState.WorkerByID(ctx, project, workerID)
}

func (s *resumeStateStub) WorkerByPane(ctx context.Context, project, paneID string) (Worker, error) {
	if s.workerByPaneErr != nil {
		return Worker{}, s.workerByPaneErr
	}
	return s.fakeState.WorkerByPane(ctx, project, paneID)
}

func (s *resumeStateStub) TasksByPane(ctx context.Context, project, paneID string) ([]Task, error) {
	if s.tasksByPaneErr != nil {
		return nil, s.tasksByPaneErr
	}
	return s.fakeState.TasksByPane(ctx, project, paneID)
}

func (s *resumeStateStub) PutWorker(ctx context.Context, worker Worker) error {
	if s.putWorkerErr != nil {
		return s.putWorkerErr
	}
	return s.fakeState.PutWorker(ctx, worker)
}

func (s *resumeStateStub) PutTask(ctx context.Context, task Task) error {
	if s.putTaskErr != nil {
		return s.putTaskErr
	}
	return s.fakeState.PutTask(ctx, task)
}

func (s *resumeStateStub) DeleteWorker(ctx context.Context, project, paneID string) error {
	if s.deleteWorkerErr != nil {
		return s.deleteWorkerErr
	}
	return s.fakeState.DeleteWorker(ctx, project, paneID)
}

type resumeAmuxStub struct {
	*fakeAmux
	spawnErr       error
	setMetadataErr error
}

func (a *resumeAmuxStub) Spawn(ctx context.Context, req SpawnRequest) (Pane, error) {
	if a.spawnErr != nil {
		return Pane{}, a.spawnErr
	}
	return a.fakeAmux.Spawn(ctx, req)
}

func (a *resumeAmuxStub) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	if a.setMetadataErr != nil {
		return a.setMetadataErr
	}
	return a.fakeAmux.SetMetadata(ctx, paneID, metadata)
}

func newResumeCoverageDaemon(t *testing.T, deps *testDeps, state StateStore, amux AmuxClient) *Daemon {
	t.Helper()

	if state == nil {
		state = deps.state
	}
	if amux == nil {
		amux = deps.amux
	}

	daemon, err := New(Options{
		Project:          "/tmp/project",
		Session:          "test-session",
		PIDPath:          deps.pidPath,
		Config:           deps.config,
		State:            state,
		Pool:             deps.pool,
		Amux:             amux,
		IssueTracker:     deps.issueTracker,
		Commands:         deps.commands,
		Events:           deps.events,
		Now:              deps.clock.Now,
		NewTicker:        deps.tickers.NewTicker,
		CaptureInterval:  5 * time.Second,
		PollInterval:     30 * time.Second,
		MergeGracePeriod: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	daemon.github = newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "/tmp/project",
		commands:    deps.commands,
		now:         deps.clock.Now,
		sleep:       noSleep,
		maxAttempts: 1,
	})
	return daemon
}
