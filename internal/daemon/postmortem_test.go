package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestEnsureFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command string
		flag    string
		want    string
	}{
		{name: "empty command", command: "", flag: "--yolo", want: ""},
		{name: "empty flag", command: "codex", flag: "", want: "codex"},
		{name: "appends missing flag", command: "codex", flag: "--yolo", want: "codex --yolo"},
		{name: "preserves existing flag", command: "codex --yolo --profile fast", flag: "--yolo", want: "codex --yolo --profile fast"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := ensureFlag(tt.command, tt.flag); got != tt.want {
				t.Fatalf("ensureFlag(%q, %q) = %q, want %q", tt.command, tt.flag, got, tt.want)
			}
		})
	}
}

func TestPaneAlreadyGone(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "pane not found", err: errors.New("amux kill pane-1: exit status 1: pane not found"), want: true},
		{name: "pane missing", err: errors.New("amux kill pane-1: exit status 1: pane missing"), want: true},
		{name: "no such pane", err: errors.New("amux kill pane-1: exit status 1: no such pane"), want: true},
		{name: "no such session mentioning pane id", err: errors.New("amux kill pane-1: exit status 1: no such session"), want: false},
		{name: "different error", err: errors.New("amux kill pane-1: exit status 1: permission denied"), want: false},
		{name: "missing without pane context", err: errors.New("session missing"), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := paneAlreadyGone(tt.err); got != tt.want {
				t.Fatalf("paneAlreadyGone(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestPostmortemStatusSendsOrSkips(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		configure        func(*testDeps)
		wantStatus       string
		wantMessagePart  string
		wantErrSubstring string
		wantSendCount    int
		wantWaitCount    int
	}{
		{
			name:            "sends postmortem when enabled",
			wantStatus:      "sent",
			wantMessagePart: "command sent",
			wantSendCount:   1,
			wantWaitCount:   1,
		},
		{
			name: "skips when profile disables postmortem",
			configure: func(deps *testDeps) {
				profile := deps.config.profiles["codex"]
				profile.PostmortemEnabled = false
				deps.config.profiles["codex"] = profile
			},
			wantStatus:      "skipped",
			wantMessagePart: "disabled",
			wantSendCount:   0,
			wantWaitCount:   0,
		},
		{
			name: "returns send error but keeps sent status",
			configure: func(deps *testDeps) {
				deps.amux.sendKeysErr = errors.New("send failed")
			},
			wantStatus:       "sent",
			wantMessagePart:  "command sent",
			wantErrSubstring: "send failed",
			wantSendCount:    0,
			wantWaitCount:    0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			if tt.configure != nil {
				tt.configure(deps)
			}
			d := deps.newDaemon(t)
			active := newPostmortemAssignment(deps)

			status, message, err := d.postmortemStatus(context.Background(), active)
			if got, want := status, tt.wantStatus; got != want {
				t.Fatalf("postmortemStatus() status = %q, want %q", got, want)
			}
			if tt.wantErrSubstring == "" && err != nil {
				t.Fatalf("postmortemStatus() error = %v, want nil", err)
			}
			if tt.wantErrSubstring != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSubstring) {
					t.Fatalf("postmortemStatus() error = %v, want substring %q", err, tt.wantErrSubstring)
				}
			}
			if !strings.Contains(message, tt.wantMessagePart) {
				t.Fatalf("postmortemStatus() message = %q, want substring %q", message, tt.wantMessagePart)
			}
			if got, want := deps.amux.countKey(active.Task.PaneID, "$postmortem\n"), tt.wantSendCount; got != want {
				t.Fatalf("postmortem prompt count = %d, want %d", got, want)
			}
			if got, want := len(deps.amux.waitIdleCalls), tt.wantWaitCount; got != want {
				t.Fatalf("waitIdle calls = %d, want %d", got, want)
			}
		})
	}
}

func TestCleanupCloneAndReleaseDefaultsCloneMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	clone := Clone{Name: deps.pool.clone.Name, Path: deps.pool.clone.Path}

	if err := d.cleanupCloneAndRelease(context.Background(), clone, "LAB-734"); err != nil {
		t.Fatalf("cleanupCloneAndRelease() error = %v", err)
	}

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-734",
		AssignedTask:  "LAB-734",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
}

func TestFinishAssignmentMergedCleanupSendsWrapUpThenPostmortem(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	deps.state.putTaskForTest(active.Task)
	if err := deps.state.PutWorker(context.Background(), active.Worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	var operations []string
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		operations = append(operations, "send:"+strings.Join(keys, "|"))
	}
	deps.amux.waitIdleHook = func(_ string, timeout, _ time.Duration) {
		operations = append(operations, "wait:"+timeout.String())
	}

	if err := d.finishAssignment(context.Background(), active, TaskStatusDone, EventTaskCompleted, true); err != nil {
		t.Fatalf("finishAssignment() error = %v", err)
	}

	if got, want := operations, []string{
		"send:PR merged, wrap up.",
		"wait:2m0s",
		"send:$postmortem|Enter",
		"wait:2m0s",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
}

func TestFinishAssignmentMergedCleanupSetsDoneMetadataAfterPostmortem(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	seedFinishAssignmentState(t, deps, active)

	var operations []string
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		operations = append(operations, "send:"+strings.Join(keys, "|"))
	}
	deps.amux.waitIdleHook = func(_ string, timeout, _ time.Duration) {
		operations = append(operations, "wait:"+timeout.String())
	}
	deps.amux.setMetadataHook = func(_ string, metadata map[string]string) {
		operations = append(operations, "metadata:"+metadata["status"])
	}

	if err := d.finishAssignment(context.Background(), active, TaskStatusDone, EventTaskCompleted, true); err != nil {
		t.Fatalf("finishAssignment() error = %v", err)
	}

	if got, want := operations, []string{
		"send:PR merged, wrap up.",
		"wait:2m0s",
		"send:$postmortem|Enter",
		"wait:2m0s",
		"metadata:done",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
	deps.amux.requireMetadata(t, active.Task.PaneID, map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"status":         "done",
		"task":           "LAB-689",
		"tracked_issues": `[{"id":"LAB-689","status":"completed"}]`,
	})
}

func TestFinishAssignmentCancelledSetsDoneMetadataBeforeKill(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	seedFinishAssignmentState(t, deps, active)

	var operations []string
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		operations = append(operations, "send:"+strings.Join(keys, "|"))
	}
	deps.amux.waitIdleHook = func(_ string, timeout, _ time.Duration) {
		operations = append(operations, "wait:"+timeout.String())
	}
	deps.amux.setMetadataHook = func(_ string, metadata map[string]string) {
		operations = append(operations, "metadata:"+metadata["status"])
	}
	deps.amux.killHook = func(paneID string) {
		operations = append(operations, "kill:"+paneID)
	}

	if err := d.finishAssignmentWithMessage(context.Background(), active, TaskStatusCancelled, EventTaskCancelled, false, ""); err != nil {
		t.Fatalf("finishAssignmentWithMessage() error = %v", err)
	}

	if got, want := operations, []string{
		"send:$postmortem|Enter",
		"wait:2m0s",
		"metadata:done",
		"kill:pane-1",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.killCalls, []string{active.Task.PaneID}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	deps.amux.requireMetadata(t, active.Task.PaneID, map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"status":         "done",
		"task":           "LAB-689",
		"tracked_issues": `[{"id":"LAB-689","status":"completed"}]`,
	})
}

func TestFinishAssignmentCancelledIgnoresMissingPaneKill(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	seedFinishAssignmentState(t, deps, active)

	deps.amux.killErr = errors.New("amux kill pane-1: exit status 1: pane not found")

	if err := d.finishAssignmentWithMessage(context.Background(), active, TaskStatusCancelled, EventTaskCancelled, false, ""); err != nil {
		t.Fatalf("finishAssignmentWithMessage() error = %v, want nil", err)
	}

	task, err := deps.state.TaskByIssue(context.Background(), d.project, active.Task.Issue)
	if err != nil {
		t.Fatalf("TaskByIssue() error = %v", err)
	}
	if got := task.Status; got != TaskStatusCancelled {
		t.Fatalf("task status = %q, want %q", got, TaskStatusCancelled)
	}

	if _, err := deps.state.WorkerByPane(context.Background(), d.project, active.Task.PaneID); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("WorkerByPane() error = %v, want ErrWorkerNotFound", err)
	}
	worker, err := deps.state.WorkerByID(context.Background(), d.project, active.Worker.WorkerID)
	if err != nil {
		t.Fatalf("WorkerByID() error = %v", err)
	}
	if got := worker.PaneID; got != "" {
		t.Fatalf("worker.PaneID = %q, want empty after cancellation", got)
	}
	if got := worker.Issue; got != "" {
		t.Fatalf("worker.Issue = %q, want empty after cancellation", got)
	}

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          active.Task.CloneName,
		Path:          active.Task.ClonePath,
		CurrentBranch: active.Task.Branch,
		AssignedTask:  active.Task.Branch,
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}

	event, ok := deps.events.lastEventOfType(EventTaskCancelled)
	if !ok {
		t.Fatalf("lastEventOfType(%q) = false, want true", EventTaskCancelled)
	}
	if got := event.Message; got != "task cancelled" {
		t.Fatalf("event.Message = %q, want %q", got, "task cancelled")
	}
}

func TestFinishAssignmentCancelledIgnoresMissingPaneCleanupErrors(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	seedFinishAssignmentState(t, deps, active)

	deps.amux.sendKeysErr = errors.New("amux send-keys pane-1: exit status 1: pane not found")
	deps.amux.setMetadataErr = errors.New("amux meta set pane-1: exit status 1: pane missing")
	deps.amux.killErr = errors.New("amux kill pane-1: exit status 1: no such pane")

	if err := d.finishAssignmentWithMessage(context.Background(), active, TaskStatusCancelled, EventTaskCancelled, false, ""); err != nil {
		t.Fatalf("finishAssignmentWithMessage() error = %v, want nil", err)
	}

	task, err := deps.state.TaskByIssue(context.Background(), d.project, active.Task.Issue)
	if err != nil {
		t.Fatalf("TaskByIssue() error = %v", err)
	}
	if got := task.Status; got != TaskStatusCancelled {
		t.Fatalf("task status = %q, want %q", got, TaskStatusCancelled)
	}

	if _, err := deps.state.WorkerByPane(context.Background(), d.project, active.Task.PaneID); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("WorkerByPane() error = %v, want ErrWorkerNotFound", err)
	}

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          active.Task.CloneName,
		Path:          active.Task.ClonePath,
		CurrentBranch: active.Task.Branch,
		AssignedTask:  active.Task.Branch,
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
}

func TestFinishAssignmentCancelledIgnoresMissingPaneWaitIdleAfterPostmortemSend(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	seedFinishAssignmentState(t, deps, active)

	var operations []string
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		operations = append(operations, "send:"+strings.Join(keys, "|"))
	}
	deps.amux.waitIdleHook = func(_ string, timeout, _ time.Duration) {
		operations = append(operations, "wait:"+timeout.String())
	}
	deps.amux.setMetadataHook = func(_ string, metadata map[string]string) {
		operations = append(operations, "metadata:"+metadata["status"])
	}
	deps.amux.killHook = func(paneID string) {
		operations = append(operations, "kill:"+paneID)
	}
	deps.amux.waitIdleErr = errors.New("amux wait idle pane-1: exit status 1: pane missing")

	if err := d.finishAssignmentWithMessage(context.Background(), active, TaskStatusCancelled, EventTaskCancelled, false, ""); err != nil {
		t.Fatalf("finishAssignmentWithMessage() error = %v, want nil", err)
	}

	if got, want := operations, []string{
		"send:$postmortem|Enter",
		"wait:2m0s",
		"metadata:done",
		"kill:pane-1",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
}

func TestFinishAssignmentCancelledPropagatesNonPaneCleanupError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	seedFinishAssignmentState(t, deps, active)

	deps.amux.waitIdleErr = errors.New("wait idle timed out")

	err := d.finishAssignmentWithMessage(context.Background(), active, TaskStatusCancelled, EventTaskCancelled, false, "")
	if err == nil || !strings.Contains(err.Error(), "wait idle timed out") {
		t.Fatalf("finishAssignmentWithMessage() error = %v, want substring %q", err, "wait idle timed out")
	}

	task, taskErr := deps.state.TaskByIssue(context.Background(), d.project, active.Task.Issue)
	if taskErr != nil {
		t.Fatalf("TaskByIssue() error = %v", taskErr)
	}
	if got := task.Status; got != TaskStatusCancelled {
		t.Fatalf("task status = %q, want %q", got, TaskStatusCancelled)
	}

	if _, workerErr := deps.state.WorkerByPane(context.Background(), d.project, active.Task.PaneID); !errors.Is(workerErr, ErrWorkerNotFound) {
		t.Fatalf("WorkerByPane() error = %v, want ErrWorkerNotFound", workerErr)
	}

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          active.Task.CloneName,
		Path:          active.Task.ClonePath,
		CurrentBranch: active.Task.Branch,
		AssignedTask:  active.Task.Branch,
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
}

func TestFinishAssignmentPreservesHistoricalTrackedMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	active.Task.PRNumber = 42

	deps.state.tasks["LAB-688"] = Task{
		Project:      d.project,
		Issue:        "LAB-688",
		Status:       TaskStatusDone,
		PaneID:       active.Task.PaneID,
		ClonePath:    deps.pool.clone.Path,
		Branch:       "LAB-688",
		AgentProfile: "codex",
		PRNumber:     41,
		CreatedAt:    deps.clock.Now().Add(-2 * time.Hour),
		UpdatedAt:    deps.clock.Now().Add(-time.Hour),
	}
	seedFinishAssignmentState(t, deps, active)
	deps.amux.metadata[active.Task.PaneID]["tracked_issues"] = `[{"id":"LAB-688","status":"completed"},{"id":"LAB-689","status":"active"}]`
	deps.amux.metadata[active.Task.PaneID]["tracked_prs"] = `[{"number":41,"status":"completed"},{"number":42,"status":"active"}]`

	if err := d.finishAssignment(context.Background(), active, TaskStatusDone, EventTaskCompleted, true); err != nil {
		t.Fatalf("finishAssignment() error = %v", err)
	}

	deps.amux.requireMetadata(t, active.Task.PaneID, map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"status":         "done",
		"task":           "LAB-689",
		"tracked_issues": `[{"id":"LAB-688","status":"completed"},{"id":"LAB-689","status":"completed"}]`,
		"tracked_prs":    `[{"number":41,"status":"completed"},{"number":42,"status":"completed"}]`,
	})
}

func newPostmortemAssignment(deps *testDeps) ActiveAssignment {
	return ActiveAssignment{
		Task: Task{
			Project:      "/tmp/project",
			Issue:        "LAB-689",
			Branch:       "LAB-689",
			WorkerID:     "worker-01",
			PaneID:       deps.amux.spawnPane.ID,
			PaneName:     "worker-01",
			CloneName:    deps.pool.clone.Name,
			ClonePath:    deps.pool.clone.Path,
			AgentProfile: deps.config.profiles["codex"].Name,
			CreatedAt:    deps.clock.Now(),
			UpdatedAt:    deps.clock.Now(),
		},
		Worker: Worker{
			Project:      "/tmp/project",
			WorkerID:     "worker-01",
			PaneID:       deps.amux.spawnPane.ID,
			PaneName:     "worker-01",
			Issue:        "LAB-689",
			ClonePath:    deps.pool.clone.Path,
			AgentProfile: deps.config.profiles["codex"].Name,
		},
	}
}

func seedFinishAssignmentState(t *testing.T, deps *testDeps, active ActiveAssignment) {
	t.Helper()

	deps.state.putTaskForTest(active.Task)
	if err := deps.state.PutWorker(context.Background(), active.Worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	initialMetadata := assignmentMetadata(active.Task.AgentProfile, active.Task.Branch, active.Task.Issue)
	initialMetadata["tracked_issues"] = `[{"id":"` + active.Task.Issue + `","status":"active"}]`
	if err := deps.amux.SetMetadata(context.Background(), active.Task.PaneID, initialMetadata); err != nil {
		t.Fatalf("SetMetadata() error = %v", err)
	}
}

func TestFinishAssignmentDefaultsFailedEventMessageWhenEmpty(t *testing.T) {
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

	if err := d.Assign(ctx, "LAB-710", "Capture diagnostics before kill", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-710")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}

	if err := d.finishAssignmentWithMessage(ctx, active, TaskStatusFailed, EventTaskFailed, false, ""); err != nil {
		t.Fatalf("finishAssignmentWithMessage() error = %v", err)
	}

	event, ok := deps.events.lastEventOfType(EventTaskFailed)
	if !ok {
		t.Fatalf("lastEventOfType(%q) = false, want true", EventTaskFailed)
	}
	if got := event.Message; got != "task failed" {
		t.Fatalf("event.Message = %q, want %q", got, "task failed")
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none for failed finish", got)
	}
}
