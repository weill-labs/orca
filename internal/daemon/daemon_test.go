package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDaemonStartStopPIDLifecycle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	data, err := os.ReadFile(deps.pidPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", deps.pidPath, err)
	}
	if got, want := strings.TrimSpace(string(data)), strconv.Itoa(os.Getpid()); got != want {
		t.Fatalf("pid file = %q, want %q", got, want)
	}

	if err := d.Start(ctx); err == nil {
		t.Fatal("Start() succeeded twice, want error")
	}

	if err := d.Stop(ctx); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}

	if _, err := os.Stat(deps.pidPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pid file still exists or unexpected error: %v", err)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventDaemonStopped)
}

func TestAssignAllocatesCloneStartsAgentAndRegistersState(t *testing.T) {
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

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusActive
	})

	task, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got, want := task.ClonePath, deps.pool.clone.Path; got != want {
		t.Fatalf("task.ClonePath = %q, want %q", got, want)
	}
	if got, want := task.Branch, "LAB-689"; got != want {
		t.Fatalf("task.Branch = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not stored in state")
	}
	if got, want := worker.AgentProfile, "codex"; got != want {
		t.Fatalf("worker.AgentProfile = %q, want %q", got, want)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-689", State: "In Progress"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "-B", "LAB-689"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantGit) {
		t.Fatalf("git calls = %#v, want %#v", got, wantGit)
	}

	if len(deps.amux.spawnRequests) != 1 {
		t.Fatalf("spawn requests = %d, want 1", len(deps.amux.spawnRequests))
	}
	spawn := deps.amux.spawnRequests[0]
	if got, want := spawn.Session, "test-session"; got != want {
		t.Fatalf("spawn.Session = %q, want %q", got, want)
	}
	if got, want := spawn.CWD, deps.pool.clone.Path; got != want {
		t.Fatalf("spawn.CWD = %q, want %q", got, want)
	}
	if got, want := spawn.Command, "codex --yolo"; got != want {
		t.Fatalf("spawn.Command = %q, want %q", got, want)
	}

	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile": "codex",
		"branch":        "LAB-689",
		"issue":         "LAB-689",
		"task":          "LAB-689",
	})
	deps.amux.requireSentKeys(t, "pane-1", []string{"Implement daemon core\n"})

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned)
}

func TestAssignConfirmsCodexTrustPromptBeforeSendingPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.captureSequence("pane-1", []string{"Do you trust this folder?"})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-720", "Implement handshake", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-720")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{"Enter", "Implement handshake", "Enter"})
	if got, want := deps.amux.captureCount("pane-1"), 2; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignEnforcesCodexYoloFlag(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex",
		PostmortemEnabled: true,
		StuckTimeout:      5 * time.Minute,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   3,
	}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-696", "Implement lifecycle enforcement", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "spawn request", func() bool {
		return len(deps.amux.spawnRequests) == 1
	})

	if got, want := deps.amux.spawnRequests[0].Command, "codex --yolo"; got != want {
		t.Fatalf("spawn.Command = %q, want %q", got, want)
	}
}

func TestAssignDoesNotBlindlyConfirmWhenTrustPromptNotPresent(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.captureSequence("pane-1", []string{"Codex is ready."})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-720", "Implement handshake", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-720")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{"Implement handshake\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignResumesCodexBeforeSendingPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.captureSequence("pane-1", []string{"Resume your previous session"})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-720", "Implement resume flow", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-720")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"codex --yolo resume\n",
		".",
		"Implement resume flow\n",
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignRollsBackOnPromptSendFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.sendKeysErr = errors.New("send failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err == nil {
		t.Fatal("Assign() succeeded, want error")
	}

	if _, ok := deps.state.task("LAB-689"); ok {
		t.Fatal("task stored despite rollback")
	}

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-689",
		AssignedTask:  "LAB-689",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}

	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "-B", "LAB-689"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantGit) {
		t.Fatalf("git calls = %#v, want %#v", got, wantGit)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssignFailed)
}

func TestAssignRollsBackOnAgentHandshakeFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitIdleErr = errors.New("wait idle failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-720", "Implement handshake", "codex"); err == nil {
		t.Fatal("Assign() succeeded, want error")
	} else if !strings.Contains(err.Error(), "agent handshake") {
		t.Fatalf("Assign() error = %v, want handshake context", err)
	}

	if _, ok := deps.state.task("LAB-720"); ok {
		t.Fatal("task stored despite handshake rollback")
	}

	deps.amux.requireSentKeys(t, "pane-1", nil)
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "-B", "LAB-720"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantGit) {
		t.Fatalf("git calls = %#v, want %#v", got, wantGit)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssignFailed)
}

func TestAssignRejectsConcurrentDuplicateIssueBeforeCloneAcquire(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker(), newFakeTicker(), newFakeTicker())
	deps.pool.acquireStarted = make(chan struct{}, 1)
	deps.pool.acquireRelease = make(chan struct{})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- d.Assign(ctx, "LAB-689", "Implement daemon core", "codex")
	}()

	select {
	case <-deps.pool.acquireStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first clone acquisition")
	}

	secondErr := d.Assign(ctx, "LAB-689", "Implement daemon core again", "codex")
	if secondErr == nil {
		t.Fatal("second Assign() succeeded, want duplicate assignment error")
	}
	if !strings.Contains(secondErr.Error(), "already assigned") {
		t.Fatalf("second Assign() error = %v, want duplicate assignment error", secondErr)
	}
	if got, want := deps.pool.acquireCallCount(), 1; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}

	close(deps.pool.acquireRelease)
	if err := <-firstErr; err != nil {
		t.Fatalf("first Assign() error = %v", err)
	}
}

func TestAssignRejectsIssueAlreadyActiveInStateBeforeCloneAcquire(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()
	deps.state.tasks["LAB-689"] = Task{
		Project:      "/tmp/project",
		Issue:        "LAB-689",
		Status:       TaskStatusActive,
		PaneID:       "pane-existing",
		ClonePath:    "/tmp/existing-clone",
		Branch:       "LAB-689",
		AgentProfile: "codex",
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex")
	if err == nil {
		t.Fatal("Assign() succeeded, want duplicate assignment error")
	}
	if !strings.Contains(err.Error(), "already assigned") {
		t.Fatalf("Assign() error = %v, want duplicate assignment error", err)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 0; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
}

func TestAssignRejectsAutonomousBacklogPickingPromptBeforePRLookupOrCloneAcquire(t *testing.T) {
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

	err := d.Assign(ctx, "LAB-689", "Pick up the next issue from the backlog and start working.", "codex")
	if err == nil {
		t.Fatal("Assign() succeeded, want prompt validation error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "backlog") {
		t.Fatalf("Assign() error = %v, want backlog-picking context", err)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 0; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
}

func TestAssignRejectsOpenPRBeforeCloneAcquire(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[{"number":42}]`, nil)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex")
	if err == nil {
		t.Fatal("Assign() succeeded, want open PR error")
	}
	if !strings.Contains(err.Error(), "open PR #42") {
		t.Fatalf("Assign() error = %v, want open PR context", err)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 1; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
}

func TestAssignAllowsReassigningInactiveStoredIssueWhenNoOpenPRExists(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.state.tasks["LAB-689"] = Task{
		Project: "/tmp/project",
		Issue:   "LAB-689",
		Status:  TaskStatusCancelled,
	}
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

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusActive
	})
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 1; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
}
func TestCancelKillsAgentCleansCloneAndFreesResources(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	postmortemDir := filepath.Join(home, "sync", "postmortems")
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		for _, key := range keys {
			if key == "$postmortem" {
				writePostmortemLog(t, postmortemDir, "LAB-689", deps.clock.Now().Add(time.Minute))
			}
		}
	}
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

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventTaskCancelled)
}

func TestCancelRequiresVerifiedPostmortemBeforeCleanup(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	postmortemDir := filepath.Join(home, "sync", "postmortems")
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		for _, key := range keys {
			if key == "$postmortem" {
				writePostmortemLog(t, postmortemDir, "LAB-689", deps.clock.Now().Add(time.Minute))
			}
		}
	}

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

	deps.amux.requireSentKeys(t, "pane-1", []string{"Implement daemon core", "Enter", "$postmortem", "Enter"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
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
}

func TestCancelSkipsPostmortemWhenSessionAlreadyRecorded(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	postmortemDir := filepath.Join(home, "sync", "postmortems")
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

	writePostmortemLog(t, postmortemDir, "LAB-689", deps.clock.Now().Add(time.Minute))

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

	deps.amux.requireSentKeys(t, "pane-1", []string{"Implement daemon core", "Enter"})
	if got := deps.amux.waitIdleCalls; len(got) != 0 {
		t.Fatalf("waitIdle calls = %#v, want none", got)
	}
	if got := deps.commands.callsByName("git"); len(got) != 0 {
		t.Fatalf("git calls = %#v, want none during daemon cleanup", got)
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
}

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

func TestPRMergePollingSendsWrapUpAndCleansClone(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	postmortemDir := filepath.Join(home, "sync", "postmortems")
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		for _, key := range keys {
			if key == "$postmortem" {
				writePostmortemLog(t, postmortemDir, "LAB-689", deps.clock.Now().Add(time.Minute))
			}
		}
	}
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task completion after merge", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusDone
	})

	task, _ := deps.state.task("LAB-689")
	if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-689",
		AssignedTask:  "LAB-689",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-689", State: "In Progress"},
		{Issue: "LAB-689", State: "Done"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		"$postmortem\n",
		"PR merged, wrap up.\n",
	})
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventPRMerged, EventTaskCompleted)
}

func TestPRMergeCleanupContinuesWhenIssueTrackerDoneUpdateFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		ResumeSequence:    []string{"codex --yolo resume", "Enter", "."},
		PostmortemEnabled: false,
		StuckTimeout:      5 * time.Minute,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   3,
	}
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
	deps.issueTracker.errors = map[string]error{
		IssueStateDone: errors.New("linear unavailable"),
	}
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task completion after merge despite tracker failure", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusDone
	})

	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker still present after merge cleanup")
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-689",
		AssignedTask:  "LAB-689",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-689", State: IssueStateInProgress},
		{Issue: "LAB-689", State: IssueStateDone},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if event, ok := deps.events.lastEventOfType(EventPRMerged); !ok {
		t.Fatal("missing PR merged event")
	} else if !strings.Contains(event.Message, "failed to update Linear issue status") {
		t.Fatalf("PR merged event message = %q, want tracker failure context", event.Message)
	}
}

func TestPRMergePollingReportsCompletionFailureWhenPostmortemNotRecorded(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-690", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-690", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task completion failure event", func() bool {
		return deps.events.countType(EventTaskCompletionFailed) == 1
	})

	task, ok := deps.state.task("LAB-690")
	if !ok {
		t.Fatal("task missing after merge completion failure")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); !ok {
		t.Fatal("worker removed after merge completion failure, want worker to remain active")
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-690", State: IssueStateInProgress},
		{Issue: "LAB-690", State: IssueStateDone},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		"$postmortem\n",
	})
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventPRMerged, EventTaskCompletionFailed)

	deps.events.mu.Lock()
	defer deps.events.mu.Unlock()
	var failure Event
	for _, event := range deps.events.events {
		if event.Type == EventTaskCompletionFailed {
			failure = event
			break
		}
	}
	if !strings.Contains(failure.Message, "postmortem not recorded") {
		t.Fatalf("failure.Message = %q, want to contain %q", failure.Message, "postmortem not recorded")
	}
}

func TestPRDetectionSyncsPaneMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)

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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "pr metadata sync", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.PRNumber == 42
	})

	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile": "codex",
		"branch":        "LAB-689",
		"issue":         "LAB-689",
		"pr":            "42",
		"task":          "LAB-689",
	})
}

func TestPRReviewPollingNudgesWorkerOncePerNewBlockingReviewBatch(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"CHANGES_REQUESTED","body":"Handle the nil case too."}]}`, nil)

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
	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update.\n"
	secondNudge := "New blocking PR review feedback on #42:\n- bob: Handle the nil case too.\n\nAddress the feedback in the PR review and push an update.\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "first review nudge", func() bool {
		return deps.amux.countKey("pane-1", firstNudge) == 1 && active.lastReviewCount.Load() == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}) == 2
	})
	if got, want := deps.amux.countKey("pane-1", firstNudge), 1; got != want {
		t.Fatalf("first review nudge count = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second review nudge", func() bool {
		return deps.amux.countKey("pane-1", secondNudge) == 1 && active.lastReviewCount.Load() == 2
	})

	if got, want := active.lastReviewCount.Load(), int64(2); got != want {
		t.Fatalf("assignment.lastReviewCount = %d, want %d", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		firstNudge,
		secondNudge,
	})
	if got, want := deps.events.countType(EventWorkerNudgedReview), 2; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestPRReviewPollingAdvancesCountWithoutNudgingForNonBlockingReviews(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."},{"author":{"login":"bob"},"state":"APPROVED","body":"Looks good after that."}]}`, nil)

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
	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update.\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial blocking review nudge", func() bool {
		return deps.amux.countKey("pane-1", firstNudge) == 1 && active.lastReviewCount.Load() == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "non-blocking review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}) == 2 &&
			active.lastReviewCount.Load() == 2
	})
	if got, want := active.lastReviewCount.Load(), int64(2); got != want {
		t.Fatalf("assignment.lastReviewCount = %d, want %d", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		firstNudge,
	})
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestPRReviewPollingIgnoresEmptyReviewPayload(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[{"author":{"login":"alice"},"state":"CHANGES_REQUESTED","body":"Please add tests."}]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)

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
	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}

	firstNudge := "New blocking PR review feedback on #42:\n- alice: Please add tests.\n\nAddress the feedback in the PR review and push an update.\n"

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial review nudge", func() bool {
		return deps.amux.countKey("pane-1", firstNudge) == 1 && active.lastReviewCount.Load() == 1
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "empty review poll processed", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}) == 2
	})

	if got, want := active.lastReviewCount.Load(), int64(1); got != want {
		t.Fatalf("assignment.lastReviewCount = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestEnqueueSerializesQueuedPRLandingsFIFO(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.pool.clones = []Clone{
		deps.pool.clone,
		{Name: "clone-02", Path: filepath.Join(t.TempDir(), "clone-02")},
	}
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker(), newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign(LAB-689) error = %v", err)
	}
	if err := d.Assign(ctx, "LAB-690", "Implement merge queue", "codex"); err != nil {
		t.Fatalf("Assign(LAB-690) error = %v", err)
	}

	first, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment(LAB-689) error = %v", err)
	}
	second, err := d.assignment("LAB-690")
	if err != nil {
		t.Fatalf("assignment(LAB-690) error = %v", err)
	}

	first.prNumber = 42
	first.task.PRNumber = 42
	second.prNumber = 43
	second.task.PRNumber = 43

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	firstChecks := deps.commands.block("gh", []string{"pr", "checks", "42", "--required", "--watch", "--fail-fast", "--interval", "10"})
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--required", "--watch", "--fail-fast", "--interval", "10"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "update-branch", "43", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "43", "--required", "--watch", "--fail-fast", "--interval", "10"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "43", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	select {
	case <-firstChecks.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first queued PR checks to start")
	}

	if _, err := d.Enqueue(ctx, 43); err != nil {
		t.Fatalf("Enqueue(43) error = %v", err)
	}

	if got := deps.commands.countCalls("gh", []string{"pr", "update-branch", "43", "--rebase"}); got != 0 {
		t.Fatalf("second queued PR started before first completed, update-branch calls = %d", got)
	}

	close(firstChecks.release)

	waitFor(t, "second queued PR merge", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "43", "--squash"}) == 1
	})

	if got, want := deps.commands.callsByName("gh"), []commandCall{
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "list", "--head", "LAB-690", "--state", "open", "--json", "number"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "update-branch", "42", "--rebase"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "checks", "42", "--required", "--watch", "--fail-fast", "--interval", "10"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "merge", "42", "--squash"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "update-branch", "43", "--rebase"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "checks", "43", "--required", "--watch", "--fail-fast", "--interval", "10"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "merge", "43", "--squash"}},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("gh calls = %#v, want %#v", got, want)
	}
}

func TestEnqueueNotifiesWorkerWhenRebaseFailsAndAllowsRequeue(t *testing.T) {
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

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}
	active.prNumber = 42
	active.task.PRNumber = 42

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, errors.New("rebase conflict"))

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	conflictNotice := "Merge queue could not rebase PR #42 onto main. Resolve the conflicts, push an update, and re-run `orca enqueue 42` when ready.\n"
	waitFor(t, "merge queue conflict notice", func() bool {
		return deps.amux.countKey("pane-1", conflictNotice) == 1
	})

	if got := deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}); got != 0 {
		t.Fatalf("unexpected merge attempt after rebase failure: %d", got)
	}

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--required", "--watch", "--fail-fast", "--interval", "10"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) after conflict error = %v", err)
	}

	waitFor(t, "merge queue re-enqueue merge", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}) == 1
	})
}

func TestEnqueueRejectsPRsThatAreNotTrackedByActiveAssignments(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if _, err := d.Enqueue(ctx, 42); err == nil {
		t.Fatal("Enqueue(42) succeeded, want error")
	} else if !strings.Contains(err.Error(), "active assignment") {
		t.Fatalf("Enqueue(42) error = %v, want active assignment error", err)
	}
}

func TestPRPollNudgesWorkerOncePerFailingCITransition(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	for range 4 {
		deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	}

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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial CI nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 1 && deps.events.countType(EventWorkerNudgedCI) == 1
	})

	if got, want := deps.events.countType(EventWorkerNudgedCI), 1; got != want {
		t.Fatalf("ci nudge event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket"), 1; got != want {
		t.Fatalf("gh pr checks calls = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second CI poll", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 2
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 1; got != want {
		t.Fatalf("nudge count after repeated failure = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedCI), 1; got != want {
		t.Fatalf("ci nudge event count after repeated failure = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "passing CI poll", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 3
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 1; got != want {
		t.Fatalf("nudge count after CI recovery = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second CI failure nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 2 && deps.events.countType(EventWorkerNudgedCI) == 2
	})

	if got, want := deps.events.countType(EventWorkerNudgedCI), 2; got != want {
		t.Fatalf("ci nudge event count after second failure = %d, want %d", got, want)
	}
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventWorkerNudgedCI)
}

func TestPRPollRetriesCINudgeAfterSendKeysFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	prTicker := newFakeTicker()
	deps.tickers.enqueue(newFakeTicker(), prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	for range 2 {
		deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	}
	deps.amux.sendKeysResults = []error{nil, errors.New("ci nudge failed"), nil}

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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "failed CI nudge attempt", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 1
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 0; got != want {
		t.Fatalf("successful nudge count after failed send = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedCI), 0; got != want {
		t.Fatalf("ci nudge event count after failed send = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "retried CI nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 1 && deps.events.countType(EventWorkerNudgedCI) == 1
	})
}

func TestEventWorkerNudgedCIUsesDotDelimitedName(t *testing.T) {
	t.Parallel()

	if got, want := EventWorkerNudgedCI, "worker.nudged_ci"; got != want {
		t.Fatalf("EventWorkerNudgedCI = %q, want %q", got, want)
	}
}

func TestNDJSONEmitterWritesLineDelimitedJSON(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	emitter := NewNDJSONEmitter(&buf)
	event := Event{
		Time:    time.Date(2026, 4, 2, 10, 11, 12, 0, time.UTC),
		Type:    EventTaskAssigned,
		Project: "/tmp/project",
		Issue:   "LAB-689",
		Branch:  "LAB-689",
		Message: "assigned",
	}

	if err := emitter.Emit(context.Background(), event); err != nil {
		t.Fatalf("Emit() error = %v", err)
	}

	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if got, want := len(lines), 1; got != want {
		t.Fatalf("line count = %d, want %d", got, want)
	}

	var decoded Event
	if err := json.Unmarshal([]byte(lines[0]), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got, want := decoded.Type, EventTaskAssigned; got != want {
		t.Fatalf("decoded.Type = %q, want %q", got, want)
	}
	if got, want := decoded.Issue, "LAB-689"; got != want {
		t.Fatalf("decoded.Issue = %q, want %q", got, want)
	}
}

func TestEnsureFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command string
		flag    string
		want    string
	}{
		{
			name:    "empty command",
			command: "",
			flag:    "--yolo",
			want:    "",
		},
		{
			name:    "empty flag",
			command: "codex",
			flag:    "",
			want:    "codex",
		},
		{
			name:    "appends missing flag",
			command: "codex",
			flag:    "--yolo",
			want:    "codex --yolo",
		},
		{
			name:    "preserves existing flag",
			command: "codex --yolo --profile fast",
			flag:    "--yolo",
			want:    "codex --yolo --profile fast",
		},
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

func TestEnsurePostmortemErrorPathsAndCache(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	startedAt := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
	newActive := func() *assignment {
		return &assignment{
			profile:   AgentProfile{Name: "codex", PostmortemEnabled: true},
			task:      Task{Issue: "LAB-730", Branch: "LAB-730"},
			pane:      Pane{ID: "pane-1"},
			startedAt: startedAt,
		}
	}

	t.Run("request failure", func(t *testing.T) {
		deps := newTestDeps(t)
		deps.amux.sendKeysErr = errors.New("send failed")
		d := deps.newDaemon(t)

		err := d.ensurePostmortem(context.Background(), newActive())
		if err == nil || !strings.Contains(err.Error(), "request postmortem") {
			t.Fatalf("ensurePostmortem() error = %v, want request postmortem failure", err)
		}
	})

	t.Run("wait failure", func(t *testing.T) {
		deps := newTestDeps(t)
		deps.amux.waitIdleErr = errors.New("idle timeout")
		d := deps.newDaemon(t)

		err := d.ensurePostmortem(context.Background(), newActive())
		if err == nil || !strings.Contains(err.Error(), "wait for postmortem") {
			t.Fatalf("ensurePostmortem() error = %v, want wait for postmortem failure", err)
		}
	})

	t.Run("cached postmortem skips send", func(t *testing.T) {
		deps := newTestDeps(t)
		d := deps.newDaemon(t)
		d.postmortems["pane-1"] = startedAt

		if err := d.ensurePostmortem(context.Background(), newActive()); err != nil {
			t.Fatalf("ensurePostmortem() error = %v", err)
		}
		if got := deps.amux.sentKeys["pane-1"]; len(got) != 0 {
			t.Fatalf("sent keys = %#v, want none", got)
		}
		if got := deps.amux.waitIdleCalls; len(got) != 0 {
			t.Fatalf("waitIdle calls = %#v, want none", got)
		}
	})
}

func TestFindPostmortemHelpers(t *testing.T) {
	writeFixture := func(t *testing.T, dir, name, issue, branch string, modTime time.Time) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", dir, err)
		}
		path := filepath.Join(dir, name)
		content := strings.Join([]string{
			"### Metadata",
			"- **Repo**: orca",
			"- **Branch**: " + branch,
			"- **Issues**: " + issue,
			"",
		}, "\n")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", path, err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatalf("Chtimes(%q) error = %v", path, err)
		}
	}

	t.Run("findPostmortemInDir filters by session and modtime", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		startedAt := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
		active := &assignment{
			startedAt: startedAt,
			task:      Task{Issue: "LAB-731", Branch: "LAB-731"},
		}

		writeFixture(t, dir, "stale.md", "LAB-731", "LAB-731", startedAt.Add(-time.Minute))
		writeFixture(t, dir, "wrong.md", "LAB-999", "LAB-999", startedAt.Add(time.Minute))
		writeFixture(t, dir, "match.md", "LAB-731", "LAB-731", startedAt.Add(2*time.Minute))

		recordedAt, ok, err := findPostmortemInDir(dir, active)
		if err != nil {
			t.Fatalf("findPostmortemInDir() error = %v", err)
		}
		if !ok {
			t.Fatal("findPostmortemInDir() ok = false, want true")
		}
		if got, want := recordedAt, startedAt.Add(2*time.Minute); !got.Equal(want) {
			t.Fatalf("recordedAt = %v, want %v", got, want)
		}
	})

	t.Run("findPostmortemInDir missing dir", func(t *testing.T) {
		t.Parallel()

		active := &assignment{startedAt: time.Now(), task: Task{Issue: "LAB-731", Branch: "LAB-731"}}
		recordedAt, ok, err := findPostmortemInDir(filepath.Join(t.TempDir(), "missing"), active)
		if err != nil {
			t.Fatalf("findPostmortemInDir() error = %v", err)
		}
		if ok {
			t.Fatal("findPostmortemInDir() ok = true, want false")
		}
		if !recordedAt.IsZero() {
			t.Fatalf("recordedAt = %v, want zero", recordedAt)
		}
	})

	t.Run("findPostmortemInDir unreadable target", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "broken.md")
		if err := os.Symlink(filepath.Join(dir, "missing-target"), path); err != nil {
			t.Fatalf("Symlink(%q) error = %v", path, err)
		}

		active := &assignment{
			startedAt: time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC),
			task:      Task{Issue: "LAB-731", Branch: "LAB-731"},
		}
		if _, _, err := findPostmortemInDir(dir, active); err == nil || !strings.Contains(err.Error(), "read postmortem") {
			t.Fatalf("findPostmortemInDir() error = %v, want read postmortem failure", err)
		}
	})

	t.Run("findPostmortem chooses newest known directory entry", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)

		startedAt := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
		active := &assignment{
			startedAt: startedAt,
			task:      Task{Issue: "LAB-732", Branch: "LAB-732"},
		}

		writeFixture(t, filepath.Join(home, "sync", "postmortems"), "older.md", "LAB-732", "LAB-732", startedAt.Add(time.Minute))
		writeFixture(t, filepath.Join(home, ".local", "share", "postmortems"), "newer.md", "LAB-732", "LAB-732", startedAt.Add(2*time.Minute))

		recordedAt, ok, err := findPostmortem(active)
		if err != nil {
			t.Fatalf("findPostmortem() error = %v", err)
		}
		if !ok {
			t.Fatal("findPostmortem() ok = false, want true")
		}
		if got, want := recordedAt, startedAt.Add(2*time.Minute); !got.Equal(want) {
			t.Fatalf("recordedAt = %v, want %v", got, want)
		}
	})
}

func TestMatchesPostmortemSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		issue   string
		branch  string
		want    bool
	}{
		{
			name:    "matches issue and branch case-insensitively",
			content: "issue lab-issue-733 branch feature/task-733",
			issue:   "LAB-ISSUE-733",
			branch:  "feature/TASK-733",
			want:    true,
		},
		{
			name:    "rejects missing issue",
			content: "branch feature/task-733",
			issue:   "LAB-ISSUE-733",
			branch:  "feature/TASK-733",
			want:    false,
		},
		{
			name:    "rejects missing branch",
			content: "issue lab-issue-733",
			issue:   "LAB-ISSUE-733",
			branch:  "feature/TASK-733",
			want:    false,
		},
		{
			name:    "branch-only match succeeds",
			content: "branch feature/task-733",
			issue:   "",
			branch:  "feature/TASK-733",
			want:    true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := matchesPostmortemSession(tt.content, tt.issue, tt.branch); got != tt.want {
				t.Fatalf("matchesPostmortemSession(%q, %q, %q) = %v, want %v", tt.content, tt.issue, tt.branch, got, tt.want)
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

type testDeps struct {
	clock        *fakeClock
	config       *fakeConfig
	state        *fakeState
	pool         *fakePool
	amux         *fakeAmux
	issueTracker *fakeIssueTracker
	commands     *fakeCommands
	events       *fakeEvents
	tickers      *fakeTickerFactory
	pidPath      string
}

func noSleep(context.Context, time.Duration) error { return nil }

func newTestDeps(t *testing.T) *testDeps {
	t.Helper()

	tmp := t.TempDir()
	clonePath := filepath.Join(tmp, "clone-01")
	if err := os.MkdirAll(clonePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", clonePath, err)
	}

	return &testDeps{
		clock: &fakeClock{now: time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)},
		config: &fakeConfig{
			profiles: map[string]AgentProfile{
				"codex": {
					Name:              "codex",
					StartCommand:      "codex --yolo",
					ResumeSequence:    []string{"codex --yolo resume", "Enter", "."},
					PostmortemEnabled: true,
					StuckTimeout:      5 * time.Minute,
					NudgeCommand:      "Enter",
					MaxNudgeRetries:   3,
				},
			},
		},
		state:        newFakeState(),
		pool:         &fakePool{clone: Clone{Name: "clone-01", Path: clonePath}},
		amux:         &fakeAmux{spawnPane: Pane{ID: "pane-1", Name: "worker-1"}, captures: make(map[string][]string)},
		issueTracker: &fakeIssueTracker{},
		commands:     newFakeCommands(),
		events:       newFakeEvents(),
		tickers:      &fakeTickerFactory{},
		pidPath:      filepath.Join(tmp, "orca.pid"),
	}
}

func (d *testDeps) newDaemon(t *testing.T) *Daemon {
	t.Helper()

	daemon, err := New(Options{
		Project:          "/tmp/project",
		Session:          "test-session",
		PIDPath:          d.pidPath,
		Config:           d.config,
		State:            d.state,
		Pool:             d.pool,
		Amux:             d.amux,
		IssueTracker:     d.issueTracker,
		Commands:         d.commands,
		Events:           d.events,
		Now:              d.clock.Now,
		NewTicker:        d.tickers.NewTicker,
		CaptureInterval:  5 * time.Second,
		PollInterval:     30 * time.Second,
		MergeGracePeriod: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	daemon.github = newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "/tmp/project",
		commands:    d.commands,
		now:         d.clock.Now,
		sleep:       noSleep,
		maxAttempts: 1,
	})
	return daemon
}

func waitFor(t *testing.T, name string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		waitForDuration(t, 10*time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", name)
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delta)
}

type fakeConfig struct {
	profiles map[string]AgentProfile
}

type issueStatusUpdate struct {
	Issue string
	State string
}

type fakeIssueTracker struct {
	mu      sync.Mutex
	updates []issueStatusUpdate
	errors  map[string]error
}

func (t *fakeIssueTracker) SetIssueStatus(_ context.Context, issue, state string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.updates = append(t.updates, issueStatusUpdate{Issue: issue, State: state})
	if err := t.errors[state]; err != nil {
		return err
	}
	return nil
}

func (t *fakeIssueTracker) statuses() []issueStatusUpdate {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]issueStatusUpdate, len(t.updates))
	copy(out, t.updates)
	return out
}

func (c *fakeConfig) AgentProfile(_ context.Context, name string) (AgentProfile, error) {
	profile, ok := c.profiles[name]
	if !ok {
		return AgentProfile{}, errors.New("profile not found")
	}
	return profile, nil
}

type fakeState struct {
	mu                    sync.Mutex
	rejectCanceledContext bool
	tasks                 map[string]Task
	workers               map[string]Worker
	events                []Event
}

func newFakeState() *fakeState {
	return &fakeState{
		tasks:   make(map[string]Task),
		workers: make(map[string]Worker),
	}
}

func (s *fakeState) TaskByIssue(ctx context.Context, project, issue string) (Task, error) {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return Task{}, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[issue]
	if !ok || task.Project != "" && task.Project != project {
		return Task{}, ErrTaskNotFound
	}
	return task, nil
}

func (s *fakeState) PutTask(ctx context.Context, task Task) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tasks[task.Issue] = task
	return nil
}

func (s *fakeState) PutWorker(ctx context.Context, worker Worker) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workers[worker.PaneID] = worker
	return nil
}

func (s *fakeState) DeleteWorker(ctx context.Context, project, paneID string) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.workers[paneID]
	if !ok || worker.Project != "" && worker.Project != project {
		return ErrWorkerNotFound
	}
	delete(s.workers, paneID)
	return nil
}

func (s *fakeState) RecordEvent(ctx context.Context, event Event) error {
	if s.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
	return nil
}

func (s *fakeState) task(issue string) (Task, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	task, ok := s.tasks[issue]
	return task, ok
}

func (s *fakeState) worker(paneID string) (Worker, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	worker, ok := s.workers[paneID]
	return worker, ok
}

type fakePool struct {
	mu                    sync.Mutex
	rejectCanceledContext bool
	acquireStarted        chan struct{}
	acquireRelease        chan struct{}
	acquireCalls          int
	clone                 Clone
	clones                []Clone
	acquired              map[string]bool
	released              []Clone
}

func (p *fakePool) Acquire(ctx context.Context, project, issue string) (Clone, error) {
	if p.rejectCanceledContext && ctx.Err() != nil {
		return Clone{}, ctx.Err()
	}
	p.mu.Lock()
	p.acquireCalls++
	callNumber := p.acquireCalls
	p.mu.Unlock()

	if callNumber == 1 && p.acquireStarted != nil {
		select {
		case p.acquireStarted <- struct{}{}:
		default:
		}
	}
	if callNumber == 1 && p.acquireRelease != nil {
		<-p.acquireRelease
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, clone := range p.availableClones() {
		if p.acquired == nil {
			p.acquired = make(map[string]bool)
		}
		if p.acquired[clone.Path] {
			continue
		}
		p.acquired[clone.Path] = true
		return clone, nil
	}
	return Clone{}, errors.New("clone already acquired")
}

func (p *fakePool) Release(ctx context.Context, project string, clone Clone) error {
	if p.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.acquired != nil {
		delete(p.acquired, clone.Path)
	}
	p.released = append(p.released, clone)
	return nil
}

func (p *fakePool) availableClones() []Clone {
	if len(p.clones) > 0 {
		return p.clones
	}
	return []Clone{p.clone}
}

func (p *fakePool) acquireCallCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.acquireCalls
}

func (p *fakePool) releasedClones() []Clone {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]Clone, len(p.released))
	copy(out, p.released)
	return out
}

type fakeAmux struct {
	mu                    sync.Mutex
	spawnPane             Pane
	listPanes             []Pane
	listPanesErr          error
	sendKeysErr           error
	sendKeysResults       []error
	sendKeysHook          func(paneID string, keys []string)
	waitIdleErr           error
	rejectCanceledContext bool
	spawnRequests         []SpawnRequest
	metadata              map[string]map[string]string
	sentKeys              map[string][]string
	captures              map[string][]string
	captureCalls          map[string]int
	killCalls             []string
	waitIdleCalls         []waitIdleCall
}

type waitIdleCall struct {
	PaneID  string
	Timeout time.Duration
}

func (a *fakeAmux) Spawn(ctx context.Context, req SpawnRequest) (Pane, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return Pane{}, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.spawnRequests = append(a.spawnRequests, req)
	if a.metadata == nil {
		a.metadata = make(map[string]map[string]string)
	}
	if a.sentKeys == nil {
		a.sentKeys = make(map[string][]string)
	}
	return a.spawnPane, nil
}

func (a *fakeAmux) ListPanes(ctx context.Context) ([]Pane, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return nil, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.listPanesErr != nil {
		return nil, a.listPanesErr
	}
	out := make([]Pane, len(a.listPanes))
	copy(out, a.listPanes)
	return out, nil
}

func (a *fakeAmux) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.metadata == nil {
		a.metadata = make(map[string]map[string]string)
	}
	copied := make(map[string]string, len(a.metadata[paneID])+len(metadata))
	for key, value := range a.metadata[paneID] {
		copied[key] = value
	}
	for key, value := range metadata {
		copied[key] = value
	}
	a.metadata[paneID] = copied
	return nil
}

func (a *fakeAmux) SendKeys(ctx context.Context, paneID string, keys ...string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	if a.sendKeysHook != nil {
		a.sendKeysHook(paneID, append([]string(nil), keys...))
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.sendKeysResults) > 0 {
		err := a.sendKeysResults[0]
		a.sendKeysResults = a.sendKeysResults[1:]
		if err != nil {
			return err
		}
	} else if a.sendKeysErr != nil {
		return a.sendKeysErr
	}

	if a.sentKeys == nil {
		a.sentKeys = make(map[string][]string)
	}
	a.sentKeys[paneID] = append(a.sentKeys[paneID], normalizeSentKeys(keys...)...)
	return nil
}

func (a *fakeAmux) Capture(ctx context.Context, paneID string) (string, error) {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return "", ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.captureCalls == nil {
		a.captureCalls = make(map[string]int)
	}
	a.captureCalls[paneID]++
	sequence := a.captures[paneID]
	if len(sequence) == 0 {
		return "", nil
	}
	if len(sequence) == 1 {
		return sequence[0], nil
	}
	value := sequence[0]
	a.captures[paneID] = sequence[1:]
	return value, nil
}

func (a *fakeAmux) KillPane(ctx context.Context, paneID string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.killCalls = append(a.killCalls, paneID)
	return nil
}

func (a *fakeAmux) WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.waitIdleCalls = append(a.waitIdleCalls, waitIdleCall{PaneID: paneID, Timeout: timeout})
	return a.waitIdleErr
}

func (a *fakeAmux) captureSequence(paneID string, sequence []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	copied := make([]string, len(sequence))
	copy(copied, sequence)
	a.captures[paneID] = copied
}

func (a *fakeAmux) countKey(paneID, key string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	count := 0
	for _, entry := range a.sentKeys[paneID] {
		if entry == key {
			count++
		}
	}
	return count
}

func (a *fakeAmux) captureCount(paneID string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.captureCalls[paneID]
}

func (a *fakeAmux) requireMetadata(t *testing.T, paneID string, want map[string]string) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if got := a.metadata[paneID]; !reflect.DeepEqual(got, want) {
		t.Fatalf("metadata[%q] = %#v, want %#v", paneID, got, want)
	}
}

func (a *fakeAmux) requireSentKeys(t *testing.T, paneID string, want []string) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	got := append([]string(nil), a.sentKeys[paneID]...)
	want = normalizeSentKeys(want...)
	if len(got) == 0 && len(want) == 0 {
		return
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sentKeys[%q] = %#v, want %#v", paneID, got, want)
	}
}

func normalizeSentKeys(keys ...string) []string {
	normalized := make([]string, 0, len(keys))
	for _, key := range keys {
		if key == "Enter" {
			if len(normalized) == 0 {
				normalized = append(normalized, "\n")
				continue
			}
			normalized[len(normalized)-1] += "\n"
			continue
		}
		normalized = append(normalized, key)
	}
	return normalized
}

func writePostmortemLog(t *testing.T, dir, issue string, modTime time.Time) {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", dir, err)
	}

	path := filepath.Join(dir, issue+".md")
	content := strings.Join([]string{
		"### Metadata",
		"- **Repo**: orca",
		"- **Branch**: " + issue,
		"- **Issues**: " + issue,
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("Chtimes(%q) error = %v", path, err)
	}
}

type fakeCommands struct {
	mu      sync.Mutex
	calls   []commandCall
	queued  map[string][]commandResult
	blocked map[string]commandBlock
}

type commandCall struct {
	Dir  string
	Name string
	Args []string
}

type commandResult struct {
	output string
	err    error
}

type commandBlock struct {
	started chan struct{}
	release chan struct{}
}

func newFakeCommands() *fakeCommands {
	return &fakeCommands{
		queued:  make(map[string][]commandResult),
		blocked: make(map[string]commandBlock),
	}
}

func (c *fakeCommands) Run(_ context.Context, dir, name string, args ...string) ([]byte, error) {
	c.mu.Lock()

	call := commandCall{
		Dir:  dir,
		Name: name,
		Args: append([]string(nil), args...),
	}
	c.calls = append(c.calls, call)

	key := c.key(name, args)
	block, blocked := c.blocked[key]
	if blocked {
		delete(c.blocked, key)
	}
	queue := c.queued[key]
	var result commandResult
	if len(queue) == 0 {
		c.mu.Unlock()
		if blocked {
			select {
			case block.started <- struct{}{}:
			default:
			}
			<-block.release
		}
		return nil, nil
	}
	result = queue[0]
	c.queued[key] = queue[1:]
	c.mu.Unlock()

	if blocked {
		select {
		case block.started <- struct{}{}:
		default:
		}
		<-block.release
	}
	return []byte(result.output), result.err
}

func (c *fakeCommands) queue(name string, args []string, output string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.key(name, args)
	c.queued[key] = append(c.queued[key], commandResult{output: output, err: err})
}

func (c *fakeCommands) block(name string, args []string) commandBlock {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.key(name, args)
	block := commandBlock{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
	c.blocked[key] = block
	return block
}

func (c *fakeCommands) callsByName(name string) []commandCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []commandCall
	for _, call := range c.calls {
		if call.Name == name {
			out = append(out, call)
		}
	}
	return out
}

func (c *fakeCommands) countCall(name string, args ...string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	count := 0
	for _, call := range c.calls {
		if call.Name == name && reflect.DeepEqual(call.Args, args) {
			count++
		}
	}
	return count
}

func (c *fakeCommands) tailGitCalls(count int) []commandCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	var gitCalls []commandCall
	for _, call := range c.calls {
		if call.Name == "git" {
			gitCalls = append(gitCalls, call)
		}
	}
	if len(gitCalls) < count {
		return gitCalls
	}
	return gitCalls[len(gitCalls)-count:]
}

func (c *fakeCommands) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = nil
}

func (c *fakeCommands) countCalls(name string, args []string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	count := 0
	for _, call := range c.calls {
		if call.Name != name {
			continue
		}
		if reflect.DeepEqual(call.Args, args) {
			count++
		}
	}
	return count
}

func (c *fakeCommands) key(name string, args []string) string {
	return name + "\x00" + strings.Join(args, "\x00")
}

type fakeEvents struct {
	mu     sync.Mutex
	events []Event
}

func newFakeEvents() *fakeEvents {
	return &fakeEvents{}
}

func (e *fakeEvents) Emit(_ context.Context, event Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
	return nil
}

func (e *fakeEvents) countType(eventType string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	count := 0
	for _, event := range e.events {
		if event.Type == eventType {
			count++
		}
	}
	return count
}

func (e *fakeEvents) lastEventOfType(eventType string) (Event, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := len(e.events) - 1; i >= 0; i-- {
		if e.events[i].Type == eventType {
			return e.events[i], true
		}
	}
	return Event{}, false
}

func (e *fakeEvents) requireTypes(t *testing.T, want ...string) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	have := make(map[string]bool, len(e.events))
	for _, event := range e.events {
		have[event.Type] = true
	}
	for _, eventType := range want {
		if !have[eventType] {
			t.Fatalf("event %q missing from %#v", eventType, e.events)
		}
	}
}

type fakeTickerFactory struct {
	mu      sync.Mutex
	tickers []*fakeTicker
}

func (f *fakeTickerFactory) enqueue(tickers ...*fakeTicker) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tickers = append(f.tickers, tickers...)
}

func (f *fakeTickerFactory) NewTicker(_ time.Duration) Ticker {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.tickers) == 0 {
		panic("no fake ticker queued")
	}
	ticker := f.tickers[0]
	f.tickers = f.tickers[1:]
	return ticker
}

type fakeTicker struct {
	ch chan time.Time
}

func newFakeTicker() *fakeTicker {
	return &fakeTicker{ch: make(chan time.Time, 16)}
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.ch
}

func (t *fakeTicker) Stop() {}

func (t *fakeTicker) tick(now time.Time) {
	t.ch <- now
}
