package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAssignEnforcesCodexYoloFlag(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex",
		PostmortemEnabled: true,
		StuckTimeout:      5 * time.Minute,
		NudgeCommand:      "\n",
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
