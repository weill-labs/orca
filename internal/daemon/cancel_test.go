package daemon

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

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
