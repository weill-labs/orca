package daemon

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
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
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"task":           "LAB-689",
		"tracked_issues": `[{"id":"LAB-689","status":"active"}]`,
	})
	deps.amux.requireSentKeys(t, "pane-1", []string{"Implement daemon core\n"})

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned)
}

func TestNewOmitsLegacyPostmortemFields(t *testing.T) {
	deps := newTestDeps(t)
	daemon, err := New(Options{
		Project:  "/tmp/project",
		Config:   deps.config,
		State:    deps.state,
		Pool:     deps.pool,
		Amux:     deps.amux,
		Commands: deps.commands,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	daemonType := reflect.TypeOf(*daemon)
	for _, fieldName := range []string{"postmortemDir", "postmortemWindow", "postmortemTimeout"} {
		if _, ok := daemonType.FieldByName(fieldName); ok {
			t.Fatalf("Daemon unexpectedly contains field %q", fieldName)
		}
	}

	optionsType := reflect.TypeOf(Options{})
	for _, fieldName := range []string{"PostmortemDir", "PostmortemWindow", "PostmortemTimeout"} {
		if _, ok := optionsType.FieldByName(fieldName); ok {
			t.Fatalf("Options unexpectedly contains field %q", fieldName)
		}
	}
}
