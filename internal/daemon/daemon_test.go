package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func TestDaemonStartReplacesStalePIDFile(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := os.WriteFile(deps.pidPath, []byte("999999999\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", deps.pidPath, err)
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	data, err := os.ReadFile(deps.pidPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", deps.pidPath, err)
	}
	if got, want := strings.TrimSpace(string(data)), strconv.Itoa(os.Getpid()); got != want {
		t.Fatalf("pid file = %q, want %q", got, want)
	}

	deps.events.requireTypes(t, EventDaemonStarted)
}

func TestDaemonStartReturnsAlreadyStartedWhenPIDIsAlive(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	if err := os.WriteFile(deps.pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", deps.pidPath, err)
	}

	err := d.Start(context.Background())
	if !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("Start() error = %v, want %v", err, ErrAlreadyStarted)
	}
	if !strings.Contains(err.Error(), "daemon already running") {
		t.Fatalf("Start() error = %v, want daemon already running message", err)
	}

	data, readErr := os.ReadFile(deps.pidPath)
	if readErr != nil {
		t.Fatalf("ReadFile(%q) error = %v", deps.pidPath, readErr)
	}
	if got, want := strings.TrimSpace(string(data)), strconv.Itoa(os.Getpid()); got != want {
		t.Fatalf("pid file = %q, want %q", got, want)
	}
}

func TestDaemonStartReturnsPIDFileReadErrorForInvalidPIDFile(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	if err := os.WriteFile(deps.pidPath, []byte("not-a-pid\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", deps.pidPath, err)
	}

	err := d.Start(context.Background())
	if err == nil {
		t.Fatal("Start() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "read pid file") {
		t.Fatalf("Start() error = %v, want read pid file error", err)
	}
}

func TestRemoveStalePIDFileReturnsProcessCheckError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	if err := os.WriteFile(deps.pidPath, []byte("123\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", deps.pidPath, err)
	}

	err := d.removeStalePIDFileWithProcessCheck(func(int) (bool, error) {
		return false, errors.New("boom")
	})
	if err == nil {
		t.Fatal("removeStalePIDFileWithProcessCheck() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "check pid file process") {
		t.Fatalf("removeStalePIDFileWithProcessCheck() error = %v, want process check error", err)
	}
	if _, statErr := os.Stat(deps.pidPath); statErr != nil {
		t.Fatalf("Stat(%q) error = %v, want pid file to remain", deps.pidPath, statErr)
	}
}

func TestDaemonStartReturnsPIDDirectoryCreationError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	parent := filepath.Join(t.TempDir(), "pid-parent")
	if err := os.WriteFile(parent, []byte("not a directory\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", parent, err)
	}
	deps.pidPath = filepath.Join(parent, "orca.pid")
	d := deps.newDaemon(t)

	err := d.Start(context.Background())
	if err == nil {
		t.Fatal("Start() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "create pid directory") {
		t.Fatalf("Start() error = %v, want create pid directory error", err)
	}
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

	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker not stored in state")
	}
	if got, want := worker.WorkerID, "worker-01"; got != want {
		t.Fatalf("worker.WorkerID = %q, want %q", got, want)
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
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.name", "Orca worker-01"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.email", "worker-01@orca.local"}},
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
	deps.amux.requireSentKeys(t, "pane-1", []string{wrappedCodexPrompt("Implement daemon core") + "\n"})

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned)
}

func TestAssignStoresStablePaneNameReference(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.spawnPane = Pane{ID: "w-LAB-854", Name: "w-LAB-854"}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-854", "Fix pane references", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-854")
		return ok && task.Status == TaskStatusActive
	})

	task, ok := deps.state.task("LAB-854")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got, want := task.PaneID, "w-LAB-854"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	if got, want := task.PaneName, "w-LAB-854"; got != want {
		t.Fatalf("task.PaneName = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker("w-LAB-854")
	if !ok {
		t.Fatal("worker not stored with stable pane ref")
	}
	if got, want := worker.PaneID, "w-LAB-854"; got != want {
		t.Fatalf("worker.PaneID = %q, want %q", got, want)
	}
	if got, want := worker.PaneName, "w-LAB-854"; got != want {
		t.Fatalf("worker.PaneName = %q, want %q", got, want)
	}

	deps.amux.requireMetadata(t, "w-LAB-854", map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-854",
		"task":           "LAB-854",
		"tracked_issues": `[{"id":"LAB-854","status":"active"}]`,
	})
	deps.amux.requireSentKeys(t, "w-LAB-854", []string{wrappedCodexPrompt("Fix pane references") + "\n"})
}

func TestDaemonStartNormalizesLeadPaneToStableName(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.listPanes = []Pane{{ID: "7", Name: "lead-pane-stable"}}
	d := deps.newDaemon(t)
	d.leadPane = "7"
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if got, want := d.leadPane, "lead-pane-stable"; got != want {
		t.Fatalf("leadPane = %q, want %q", got, want)
	}

	if err := d.Assign(ctx, "LAB-855", "Verify lead pane normalization", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "spawn request", func() bool {
		return len(deps.amux.spawnRequests) == 1
	})

	if got, want := deps.amux.spawnRequests[0].AtPane, "lead-pane-stable"; got != want {
		t.Fatalf("spawn.AtPane = %q, want %q", got, want)
	}
}

func TestAssignWithCallerPaneUsesCallerPaneForWorkerSpawn(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	d.leadPane = "fallback-lead-pane"
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.AssignWithCallerPane(ctx, "LAB-932", "Use caller pane for worker spawn", "codex", "pane-13"); err != nil {
		t.Fatalf("AssignWithCallerPane() error = %v", err)
	}

	waitFor(t, "spawn request", func() bool {
		return len(deps.amux.spawnRequests) == 1
	})

	if got, want := deps.amux.spawnRequests[0].AtPane, "pane-13"; got != want {
		t.Fatalf("spawn.AtPane = %q, want %q", got, want)
	}

	task, ok := deps.state.task("LAB-932")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got, want := task.CallerPane, "pane-13"; got != want {
		t.Fatalf("task.CallerPane = %q, want %q", got, want)
	}
}

func TestAssignWithCallerLeadPaneUsesNonLeadPaneInSameWindowForWorkerSpawn(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.listPanes = []Pane{
		{ID: "2", Name: "pane-2", Window: "orca", Lead: true},
		{ID: "155", Name: "worker-07", Window: "orca"},
		{ID: "13", Name: "pane-13", Window: "alphaos", Lead: true},
	}
	d := deps.newDaemon(t)
	d.leadPane = "fallback-lead-pane"
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.AssignWithCallerPane(ctx, "LAB-943", "Use a non-lead pane in the caller window", "codex", "pane-2"); err != nil {
		t.Fatalf("AssignWithCallerPane() error = %v", err)
	}

	waitFor(t, "spawn request", func() bool {
		return len(deps.amux.spawnRequests) == 1
	})

	if got, want := deps.amux.spawnRequests[0].AtPane, "worker-07"; got != want {
		t.Fatalf("spawn.AtPane = %q, want %q", got, want)
	}

	task, ok := deps.state.task("LAB-943")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got, want := task.CallerPane, "pane-2"; got != want {
		t.Fatalf("task.CallerPane = %q, want %q", got, want)
	}
}

func TestNormalizeLeadPaneFallbacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		leadPane  string
		listPanes []Pane
		listErr   error
		want      string
	}{
		{
			name: "leaves empty lead pane unchanged",
			want: "",
		},
		{
			name:     "keeps numeric lead pane when list fails",
			leadPane: "7",
			listErr:  errors.New("amux unavailable"),
			want:     "7",
		},
		{
			name:      "keeps numeric lead pane when no stable name matches",
			leadPane:  "7",
			listPanes: []Pane{{ID: "8", Name: "w-LAB-999"}},
			want:      "7",
		},
		{
			name:      "ignores panes without names",
			leadPane:  "7",
			listPanes: []Pane{{ID: "7"}},
			want:      "7",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.amux.listPanes = append([]Pane(nil), tt.listPanes...)
			deps.amux.listPanesErr = tt.listErr

			d := deps.newDaemon(t)
			d.leadPane = tt.leadPane
			d.normalizeLeadPane(context.Background())

			if got := d.leadPane; got != tt.want {
				t.Fatalf("leadPane = %q, want %q", got, tt.want)
			}
		})
	}
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

	daemonType := reflect.TypeOf(daemon).Elem()
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
