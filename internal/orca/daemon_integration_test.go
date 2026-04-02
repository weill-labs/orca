package orca_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/orca"
)

func TestDaemonLifecycleIntegration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := newTestEnv(t)
	defer closeDaemon(t, env.daemon)

	if err := env.daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	clones, err := env.daemon.Clones(ctx)
	if err != nil {
		t.Fatalf("list clones after start: %v", err)
	}
	if len(clones) != 2 {
		t.Fatalf("expected 2 eligible clones, got %d", len(clones))
	}

	assignment, err := env.daemon.Assign(ctx, orca.AssignRequest{
		IssueID: "LAB-691",
		Prompt:  "Implement the lifecycle integration test.",
	})
	if err != nil {
		t.Fatalf("assign task: %v", err)
	}

	if assignment.ClonePath != env.clones[0] {
		t.Fatalf("expected first sorted clone %q, got %q", env.clones[0], assignment.ClonePath)
	}

	pane := env.amux.snapshot(assignment.PaneID)
	if pane.Request.Command != "codex --yolo" {
		t.Fatalf("expected start command %q, got %q", "codex --yolo", pane.Request.Command)
	}
	if pane.Request.CWD != assignment.ClonePath {
		t.Fatalf("expected pane cwd %q, got %q", assignment.ClonePath, pane.Request.CWD)
	}
	if pane.Metadata["issue"] != "LAB-691" || pane.Metadata["branch"] != "LAB-691" {
		t.Fatalf("unexpected pane metadata: %#v", pane.Metadata)
	}
	if !containsString(pane.SentKeys, "Implement the lifecycle integration test.\n") {
		t.Fatalf("expected prompt to be sent to pane, got %#v", pane.SentKeys)
	}

	if got := readFile(t, filepath.Join(assignment.ClonePath, ".branch")); got != "LAB-691" {
		t.Fatalf("expected prepared clone branch to be LAB-691, got %q", got)
	}

	if err := os.WriteFile(filepath.Join(assignment.ClonePath, "scratch.txt"), []byte("temporary"), 0o644); err != nil {
		t.Fatalf("write scratch file: %v", err)
	}

	env.amux.setScreen(assignment.PaneID, "permission prompt: command requires approval")
	if err := env.daemon.MonitorOnce(ctx); err != nil {
		t.Fatalf("monitor after stuck pattern: %v", err)
	}

	task, err := env.daemon.Task(ctx, "LAB-691")
	if err != nil {
		t.Fatalf("load task after nudge: %v", err)
	}
	if task.Status != orca.TaskStatusActive {
		t.Fatalf("expected active task after nudge, got %q", task.Status)
	}
	if task.NudgeCount != 1 {
		t.Fatalf("expected 1 nudge, got %d", task.NudgeCount)
	}

	worker, err := env.daemon.Worker(ctx, assignment.PaneID)
	if err != nil {
		t.Fatalf("load worker after nudge: %v", err)
	}
	if worker.State != orca.WorkerStateNudged {
		t.Fatalf("expected nudged worker state, got %q", worker.State)
	}

	pane = env.amux.snapshot(assignment.PaneID)
	if !containsString(pane.SentKeys, "y\n") {
		t.Fatalf("expected nudge command to be sent, got %#v", pane.SentKeys)
	}

	env.prs.set("LAB-691", orca.PRStatus{Number: 17, Merged: true})
	env.amux.setScreen(assignment.PaneID, "done")
	if err := env.daemon.MonitorOnce(ctx); err != nil {
		t.Fatalf("monitor after merge: %v", err)
	}

	task, err = env.daemon.Task(ctx, "LAB-691")
	if err != nil {
		t.Fatalf("load task after merge: %v", err)
	}
	if task.Status != orca.TaskStatusDone {
		t.Fatalf("expected done task after merge, got %q", task.Status)
	}
	if task.PRNumber != 17 {
		t.Fatalf("expected PR number 17, got %d", task.PRNumber)
	}

	worker, err = env.daemon.Worker(ctx, assignment.PaneID)
	if err != nil {
		t.Fatalf("load worker after merge: %v", err)
	}
	if worker.State != orca.WorkerStateExited {
		t.Fatalf("expected exited worker after merge, got %q", worker.State)
	}

	clone, err := env.daemon.Clone(ctx, assignment.ClonePath)
	if err != nil {
		t.Fatalf("load clone after cleanup: %v", err)
	}
	if clone.Status != orca.CloneStatusFree {
		t.Fatalf("expected free clone after cleanup, got %q", clone.Status)
	}
	if clone.Branch != "main" {
		t.Fatalf("expected clone branch reset to main, got %q", clone.Branch)
	}
	if clone.TaskID != "" {
		t.Fatalf("expected clone task cleared after cleanup, got %q", clone.TaskID)
	}

	if _, err := os.Stat(filepath.Join(assignment.ClonePath, "scratch.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected scratch file to be removed during cleanup, stat err=%v", err)
	}
	if _, err := os.Stat(filepath.Join(assignment.ClonePath, ".orca-pool")); err != nil {
		t.Fatalf("expected clone pool marker to survive cleanup: %v", err)
	}
	if got := readFile(t, filepath.Join(assignment.ClonePath, ".branch")); got != "main" {
		t.Fatalf("expected cleaned clone branch to be main, got %q", got)
	}

	pane = env.amux.snapshot(assignment.PaneID)
	if !pane.Killed {
		t.Fatalf("expected pane to be killed after merge cleanup")
	}
	if !containsString(pane.SentKeys, "PR merged, wrap up.\n") {
		t.Fatalf("expected merge wrap-up message, got %#v", pane.SentKeys)
	}

	events, err := env.daemon.Events(ctx)
	if err != nil {
		t.Fatalf("load events: %v", err)
	}

	for _, want := range []string{"daemon_started", "task_assigned", "worker_nudged", "pr_merged", "clone_cleaned"} {
		if !hasEventType(events, want) {
			t.Fatalf("expected event %q in %#v", want, eventTypes(events))
		}
	}
}

func TestDaemonNudgesAfterIdleTimeoutWithoutPR(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := newTestEnv(t)
	defer closeDaemon(t, env.daemon)

	if err := env.daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	assignment, err := env.daemon.Assign(ctx, orca.AssignRequest{
		IssueID: "LAB-692",
		Prompt:  "Simulate an idle worker.",
	})
	if err != nil {
		t.Fatalf("assign task: %v", err)
	}

	env.amux.setScreen(assignment.PaneID, "working through the task")
	if err := env.daemon.MonitorOnce(ctx); err != nil {
		t.Fatalf("prime last activity: %v", err)
	}

	env.clock.Advance(6 * time.Minute)
	if err := env.daemon.MonitorOnce(ctx); err != nil {
		t.Fatalf("monitor after idle timeout: %v", err)
	}

	task, err := env.daemon.Task(ctx, "LAB-692")
	if err != nil {
		t.Fatalf("load task after idle timeout: %v", err)
	}
	if task.NudgeCount != 1 {
		t.Fatalf("expected 1 idle-timeout nudge, got %d", task.NudgeCount)
	}

	pane := env.amux.snapshot(assignment.PaneID)
	if got := countString(pane.SentKeys, "y\n"); got != 1 {
		t.Fatalf("expected exactly one nudge command, got %d in %#v", got, pane.SentKeys)
	}
}

func TestAssignKillsSpawnedPaneOnPartialFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := newTestEnv(t)
	defer closeDaemon(t, env.daemon)

	if err := env.daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	env.amux.setMetadataErr = errors.New("metadata failed")
	_, err := env.daemon.Assign(ctx, orca.AssignRequest{
		IssueID: "LAB-693",
		Prompt:  "This should fail after pane spawn.",
	})
	if err == nil {
		t.Fatal("expected assign failure after pane spawn")
	}

	pane := env.amux.snapshot("pane-1")
	if !pane.Killed {
		t.Fatalf("expected spawned pane to be killed on assign failure")
	}

	clone, err := env.daemon.Clone(ctx, env.clones[0])
	if err != nil {
		t.Fatalf("load clone after assign failure: %v", err)
	}
	if clone.Status != orca.CloneStatusFree {
		t.Fatalf("expected clone to be released after failure, got %q", clone.Status)
	}
	if clone.TaskID != "" {
		t.Fatalf("expected clone task cleared after failure, got %q", clone.TaskID)
	}

	if _, err := env.daemon.Task(ctx, "LAB-693"); !errors.Is(err, orca.ErrTaskNotFound) {
		t.Fatalf("expected no task to be persisted, got err=%v", err)
	}
	if _, err := env.daemon.Worker(ctx, "pane-1"); !errors.Is(err, orca.ErrWorkerNotFound) {
		t.Fatalf("expected no worker to be persisted, got err=%v", err)
	}
}

func TestMonitorOnceEscalatesWorkerOnlyOnce(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	env := newTestEnv(t)
	defer closeDaemon(t, env.daemon)

	if err := env.daemon.Start(ctx); err != nil {
		t.Fatalf("start daemon: %v", err)
	}

	assignment, err := env.daemon.Assign(ctx, orca.AssignRequest{
		IssueID: "LAB-694",
		Prompt:  "Drive the worker into attention state.",
	})
	if err != nil {
		t.Fatalf("assign task: %v", err)
	}

	env.amux.setScreen(assignment.PaneID, "permission prompt: still waiting")
	for range 5 {
		if err := env.daemon.MonitorOnce(ctx); err != nil {
			t.Fatalf("monitor attention cycle: %v", err)
		}
	}

	worker, err := env.daemon.Worker(ctx, assignment.PaneID)
	if err != nil {
		t.Fatalf("load worker after escalation: %v", err)
	}
	if worker.State != orca.WorkerStateAttention {
		t.Fatalf("expected attention state after repeated stuck cycles, got %q", worker.State)
	}

	events, err := env.daemon.Events(ctx)
	if err != nil {
		t.Fatalf("load events after escalation: %v", err)
	}
	if got := countEventType(events, "worker_escalated"); got != 1 {
		t.Fatalf("expected exactly one worker_escalated event, got %d", got)
	}
}

type testEnv struct {
	daemon *orca.Daemon
	amux   *fakeAMUX
	repos  *fakeRepos
	prs    *fakePRs
	clock  *fakeClock
	clones []string
}

func newTestEnv(t *testing.T) testEnv {
	t.Helper()

	root := t.TempDir()
	poolRoot := filepath.Join(root, "pool")
	if err := os.MkdirAll(poolRoot, 0o755); err != nil {
		t.Fatalf("create pool root: %v", err)
	}

	cloneOne := createClone(t, poolRoot, "orca01", true)
	cloneTwo := createClone(t, poolRoot, "orca02", true)
	_ = createClone(t, poolRoot, "orca-human", false)

	projectPath := filepath.Join(root, "project")
	if err := os.MkdirAll(projectPath, 0o755); err != nil {
		t.Fatalf("create project path: %v", err)
	}

	statePath := filepath.Join(root, "state.db")
	clock := newFakeClock(time.Date(2026, time.April, 2, 12, 0, 0, 0, time.UTC))
	amux := newFakeAMUX()
	repos := &fakeRepos{}
	prs := newFakePRs()

	daemon, err := orca.NewDaemon(orca.Config{
		ProjectPath: projectPath,
		StatePath:   statePath,
		Session:     "test-session",
		PoolPattern: filepath.Join(poolRoot, "orca*"),
		Agent: orca.AgentProfile{
			Name:              "codex",
			StartCommand:      "codex --yolo",
			StuckTextPatterns: []string{"permission prompt"},
			StuckTimeout:      5 * time.Minute,
			NudgeCommand:      "y\n",
			MaxNudgeRetries:   3,
		},
	}, orca.Dependencies{
		AMUX:  amux,
		Repos: repos,
		PRs:   prs,
		Clock: clock,
	})
	if err != nil {
		t.Fatalf("new daemon: %v", err)
	}

	return testEnv{
		daemon: daemon,
		amux:   amux,
		repos:  repos,
		prs:    prs,
		clock:  clock,
		clones: []string{cloneOne, cloneTwo},
	}
}

func closeDaemon(t *testing.T, daemon *orca.Daemon) {
	t.Helper()
	if err := daemon.Close(); err != nil {
		t.Fatalf("close daemon: %v", err)
	}
}

func createClone(t *testing.T, root, name string, eligible bool) string {
	t.Helper()

	path := filepath.Join(root, name)
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("create clone %s: %v", name, err)
	}
	if eligible {
		if err := os.WriteFile(filepath.Join(path, ".orca-pool"), []byte(""), 0o644); err != nil {
			t.Fatalf("write clone marker: %v", err)
		}
	}
	if err := os.WriteFile(filepath.Join(path, "README.md"), []byte(name), 0o644); err != nil {
		t.Fatalf("write clone baseline file: %v", err)
	}
	return path
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func countString(values []string, want string) int {
	count := 0
	for _, value := range values {
		if value == want {
			count++
		}
	}
	return count
}

func hasEventType(events []orca.Event, want string) bool {
	for _, event := range events {
		if event.Type == want {
			return true
		}
	}
	return false
}

func eventTypes(events []orca.Event) []string {
	types := make([]string, 0, len(events))
	for _, event := range events {
		types = append(types, event.Type)
	}
	return types
}

func countEventType(events []orca.Event, want string) int {
	count := 0
	for _, event := range events {
		if event.Type == want {
			count++
		}
	}
	return count
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock(start time.Time) *fakeClock {
	return &fakeClock{now: start}
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

type fakeRepos struct {
	mu      sync.Mutex
	prepare []repoCall
	cleanup []repoCall
}

type repoCall struct {
	Path   string
	Branch string
}

func (r *fakeRepos) Prepare(_ context.Context, clonePath, branch string) error {
	r.mu.Lock()
	r.prepare = append(r.prepare, repoCall{Path: clonePath, Branch: branch})
	r.mu.Unlock()

	return os.WriteFile(filepath.Join(clonePath, ".branch"), []byte(branch), 0o644)
}

func (r *fakeRepos) Cleanup(_ context.Context, clonePath, branch string) error {
	r.mu.Lock()
	r.cleanup = append(r.cleanup, repoCall{Path: clonePath, Branch: branch})
	r.mu.Unlock()

	entries, err := os.ReadDir(clonePath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == ".orca-pool" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(clonePath, entry.Name())); err != nil {
			return err
		}
	}
	return os.WriteFile(filepath.Join(clonePath, ".branch"), []byte("main"), 0o644)
}

type fakePRs struct {
	mu       sync.Mutex
	statuses map[string]orca.PRStatus
}

func newFakePRs() *fakePRs {
	return &fakePRs{statuses: make(map[string]orca.PRStatus)}
}

func (p *fakePRs) BranchStatus(_ context.Context, branch string) (orca.PRStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statuses[branch], nil
}

func (p *fakePRs) set(branch string, status orca.PRStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.statuses[branch] = status
}

type fakeAMUX struct {
	mu             sync.Mutex
	next           int
	panes          map[string]*fakePane
	setMetadataErr error
	sendKeysErr    error
	killPaneErr    error
}

type fakePane struct {
	Request  orca.SpawnRequest
	Metadata map[string]string
	SentKeys []string
	Screen   string
	Killed   bool
}

func newFakeAMUX() *fakeAMUX {
	return &fakeAMUX{panes: make(map[string]*fakePane)}
}

func (a *fakeAMUX) SpawnPane(_ context.Context, req orca.SpawnRequest) (orca.Pane, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.next++
	id := "pane-" + strconv.Itoa(a.next)
	name := "worker-" + strconv.Itoa(a.next)
	a.panes[id] = &fakePane{
		Request:  req,
		Metadata: make(map[string]string),
	}
	return orca.Pane{ID: id, Name: name}, nil
}

func (a *fakeAMUX) SendKeys(_ context.Context, paneID, keys string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sendKeysErr != nil {
		return a.sendKeysErr
	}
	a.panes[paneID].SentKeys = append(a.panes[paneID].SentKeys, keys)
	return nil
}

func (a *fakeAMUX) SetMetadata(_ context.Context, paneID string, metadata map[string]string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.setMetadataErr != nil {
		return a.setMetadataErr
	}
	for key, value := range metadata {
		a.panes[paneID].Metadata[key] = value
	}
	return nil
}

func (a *fakeAMUX) Capture(_ context.Context, paneID string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.panes[paneID].Screen, nil
}

func (a *fakeAMUX) KillPane(_ context.Context, paneID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.killPaneErr != nil {
		return a.killPaneErr
	}
	a.panes[paneID].Killed = true
	return nil
}

func (a *fakeAMUX) setScreen(paneID, screen string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.panes[paneID].Screen = screen
}

func (a *fakeAMUX) snapshot(paneID string) fakePane {
	a.mu.Lock()
	defer a.mu.Unlock()

	pane := a.panes[paneID]
	metadata := make(map[string]string, len(pane.Metadata))
	for key, value := range pane.Metadata {
		metadata[key] = value
	}
	sentKeys := append([]string(nil), pane.SentKeys...)
	return fakePane{
		Request:  pane.Request,
		Metadata: metadata,
		SentKeys: sentKeys,
		Screen:   pane.Screen,
		Killed:   pane.Killed,
	}
}
