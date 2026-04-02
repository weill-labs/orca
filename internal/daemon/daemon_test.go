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

	if got, want := deps.pool.releasedClones(), []Clone{deps.pool.clone}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}

	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}

	wantCleanup := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"reset", "--hard"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"clean", "-fdx", "--exclude=.orca-pool"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"branch", "-D", "LAB-689"}},
	}
	if got := deps.commands.tailGitCalls(5); !reflect.DeepEqual(got, wantCleanup) {
		t.Fatalf("cleanup git calls = %#v, want %#v", got, wantCleanup)
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

func TestCancelKillsAgentCleansCloneAndFreesResources(t *testing.T) {
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
	if got, want := deps.pool.releasedClones(), []Clone{deps.pool.clone}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}

	wantCleanup := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"reset", "--hard"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"clean", "-fdx", "--exclude=.orca-pool"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"branch", "-D", "LAB-689"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantCleanup) {
		t.Fatalf("cleanup git calls = %#v, want %#v", got, wantCleanup)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventTaskCancelled)
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
		NudgeCommand:      "y\n",
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
		return deps.amux.countKey("pane-1", "y\n") == 1
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "second nudge", func() bool {
		return deps.amux.countKey("pane-1", "y\n") == 2
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "escalation event", func() bool {
		return deps.events.countType(EventWorkerEscalated) == 1
	})

	if got, want := deps.amux.countKey("pane-1", "y\n"), 2; got != want {
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
		NudgeCommand:    "\n",
		MaxNudgeRetries: 1,
	}
	deps.amux.captureSequence("pane-1", []string{
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
	waitFor(t, "initial capture", func() bool {
		return deps.amux.captureCount("pane-1") == 1
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
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
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
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{{PaneID: "pane-1", Timeout: 2 * time.Minute}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.pool.releasedClones(), []Clone{deps.pool.clone}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		"PR merged, wrap up.\n",
	})
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventPRMerged, EventTaskCompleted)
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

type testDeps struct {
	clock    *fakeClock
	config   *fakeConfig
	state    *fakeState
	pool     *fakePool
	amux     *fakeAmux
	commands *fakeCommands
	events   *fakeEvents
	tickers  *fakeTickerFactory
	pidPath  string
}

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
					Name:            "codex",
					StartCommand:    "codex --yolo",
					StuckTimeout:    5 * time.Minute,
					NudgeCommand:    "\n",
					MaxNudgeRetries: 3,
				},
			},
		},
		state:    newFakeState(),
		pool:     &fakePool{clone: Clone{Name: "clone-01", Path: clonePath}},
		amux:     &fakeAmux{spawnPane: Pane{ID: "pane-1", Name: "worker-1"}, captures: make(map[string][]string)},
		commands: newFakeCommands(),
		events:   newFakeEvents(),
		tickers:  &fakeTickerFactory{},
		pidPath:  filepath.Join(tmp, "orca.pid"),
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
	return daemon
}

func waitFor(t *testing.T, name string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
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
	acquired              bool
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
	if p.acquired {
		return Clone{}, errors.New("clone already acquired")
	}
	p.acquired = true
	return p.clone, nil
}

func (p *fakePool) Release(ctx context.Context, project string, clone Clone) error {
	if p.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.acquired = false
	p.released = append(p.released, clone)
	return nil
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
	sendKeysErr           error
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

func (a *fakeAmux) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.metadata == nil {
		a.metadata = make(map[string]map[string]string)
	}
	copied := make(map[string]string, len(metadata))
	for key, value := range metadata {
		copied[key] = value
	}
	a.metadata[paneID] = copied
	return nil
}

func (a *fakeAmux) SendKeys(ctx context.Context, paneID, keys string) error {
	if a.rejectCanceledContext && ctx.Err() != nil {
		return ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sentKeys == nil {
		a.sentKeys = make(map[string][]string)
	}
	a.sentKeys[paneID] = append(a.sentKeys[paneID], keys)
	return a.sendKeysErr
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
	if got := a.sentKeys[paneID]; !reflect.DeepEqual(got, want) {
		t.Fatalf("sentKeys[%q] = %#v, want %#v", paneID, got, want)
	}
}

type fakeCommands struct {
	mu     sync.Mutex
	calls  []commandCall
	queued map[string][]commandResult
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

func newFakeCommands() *fakeCommands {
	return &fakeCommands{
		queued: make(map[string][]commandResult),
	}
}

func (c *fakeCommands) Run(_ context.Context, dir, name string, args ...string) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	call := commandCall{
		Dir:  dir,
		Name: name,
		Args: append([]string(nil), args...),
	}
	c.calls = append(c.calls, call)

	key := c.key(name, args)
	queue := c.queued[key]
	if len(queue) == 0 {
		return nil, nil
	}
	result := queue[0]
	c.queued[key] = queue[1:]
	return []byte(result.output), result.err
}

func (c *fakeCommands) queue(name string, args []string, output string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.key(name, args)
	c.queued[key] = append(c.queued[key], commandResult{output: output, err: err})
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
