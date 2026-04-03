package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

type testDeps struct {
	clock         *fakeClock
	config        *fakeConfig
	state         *fakeState
	pool          *fakePool
	amux          *fakeAmux
	issueTracker  *fakeIssueTracker
	commands      *fakeCommands
	events        *fakeEvents
	tickers       *fakeTickerFactory
	pidPath       string
	postmortemDir string

	mu      sync.Mutex
	signals []signalCall
	sleeps  []time.Duration
}

func noSleep(context.Context, time.Duration) error { return nil }

type signalCall struct {
	PID    int
	Signal syscall.Signal
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
		state:         newFakeState(),
		pool:          &fakePool{clone: Clone{Name: "clone-01", Path: clonePath}},
		amux:          &fakeAmux{spawnPane: Pane{ID: "pane-1", Name: "worker-1"}, captures: make(map[string][]string)},
		issueTracker:  &fakeIssueTracker{},
		commands:      newFakeCommands(),
		events:        newFakeEvents(),
		tickers:       &fakeTickerFactory{},
		pidPath:       filepath.Join(tmp, "orca.pid"),
		postmortemDir: filepath.Join(tmp, ".local", "share", "postmortems"),
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
		PostmortemDir:    d.postmortemDir,
		Sleep:            d.recordSleep,
		SignalProcess:    d.recordSignal,
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

func (d *testDeps) recordSignal(pid int, signal syscall.Signal) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.signals = append(d.signals, signalCall{PID: pid, Signal: signal})
	return nil
}

func (d *testDeps) recordSleep(_ context.Context, delay time.Duration) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.sleeps = append(d.sleeps, delay)
	return nil
}

func (d *testDeps) signalCalls() []signalCall {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]signalCall(nil), d.signals...)
}

func (d *testDeps) sleepCalls() []time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]time.Duration(nil), d.sleeps...)
}

func waitFor(t *testing.T, name string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		<-time.After(10 * time.Millisecond)
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
