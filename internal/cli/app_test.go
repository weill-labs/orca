package cli

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestAppRunDispatchesCommands(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 15, 4, 5, 0, time.UTC)

	tests := []struct {
		name          string
		args          []string
		prepareDaemon func(*fakeDaemon)
		prepareState  func(*fakeState)
		assert        func(t *testing.T, d *fakeDaemon, s *fakeState, stdout, stderr string)
	}{
		{
			name: "start",
			args: []string{"start", "--session", "alpha", "--project", "/tmp/orca"},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, stderr string) {
				t.Helper()
				if d.startRequest == nil {
					t.Fatal("expected start to be called")
				}
				if d.startRequest.Session != "alpha" {
					t.Fatalf("expected session alpha, got %q", d.startRequest.Session)
				}
				if d.startRequest.Project != "/tmp/orca" {
					t.Fatalf("expected project /tmp/orca, got %q", d.startRequest.Project)
				}
				if !strings.Contains(stdout, "started") {
					t.Fatalf("expected human output, got %q", stdout)
				}
				if stderr != "" {
					t.Fatalf("expected empty stderr, got %q", stderr)
				}
			},
		},
		{
			name: "stop",
			args: []string{"stop"},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string) {
				t.Helper()
				if d.stopRequest == nil {
					t.Fatal("expected stop to be called")
				}
				if d.stopRequest.Project != "/repo" {
					t.Fatalf("expected cwd project, got %q", d.stopRequest.Project)
				}
				if !strings.Contains(stdout, "stopped") {
					t.Fatalf("expected stop output, got %q", stdout)
				}
			},
		},
		{
			name: "status project",
			args: []string{"status"},
			prepareState: func(s *fakeState) {
				s.projectStatus = state.ProjectStatus{
					Project: "/repo",
					Daemon: &state.DaemonStatus{
						Session:   "alpha",
						PID:       42,
						Status:    "running",
						StartedAt: now,
						UpdatedAt: now,
					},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string) {
				t.Helper()
				if s.projectStatusProject != "/repo" {
					t.Fatalf("expected project status lookup for /repo, got %q", s.projectStatusProject)
				}
				if !strings.Contains(stdout, "daemon: running") {
					t.Fatalf("expected daemon summary, got %q", stdout)
				}
			},
		},
		{
			name: "status issue json",
			args: []string{"status", "LAB-690", "--json"},
			prepareState: func(s *fakeState) {
				s.taskStatus = state.TaskStatus{
					Task: state.Task{
						Issue:  "LAB-690",
						Status: "queued",
						Agent:  "codex",
					},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string) {
				t.Helper()
				if s.taskStatusProject != "/repo" || s.taskStatusIssue != "LAB-690" {
					t.Fatalf("expected task status lookup for /repo/LAB-690, got %q/%q", s.taskStatusProject, s.taskStatusIssue)
				}
				if !strings.Contains(stdout, "\"issue\":\"LAB-690\"") {
					t.Fatalf("expected json output, got %q", stdout)
				}
			},
		},
		{
			name: "assign default agent",
			args: []string{"assign", "LAB-690", "--prompt", "Implement CLI wiring"},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string) {
				t.Helper()
				if d.assignRequest == nil {
					t.Fatal("expected assign to be called")
				}
				if d.assignRequest.Issue != "LAB-690" {
					t.Fatalf("expected issue LAB-690, got %q", d.assignRequest.Issue)
				}
				if d.assignRequest.Prompt != "Implement CLI wiring" {
					t.Fatalf("expected prompt to be parsed, got %q", d.assignRequest.Prompt)
				}
				if d.assignRequest.Agent != "codex" {
					t.Fatalf("expected default agent codex, got %q", d.assignRequest.Agent)
				}
				if !strings.Contains(stdout, "LAB-690") {
					t.Fatalf("expected issue in output, got %q", stdout)
				}
			},
		},
		{
			name: "assign flags before issue",
			args: []string{"assign", "--prompt", "Implement CLI wiring", "--agent", "claude", "LAB-690"},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string) {
				t.Helper()
				if d.assignRequest == nil {
					t.Fatal("expected assign to be called")
				}
				if d.assignRequest.Issue != "LAB-690" {
					t.Fatalf("expected issue LAB-690, got %q", d.assignRequest.Issue)
				}
				if d.assignRequest.Agent != "claude" {
					t.Fatalf("expected agent claude, got %q", d.assignRequest.Agent)
				}
				if !strings.Contains(stdout, "LAB-690") {
					t.Fatalf("expected issue in output, got %q", stdout)
				}
			},
		},
		{
			name: "cancel",
			args: []string{"cancel", "LAB-690"},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string) {
				t.Helper()
				if d.cancelRequest == nil {
					t.Fatal("expected cancel to be called")
				}
				if d.cancelRequest.Issue != "LAB-690" {
					t.Fatalf("expected issue LAB-690, got %q", d.cancelRequest.Issue)
				}
				if !strings.Contains(stdout, "cancelled") {
					t.Fatalf("expected cancel output, got %q", stdout)
				}
			},
		},
		{
			name: "workers json",
			args: []string{"workers", "--json"},
			prepareState: func(s *fakeState) {
				s.workers = []state.Worker{
					{PaneID: "pane-3", Agent: "codex", State: "healthy", Issue: "LAB-690", ClonePath: "/clones/orca01", UpdatedAt: now},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string) {
				t.Helper()
				if s.workersProject != "/repo" {
					t.Fatalf("expected workers lookup for /repo, got %q", s.workersProject)
				}
				if !strings.Contains(stdout, "\"pane_id\":\"pane-3\"") {
					t.Fatalf("expected json worker output, got %q", stdout)
				}
			},
		},
		{
			name: "pool",
			args: []string{"pool"},
			prepareState: func(s *fakeState) {
				s.clones = []state.Clone{
					{Path: "/clones/orca01", Status: "free", Branch: "main", UpdatedAt: now},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string) {
				t.Helper()
				if s.clonesProject != "/repo" {
					t.Fatalf("expected pool lookup for /repo, got %q", s.clonesProject)
				}
				if !strings.Contains(stdout, "/clones/orca01") {
					t.Fatalf("expected human pool output, got %q", stdout)
				}
			},
		},
		{
			name: "events",
			args: []string{"events"},
			prepareState: func(s *fakeState) {
				s.events = []state.Event{
					{ID: 1, Project: "/repo", Kind: "task.assigned", Issue: "LAB-690", Message: "LAB-690 queued", CreatedAt: now},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string) {
				t.Helper()
				if s.eventsProject != "/repo" {
					t.Fatalf("expected events lookup for /repo, got %q", s.eventsProject)
				}
				if !strings.Contains(stdout, "\"kind\":\"task.assigned\"") {
					t.Fatalf("expected ndjson event output, got %q", stdout)
				}
			},
		},
		{
			name: "version",
			args: []string{"version"},
			assert: func(t *testing.T, _ *fakeDaemon, _ *fakeState, stdout, _ string) {
				t.Helper()
				if strings.TrimSpace(stdout) != "orca build-123" {
					t.Fatalf("expected version output, got %q", stdout)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := &fakeDaemon{
				startResult:  daemon.StartResult{Project: "/tmp/orca", Session: "alpha", PID: 321, StartedAt: now},
				stopResult:   daemon.StopResult{Project: "/repo", PID: 321, StoppedAt: now},
				assignResult: daemon.TaskActionResult{Project: "/repo", Issue: "LAB-690", Status: "queued", Agent: "codex", UpdatedAt: now},
				cancelResult: daemon.TaskActionResult{Project: "/repo", Issue: "LAB-690", Status: "cancelled", UpdatedAt: now},
			}
			s := &fakeState{}
			if tt.prepareDaemon != nil {
				tt.prepareDaemon(d)
			}
			if tt.prepareState != nil {
				tt.prepareState(s)
			}

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			app := New(Options{
				Daemon:  d,
				State:   s,
				Stdout:  &stdout,
				Stderr:  &stderr,
				Version: "build-123",
				Cwd: func() (string, error) {
					return "/repo", nil
				},
			})

			if err := app.Run(context.Background(), tt.args); err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			tt.assert(t, d, s, stdout.String(), stderr.String())
		})
	}
}

func TestAppRunParseErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "missing command", args: nil, wantErr: "usage: orca <command>"},
		{name: "unknown command", args: []string{"bogus"}, wantErr: "unknown command"},
		{name: "status too many args", args: []string{"status", "LAB-690", "extra"}, wantErr: "status accepts at most one issue"},
		{name: "assign missing issue", args: []string{"assign", "--prompt", "x"}, wantErr: "assign requires ISSUE"},
		{name: "assign missing prompt", args: []string{"assign", "LAB-690"}, wantErr: "assign requires --prompt"},
		{name: "cancel missing issue", args: []string{"cancel"}, wantErr: "cancel requires ISSUE"},
		{name: "workers extra arg", args: []string{"workers", "extra"}, wantErr: "workers does not accept positional arguments"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			app := New(Options{
				Daemon:  &fakeDaemon{},
				State:   &fakeState{},
				Stdout:  &stdout,
				Stderr:  &stderr,
				Version: "build-123",
				Cwd: func() (string, error) {
					return "/repo", nil
				},
			})

			err := app.Run(context.Background(), tt.args)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

type fakeDaemon struct {
	startRequest  *daemon.StartRequest
	stopRequest   *daemon.StopRequest
	assignRequest *daemon.AssignRequest
	cancelRequest *daemon.CancelRequest

	startResult  daemon.StartResult
	stopResult   daemon.StopResult
	assignResult daemon.TaskActionResult
	cancelResult daemon.TaskActionResult

	err error
}

func (f *fakeDaemon) Start(_ context.Context, req daemon.StartRequest) (daemon.StartResult, error) {
	if f.err != nil {
		return daemon.StartResult{}, f.err
	}
	f.startRequest = &req
	return f.startResult, nil
}

func (f *fakeDaemon) Stop(_ context.Context, req daemon.StopRequest) (daemon.StopResult, error) {
	if f.err != nil {
		return daemon.StopResult{}, f.err
	}
	f.stopRequest = &req
	return f.stopResult, nil
}

func (f *fakeDaemon) Assign(_ context.Context, req daemon.AssignRequest) (daemon.TaskActionResult, error) {
	if f.err != nil {
		return daemon.TaskActionResult{}, f.err
	}
	f.assignRequest = &req
	return f.assignResult, nil
}

func (f *fakeDaemon) Cancel(_ context.Context, req daemon.CancelRequest) (daemon.TaskActionResult, error) {
	if f.err != nil {
		return daemon.TaskActionResult{}, f.err
	}
	f.cancelRequest = &req
	return f.cancelResult, nil
}

type fakeState struct {
	projectStatusProject string
	taskStatusProject    string
	taskStatusIssue      string
	workersProject       string
	clonesProject        string
	eventsProject        string

	projectStatus state.ProjectStatus
	taskStatus    state.TaskStatus
	workers       []state.Worker
	clones        []state.Clone
	events        []state.Event

	err error
}

func (f *fakeState) ProjectStatus(_ context.Context, project string) (state.ProjectStatus, error) {
	if f.err != nil {
		return state.ProjectStatus{}, f.err
	}
	f.projectStatusProject = project
	return f.projectStatus, nil
}

func (f *fakeState) TaskStatus(_ context.Context, project, issue string) (state.TaskStatus, error) {
	if f.err != nil {
		return state.TaskStatus{}, f.err
	}
	f.taskStatusProject = project
	f.taskStatusIssue = issue
	if f.taskStatus.Task.Issue == "" {
		return state.TaskStatus{}, state.ErrNotFound
	}
	return f.taskStatus, nil
}

func (f *fakeState) ListWorkers(_ context.Context, project string) ([]state.Worker, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.workersProject = project
	return f.workers, nil
}

func (f *fakeState) ListClones(_ context.Context, project string) ([]state.Clone, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.clonesProject = project
	return f.clones, nil
}

func (f *fakeState) Events(_ context.Context, project string, _ int64) (<-chan state.Event, <-chan error) {
	eventsCh := make(chan state.Event, len(f.events))
	errCh := make(chan error, 1)

	f.eventsProject = project
	if f.err != nil {
		errCh <- f.err
		close(eventsCh)
		close(errCh)
		return eventsCh, errCh
	}

	for _, event := range f.events {
		eventsCh <- event
	}
	close(eventsCh)
	close(errCh)
	return eventsCh, errCh
}

var _ daemon.Controller = (*fakeDaemon)(nil)
var _ state.Reader = (*fakeState)(nil)

func TestNewRejectsNilDependencies(t *testing.T) {
	t.Parallel()

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	tests := []struct {
		name    string
		options Options
	}{
		{
			name: "missing daemon",
			options: Options{
				State:   &fakeState{},
				Stdout:  &stdout,
				Stderr:  &stderr,
				Version: "build-123",
				Cwd: func() (string, error) {
					return "/repo", nil
				},
			},
		},
		{
			name: "missing state",
			options: Options{
				Daemon:  &fakeDaemon{},
				Stdout:  &stdout,
				Stderr:  &stderr,
				Version: "build-123",
				Cwd: func() (string, error) {
					return "/repo", nil
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if recovered := recover(); !errors.Is(asError(recovered), errInvalidOptions) {
					t.Fatalf("expected errInvalidOptions panic, got %v", recovered)
				}
			}()
			New(tt.options)
		})
	}
}

func asError(v any) error {
	if err, ok := v.(error); ok {
		return err
	}
	return nil
}
