package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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
		args          func(repoRoot, otherRepo string) []string
		prepareDaemon func(*fakeDaemon)
		prepareState  func(*fakeState)
		assert        func(t *testing.T, d *fakeDaemon, s *fakeState, stdout, stderr string, repoRoot, otherRepo string)
	}{
		{
			name: "start",
			args: func(_, otherRepo string) []string {
				return []string{"start", "--session", "alpha", "--project", filepath.Join(otherRepo, "cmd")}
			},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, stderr string, _, otherRepo string) {
				t.Helper()
				if d.startRequest == nil {
					t.Fatal("expected start to be called")
				}
				if d.startRequest.Session != "alpha" {
					t.Fatalf("expected session alpha, got %q", d.startRequest.Session)
				}
				if d.startRequest.Project != otherRepo {
					t.Fatalf("expected project %q, got %q", otherRepo, d.startRequest.Project)
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
			args: func(_, _ string) []string { return []string{"stop"} },
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if d.stopRequest == nil {
					t.Fatal("expected stop to be called")
				}
				if d.stopRequest.Project != repoRoot {
					t.Fatalf("expected cwd project %q, got %q", repoRoot, d.stopRequest.Project)
				}
				if !strings.Contains(stdout, "stopped") {
					t.Fatalf("expected stop output, got %q", stdout)
				}
			},
		},
		{
			name: "stop force",
			args: func(_, _ string) []string { return []string{"stop", "--force"} },
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if d.stopRequest == nil {
					t.Fatal("expected stop to be called")
				}
				if d.stopRequest.Project != repoRoot {
					t.Fatalf("expected cwd project %q, got %q", repoRoot, d.stopRequest.Project)
				}
				if !d.stopRequest.Force {
					t.Fatal("expected stop --force to set force request")
				}
				if !strings.Contains(stdout, "stopped") {
					t.Fatalf("expected stop output, got %q", stdout)
				}
			},
		},
		{
			name: "status project",
			args: func(_, _ string) []string { return []string{"status"} },
			prepareState: func(s *fakeState) {
				s.projectStatus = state.ProjectStatus{
					Project: "repo",
					Daemon: &state.DaemonStatus{
						Session:   "alpha",
						PID:       42,
						Status:    "running",
						StartedAt: now,
						UpdatedAt: now,
					},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if s.projectStatusProject != repoRoot {
					t.Fatalf("expected project status lookup for %q, got %q", repoRoot, s.projectStatusProject)
				}
				if !strings.Contains(stdout, "daemon: running") {
					t.Fatalf("expected daemon summary, got %q", stdout)
				}
			},
		},
		{
			name: "status issue json",
			args: func(_, _ string) []string { return []string{"status", "LAB-690", "--json"} },
			prepareState: func(s *fakeState) {
				s.taskStatus = state.TaskStatus{
					Task: state.Task{
						Issue:  "LAB-690",
						Status: "queued",
						Agent:  "codex",
					},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if s.taskStatusProject != repoRoot || s.taskStatusIssue != "LAB-690" {
					t.Fatalf("expected task status lookup for %q/LAB-690, got %q/%q", repoRoot, s.taskStatusProject, s.taskStatusIssue)
				}
				if !strings.Contains(stdout, "\"issue\":\"LAB-690\"") {
					t.Fatalf("expected json output, got %q", stdout)
				}
			},
		},
		{
			name: "assign default agent",
			args: func(_, _ string) []string { return []string{"assign", "LAB-690", "--prompt", "Implement CLI wiring"} },
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, _, _ string) {
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
			args: func(_, _ string) []string {
				return []string{"assign", "--prompt", "Implement CLI wiring", "--agent", "claude", "LAB-690"}
			},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, _, _ string) {
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
			name: "spawn",
			args: func(_, _ string) []string { return []string{"spawn", "--title", "Scratch pane"} },
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if d.spawnRequest == nil {
					t.Fatal("expected spawn to be called")
				}
				if got, want := d.spawnRequest.Project, repoRoot; got != want {
					t.Fatalf("spawn project = %q, want %q", got, want)
				}
				if got, want := d.spawnRequest.Title, "Scratch pane"; got != want {
					t.Fatalf("spawn title = %q, want %q", got, want)
				}
				if !strings.Contains(stdout, "pane-7") || !strings.Contains(stdout, "/clones/orca01") {
					t.Fatalf("expected spawn output, got %q", stdout)
				}
			},
		},
		{
			name: "enqueue",
			args: func(_, _ string) []string { return []string{"enqueue", "42"} },
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, _, _ string) {
				t.Helper()
				if d.enqueueRequest == nil {
					t.Fatal("expected enqueue to be called")
				}
				if d.enqueueRequest.PRNumber != 42 {
					t.Fatalf("expected PR 42, got %d", d.enqueueRequest.PRNumber)
				}
				if !strings.Contains(stdout, "#42") {
					t.Fatalf("expected PR number in output, got %q", stdout)
				}
			},
		},
		{
			name: "cancel",
			args: func(_, _ string) []string { return []string{"cancel", "LAB-690"} },
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, _, _ string) {
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
			name: "resume",
			args: func(_, _ string) []string { return []string{"resume", "LAB-690"} },
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, _, _ string) {
				t.Helper()
				if d.resumeRequest == nil {
					t.Fatal("expected resume to be called")
				}
				if d.resumeRequest.Issue != "LAB-690" {
					t.Fatalf("expected issue LAB-690, got %q", d.resumeRequest.Issue)
				}
				if !strings.Contains(stdout, "resumed") {
					t.Fatalf("expected resume output, got %q", stdout)
				}
			},
		},
		{
			name: "resume with prompt",
			args: func(_, _ string) []string {
				return []string{"resume", "LAB-690", "--prompt", "Continue from the latest review feedback"}
			},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, _, _ string) {
				t.Helper()
				if d.resumeRequest == nil {
					t.Fatal("expected resume to be called")
				}
				if got, want := d.resumeRequest.Prompt, "Continue from the latest review feedback"; got != want {
					t.Fatalf("resume prompt = %q, want %q", got, want)
				}
				if !strings.Contains(stdout, "resumed") {
					t.Fatalf("expected resume output, got %q", stdout)
				}
			},
		},
		{
			name: "workers json",
			args: func(_, _ string) []string { return []string{"workers", "--json"} },
			prepareState: func(s *fakeState) {
				s.workers = []state.Worker{
					{WorkerID: "worker-03", CurrentPaneID: "pane-3", Agent: "codex", State: "healthy", Issue: "LAB-690", ClonePath: "/clones/orca01", CreatedAt: now, LastSeenAt: now},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if s.workersProject != repoRoot {
					t.Fatalf("expected workers lookup for %q, got %q", repoRoot, s.workersProject)
				}
				if !strings.Contains(stdout, "\"worker_id\":\"worker-03\"") || !strings.Contains(stdout, "\"current_pane_id\":\"pane-3\"") {
					t.Fatalf("expected json worker output, got %q", stdout)
				}
			},
		},
		{
			name: "pool",
			args: func(_, _ string) []string { return []string{"pool"} },
			prepareState: func(s *fakeState) {
				s.clones = []state.Clone{
					{Path: "/clones/orca01", Status: "free", Branch: "main", UpdatedAt: now},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if s.clonesProject != repoRoot {
					t.Fatalf("expected pool lookup for %q, got %q", repoRoot, s.clonesProject)
				}
				if !strings.Contains(stdout, "/clones/orca01") {
					t.Fatalf("expected human pool output, got %q", stdout)
				}
			},
		},
		{
			name: "events",
			args: func(_, _ string) []string { return []string{"events"} },
			prepareState: func(s *fakeState) {
				s.events = []state.Event{
					{ID: 1, Project: "repo", Kind: "task.assigned", Issue: "LAB-690", Message: "LAB-690 queued", CreatedAt: now},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if s.eventsProject != repoRoot {
					t.Fatalf("expected events lookup for %q, got %q", repoRoot, s.eventsProject)
				}
				if !strings.Contains(stdout, "\"kind\":\"task.assigned\"") {
					t.Fatalf("expected ndjson event output, got %q", stdout)
				}
			},
		},
		{
			name: "version",
			args: func(_, _ string) []string { return []string{"version"} },
			assert: func(t *testing.T, _ *fakeDaemon, _ *fakeState, stdout, _ string, _, _ string) {
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

			repoRoot := newRepoRoot(t)
			cwdPath := filepath.Join(repoRoot, "internal", "cli")
			if err := os.MkdirAll(cwdPath, 0o755); err != nil {
				t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
			}
			otherRepo := newRepoRoot(t)
			if err := os.MkdirAll(filepath.Join(otherRepo, "cmd"), 0o755); err != nil {
				t.Fatalf("MkdirAll(%q): %v", filepath.Join(otherRepo, "cmd"), err)
			}

			d := &fakeDaemon{
				startResult:   daemon.StartResult{Project: otherRepo, Session: "alpha", PID: 321, StartedAt: now},
				stopResult:    daemon.StopResult{Project: repoRoot, PID: 321, StoppedAt: now},
				assignResult:  daemon.TaskActionResult{Project: repoRoot, Issue: "LAB-690", Status: "queued", Agent: "codex", UpdatedAt: now},
				spawnResult:   daemon.SpawnPaneResult{Project: repoRoot, PaneID: "pane-7", PaneName: "Scratch pane", ClonePath: "/clones/orca01"},
				enqueueResult: daemon.MergeQueueActionResult{Project: repoRoot, PRNumber: 42, Status: "queued", Position: 1, UpdatedAt: now},
				cancelResult:  daemon.TaskActionResult{Project: repoRoot, Issue: "LAB-690", Status: "cancelled", UpdatedAt: now},
				resumeResult:  daemon.TaskActionResult{Project: repoRoot, Issue: "LAB-690", Status: "active", Agent: "codex", UpdatedAt: now},
			}
			s := &fakeState{}
			if tt.prepareDaemon != nil {
				tt.prepareDaemon(d)
			}
			if tt.prepareState != nil {
				tt.prepareState(s)
			}
			if s.projectStatus.Project != "" {
				s.projectStatus.Project = repoRoot
			}
			for i := range s.events {
				if s.events[i].Project != "" {
					s.events[i].Project = repoRoot
				}
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
					return cwdPath, nil
				},
				ProjectStatusRPC: func(context.Context, string) (daemon.ProjectStatusRPCResult, error) {
					return daemon.ProjectStatusRPCResult{ProjectStatus: s.projectStatus}, nil
				},
			})

			if err := app.Run(context.Background(), tt.args(repoRoot, otherRepo)); err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			tt.assert(t, d, s, stdout.String(), stderr.String(), repoRoot, otherRepo)
		})
	}
}

func TestRunCancelClientTimeoutBehavior(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         []string
		wantDeadline bool
	}{
		{
			name:         "default cancel sets 10s deadline",
			args:         []string{"cancel", "LAB-690"},
			wantDeadline: true,
		},
		{
			name:         "force cancel skips client deadline",
			args:         []string{"cancel", "--force", "LAB-690"},
			wantDeadline: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repoRoot := newRepoRoot(t)
			cwdPath := filepath.Join(repoRoot, "internal", "cli")
			if err := os.MkdirAll(cwdPath, 0o755); err != nil {
				t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
			}

			d := &fakeDaemon{
				cancelHook: func(ctx context.Context, req daemon.CancelRequest) (daemon.TaskActionResult, error) {
					t.Helper()

					if got, want := req.Issue, "LAB-690"; got != want {
						t.Fatalf("cancel issue = %q, want %q", got, want)
					}

					deadline, ok := ctx.Deadline()
					if got, want := ok, tt.wantDeadline; got != want {
						t.Fatalf("ctx has deadline = %t, want %t", got, want)
					}
					if tt.wantDeadline {
						remaining := time.Until(deadline)
						if remaining < 9*time.Second || remaining > 10*time.Second {
							t.Fatalf("deadline remaining = %s, want between 9s and 10s", remaining)
						}
					}

					return daemon.TaskActionResult{
						Project:   repoRoot,
						Issue:     req.Issue,
						Status:    "cancelled",
						UpdatedAt: time.Now().UTC(),
					}, nil
				},
			}

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			app := New(Options{
				Daemon:  d,
				State:   &fakeState{},
				Stdout:  &stdout,
				Stderr:  &stderr,
				Version: "build-123",
				Cwd: func() (string, error) {
					return cwdPath, nil
				},
			})

			if err := app.Run(context.Background(), tt.args); err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if got, want := strings.TrimSpace(stdout.String()), "LAB-690 cancelled"; got != want {
				t.Fatalf("stdout = %q, want %q", got, want)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestRunCancelTimeoutGuidance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		err     error
		wantErr string
	}{
		{
			name:    "default cancel suggests force and stop",
			args:    []string{"cancel", "LAB-690"},
			err:     fmt.Errorf("decode cancel response: %w", os.ErrDeadlineExceeded),
			wantErr: "cancel timed out after 10s waiting for the daemon; try `orca cancel --force LAB-690` or `orca stop --force`",
		},
		{
			name:    "force cancel suggests stop force",
			args:    []string{"cancel", "--force", "LAB-690"},
			err:     context.DeadlineExceeded,
			wantErr: "cancel timed out waiting for the daemon; try `orca stop --force`",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repoRoot := newRepoRoot(t)
			cwdPath := filepath.Join(repoRoot, "internal", "cli")
			if err := os.MkdirAll(cwdPath, 0o755); err != nil {
				t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
			}

			app := New(Options{
				Daemon: &fakeDaemon{
					cancelHook: func(context.Context, daemon.CancelRequest) (daemon.TaskActionResult, error) {
						return daemon.TaskActionResult{}, tt.err
					},
				},
				State:   &fakeState{},
				Stdout:  &bytes.Buffer{},
				Stderr:  &bytes.Buffer{},
				Version: "build-123",
				Cwd: func() (string, error) {
					return cwdPath, nil
				},
			})

			err := app.Run(context.Background(), tt.args)
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("Run() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestAppRunStatusDaemonVersionAnnotation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		installedCommit string
		daemonCommit    string
		rpcErr          error
		wantLine        string
		wantNotContains string
	}{
		{
			name:            "different commits show warning",
			installedCommit: "def5678",
			daemonCommit:    "abc1234",
			wantLine:        "daemon: running (version abc1234, installed def5678 — restart recommended)\n",
		},
		{
			name:            "matching commits omit warning",
			installedCommit: "abc1234",
			daemonCommit:    "abc1234",
			wantLine:        "daemon: running\n",
			wantNotContains: "restart recommended",
		},
		{
			name:            "rpc failure falls back to plain running status",
			installedCommit: "def5678",
			rpcErr:          errors.New("daemon unavailable"),
			wantLine:        "daemon: running\n",
			wantNotContains: "restart recommended",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			projectPath := t.TempDir()
			if err := os.Mkdir(filepath.Join(projectPath, ".git"), 0o755); err != nil {
				t.Fatalf("Mkdir(.git) error = %v", err)
			}

			projectStatus := state.ProjectStatus{
				Project: projectPath,
				Daemon: &state.DaemonStatus{
					Status:  "running",
					Session: "alpha",
					PID:     42,
				},
			}

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			app := New(Options{
				Daemon: &fakeDaemon{},
				State: &fakeState{
					projectStatus: projectStatus,
				},
				Stdout:  &stdout,
				Stderr:  &stderr,
				Version: tt.installedCommit,
				Cwd: func() (string, error) {
					return projectPath, nil
				},
				ProjectStatusRPC: func(context.Context, string) (daemon.ProjectStatusRPCResult, error) {
					if tt.rpcErr != nil {
						return daemon.ProjectStatusRPCResult{}, tt.rpcErr
					}
					return daemon.ProjectStatusRPCResult{
						ProjectStatus: projectStatus,
						BuildCommit:   tt.daemonCommit,
					}, nil
				},
			})

			if err := app.Run(context.Background(), []string{"status"}); err != nil {
				t.Fatalf("Run(status) error = %v", err)
			}
			if got := stdout.String(); !strings.Contains(got, tt.wantLine) {
				t.Fatalf("stdout = %q, want substring %q", got, tt.wantLine)
			}
			if tt.wantNotContains != "" && strings.Contains(stdout.String(), tt.wantNotContains) {
				t.Fatalf("stdout = %q, want no substring %q", stdout.String(), tt.wantNotContains)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestAppRunAssignRejectsRemovedTitleFlag(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  &fakeDaemon{},
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	err := app.Run(context.Background(), []string{"assign", "LAB-690", "--prompt", "Implement CLI wiring", "--title", "Worker pane title"})
	if err == nil {
		t.Fatal("Run() error = nil, want parse error")
	}
	if !strings.Contains(err.Error(), "flag provided but not defined: -title") {
		t.Fatalf("Run() error = %v, want removed flag error", err)
	}
}

func TestAppRunStartDefaultsSessionFromAMUXSessionEnv(t *testing.T) {
	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	otherRepo := newRepoRoot(t)
	targetPath := filepath.Join(otherRepo, "cmd")
	if err := os.MkdirAll(targetPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", targetPath, err)
	}

	t.Setenv("AMUX_SESSION", "from-env")

	d := &fakeDaemon{
		startResult: daemon.StartResult{
			Project:   otherRepo,
			Session:   "from-env",
			PID:       321,
			StartedAt: time.Now().UTC(),
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	if err := app.Run(context.Background(), []string{"start", "--project", targetPath}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.startRequest == nil {
		t.Fatal("expected start to be called")
	}
	if got, want := d.startRequest.Session, "from-env"; got != want {
		t.Fatalf("start session = %q, want %q", got, want)
	}
	if stderr.String() != "" {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}

func TestAppRunStartGlobalFlagSkipsProjectResolution(t *testing.T) {
	t.Parallel()

	d := &fakeDaemon{
		startResult: daemon.StartResult{
			Session:   "from-env",
			PID:       321,
			StartedAt: time.Now().UTC(),
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return "", errors.New("cwd should not be resolved for --global")
		},
	})

	if err := app.Run(context.Background(), []string{"start", "--global"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.startRequest == nil {
		t.Fatal("expected start to be called")
	}
	if got := d.startRequest.Project; got != "" {
		t.Fatalf("start project = %q, want empty for --global", got)
	}
}

func TestAppRunStopGlobalFlagSkipsProjectResolution(t *testing.T) {
	t.Parallel()

	d := &fakeDaemon{
		stopResult: daemon.StopResult{
			PID:       321,
			StoppedAt: time.Now().UTC(),
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return "", errors.New("cwd should not be resolved for --global")
		},
	})

	if err := app.Run(context.Background(), []string{"stop", "--global"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.stopRequest == nil {
		t.Fatal("expected stop to be called")
	}
	if got := d.stopRequest.Project; got != "" {
		t.Fatalf("stop project = %q, want empty for --global", got)
	}
}

func TestAppRunSpawnDefaultsSessionFromAMUXSessionEnv(t *testing.T) {
	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	t.Setenv("AMUX_SESSION", "spawn-from-env")

	d := &fakeDaemon{
		spawnResult: daemon.SpawnPaneResult{
			Project:   repoRoot,
			PaneID:    "pane-7",
			ClonePath: "/clones/orca01",
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	if err := app.Run(context.Background(), []string{"spawn", "--title", "Scratch pane"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.spawnRequest == nil {
		t.Fatal("expected spawn to be called")
	}
	if got, want := d.spawnRequest.Session, "spawn-from-env"; got != want {
		t.Fatalf("spawn session = %q, want %q", got, want)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAppRunAssignDefaultsCallerPaneFromAMUXPaneEnv(t *testing.T) {
	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	t.Setenv("AMUX_PANE", "pane-13")

	d := &fakeDaemon{
		assignResult: daemon.TaskActionResult{
			Project:   repoRoot,
			Issue:     "LAB-932",
			Status:    "queued",
			Agent:     "codex",
			UpdatedAt: time.Now().UTC(),
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	if err := app.Run(context.Background(), []string{"assign", "LAB-932", "--prompt", "Replace lead pane with caller pane"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.assignRequest == nil {
		t.Fatal("expected assign to be called")
	}
	if got, want := d.assignRequest.CallerPane, "pane-13"; got != want {
		t.Fatalf("assign caller pane = %q, want %q", got, want)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestHelpTextOmitsDeprecatedLeadPaneFromStartUsage(t *testing.T) {
	t.Parallel()

	usage, ok := HelpText([]string{"help", "start"})
	if !ok {
		t.Fatal("HelpText(help start) = not handled, want handled")
	}
	if strings.Contains(usage, "--lead-pane") {
		t.Fatalf("start help unexpectedly mentions deprecated --lead-pane: %q", usage)
	}
}

type subcommandHelpCase struct {
	command   string
	wantUsage string
}

func allSubcommandHelpCases() []subcommandHelpCase {
	return []subcommandHelpCase{
		{command: "start", wantUsage: "usage: orca start"},
		{command: "stop", wantUsage: "usage: orca stop"},
		{command: "reload", wantUsage: "usage: orca reload"},
		{command: "status", wantUsage: "usage: orca status"},
		{command: "assign", wantUsage: "usage: orca assign ISSUE"},
		{command: "batch", wantUsage: "usage: orca batch MANIFEST"},
		{command: "spawn", wantUsage: "usage: orca spawn"},
		{command: "enqueue", wantUsage: "usage: orca enqueue PR_NUMBER"},
		{command: "cancel", wantUsage: "usage: orca cancel ISSUE"},
		{command: "resume", wantUsage: "usage: orca resume ISSUE"},
		{command: "workers", wantUsage: "usage: orca workers"},
		{command: "pool", wantUsage: "usage: orca pool"},
		{command: "events", wantUsage: "usage: orca events"},
		{command: "help", wantUsage: "usage: orca help [COMMAND]"},
		{command: "version", wantUsage: "usage: orca version"},
	}
}

func TestAppRunHelpTopicsForAllSubcommands(t *testing.T) {
	t.Parallel()

	for _, tt := range allSubcommandHelpCases() {
		tt := tt
		t.Run(tt.command, func(t *testing.T) {
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
					return newRepoRoot(t), nil
				},
			})

			if err := app.Run(context.Background(), []string{"help", tt.command}); err != nil {
				t.Fatalf("Run(help %s) error = %v", tt.command, err)
			}
			if !strings.Contains(stdout.String(), tt.wantUsage) {
				t.Fatalf("stdout = %q, want substring %q", stdout.String(), tt.wantUsage)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestAppRunHelpFlagsForAllSubcommands(t *testing.T) {
	t.Parallel()

	for _, tt := range allSubcommandHelpCases() {
		tt := tt
		t.Run(tt.command, func(t *testing.T) {
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
					return newRepoRoot(t), nil
				},
			})

			if err := app.Run(context.Background(), []string{tt.command, "--help"}); err != nil {
				t.Fatalf("Run(%s --help) error = %v", tt.command, err)
			}
			if !strings.Contains(stdout.String(), tt.wantUsage) {
				t.Fatalf("stdout = %q, want substring %q", stdout.String(), tt.wantUsage)
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
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
		{name: "help unknown command", args: []string{"help", "bogus"}, wantErr: "unknown help topic \"bogus\""},
		{name: "unknown command help flag", args: []string{"bogus", "--help"}, wantErr: "unknown command"},
		{name: "stop help unknown flag", args: []string{"stop", "--bogus"}, wantErr: "flag provided but not defined"},
		{name: "status too many args", args: []string{"status", "LAB-690", "extra"}, wantErr: "status accepts at most one issue"},
		{name: "assign missing issue", args: []string{"assign", "--prompt", "x"}, wantErr: "assign requires ISSUE"},
		{name: "assign missing prompt", args: []string{"assign", "LAB-690"}, wantErr: "assign requires --prompt"},
		{name: "batch missing manifest", args: []string{"batch"}, wantErr: "batch requires MANIFEST"},
		{name: "spawn extra arg", args: []string{"spawn", "extra"}, wantErr: "spawn does not accept positional arguments"},
		{name: "enqueue missing pr number", args: []string{"enqueue"}, wantErr: "enqueue requires PR_NUMBER"},
		{name: "cancel missing issue", args: []string{"cancel"}, wantErr: "cancel requires ISSUE"},
		{name: "resume missing issue", args: []string{"resume"}, wantErr: "resume requires ISSUE"},
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
					return newRepoRoot(t), nil
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

func TestAppRunOutputModes(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 15, 4, 5, 0, time.UTC)
	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	tests := []struct {
		name    string
		args    []string
		daemon  *fakeDaemon
		state   *fakeState
		wantErr string
		assert  func(t *testing.T, stdout string, d *fakeDaemon, s *fakeState)
	}{
		{
			name: "stop json",
			args: []string{"stop", "--json"},
			daemon: &fakeDaemon{
				stopResult: daemon.StopResult{Project: repoRoot, PID: 321, StoppedAt: now},
			},
			state: &fakeState{},
			assert: func(t *testing.T, stdout string, d *fakeDaemon, _ *fakeState) {
				t.Helper()
				if d.stopRequest == nil {
					t.Fatal("expected stop to be called")
				}
				if !strings.Contains(stdout, "\"pid\":321") {
					t.Fatalf("stdout = %q, want stop json", stdout)
				}
			},
		},
		{
			name:   "status issue human",
			args:   []string{"status", "LAB-690"},
			daemon: &fakeDaemon{},
			state: &fakeState{
				taskStatus: state.TaskStatus{
					Task: state.Task{
						Issue:     "LAB-690",
						Status:    "queued",
						Agent:     "codex",
						WorkerID:  "pane-7",
						ClonePath: "/clones/orca01",
						UpdatedAt: now,
					},
				},
			},
			assert: func(t *testing.T, stdout string, _ *fakeDaemon, s *fakeState) {
				t.Helper()
				if got, want := s.taskStatusIssue, "LAB-690"; got != want {
					t.Fatalf("task status issue = %q, want %q", got, want)
				}
				if !strings.Contains(stdout, "issue: LAB-690") || !strings.Contains(stdout, "agent: codex") {
					t.Fatalf("stdout = %q, want task status details", stdout)
				}
			},
		},
		{
			name:   "workers human",
			args:   []string{"workers"},
			daemon: &fakeDaemon{},
			state: &fakeState{
				workers: []state.Worker{
					{WorkerID: "worker-03", CurrentPaneID: "pane-3", Agent: "codex", State: "healthy", Issue: "LAB-690", ClonePath: "/clones/orca01", CreatedAt: now, LastSeenAt: now},
				},
			},
			assert: func(t *testing.T, stdout string, _ *fakeDaemon, s *fakeState) {
				t.Helper()
				if got, want := s.workersProject, repoRoot; got != want {
					t.Fatalf("workers project = %q, want %q", got, want)
				}
				if !strings.Contains(stdout, "pane-3") || !strings.Contains(stdout, "LAB-690") {
					t.Fatalf("stdout = %q, want worker table", stdout)
				}
			},
		},
		{
			name:   "pool json",
			args:   []string{"pool", "--json"},
			daemon: &fakeDaemon{},
			state: &fakeState{
				clones: []state.Clone{
					{Path: "/clones/orca01", Status: "free", Branch: "main", UpdatedAt: now},
				},
			},
			assert: func(t *testing.T, stdout string, _ *fakeDaemon, s *fakeState) {
				t.Helper()
				if got, want := s.clonesProject, repoRoot; got != want {
					t.Fatalf("clones project = %q, want %q", got, want)
				}
				if !strings.Contains(stdout, "\"path\":\"/clones/orca01\"") {
					t.Fatalf("stdout = %q, want clone json", stdout)
				}
			},
		},
		{
			name:    "events extra arg",
			args:    []string{"events", "extra"},
			daemon:  &fakeDaemon{},
			state:   &fakeState{},
			wantErr: "events does not accept positional arguments",
			assert: func(t *testing.T, stdout string, _ *fakeDaemon, _ *fakeState) {
				t.Helper()
				_ = stdout
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			app := New(Options{
				Daemon:  tt.daemon,
				State:   tt.state,
				Stdout:  &stdout,
				Stderr:  &stderr,
				Version: "build-123",
				Cwd: func() (string, error) {
					return cwdPath, nil
				},
			})

			err := app.Run(context.Background(), tt.args)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Run() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			tt.assert(t, stdout.String(), tt.daemon, tt.state)
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestAppRunHelp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		args            []string
		wantInStdout    []string
		wantEmptyStderr bool
	}{
		{
			name:            "root long flag",
			args:            []string{"--help"},
			wantInStdout:    []string{"usage: orca <command>", "commands:"},
			wantEmptyStderr: true,
		},
		{
			name:            "root short flag",
			args:            []string{"-h"},
			wantInStdout:    []string{"usage: orca <command>", "commands:"},
			wantEmptyStderr: true,
		},
		{
			name:            "subcommand long flag",
			args:            []string{"assign", "--help"},
			wantInStdout:    []string{"usage: orca assign ISSUE", "--prompt"},
			wantEmptyStderr: true,
		},
		{
			name:            "subcommand short flag",
			args:            []string{"assign", "-h"},
			wantInStdout:    []string{"usage: orca assign ISSUE", "--prompt"},
			wantEmptyStderr: true,
		},
		{
			name:            "help command",
			args:            []string{"help", "assign"},
			wantInStdout:    []string{"usage: orca assign ISSUE", "--prompt"},
			wantEmptyStderr: true,
		},
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
					return newRepoRoot(t), nil
				},
			})

			if err := app.Run(context.Background(), tt.args); err != nil {
				t.Fatalf("Run(%q) error = %v", tt.args, err)
			}

			for _, want := range tt.wantInStdout {
				if !strings.Contains(stdout.String(), want) {
					t.Fatalf("stdout = %q, want substring %q", stdout.String(), want)
				}
			}
			if tt.wantEmptyStderr && stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestAppCommandsAcceptProjectFlag(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	targetRepo := newRepoRoot(t)
	targetSubdir := filepath.Join(targetRepo, "cmd", "orca")
	if err := os.MkdirAll(targetSubdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", targetSubdir, err)
	}

	tests := []struct {
		name   string
		args   []string
		assert func(t *testing.T, d *fakeDaemon, s *fakeState)
	}{
		{
			name: "stop",
			args: []string{"stop", "--project", targetSubdir},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState) {
				if d.stopRequest == nil || d.stopRequest.Project != targetRepo {
					t.Fatalf("stop project = %#v, want %q", d.stopRequest, targetRepo)
				}
			},
		},
		{
			name: "status",
			args: []string{"status", "--project", targetSubdir},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState) {
				if s.projectStatusProject != targetRepo {
					t.Fatalf("status project = %q, want %q", s.projectStatusProject, targetRepo)
				}
			},
		},
		{
			name: "assign",
			args: []string{"assign", "LAB-690", "--project", targetSubdir, "--prompt", "Implement CLI wiring"},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState) {
				if d.assignRequest == nil || d.assignRequest.Project != targetRepo {
					t.Fatalf("assign project = %#v, want %q", d.assignRequest, targetRepo)
				}
			},
		},
		{
			name: "batch",
			args: []string{"batch", "--project", targetSubdir, filepath.Join(targetRepo, "tasks.json")},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState) {
				if d.batchRequest == nil || d.batchRequest.Project != targetRepo {
					t.Fatalf("batch project = %#v, want %q", d.batchRequest, targetRepo)
				}
			},
		},
		{
			name: "spawn",
			args: []string{"spawn", "--project", targetSubdir},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState) {
				if d.spawnRequest == nil || d.spawnRequest.Project != targetRepo {
					t.Fatalf("spawn project = %#v, want %q", d.spawnRequest, targetRepo)
				}
			},
		},
		{
			name: "enqueue",
			args: []string{"enqueue", "42", "--project", targetSubdir},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState) {
				if d.enqueueRequest == nil || d.enqueueRequest.Project != targetRepo {
					t.Fatalf("enqueue project = %#v, want %q", d.enqueueRequest, targetRepo)
				}
			},
		},
		{
			name: "cancel",
			args: []string{"cancel", "LAB-690", "--project", targetSubdir},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState) {
				if d.cancelRequest == nil || d.cancelRequest.Project != targetRepo {
					t.Fatalf("cancel project = %#v, want %q", d.cancelRequest, targetRepo)
				}
			},
		},
		{
			name: "resume",
			args: []string{"resume", "LAB-690", "--project", targetSubdir},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState) {
				if d.resumeRequest == nil || d.resumeRequest.Project != targetRepo {
					t.Fatalf("resume project = %#v, want %q", d.resumeRequest, targetRepo)
				}
			},
		},
		{
			name: "workers",
			args: []string{"workers", "--project", targetSubdir},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState) {
				if s.workersProject != targetRepo {
					t.Fatalf("workers project = %q, want %q", s.workersProject, targetRepo)
				}
			},
		},
		{
			name: "pool",
			args: []string{"pool", "--project", targetSubdir},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState) {
				if s.clonesProject != targetRepo {
					t.Fatalf("pool project = %q, want %q", s.clonesProject, targetRepo)
				}
			},
		},
		{
			name: "events",
			args: []string{"events", "--project", targetSubdir},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState) {
				if s.eventsProject != targetRepo {
					t.Fatalf("events project = %q, want %q", s.eventsProject, targetRepo)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := &fakeDaemon{
				stopResult:    daemon.StopResult{Project: targetRepo, PID: 1, StoppedAt: time.Now().UTC()},
				assignResult:  daemon.TaskActionResult{Project: targetRepo, Issue: "LAB-690", Status: "queued", UpdatedAt: time.Now().UTC()},
				batchResult:   daemon.BatchResult{Project: targetRepo, Results: []daemon.TaskActionResult{{Project: targetRepo, Issue: "LAB-690", Status: "queued", UpdatedAt: time.Now().UTC()}}},
				enqueueResult: daemon.MergeQueueActionResult{Project: targetRepo, PRNumber: 42, Status: "queued", Position: 1, UpdatedAt: time.Now().UTC()},
				cancelResult:  daemon.TaskActionResult{Project: targetRepo, Issue: "LAB-690", Status: "cancelled", UpdatedAt: time.Now().UTC()},
				resumeResult:  daemon.TaskActionResult{Project: targetRepo, Issue: "LAB-690", Status: "active", UpdatedAt: time.Now().UTC()},
			}
			s := &fakeState{
				projectStatus: state.ProjectStatus{Project: targetRepo},
				taskStatus:    state.TaskStatus{Task: state.Task{Issue: "LAB-690"}},
			}

			app := New(Options{
				Daemon:  d,
				State:   s,
				Stdout:  &bytes.Buffer{},
				Stderr:  &bytes.Buffer{},
				Version: "build-123",
				Cwd: func() (string, error) {
					return cwdPath, nil
				},
			})

			if tt.name == "batch" {
				manifestPath := filepath.Join(targetRepo, "tasks.json")
				if err := os.WriteFile(manifestPath, []byte(`[{"issue":"LAB-690","agent":"claude","prompt":"Implement CLI wiring"}]`), 0o644); err != nil {
					t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
				}
			}

			if err := app.Run(context.Background(), tt.args); err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			tt.assert(t, d, s)
		})
	}
}

func TestWriteProjectStatusAndTaskStatus(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 15, 4, 5, 0, time.UTC)

	tests := []struct {
		name  string
		write func(*bytes.Buffer) error
		wants []string
	}{
		{
			name: "project status with daemon and tasks",
			write: func(buf *bytes.Buffer) error {
				return writeProjectStatusAt(buf, state.ProjectStatus{
					Project: "repo",
					Daemon: &state.DaemonStatus{
						Session:   "alpha",
						PID:       42,
						Status:    "running",
						UpdatedAt: now.Add(-30 * time.Second),
					},
					Summary: state.Summary{
						Tasks:          2,
						Queued:         1,
						Active:         1,
						Workers:        1,
						HealthyWorkers: 1,
						Clones:         2,
						FreeClones:     1,
					},
					Tasks: []state.Task{
						{
							Issue:     "LAB-804",
							Status:    "active",
							Agent:     "codex",
							WorkerID:  "pane-7",
							ClonePath: "/clones/orca01",
							UpdatedAt: now,
						},
					},
				}, "", "", now)
			},
			wants: []string{
				"project: repo",
				"daemon: running",
				"session: alpha",
				"pid: 42",
				"heartbeat age: 30s",
				"tasks: 2 total, 1 queued, 1 active, 0 done, 0 cancelled",
				"ISSUE",
				"LAB-804",
			},
		},
		{
			name: "project status without daemon",
			write: func(buf *bytes.Buffer) error {
				return writeProjectStatus(buf, state.ProjectStatus{
					Project: "repo",
				}, "", "")
			},
			wants: []string{
				"project: repo",
				"daemon: stopped",
				"tasks: 0 total, 0 queued, 0 active, 0 done, 0 cancelled",
			},
		},
		{
			name: "project status includes heartbeat age",
			write: func(buf *bytes.Buffer) error {
				return writeProjectStatusAt(buf, state.ProjectStatus{
					Project: "repo",
					Daemon: &state.DaemonStatus{
						Session:   "alpha",
						PID:       42,
						Status:    "unhealthy",
						UpdatedAt: now.Add(-2 * time.Minute),
					},
				}, "", "", now)
			},
			wants: []string{
				"project: repo",
				"daemon: unhealthy",
				"heartbeat age: 2m0s",
			},
		},
		{
			name: "project status with daemon but no explicit status",
			write: func(buf *bytes.Buffer) error {
				return writeProjectStatus(buf, state.ProjectStatus{
					Project: "repo",
					Daemon: &state.DaemonStatus{
						Session: "alpha",
					},
					Tasks: []state.Task{
						{Issue: "LAB-805"},
					},
				}, "", "")
			},
			wants: []string{
				"project: repo",
				"daemon: stopped",
				"session: alpha",
				"LAB-805",
				"-",
			},
		},
		{
			name: "task status with events",
			write: func(buf *bytes.Buffer) error {
				return writeTaskStatus(buf, state.TaskStatus{
					Task: state.Task{
						Issue:     "LAB-804",
						Status:    "queued",
						WorkerID:  "pane-7",
						ClonePath: "/clones/orca01",
						UpdatedAt: now,
					},
					Events: []state.Event{
						{Kind: "task.assigned", Message: "queued", CreatedAt: now},
					},
				})
			},
			wants: []string{
				"issue: LAB-804",
				"agent: -",
				"worker: pane-7",
				"clone: /clones/orca01",
				"events:",
				"task.assigned",
				"queued",
			},
		},
		{
			name: "task status shows github rate limit warning",
			write: func(buf *bytes.Buffer) error {
				return writeTaskStatus(buf, state.TaskStatus{
					Task: state.Task{
						Issue:     "LAB-804",
						Status:    "active",
						UpdatedAt: now,
					},
					Events: []state.Event{
						{
							Kind:    "pr.rate_limited",
							Message: "github: rate limited until 09:02",
							Payload: []byte(`{"github_rate_limited_until":"2099-04-02T09:02:00Z"}`),
						},
					},
				})
			},
			wants: []string{
				"issue: LAB-804",
				"\ngithub: rate limited until 09:02\n",
			},
		},
		{
			name: "task status without events",
			write: func(buf *bytes.Buffer) error {
				return writeTaskStatus(buf, state.TaskStatus{
					Task: state.Task{
						Issue:  "LAB-806",
						Status: "done",
					},
				})
			},
			wants: []string{
				"issue: LAB-806",
				"status: done",
				"agent: -",
				"worker: -",
				"clone: -",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			if err := tt.write(&buf); err != nil {
				t.Fatalf("write error = %v", err)
			}
			for _, want := range tt.wants {
				if !strings.Contains(buf.String(), want) {
					t.Fatalf("output = %q, want substring %q", buf.String(), want)
				}
			}
		})
	}
}

func TestHeartbeatAge(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 15, 4, 5, 0, time.UTC)

	tests := []struct {
		name        string
		heartbeatAt time.Time
		want        string
	}{
		{
			name:        "returns elapsed duration",
			heartbeatAt: now.Add(-90 * time.Second),
			want:        "1m30s",
		},
		{
			name:        "clamps future heartbeats",
			heartbeatAt: now.Add(5 * time.Second),
			want:        "0s",
		},
		{
			name:        "clamps sub-second durations",
			heartbeatAt: now.Add(-500 * time.Millisecond),
			want:        "0s",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := heartbeatAge(now, tt.heartbeatAt); got != tt.want {
				t.Fatalf("heartbeatAge() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseOptionalSinglePositionalAndFormatTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		args      []string
		wantValue string
		wantErr   string
	}{
		{
			name:      "flags before positional",
			args:      []string{"--json", "LAB-804"},
			wantValue: "LAB-804",
		},
		{
			name:      "no positional",
			args:      []string{"--json"},
			wantValue: "",
		},
		{
			name:    "too many positionals",
			args:    []string{"LAB-804", "extra"},
			wantErr: "status accepts at most one issue",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := newFlagSet("status")
			var jsonOutput bool
			fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

			got, err := parseOptionalSinglePositional(fs, tt.args)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseOptionalSinglePositional() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseOptionalSinglePositional() error = %v", err)
			}
			if got != tt.wantValue {
				t.Fatalf("parseOptionalSinglePositional() = %q, want %q", got, tt.wantValue)
			}
		})
	}

	if got, want := formatTimestamp(time.Time{}), "-"; got != want {
		t.Fatalf("formatTimestamp(zero) = %q, want %q", got, want)
	}
	if got, want := formatTimestamp(time.Date(2026, 4, 2, 15, 4, 5, 0, time.FixedZone("UTC-4", -4*60*60))), "2026-04-02T19:04:05Z"; got != want {
		t.Fatalf("formatTimestamp(non-zero) = %q, want %q", got, want)
	}
}

func TestWriteClonesAndRunEventsError(t *testing.T) {
	t.Parallel()

	t.Run("write clones empty", func(t *testing.T) {
		t.Parallel()

		var buf bytes.Buffer
		if err := writeClones(&buf, nil); err != nil {
			t.Fatalf("writeClones() error = %v", err)
		}
		if got, want := strings.TrimSpace(buf.String()), "no clones"; got != want {
			t.Fatalf("writeClones() = %q, want %q", got, want)
		}
	})

	t.Run("events returns state error", func(t *testing.T) {
		t.Parallel()

		repoRoot := newRepoRoot(t)
		cwdPath := filepath.Join(repoRoot, "internal", "cli")
		if err := os.MkdirAll(cwdPath, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
		}

		var stdout bytes.Buffer
		var stderr bytes.Buffer
		app := New(Options{
			Daemon:  &fakeDaemon{},
			State:   &fakeState{err: errors.New("events failed")},
			Stdout:  &stdout,
			Stderr:  &stderr,
			Version: "build-123",
			Cwd: func() (string, error) {
				return cwdPath, nil
			},
		})

		err := app.Run(context.Background(), []string{"events"})
		if err == nil || !strings.Contains(err.Error(), "events failed") {
			t.Fatalf("Run(events) error = %v, want events failure", err)
		}
	})
}

func newRepoRoot(t *testing.T) string {
	t.Helper()

	root := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.git): %v", err)
	}

	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", root, err)
	}
	return resolvedRoot
}

type fakeDaemon struct {
	startRequest   *daemon.StartRequest
	stopRequest    *daemon.StopRequest
	reloadRequest  *daemon.ReloadRequest
	assignRequest  *daemon.AssignRequest
	batchRequest   *daemon.BatchRequest
	spawnRequest   *daemon.SpawnPaneRequest
	enqueueRequest *daemon.EnqueueRequest
	cancelRequest  *daemon.CancelRequest
	resumeRequest  *daemon.ResumeRequest

	startResult   daemon.StartResult
	stopResult    daemon.StopResult
	reloadResult  daemon.ReloadResult
	assignResult  daemon.TaskActionResult
	batchResult   daemon.BatchResult
	spawnResult   daemon.SpawnPaneResult
	enqueueResult daemon.MergeQueueActionResult
	cancelResult  daemon.TaskActionResult
	resumeResult  daemon.TaskActionResult

	batchHook  func(context.Context, daemon.BatchRequest) (daemon.BatchResult, error)
	cancelHook func(context.Context, daemon.CancelRequest) (daemon.TaskActionResult, error)
	err        error
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

func (f *fakeDaemon) Reload(_ context.Context, req daemon.ReloadRequest) (daemon.ReloadResult, error) {
	if f.err != nil {
		return daemon.ReloadResult{}, f.err
	}
	f.reloadRequest = &req
	return f.reloadResult, nil
}

func (f *fakeDaemon) Assign(_ context.Context, req daemon.AssignRequest) (daemon.TaskActionResult, error) {
	if f.err != nil {
		return daemon.TaskActionResult{}, f.err
	}
	f.assignRequest = &req
	return f.assignResult, nil
}

func (f *fakeDaemon) Batch(ctx context.Context, req daemon.BatchRequest) (daemon.BatchResult, error) {
	if f.err != nil {
		return daemon.BatchResult{}, f.err
	}
	f.batchRequest = &req
	if f.batchHook != nil {
		return f.batchHook(ctx, req)
	}
	return f.batchResult, nil
}

func (f *fakeDaemon) Spawn(_ context.Context, req daemon.SpawnPaneRequest) (daemon.SpawnPaneResult, error) {
	if f.err != nil {
		return daemon.SpawnPaneResult{}, f.err
	}
	f.spawnRequest = &req
	return f.spawnResult, nil
}

func (f *fakeDaemon) Enqueue(_ context.Context, req daemon.EnqueueRequest) (daemon.MergeQueueActionResult, error) {
	if f.err != nil {
		return daemon.MergeQueueActionResult{}, f.err
	}
	f.enqueueRequest = &req
	return f.enqueueResult, nil
}

func (f *fakeDaemon) Cancel(ctx context.Context, req daemon.CancelRequest) (daemon.TaskActionResult, error) {
	if f.err != nil {
		return daemon.TaskActionResult{}, f.err
	}
	f.cancelRequest = &req
	if f.cancelHook != nil {
		return f.cancelHook(ctx, req)
	}
	return f.cancelResult, nil
}

func (f *fakeDaemon) Resume(_ context.Context, req daemon.ResumeRequest) (daemon.TaskActionResult, error) {
	if f.err != nil {
		return daemon.TaskActionResult{}, f.err
	}
	f.resumeRequest = &req
	return f.resumeResult, nil
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

func TestUsageTextIncludesResumeCommand(t *testing.T) {
	t.Parallel()

	if !strings.Contains(UsageText(), "resume") {
		t.Fatalf("UsageText() = %q, want resume command", UsageText())
	}
	if !strings.Contains(UsageText(), "spawn") {
		t.Fatalf("UsageText() = %q, want spawn command", UsageText())
	}
	if !strings.Contains(UsageText(), "batch") {
		t.Fatalf("UsageText() = %q, want batch command", UsageText())
	}
	if !strings.Contains(UsageText(), "help") {
		t.Fatalf("UsageText() = %q, want help command", UsageText())
	}
}

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
