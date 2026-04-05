package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
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
				if d.assignRequest.Agent != "claude" {
					t.Fatalf("expected default agent claude, got %q", d.assignRequest.Agent)
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
			name: "assign title flag",
			args: func(_, _ string) []string {
				return []string{"assign", "LAB-690", "--prompt", "Implement CLI wiring", "--title", "Worker pane title"}
			},
			assert: func(t *testing.T, d *fakeDaemon, _ *fakeState, stdout, _ string, _, _ string) {
				t.Helper()
				if d.assignRequest == nil {
					t.Fatal("expected assign to be called")
				}
				field := reflect.ValueOf(*d.assignRequest).FieldByName("Title")
				if !field.IsValid() {
					t.Fatal("AssignRequest missing Title field")
				}
				if got, want := field.String(), "Worker pane title"; got != want {
					t.Fatalf("assign title = %q, want %q", got, want)
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
			name: "workers json",
			args: func(_, _ string) []string { return []string{"workers", "--json"} },
			prepareState: func(s *fakeState) {
				s.workers = []state.Worker{
					{PaneID: "pane-3", Agent: "codex", State: "healthy", Issue: "LAB-690", ClonePath: "/clones/orca01", UpdatedAt: now},
				}
			},
			assert: func(t *testing.T, _ *fakeDaemon, s *fakeState, stdout, _ string, repoRoot, _ string) {
				t.Helper()
				if s.workersProject != repoRoot {
					t.Fatalf("expected workers lookup for %q, got %q", repoRoot, s.workersProject)
				}
				if !strings.Contains(stdout, "\"pane_id\":\"pane-3\"") {
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
			})

			if err := app.Run(context.Background(), tt.args(repoRoot, otherRepo)); err != nil {
				t.Fatalf("Run() error = %v", err)
			}

			tt.assert(t, d, s, stdout.String(), stderr.String(), repoRoot, otherRepo)
		})
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
	assignRequest  *daemon.AssignRequest
	batchRequest   *daemon.BatchRequest
	spawnRequest   *daemon.SpawnPaneRequest
	enqueueRequest *daemon.EnqueueRequest
	cancelRequest  *daemon.CancelRequest
	resumeRequest  *daemon.ResumeRequest

	startResult   daemon.StartResult
	stopResult    daemon.StopResult
	assignResult  daemon.TaskActionResult
	batchResult   daemon.BatchResult
	spawnResult   daemon.SpawnPaneResult
	enqueueResult daemon.MergeQueueActionResult
	cancelResult  daemon.TaskActionResult
	resumeResult  daemon.TaskActionResult

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

func (f *fakeDaemon) Batch(_ context.Context, req daemon.BatchRequest) (daemon.BatchResult, error) {
	if f.err != nil {
		return daemon.BatchResult{}, f.err
	}
	f.batchRequest = &req
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

func (f *fakeDaemon) Cancel(_ context.Context, req daemon.CancelRequest) (daemon.TaskActionResult, error) {
	if f.err != nil {
		return daemon.TaskActionResult{}, f.err
	}
	f.cancelRequest = &req
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
