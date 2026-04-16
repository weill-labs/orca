package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/cli"
	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestVersionOutput(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestVersionHelper")
	cmd.Env = append(os.Environ(), "ORCA_TEST_HELPER=version")
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(out), "orca ") {
		t.Errorf("expected 'orca ...' prefix, got %q", string(out))
	}
}

func TestVersionHelper(t *testing.T) {
	if os.Getenv("ORCA_TEST_HELPER") != "version" {
		return
	}
	os.Args = []string{"orca", "version"}
	main()
}

func TestUsageExitsNonZero(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=TestUsageHelper")
	cmd.Env = append(os.Environ(), "ORCA_TEST_HELPER=usage")
	err := cmd.Run()
	if err == nil {
		t.Fatal("expected non-zero exit")
	}
}

func TestUsageHelper(t *testing.T) {
	if os.Getenv("ORCA_TEST_HELPER") != "usage" {
		return
	}
	os.Args = []string{"orca"}
	main()
}

func TestRunHelpFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
	}{
		{name: "long flag", args: []string{"--help"}},
		{name: "short flag", args: []string{"-h"}},
		{name: "help command", args: []string{"help"}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			var stderr bytes.Buffer

			exitCode := run(tt.args, &stdout, &stderr)
			if exitCode != 0 {
				t.Fatalf("run(%q) exit code = %d, want 0", tt.args, exitCode)
			}
			if !strings.Contains(stdout.String(), "usage: orca <command>") {
				t.Fatalf("stdout = %q, want root usage", stdout.String())
			}
			if stderr.Len() != 0 {
				t.Fatalf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

func TestRunVersionAndUnknownHelpTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		args            []string
		wantExitCode    int
		wantStdout      string
		wantStderr      string
		wantBuildCommit string
	}{
		{
			name:            "version command",
			args:            []string{"version"},
			wantExitCode:    0,
			wantStdout:      "orca build-804\n",
			wantStderr:      "",
			wantBuildCommit: "build-804",
		},
		{
			name:         "unknown help topic",
			args:         []string{"help", "bogus"},
			wantExitCode: 1,
			wantStdout:   "",
			wantStderr:   "unknown help topic \"bogus\"\n",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var stdout bytes.Buffer
			var stderr bytes.Buffer

			exitCode := runWithDeps(tt.args, &stdout, &stderr, defaultRunDependencies, tt.wantBuildCommit)
			if got, want := exitCode, tt.wantExitCode; got != want {
				t.Fatalf("run(%q) exit code = %d, want %d", tt.args, got, want)
			}
			if got, want := stdout.String(), tt.wantStdout; got != want {
				t.Fatalf("stdout = %q, want %q", got, want)
			}
			if got, want := stderr.String(), tt.wantStderr; got != want {
				t.Fatalf("stderr = %q, want %q", got, want)
			}
		})
	}
}

func TestRunDaemonProcessValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "flag parse error",
			args:    []string{"--unknown"},
			wantErr: "flag provided but not defined",
		},
		{
			name:    "missing state db",
			args:    []string{"--pid-file", "/tmp/orca.pid"},
			wantErr: "__daemon-serve requires --state-db",
		},
		{
			name:    "missing pid file",
			args:    []string{"--state-db", "/tmp/orca.db"},
			wantErr: "__daemon-serve requires --pid-file",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := runDaemonProcess(tt.args, "")
			if err == nil {
				t.Fatal("runDaemonProcess() error = nil, want non-nil")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("runDaemonProcess() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestRunDaemonProcessPassesBuildCommit(t *testing.T) {
	t.Parallel()

	var gotRequest daemon.ServeRequest
	runDaemonServe := func(_ context.Context, req daemon.ServeRequest) error {
		gotRequest = req
		return nil
	}

	err := runDaemonProcessWithServe([]string{
		"--session", "alpha",
		"--state-db", "/tmp/orca.db",
		"--pid-file", "/tmp/orca.pid",
	}, "build-851", runDaemonServe)
	if err != nil {
		t.Fatalf("runDaemonProcess() error = %v", err)
	}
	if got, want := gotRequest.BuildCommit, "build-851"; got != want {
		t.Fatalf("ServeRequest.BuildCommit = %q, want %q", got, want)
	}
	if got, want := gotRequest.Session, "alpha"; got != want {
		t.Fatalf("ServeRequest.Session = %q, want %q", got, want)
	}
}

func TestRunDaemonProcessDefaultsBuildCommitToDev(t *testing.T) {
	t.Parallel()

	var gotRequest daemon.ServeRequest
	runDaemonServe := func(_ context.Context, req daemon.ServeRequest) error {
		gotRequest = req
		return nil
	}

	err := runDaemonProcessWithServe([]string{
		"--state-db", "/tmp/orca.db",
		"--pid-file", "/tmp/orca.pid",
	}, "", runDaemonServe)
	if err != nil {
		t.Fatalf("runDaemonProcess() error = %v", err)
	}
	if got, want := gotRequest.BuildCommit, "dev"; got != want {
		t.Fatalf("ServeRequest.BuildCommit = %q, want %q", got, want)
	}
}

func TestRunDaemonProcessRejectsLegacyProjectFlag(t *testing.T) {
	t.Parallel()

	err := runDaemonProcess([]string{"--project", "/tmp/project", "--state-db", "/tmp/orca.db", "--pid-file", "/tmp/orca.pid"}, "")
	if err == nil {
		t.Fatal("runDaemonProcess() error = nil, want parse error")
	}
	if !strings.Contains(err.Error(), "flag provided but not defined") {
		t.Fatalf("runDaemonProcess() error = %v, want parse error for legacy --project", err)
	}
}

func TestOpenDefaultStateStoreUsesSQLiteByDefault(t *testing.T) {
	t.Setenv("ORCA_STATE_DSN", "")

	originalSQLite := openSQLiteStateStore
	originalPostgres := openPostgresStateStore
	t.Cleanup(func() {
		openSQLiteStateStore = originalSQLite
		openPostgresStateStore = originalPostgres
	})

	var gotPath string
	openSQLiteStateStore = func(path string) (stateStore, error) {
		gotPath = path
		return &stubStateStore{}, nil
	}
	openPostgresStateStore = func(string) (stateStore, error) {
		t.Fatal("openPostgresStateStore should not be called when ORCA_STATE_DSN is unset")
		return nil, nil
	}

	store, err := openDefaultStateStore("/tmp/orca.db")
	if err != nil {
		t.Fatalf("openDefaultStateStore() error = %v", err)
	}
	if got, want := gotPath, "/tmp/orca.db"; got != want {
		t.Fatalf("sqlite path = %q, want %q", got, want)
	}
	if store == nil {
		t.Fatal("openDefaultStateStore() returned nil store")
	}
}

func TestOpenDefaultStateStoreUsesPostgresWhenConfigured(t *testing.T) {
	t.Setenv("ORCA_STATE_DSN", "postgres://orca:orca@localhost:5432/orca?sslmode=disable")

	originalSQLite := openSQLiteStateStore
	originalPostgres := openPostgresStateStore
	t.Cleanup(func() {
		openSQLiteStateStore = originalSQLite
		openPostgresStateStore = originalPostgres
	})

	var gotDSN string
	openSQLiteStateStore = func(string) (stateStore, error) {
		t.Fatal("openSQLiteStateStore should not be called when ORCA_STATE_DSN is set")
		return nil, nil
	}
	openPostgresStateStore = func(dsn string) (stateStore, error) {
		gotDSN = dsn
		return &stubStateStore{}, nil
	}

	store, err := openDefaultStateStore("/tmp/orca.db")
	if err != nil {
		t.Fatalf("openDefaultStateStore() error = %v", err)
	}
	if got, want := gotDSN, "postgres://orca:orca@localhost:5432/orca?sslmode=disable"; got != want {
		t.Fatalf("postgres dsn = %q, want %q", got, want)
	}
	if store == nil {
		t.Fatal("openDefaultStateStore() returned nil store")
	}
}

func TestRunWithDepsCoversProcessSetupBranches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		args         []string
		deps         runDependencies
		wantExitCode int
		wantStdout   string
		wantStderr   string
		assert       func(t *testing.T, store *stubStateStore, app *stubAppRunner)
	}{
		{
			name: "daemon serve dispatches without requiring project flag",
			args: []string{"__daemon-serve", "--state-db", "/tmp/orca.db", "--pid-file", "/tmp/orca.pid"},
			deps: runDependencies{
				runDaemonProcess: func(args []string, buildCommit string) error {
					if got, want := strings.Join(args, " "), "--state-db /tmp/orca.db --pid-file /tmp/orca.pid"; got != want {
						t.Fatalf("daemon args = %q, want %q", got, want)
					}
					if got, want := buildCommit, "dev"; got != want {
						t.Fatalf("build commit = %q, want %q", got, want)
					}
					return nil
				},
			},
			wantExitCode: 0,
		},
		{
			name: "migrate state bypasses default state bootstrap",
			args: []string{"migrate-state", "--from", "sqlite:///tmp/source.db", "--to", "postgres://orca:secret@localhost:5432/orca?sslmode=disable", "--dry-run"},
			deps: runDependencies{
				openStateStore: func(string) (stateStore, error) {
					t.Fatal("openStateStore should not be called for migrate-state")
					return nil, nil
				},
				newController: func(daemon.ControllerOptions) (daemon.Controller, error) {
					t.Fatal("newController should not be called for migrate-state")
					return nil, nil
				},
				newApp: func(cli.Options) appRunner {
					t.Fatal("newApp should not be called for migrate-state")
					return nil
				},
				runMigrateState: func(_ context.Context, w io.Writer, args []string) error {
					if got, want := strings.Join(args, " "), "--from sqlite:///tmp/source.db --to postgres://orca:secret@localhost:5432/orca?sslmode=disable --dry-run"; got != want {
						t.Fatalf("migrate-state args = %q, want %q", got, want)
					}
					_, err := io.WriteString(w, "migration complete\n")
					return err
				},
			},
			wantExitCode: 0,
			wantStdout:   "migration complete\n",
		},
		{
			name: "resolve paths failure",
			args: []string{"status"},
			deps: runDependencies{
				resolvePaths: func() (daemon.Paths, error) {
					return daemon.Paths{}, errors.New("resolve failed")
				},
			},
			wantExitCode: 1,
			wantStderr:   "resolve failed\n",
		},
		{
			name: "open state store failure",
			args: []string{"status"},
			deps: runDependencies{
				resolvePaths: func() (daemon.Paths, error) {
					return daemon.Paths{StateDB: "/tmp/orca.db"}, nil
				},
				openStateStore: func(string) (stateStore, error) {
					return nil, errors.New("open failed")
				},
			},
			wantExitCode: 1,
			wantStderr:   "open failed\n",
		},
		{
			name: "controller creation failure",
			args: []string{"status"},
			deps: runDependencies{
				resolvePaths: func() (daemon.Paths, error) {
					return daemon.Paths{StateDB: "/tmp/orca.db"}, nil
				},
				openStateStore: func(string) (stateStore, error) {
					return &stubStateStore{}, nil
				},
				newController: func(daemon.ControllerOptions) (daemon.Controller, error) {
					return nil, errors.New("controller failed")
				},
			},
			wantExitCode: 1,
			wantStderr:   "controller failed\n",
		},
		{
			name: "app run failure",
			args: []string{"status"},
			deps: runDependencies{
				resolvePaths: func() (daemon.Paths, error) {
					return daemon.Paths{StateDB: "/tmp/orca.db"}, nil
				},
				openStateStore: func(string) (stateStore, error) {
					return &stubStateStore{}, nil
				},
				newController: func(daemon.ControllerOptions) (daemon.Controller, error) {
					return &stubController{}, nil
				},
				newApp: func(cli.Options) appRunner {
					return &stubAppRunner{err: errors.New("app failed")}
				},
			},
			wantExitCode: 1,
			wantStderr:   "app failed\n",
		},
		{
			name: "app run success closes store",
			args: []string{"status"},
			deps: runDependencies{
				resolvePaths: func() (daemon.Paths, error) {
					return daemon.Paths{StateDB: "/tmp/orca.db"}, nil
				},
				newController: func(daemon.ControllerOptions) (daemon.Controller, error) {
					return &stubController{}, nil
				},
			},
			wantExitCode: 0,
			assert: func(t *testing.T, store *stubStateStore, app *stubAppRunner) {
				t.Helper()
				if !store.closed {
					t.Fatal("expected store.Close() to be called")
				}
				if got, want := app.args, []string{"status"}; strings.Join(got, " ") != strings.Join(want, " ") {
					t.Fatalf("app args = %q, want %q", got, want)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &stubStateStore{}
			app := &stubAppRunner{}
			deps := fillRunDependencies(tt.deps, store, app)

			var stdout bytes.Buffer
			var stderr bytes.Buffer
			exitCode := runWithDeps(tt.args, &stdout, &stderr, deps, "")
			if got, want := exitCode, tt.wantExitCode; got != want {
				t.Fatalf("runWithDeps(%q) exit code = %d, want %d", tt.args, got, want)
			}
			if got, want := stdout.String(), tt.wantStdout; got != want {
				t.Fatalf("stdout = %q, want %q", got, want)
			}
			if got, want := stderr.String(), tt.wantStderr; got != want {
				t.Fatalf("stderr = %q, want %q", got, want)
			}
			if tt.assert != nil {
				tt.assert(t, store, app)
			}
		})
	}
}

type stubStateStore struct {
	closed bool
}

func (s *stubStateStore) Close() error {
	s.closed = true
	return nil
}

func (s *stubStateStore) ProjectStatus(context.Context, string) (state.ProjectStatus, error) {
	return state.ProjectStatus{}, nil
}

func (s *stubStateStore) EnsureSchema(context.Context) error {
	return nil
}

func (s *stubStateStore) UpsertDaemon(context.Context, string, state.DaemonStatus) error {
	return nil
}

func (s *stubStateStore) MarkDaemonStopped(context.Context, string, time.Time) error {
	return nil
}

func (s *stubStateStore) UpsertTask(context.Context, string, state.Task) error {
	return nil
}

func (s *stubStateStore) UpdateTaskStatus(context.Context, string, string, string, time.Time) (state.Task, error) {
	return state.Task{}, nil
}

func (s *stubStateStore) UpdateTaskBranch(context.Context, string, string, string, time.Time) (state.Task, error) {
	return state.Task{}, nil
}

func (s *stubStateStore) AppendEvent(context.Context, state.Event) (state.Event, error) {
	return state.Event{}, nil
}

func (s *stubStateStore) TaskStatus(context.Context, string, string) (state.TaskStatus, error) {
	return state.TaskStatus{}, state.ErrNotFound
}

func (s *stubStateStore) ListWorkers(context.Context, string) ([]state.Worker, error) {
	return nil, nil
}

func (s *stubStateStore) ListClones(context.Context, string) ([]state.Clone, error) {
	return nil, nil
}

func (s *stubStateStore) Events(context.Context, string, int64) (<-chan state.Event, <-chan error) {
	eventsCh := make(chan state.Event)
	errCh := make(chan error)
	close(eventsCh)
	close(errCh)
	return eventsCh, errCh
}

type stubAppRunner struct {
	args []string
	err  error
}

func (a *stubAppRunner) Run(_ context.Context, args []string) error {
	a.args = append([]string(nil), args...)
	return a.err
}

type stubController struct{}

func (*stubController) Start(context.Context, daemon.StartRequest) (daemon.StartResult, error) {
	return daemon.StartResult{}, nil
}

func (*stubController) Stop(context.Context, daemon.StopRequest) (daemon.StopResult, error) {
	return daemon.StopResult{}, nil
}

func (*stubController) Reload(context.Context, daemon.ReloadRequest) (daemon.ReloadResult, error) {
	return daemon.ReloadResult{}, nil
}

func (*stubController) Assign(context.Context, daemon.AssignRequest) (daemon.TaskActionResult, error) {
	return daemon.TaskActionResult{}, nil
}

func (*stubController) Batch(context.Context, daemon.BatchRequest) (daemon.BatchResult, error) {
	return daemon.BatchResult{}, nil
}

func (*stubController) Spawn(context.Context, daemon.SpawnPaneRequest) (daemon.SpawnPaneResult, error) {
	return daemon.SpawnPaneResult{}, nil
}

func (*stubController) Enqueue(context.Context, daemon.EnqueueRequest) (daemon.MergeQueueActionResult, error) {
	return daemon.MergeQueueActionResult{}, nil
}

func (*stubController) Cancel(context.Context, daemon.CancelRequest) (daemon.TaskActionResult, error) {
	return daemon.TaskActionResult{}, nil
}

func (*stubController) Resume(context.Context, daemon.ResumeRequest) (daemon.TaskActionResult, error) {
	return daemon.TaskActionResult{}, nil
}

func fillRunDependencies(overrides runDependencies, store *stubStateStore, app *stubAppRunner) runDependencies {
	deps := runDependencies{
		resolvePaths: func() (daemon.Paths, error) {
			return daemon.Paths{StateDB: "/tmp/orca.db"}, nil
		},
		openStateStore: func(string) (stateStore, error) {
			return store, nil
		},
		newController: func(daemon.ControllerOptions) (daemon.Controller, error) {
			return &stubController{}, nil
		},
		newApp: func(cli.Options) appRunner {
			return app
		},
		runMigrateState: func(context.Context, io.Writer, []string) error {
			return nil
		},
		runDaemonProcess: func([]string, string) error {
			return nil
		},
	}

	if overrides.resolvePaths != nil {
		deps.resolvePaths = overrides.resolvePaths
	}
	if overrides.openStateStore != nil {
		deps.openStateStore = overrides.openStateStore
	}
	if overrides.newController != nil {
		deps.newController = overrides.newController
	}
	if overrides.newApp != nil {
		deps.newApp = overrides.newApp
	}
	if overrides.runMigrateState != nil {
		deps.runMigrateState = overrides.runMigrateState
	}
	if overrides.runDaemonProcess != nil {
		deps.runDaemonProcess = overrides.runDaemonProcess
	}
	return deps
}
