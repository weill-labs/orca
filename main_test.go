package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/cli"
	"github.com/weill-labs/orca/internal/config"
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

func TestRunDaemonProcessReturnsLogRedirectError(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "blocked")
	if err := os.WriteFile(blocker, []byte("file"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", blocker, err)
	}

	serveCalled := false
	runDaemonServe := func(context.Context, daemon.ServeRequest) error {
		serveCalled = true
		return nil
	}

	err := runDaemonProcessWithServe([]string{
		"--state-db", "/tmp/orca.db",
		"--pid-file", "/tmp/orca.pid",
		"--log-file", filepath.Join(blocker, "daemon.log"),
	}, "", runDaemonServe)
	if err == nil || !strings.Contains(err.Error(), "create daemon log directory") {
		t.Fatalf("runDaemonProcess() error = %v, want daemon log redirect error", err)
	}
	if serveCalled {
		t.Fatal("serve called after daemon log redirect failed")
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

func TestOpenDefaultStateStoreUsesConfiguredPostgresByDefault(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "")
	t.Setenv("ORCA_STATE_DSN", "")

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(strings.Join([]string{
		"[state]",
		`dsn = "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"`,
		"",
	}, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}

	originalSQLite := openSQLiteStateStore
	originalPostgres := openPostgresStateStore
	t.Cleanup(func() {
		openSQLiteStateStore = originalSQLite
		openPostgresStateStore = originalPostgres
	})

	var gotDSN string
	openSQLiteStateStore = func(string) (stateStore, error) {
		t.Fatal("openSQLiteStateStore should not be called when config.toml selects Postgres")
		return nil, nil
	}
	openPostgresStateStore = func(dsn string) (stateStore, error) {
		gotDSN = dsn
		return &stubStateStore{}, nil
	}

	store, err := openDefaultStateStore(filepath.Join(configDir, "state.db"))
	if err != nil {
		t.Fatalf("openDefaultStateStore() error = %v", err)
	}
	if got, want := gotDSN, "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"; got != want {
		t.Fatalf("postgres dsn = %q, want %q", got, want)
	}
	if store == nil {
		t.Fatal("openDefaultStateStore() returned nil store")
	}
}

func TestOpenDefaultStateStoreUsesExplicitSQLiteOverride(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "sqlite:///tmp/orca-shell.db")

	originalSQLite := openSQLiteStateStore
	originalPostgres := openPostgresStateStore
	t.Cleanup(func() {
		openSQLiteStateStore = originalSQLite
		openPostgresStateStore = originalPostgres
	})

	var gotDSN string
	openSQLiteStateStore = func(path string) (stateStore, error) {
		gotDSN = path
		return &stubStateStore{}, nil
	}
	openPostgresStateStore = func(string) (stateStore, error) {
		t.Fatal("openPostgresStateStore should not be called when ORCA_STATE_DB selects SQLite")
		return nil, nil
	}

	store, err := openDefaultStateStore("/tmp/orca.db")
	if err != nil {
		t.Fatalf("openDefaultStateStore() error = %v", err)
	}
	if got, want := gotDSN, "/tmp/orca-shell.db"; got != want {
		t.Fatalf("sqlite path = %q, want %q", got, want)
	}
	if store == nil {
		t.Fatal("openDefaultStateStore() returned nil store")
	}
}

func TestOpenDefaultStateStoreReturnsBackendResolutionError(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "")
	t.Setenv("ORCA_STATE_DSN", "")

	_, err := openDefaultStateStore(filepath.Join(t.TempDir(), "state.db"))
	if err == nil {
		t.Fatal("openDefaultStateStore() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "make dev-postgres") {
		t.Fatalf("openDefaultStateStore() error = %v, want setup hint", err)
	}
}

func TestBootstrapLegacySQLiteStateMigratesIntoEmptyPostgresOnStart(t *testing.T) {
	t.Parallel()

	paths := daemon.Paths{StateDB: "/tmp/orca/state.db"}
	sourceStore := &stubStateStore{}
	destinationStore := &stubStateStore{}
	migrateCalls := 0

	err := bootstrapLegacySQLiteState(context.Background(), "start", paths, stateBootstrapDeps{
		resolveStateBackend: func(path string) (config.StateBackend, error) {
			if got, want := path, paths.StateDB; got != want {
				t.Fatalf("resolveStateBackend path = %q, want %q", got, want)
			}
			return config.StateBackend{
				Kind: config.StateBackendPostgres,
				DSN:  "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable",
			}, nil
		},
		stat: func(path string) (os.FileInfo, error) {
			if got, want := path, paths.StateDB; got != want {
				t.Fatalf("stat path = %q, want %q", got, want)
			}
			return stubFileInfo{}, nil
		},
		openSQLiteStore: func(path string) (stateStore, error) {
			if got, want := path, paths.StateDB; got != want {
				t.Fatalf("openSQLiteStore path = %q, want %q", got, want)
			}
			return sourceStore, nil
		},
		openPostgresStore: func(dsn string) (stateStore, error) {
			if got, want := dsn, "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"; got != want {
				t.Fatalf("openPostgresStore dsn = %q, want %q", got, want)
			}
			return destinationStore, nil
		},
		migrate: func(_ context.Context, from, to state.Store, options state.MigrationOptions) (state.MigrationSummary, error) {
			if got, want := from, state.Store(sourceStore); got != want {
				t.Fatalf("migrate from = %#v, want %#v", got, want)
			}
			if got, want := to, state.Store(destinationStore); got != want {
				t.Fatalf("migrate to = %#v, want %#v", got, want)
			}
			migrateCalls++
			if migrateCalls == 1 {
				if !options.DryRun {
					t.Fatalf("first migrate call options = %#v, want dry run", options)
				}
				return state.MigrationSummary{
					DryRun: true,
					Tables: []state.TableMigrationSummary{{Table: "tasks", DestinationRowsBefore: 0}},
				}, nil
			}
			if migrateCalls == 2 {
				if options.DryRun {
					t.Fatalf("second migrate call options = %#v, want live migration", options)
				}
				return state.MigrationSummary{}, nil
			}
			t.Fatalf("migrate called %d times, want 2", migrateCalls)
			return state.MigrationSummary{}, nil
		},
	})
	if err != nil {
		t.Fatalf("bootstrapLegacySQLiteState() error = %v", err)
	}
	if got, want := migrateCalls, 2; got != want {
		t.Fatalf("migrate calls = %d, want %d", got, want)
	}
	if !sourceStore.closed {
		t.Fatal("expected source store to be closed")
	}
	if !destinationStore.closed {
		t.Fatal("expected destination store to be closed")
	}
}

func TestBootstrapLegacySQLiteStateSkipsNonStartCommandsAndNonEmptyPostgres(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		command      string
		destination  int64
		wantMigrate  int
		wantOpenPost bool
	}{
		{
			name:        "status command skips bootstrap",
			command:     "status",
			destination: 0,
			wantMigrate: 0,
		},
		{
			name:         "start skips when destination already has rows",
			command:      "start",
			destination:  3,
			wantMigrate:  1,
			wantOpenPost: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			paths := daemon.Paths{StateDB: "/tmp/orca/state.db"}
			migrateCalls := 0
			openPostgresCalls := 0

			err := bootstrapLegacySQLiteState(context.Background(), tt.command, paths, stateBootstrapDeps{
				resolveStateBackend: func(string) (config.StateBackend, error) {
					return config.StateBackend{
						Kind: config.StateBackendPostgres,
						DSN:  "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable",
					}, nil
				},
				stat: func(string) (os.FileInfo, error) {
					return stubFileInfo{}, nil
				},
				openSQLiteStore: func(string) (stateStore, error) {
					return &stubStateStore{}, nil
				},
				openPostgresStore: func(string) (stateStore, error) {
					openPostgresCalls++
					return &stubStateStore{}, nil
				},
				migrate: func(context.Context, state.Store, state.Store, state.MigrationOptions) (state.MigrationSummary, error) {
					migrateCalls++
					return state.MigrationSummary{
						DryRun: true,
						Tables: []state.TableMigrationSummary{{Table: "tasks", DestinationRowsBefore: tt.destination}},
					}, nil
				},
			})
			if err != nil {
				t.Fatalf("bootstrapLegacySQLiteState() error = %v", err)
			}
			if got, want := migrateCalls, tt.wantMigrate; got != want {
				t.Fatalf("migrate calls = %d, want %d", got, want)
			}
			if tt.wantOpenPost {
				if got, want := openPostgresCalls, 1; got != want {
					t.Fatalf("openPostgresStore calls = %d, want %d", got, want)
				}
				return
			}
			if openPostgresCalls != 0 {
				t.Fatalf("openPostgresStore calls = %d, want 0", openPostgresCalls)
			}
		})
	}
}

func TestBootstrapLegacySQLiteStateSkipsSQLiteBackendsAndMissingLegacyState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		backend       config.StateBackend
		statErr       error
		wantOpenCalls int
	}{
		{
			name: "configured sqlite backend",
			backend: config.StateBackend{
				Kind:       config.StateBackendSQLite,
				SQLitePath: "/tmp/override.db",
			},
			wantOpenCalls: 0,
		},
		{
			name: "missing legacy sqlite file",
			backend: config.StateBackend{
				Kind: config.StateBackendPostgres,
				DSN:  "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable",
			},
			statErr:       os.ErrNotExist,
			wantOpenCalls: 0,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			paths := daemon.Paths{StateDB: "/tmp/orca/state.db"}
			openCalls := 0

			err := bootstrapLegacySQLiteState(context.Background(), "start", paths, stateBootstrapDeps{
				resolveStateBackend: func(string) (config.StateBackend, error) {
					return tt.backend, nil
				},
				stat: func(string) (os.FileInfo, error) {
					if tt.statErr != nil {
						return nil, tt.statErr
					}
					return stubFileInfo{}, nil
				},
				openSQLiteStore: func(string) (stateStore, error) {
					openCalls++
					return &stubStateStore{}, nil
				},
				openPostgresStore: func(string) (stateStore, error) {
					openCalls++
					return &stubStateStore{}, nil
				},
				migrate: func(context.Context, state.Store, state.Store, state.MigrationOptions) (state.MigrationSummary, error) {
					t.Fatal("migrate should not be called")
					return state.MigrationSummary{}, nil
				},
			})
			if err != nil {
				t.Fatalf("bootstrapLegacySQLiteState() error = %v", err)
			}
			if got, want := openCalls, tt.wantOpenCalls; got != want {
				t.Fatalf("open calls = %d, want %d", got, want)
			}
		})
	}
}

func TestBootstrapLegacySQLiteStateReturnsErrors(t *testing.T) {
	t.Parallel()

	paths := daemon.Paths{StateDB: "/tmp/orca/state.db"}
	postgresBackend := config.StateBackend{
		Kind: config.StateBackendPostgres,
		DSN:  "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable",
	}

	tests := []struct {
		name    string
		deps    stateBootstrapDeps
		wantErr string
	}{
		{
			name: "stat failure",
			deps: stateBootstrapDeps{
				resolveStateBackend: func(string) (config.StateBackend, error) { return postgresBackend, nil },
				stat:                func(string) (os.FileInfo, error) { return nil, errors.New("stat failed") },
			},
			wantErr: "stat legacy sqlite state /tmp/orca/state.db: stat failed",
		},
		{
			name: "open sqlite failure",
			deps: stateBootstrapDeps{
				resolveStateBackend: func(string) (config.StateBackend, error) { return postgresBackend, nil },
				stat:                func(string) (os.FileInfo, error) { return stubFileInfo{}, nil },
				openSQLiteStore:     func(string) (stateStore, error) { return nil, errors.New("sqlite open failed") },
			},
			wantErr: "open legacy sqlite state /tmp/orca/state.db: sqlite open failed",
		},
		{
			name: "open postgres failure",
			deps: stateBootstrapDeps{
				resolveStateBackend: func(string) (config.StateBackend, error) { return postgresBackend, nil },
				stat:                func(string) (os.FileInfo, error) { return stubFileInfo{}, nil },
				openSQLiteStore:     func(string) (stateStore, error) { return &stubStateStore{}, nil },
				openPostgresStore:   func(string) (stateStore, error) { return nil, errors.New("postgres open failed") },
			},
			wantErr: "open configured postgres state: postgres open failed",
		},
		{
			name: "dry run migration failure",
			deps: stateBootstrapDeps{
				resolveStateBackend: func(string) (config.StateBackend, error) { return postgresBackend, nil },
				stat:                func(string) (os.FileInfo, error) { return stubFileInfo{}, nil },
				openSQLiteStore:     func(string) (stateStore, error) { return &stubStateStore{}, nil },
				openPostgresStore:   func(string) (stateStore, error) { return &stubStateStore{}, nil },
				migrate: func(context.Context, state.Store, state.Store, state.MigrationOptions) (state.MigrationSummary, error) {
					return state.MigrationSummary{}, errors.New("dry run failed")
				},
			},
			wantErr: "check legacy sqlite migration: dry run failed",
		},
		{
			name: "live migration failure",
			deps: stateBootstrapDeps{
				resolveStateBackend: func(string) (config.StateBackend, error) { return postgresBackend, nil },
				stat:                func(string) (os.FileInfo, error) { return stubFileInfo{}, nil },
				openSQLiteStore:     func(string) (stateStore, error) { return &stubStateStore{}, nil },
				openPostgresStore:   func(string) (stateStore, error) { return &stubStateStore{}, nil },
				migrate: func(_ context.Context, _ state.Store, _ state.Store, options state.MigrationOptions) (state.MigrationSummary, error) {
					if options.DryRun {
						return state.MigrationSummary{
							DryRun: true,
							Tables: []state.TableMigrationSummary{{Table: "tasks", DestinationRowsBefore: 0}},
						}, nil
					}
					return state.MigrationSummary{}, errors.New("live migrate failed")
				},
			},
			wantErr: "auto-migrate legacy sqlite state: live migrate failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := bootstrapLegacySQLiteState(context.Background(), "start", paths, tt.deps)
			if err == nil {
				t.Fatal("bootstrapLegacySQLiteState() error = nil, want non-nil")
			}
			if got := err.Error(); got != tt.wantErr {
				t.Fatalf("bootstrapLegacySQLiteState() error = %q, want %q", got, tt.wantErr)
			}
		})
	}
}

func TestBootstrapLegacySQLiteStateReturnsBackendResolutionError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("resolve backend failed")
	err := bootstrapLegacySQLiteState(context.Background(), "start", daemon.Paths{StateDB: "/tmp/orca/state.db"}, stateBootstrapDeps{
		resolveStateBackend: func(string) (config.StateBackend, error) {
			return config.StateBackend{}, wantErr
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("bootstrapLegacySQLiteState() error = %v, want wrapped %v", err, wantErr)
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
			name: "nil bootstrap state skips bootstrap",
			args: []string{"status"},
			deps: runDependencies{
				resolvePaths: func() (daemon.Paths, error) {
					return daemon.Paths{StateDB: "/tmp/orca.db"}, nil
				},
				bootstrapState: nil,
				newController: func(daemon.ControllerOptions) (daemon.Controller, error) {
					return &stubController{}, nil
				},
			},
			wantExitCode: 0,
		},
		{
			name: "bootstrap state failure",
			args: []string{"status"},
			deps: runDependencies{
				resolvePaths: func() (daemon.Paths, error) {
					return daemon.Paths{StateDB: "/tmp/orca.db"}, nil
				},
				bootstrapState: func(context.Context, string, daemon.Paths) error {
					return errors.New("bootstrap failed")
				},
			},
			wantExitCode: 1,
			wantStderr:   "bootstrap failed\n",
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

func (s *stubStateStore) AppendEvent(context.Context, state.Event) (state.Event, error) {
	return state.Event{}, nil
}

func (s *stubStateStore) TaskStatus(context.Context, string, string) (state.TaskStatus, error) {
	return state.TaskStatus{}, state.ErrNotFound
}

func (s *stubStateStore) KnownProjects(context.Context) ([]string, error) {
	return nil, nil
}

func (s *stubStateStore) ListWorkers(context.Context, string) ([]state.Worker, error) {
	return nil, nil
}

func (s *stubStateStore) ListClones(context.Context, string) ([]state.Clone, error) {
	return nil, nil
}

type stubFileInfo struct{}

func (stubFileInfo) Name() string       { return "state.db" }
func (stubFileInfo) Size() int64        { return 0 }
func (stubFileInfo) Mode() os.FileMode  { return 0o644 }
func (stubFileInfo) ModTime() time.Time { return time.Time{} }
func (stubFileInfo) IsDir() bool        { return false }
func (stubFileInfo) Sys() any           { return nil }

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

func (*stubController) Reconcile(context.Context, daemon.ReconcileRequest) (daemon.ReconcileResult, error) {
	return daemon.ReconcileResult{}, nil
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
	if overrides.bootstrapState != nil {
		deps.bootstrapState = overrides.bootstrapState
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
