package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestStartKillsProcessWhenStartupTimesOut(t *testing.T) {
	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, pidPath := newTestController(t, store, projectPath, scriptOptions{
		ignoreTERM: false,
	})
	startErr := make(chan error, 1)

	go func() {
		_, err := controller.Start(context.Background(), StartRequest{
			Session: "test",
			Project: projectPath,
		})
		startErr <- err
	}()

	pid := waitForPID(t, pidPath)

	err := <-startErr
	if err == nil || !strings.Contains(err.Error(), "daemon failed to report running state") {
		t.Fatalf("expected timeout error, got %v", err)
	}

	waitForProcessExit(t, pid)

	if !store.markDaemonStoppedCalled {
		t.Fatal("expected timed-out start to mark daemon stopped")
	}
}

func TestStartReturnsContextErrorAndKillsProcess(t *testing.T) {
	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, pidPath := newTestController(t, store, projectPath, scriptOptions{
		ignoreTERM: false,
	})
	ctx, cancel := context.WithCancel(context.Background())
	startErr := make(chan error, 1)

	go func() {
		_, err := controller.Start(ctx, StartRequest{
			Session: "test",
			Project: projectPath,
		})
		startErr <- err
	}()

	pid := waitForPID(t, pidPath)
	cancel()

	err := <-startErr
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}

	waitForProcessExit(t, pid)
}

func TestStartLaunchesDaemonInOwnProcessGroup(t *testing.T) {
	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, pidPath := newTestController(t, store, projectPath, scriptOptions{
		ignoreTERM: false,
	})
	startErr := make(chan error, 1)

	go func() {
		_, err := controller.Start(context.Background(), StartRequest{
			Session: "test",
			Project: projectPath,
		})
		startErr <- err
	}()

	pid := waitForPID(t, pidPath)
	pgid, err := syscall.Getpgid(pid)
	if err != nil {
		t.Fatalf("Getpgid(%d) error = %v", pid, err)
	}
	if got, want := pgid, pid; got != want {
		t.Fatalf("daemon process group = %d, want %d", got, want)
	}

	err = <-startErr
	if err == nil || !strings.Contains(err.Error(), "daemon failed to report running state") {
		t.Fatalf("expected timeout error, got %v", err)
	}

	waitForProcessExit(t, pid)
}

func TestPreparePIDStateExistingPIDFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		useLivePID            bool
		wantErr               error
		wantMarkDaemonStopped bool
		wantPIDFileExists     bool
	}{
		{
			name:                  "stale pid file is removed",
			useLivePID:            false,
			wantMarkDaemonStopped: true,
			wantPIDFileExists:     false,
		},
		{
			name:              "live pid file blocks startup",
			useLivePID:        true,
			wantErr:           ErrDaemonAlreadyRunning,
			wantPIDFileExists: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &fakeStore{}
			projectPath := testProjectPath(t)
			controller, _ := newTestController(t, store, projectPath, scriptOptions{
				ignoreTERM: false,
			})

			process := startDirectSleepProcess(t, true)
			pid := process.Process.Pid
			if !tt.useLivePID {
				if err := process.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
					t.Fatalf("Kill(%d) error = %v", pid, err)
				}
				_, _ = process.Process.Wait()
				waitForProcessExit(t, pid)
			} else {
				t.Cleanup(func() {
					if err := process.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
						t.Fatalf("Kill(%d) error = %v", pid, err)
					}
					_, _ = process.Process.Wait()
				})
			}

			pidFile := controller.paths.pidFile(projectPath)
			if err := os.MkdirAll(filepath.Dir(pidFile), 0o755); err != nil {
				t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(pidFile), err)
			}
			if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0o644); err != nil {
				t.Fatalf("WriteFile(%q) error = %v", pidFile, err)
			}

			err := controller.preparePIDState(context.Background(), projectPath)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("preparePIDState() error = %v, want %v", err, tt.wantErr)
			}
			if got, want := store.markDaemonStoppedCalled, tt.wantMarkDaemonStopped; got != want {
				t.Fatalf("markDaemonStoppedCalled = %t, want %t", got, want)
			}

			_, statErr := os.Stat(pidFile)
			if tt.wantPIDFileExists {
				if statErr != nil {
					t.Fatalf("Stat(%q) error = %v, want existing pid file", pidFile, statErr)
				}
				return
			}
			if !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("Stat(%q) error = %v, want not exist", pidFile, statErr)
			}
		})
	}
}

func TestStopReturnsContextErrorWhenPollingCancelled(t *testing.T) {
	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, _ := newTestController(t, store, projectPath, scriptOptions{
		ignoreTERM: true,
	})

	process := startDirectSleepProcess(t, true)
	pid := process.Process.Pid

	if err := os.MkdirAll(controller.paths.PIDDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(controller.paths.pidFile(projectPath), []byte(fmt.Sprintf("%d", pid)), 0o644); err != nil {
		t.Fatalf("write controller pid file: %v", err)
	}
	store.projectStatus = state.ProjectStatus{
		Project: projectPath,
		Daemon: &state.DaemonStatus{
			PID:    pid,
			Status: "running",
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopErr := make(chan error, 1)
	startedAt := time.Now()

	go func() {
		_, err := controller.Stop(ctx, StopRequest{Project: projectPath})
		stopErr <- err
	}()

	waitForDuration(t, 75*time.Millisecond)
	cancel()

	err := <-stopErr
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation, got %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed >= controller.stopTimeout {
		t.Fatalf("expected stop to exit before timeout, elapsed %s", elapsed)
	}

	if err := process.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("Kill(%d) error = %v", pid, err)
	}
	_, _ = process.Process.Wait()
	waitForProcessExit(t, pid)
}

func TestLocalControllerAssignAndBatchRPC(t *testing.T) {
	projectPath := testProjectPath(t)
	tempDir := t.TempDir()
	paths := Paths{
		ConfigDir: tempDir,
		StateDB:   filepath.Join(tempDir, "state.db"),
		PIDDir:    filepath.Join(tempDir, "pids"),
	}
	store := &fakeStore{
		projectStatus: state.ProjectStatus{
			Project: projectPath,
			Daemon: &state.DaemonStatus{
				PID:    os.Getpid(),
				Status: "running",
			},
		},
	}

	if err := os.MkdirAll(paths.PIDDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", paths.PIDDir, err)
	}
	if err := os.WriteFile(paths.pidFile(projectPath), []byte(fmt.Sprintf("%d", os.Getpid())), 0o644); err != nil {
		t.Fatalf("WriteFile(pidFile) error = %v", err)
	}

	socketPath := paths.socketFile(projectPath)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen(%q) error = %v", socketPath, err)
	}
	defer listener.Close()

	requests := make(chan rpcRequest, 2)
	go func() {
		for i := 0; i < 2; i++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			var req rpcRequest
			if err := json.NewDecoder(conn).Decode(&req); err == nil {
				requests <- req
				switch req.Method {
				case "assign":
					_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, TaskActionResult{Project: projectPath, Issue: "LAB-718", Status: TaskStatusActive, Agent: "claude"}))
				case "batch":
					_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, BatchResult{Project: projectPath, Results: []TaskActionResult{{Project: projectPath, Issue: "LAB-719", Status: TaskStatusActive, Agent: "codex"}}}))
				}
			}
			_ = conn.Close()
		}
	}()

	controller, err := NewLocalController(ControllerOptions{
		Store: store,
		Paths: paths,
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	assignResult, err := controller.Assign(context.Background(), AssignRequest{
		Project: projectPath,
		Issue:   "  LAB-718  ",
		Prompt:  "Implement controller assign.",
		Agent:   "  claude  ",
		Title:   "  Assign title  ",
	})
	if err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	if got, want := assignResult.Issue, "LAB-718"; got != want {
		t.Fatalf("assign issue = %q, want %q", got, want)
	}

	batchResult, err := controller.Batch(context.Background(), BatchRequest{
		Project: projectPath,
		Entries: []BatchEntry{{Issue: "  LAB-719  ", Agent: "  codex  ", Prompt: "Implement controller batch.", Title: "  Batch title  "}},
		Delay:   7 * time.Second,
	})
	if err != nil {
		t.Fatalf("Batch() error = %v", err)
	}
	if got, want := len(batchResult.Results), 1; got != want {
		t.Fatalf("batch result count = %d, want %d", got, want)
	}

	assignReq := <-requests
	if got, want := assignReq.Method, "assign"; got != want {
		t.Fatalf("assign method = %q, want %q", got, want)
	}
	var assignParams assignRPCParams
	if err := json.Unmarshal(assignReq.Params, &assignParams); err != nil {
		t.Fatalf("json.Unmarshal(assign params) error = %v", err)
	}
	if got, want := assignParams, (assignRPCParams{Issue: "LAB-718", Prompt: "Implement controller assign.", Agent: "claude", Title: "Assign title"}); !reflect.DeepEqual(got, want) {
		t.Fatalf("assign params = %#v, want %#v", got, want)
	}

	batchReq := <-requests
	if got, want := batchReq.Method, "batch"; got != want {
		t.Fatalf("batch method = %q, want %q", got, want)
	}
	var batchParams batchRPCParams
	if err := json.Unmarshal(batchReq.Params, &batchParams); err != nil {
		t.Fatalf("json.Unmarshal(batch params) error = %v", err)
	}
	if got, want := batchParams.Entries, []BatchEntry{{Issue: "LAB-719", Agent: "codex", Prompt: "Implement controller batch.", Title: "Batch title"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batch entries = %#v, want %#v", got, want)
	}
	if got, want := batchParams.Delay, "7s"; got != want {
		t.Fatalf("batch delay = %q, want %q", got, want)
	}
}

func TestLocalControllerBatchErrorBranches(t *testing.T) {
	projectPath := testProjectPath(t)
	tempDir := t.TempDir()
	paths := Paths{
		ConfigDir: tempDir,
		StateDB:   filepath.Join(tempDir, "state.db"),
		PIDDir:    filepath.Join(tempDir, "pids"),
	}
	validEntries := []BatchEntry{{Issue: "LAB-719", Agent: "codex", Prompt: "Implement controller batch."}}

	t.Run("invalid project", func(t *testing.T) {
		controller, err := NewLocalController(ControllerOptions{Store: &fakeStore{}, Paths: paths})
		if err != nil {
			t.Fatalf("NewLocalController() error = %v", err)
		}
		_, err = controller.Batch(context.Background(), BatchRequest{
			Project: t.TempDir(),
			Entries: validEntries,
		})
		if err == nil || !strings.Contains(err.Error(), "not inside a git repository") {
			t.Fatalf("Batch() error = %v, want canonical path error", err)
		}
	})

	t.Run("invalid entries", func(t *testing.T) {
		controller, err := NewLocalController(ControllerOptions{Store: &fakeStore{}, Paths: paths})
		if err != nil {
			t.Fatalf("NewLocalController() error = %v", err)
		}
		_, err = controller.Batch(context.Background(), BatchRequest{
			Project: projectPath,
			Entries: []BatchEntry{{Issue: "LAB-719", Prompt: "Implement controller batch."}},
		})
		if err == nil || !strings.Contains(err.Error(), "batch manifest entry 1 requires agent") {
			t.Fatalf("Batch() error = %v, want validation error", err)
		}
	})

	t.Run("negative delay", func(t *testing.T) {
		controller, err := NewLocalController(ControllerOptions{Store: &fakeStore{}, Paths: paths})
		if err != nil {
			t.Fatalf("NewLocalController() error = %v", err)
		}
		_, err = controller.Batch(context.Background(), BatchRequest{
			Project: projectPath,
			Entries: validEntries,
			Delay:   -time.Second,
		})
		if err == nil || !strings.Contains(err.Error(), "batch delay must be non-negative") {
			t.Fatalf("Batch() error = %v, want delay error", err)
		}
	})

	t.Run("daemon not running", func(t *testing.T) {
		controller, err := NewLocalController(ControllerOptions{Store: &fakeStore{}, Paths: paths})
		if err != nil {
			t.Fatalf("NewLocalController() error = %v", err)
		}
		_, err = controller.Batch(context.Background(), BatchRequest{
			Project: projectPath,
			Entries: validEntries,
		})
		if !errors.Is(err, ErrDaemonNotRunning) {
			t.Fatalf("Batch() error = %v, want %v", err, ErrDaemonNotRunning)
		}
	})

	t.Run("rpc error", func(t *testing.T) {
		store := &fakeStore{
			projectStatus: state.ProjectStatus{
				Project: projectPath,
				Daemon: &state.DaemonStatus{
					PID:    os.Getpid(),
					Status: "running",
				},
			},
		}
		controller, err := NewLocalController(ControllerOptions{Store: store, Paths: paths})
		if err != nil {
			t.Fatalf("NewLocalController() error = %v", err)
		}
		if err := os.MkdirAll(paths.PIDDir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", paths.PIDDir, err)
		}
		if err := os.WriteFile(paths.pidFile(projectPath), []byte(fmt.Sprintf("%d", os.Getpid())), 0o644); err != nil {
			t.Fatalf("WriteFile(pidFile) error = %v", err)
		}
		_, err = controller.Batch(context.Background(), BatchRequest{
			Project: projectPath,
			Entries: validEntries,
			Delay:   time.Second,
		})
		if !errors.Is(err, ErrDaemonNotRunning) {
			t.Fatalf("Batch() error = %v, want %v", err, ErrDaemonNotRunning)
		}
	})
}

type scriptOptions struct {
	ignoreTERM bool
}

func newTestController(t *testing.T, store *fakeStore, projectPath string, options scriptOptions) (*LocalController, string) {
	t.Helper()

	tempDir := t.TempDir()
	scriptPath := filepath.Join(tempDir, "daemon.sh")
	pidOutputPath := filepath.Join(tempDir, "daemon-child.pid")

	script := "#!/bin/bash\nset -eu\nprintf '%s' \"$$\" > \"" + pidOutputPath + "\"\n"
	if options.ignoreTERM {
		script += "trap '' TERM\n"
	}
	script += "exec sleep 1000\n"

	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write helper daemon script: %v", err)
	}

	controller, err := NewLocalController(ControllerOptions{
		Store:        store,
		Paths:        Paths{StateDB: filepath.Join(tempDir, "state.db"), PIDDir: filepath.Join(tempDir, "pids")},
		Executable:   scriptPath,
		Now:          func() time.Time { return time.Unix(1, 0).UTC() },
		StartTimeout: 1500 * time.Millisecond,
		StopTimeout:  time.Second,
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	return controller, pidOutputPath
}

func waitForPID(t *testing.T, path string) int {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil && strings.TrimSpace(string(data)) != "" {
			var pid int
			if _, scanErr := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); scanErr == nil {
				return pid
			}
		}
		waitForDuration(t, 20*time.Millisecond)
	}

	t.Fatalf("timed out waiting for pid file %s", path)
	return 0
}

func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		alive, err := processAlive(pid)
		if err != nil {
			t.Fatalf("processAlive(%d) error = %v", pid, err)
		}
		if !alive {
			return
		}
		waitForDuration(t, 20*time.Millisecond)
	}

	t.Fatalf("process %d still alive after deadline", pid)
}

func killTestProcess(t *testing.T, pid int) {
	t.Helper()

	process, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("FindProcess(%d) error = %v", pid, err)
	}
	if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("Kill(%d) error = %v", pid, err)
	}
	_ = process.Release()
	waitForProcessExit(t, pid)
}

func waitForDuration(t *testing.T, duration time.Duration) {
	t.Helper()

	if err := amux.Wait(context.Background(), duration); err != nil {
		t.Fatalf("Wait(%s) error = %v", duration, err)
	}
}

func startDirectSleepProcess(t *testing.T, ignoreTERM bool) *exec.Cmd {
	t.Helper()

	command := "exec sleep 1000"
	if ignoreTERM {
		command = "trap '' TERM; exec sleep 1000"
	}

	cmd := exec.Command("/bin/bash", "-lc", command)
	if err := cmd.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	return cmd
}

func testProjectPath(t *testing.T) string {
	t.Helper()

	projectPath := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(filepath.Join(projectPath, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.git) error = %v", err)
	}

	resolvedProjectPath, err := filepath.EvalSymlinks(projectPath)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q) error = %v", projectPath, err)
	}
	return resolvedProjectPath
}

type fakeStore struct {
	projectStatus           state.ProjectStatus
	projectStatusCalls      int
	markDaemonStoppedCalled bool
}

func (f *fakeStore) ProjectStatus(_ context.Context, _ string) (state.ProjectStatus, error) {
	f.projectStatusCalls++
	return f.projectStatus, nil
}

func (f *fakeStore) TaskStatus(_ context.Context, _, _ string) (state.TaskStatus, error) {
	return state.TaskStatus{}, nil
}

func (f *fakeStore) ListWorkers(_ context.Context, _ string) ([]state.Worker, error) {
	return nil, nil
}

func (f *fakeStore) ListClones(_ context.Context, _ string) ([]state.Clone, error) {
	return nil, nil
}

func (f *fakeStore) Events(_ context.Context, _ string, _ int64) (<-chan state.Event, <-chan error) {
	eventsCh := make(chan state.Event)
	errCh := make(chan error)
	close(eventsCh)
	close(errCh)
	return eventsCh, errCh
}

func (f *fakeStore) EnsureSchema(_ context.Context) error {
	return nil
}

func (f *fakeStore) UpsertDaemon(_ context.Context, _ string, _ state.DaemonStatus) error {
	return nil
}

func (f *fakeStore) MarkDaemonStopped(_ context.Context, _ string, _ time.Time) error {
	f.markDaemonStoppedCalled = true
	return nil
}

func (f *fakeStore) UpsertTask(_ context.Context, _ string, _ state.Task) error {
	return nil
}

func (f *fakeStore) UpdateTaskStatus(_ context.Context, _, _, _ string, _ time.Time) (state.Task, error) {
	return state.Task{}, nil
}

func (f *fakeStore) AppendEvent(_ context.Context, event state.Event) (state.Event, error) {
	return event, nil
}

func (f *fakeStore) Close() error {
	return nil
}

var _ state.Store = (*fakeStore)(nil)
