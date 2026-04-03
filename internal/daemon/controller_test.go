package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

	projectPath, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatalf("filepath.Abs() error = %v", err)
	}
	return projectPath
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
