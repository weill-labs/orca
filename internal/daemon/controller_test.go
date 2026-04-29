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

func TestStartRedirectsDaemonOutputToDefaultLogFile(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, _ := newTestController(t, store, projectPath, scriptOptions{
		startTimeout: 150 * time.Millisecond,
		stdoutLine:   "daemon stdout marker",
		stderrLine:   "daemon stderr marker",
	})

	_, err := controller.Start(context.Background(), StartRequest{
		Session: "test",
		Project: projectPath,
	})
	if err == nil || !strings.Contains(err.Error(), "daemon failed to report running state") {
		t.Fatalf("Start() error = %v, want startup timeout", err)
	}

	logPath := filepath.Join(homeDir, ".local", "state", "orca", "daemon.log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", logPath, err)
	}
	output := string(data)
	for _, want := range []string{"daemon stdout marker", "daemon stderr marker"} {
		if !strings.Contains(output, want) {
			t.Fatalf("daemon log = %q, want %q", output, want)
		}
	}
}

func TestStartRotatesOversizedDaemonLogBeforeWriting(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	logPath := filepath.Join(homeDir, ".local", "state", "orca", "daemon.log")
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(logPath), err)
	}
	if err := os.WriteFile(logPath, nil, 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", logPath, err)
	}
	if err := os.Truncate(logPath, 10*1024*1024+1); err != nil {
		t.Fatalf("Truncate(%q) error = %v", logPath, err)
	}

	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, _ := newTestController(t, store, projectPath, scriptOptions{
		startTimeout: 150 * time.Millisecond,
		stderrLine:   "fresh daemon output",
	})

	_, err := controller.Start(context.Background(), StartRequest{
		Session: "test",
		Project: projectPath,
	})
	if err == nil || !strings.Contains(err.Error(), "daemon failed to report running state") {
		t.Fatalf("Start() error = %v, want startup timeout", err)
	}

	rotatedPath := logPath + ".1"
	rotatedInfo, err := os.Stat(rotatedPath)
	if err != nil {
		t.Fatalf("Stat(%q) error = %v", rotatedPath, err)
	}
	if got, want := rotatedInfo.Size(), int64(10*1024*1024+1); got != want {
		t.Fatalf("rotated log size = %d, want %d", got, want)
	}

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", logPath, err)
	}
	if got := string(data); !strings.Contains(got, "fresh daemon output") {
		t.Fatalf("daemon log = %q, want fresh output", got)
	}
}

func TestResolvePathsAcceptsSQLiteURIOverride(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "sqlite:///tmp/orca-state.db")

	paths, err := ResolvePaths()
	if err != nil {
		t.Fatalf("ResolvePaths() error = %v", err)
	}
	if got, want := paths.StateDB, "/tmp/orca-state.db"; got != want {
		t.Fatalf("paths.StateDB = %q, want %q", got, want)
	}
	if got, want := paths.ConfigDir, "/tmp"; got != want {
		t.Fatalf("paths.ConfigDir = %q, want %q", got, want)
	}
	if got, want := paths.PIDDir, filepath.Join("/tmp", "pids"); got != want {
		t.Fatalf("paths.PIDDir = %q, want %q", got, want)
	}
}

func TestResolvePathsRejectsInvalidSQLiteURIOverride(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "sqlite://tmp/orca-state.db")

	_, err := ResolvePaths()
	if err == nil {
		t.Fatal("ResolvePaths() error = nil, want non-nil")
	}
	if !strings.Contains(err.Error(), "sqlite:///absolute/path") {
		t.Fatalf("ResolvePaths() error = %v, want sqlite path guidance", err)
	}
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

			pidFile := controller.paths.pidFile()
			if err := os.MkdirAll(filepath.Dir(pidFile), 0o755); err != nil {
				t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(pidFile), err)
			}
			if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0o644); err != nil {
				t.Fatalf("WriteFile(%q) error = %v", pidFile, err)
			}

			err := controller.preparePIDState(context.Background())
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

func TestPathsUseGlobalDaemonFiles(t *testing.T) {
	t.Parallel()

	paths := Paths{
		ConfigDir: "/tmp/orca",
		PIDDir:    "/tmp/orca/pids",
	}

	pidA := paths.pidFile()
	pidB := paths.pidFile()
	if got, want := pidA, "/tmp/orca/pids/orca.pid"; got != want {
		t.Fatalf("pidFile() = %q, want %q", got, want)
	}
	if got, want := pidB, "/tmp/orca/pids/orca.pid"; got != want {
		t.Fatalf("pidFile() = %q, want %q", got, want)
	}

	socketA := paths.socketFile()
	socketB := paths.socketFile()
	if got, want := socketA, "/tmp/orca/orca.sock"; got != want {
		t.Fatalf("socketFile() = %q, want %q", got, want)
	}
	if got, want := socketB, "/tmp/orca/orca.sock"; got != want {
		t.Fatalf("socketFile() = %q, want %q", got, want)
	}
}

func TestStopReturnsContextErrorWhenPollingCancelled(t *testing.T) {
	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, _ := newTestController(t, store, projectPath, scriptOptions{
		ignoreTERM: true,
	})

	pid := startDetachedSleepProcess(t, true)

	if err := os.MkdirAll(controller.paths.PIDDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	if err := os.WriteFile(controller.paths.pidFile(), []byte(fmt.Sprintf("%d", pid)), 0o644); err != nil {
		t.Fatalf("write controller pid file: %v", err)
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

	killTestProcess(t, pid)
}

func TestStopWithoutForceReturnsTimeoutWhenProcessIgnoresTERM(t *testing.T) {
	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, _ := newTestController(t, store, projectPath, scriptOptions{
		ignoreTERM:  true,
		stopTimeout: 150 * time.Millisecond,
	})

	pid := startDetachedSleepProcess(t, true)

	if err := os.MkdirAll(controller.paths.PIDDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	pidFile := controller.paths.pidFile()
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0o644); err != nil {
		t.Fatalf("write controller pid file: %v", err)
	}

	_, err := controller.Stop(context.Background(), StopRequest{Project: projectPath})
	if err == nil || !strings.Contains(err.Error(), "daemon did not stop within") {
		t.Fatalf("Stop() error = %v, want timeout", err)
	}

	alive, err := processAlive(pid)
	if err != nil {
		t.Fatalf("processAlive(%d) error = %v", pid, err)
	}
	if !alive {
		t.Fatal("expected default stop to leave stubborn daemon running")
	}
	if store.markDaemonStoppedCalled {
		t.Fatal("expected timed out stop to leave daemon state running")
	}
	if _, err := os.Stat(pidFile); err != nil {
		t.Fatalf("Stat(%q) error = %v, want pid file to remain", pidFile, err)
	}

	killTestProcess(t, pid)
}

func TestStopStopsResponsiveProcess(t *testing.T) {
	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, _ := newTestController(t, store, projectPath, scriptOptions{
		stopTimeout: 150 * time.Millisecond,
	})

	pid := startDetachedSleepProcess(t, false)

	if err := os.MkdirAll(controller.paths.PIDDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	pidFile := controller.paths.pidFile()
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0o644); err != nil {
		t.Fatalf("write controller pid file: %v", err)
	}

	result, err := controller.Stop(context.Background(), StopRequest{Project: projectPath})
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if got, want := result.PID, pid; got != want {
		t.Fatalf("result pid = %d, want %d", got, want)
	}
	waitForProcessExit(t, pid)
	if !store.markDaemonStoppedCalled {
		t.Fatal("expected responsive stop to mark daemon stopped")
	}
	if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat(%q) error = %v, want pid file removed", pidFile, err)
	}
}

func TestStopForceKillsProcessAfterGracePeriod(t *testing.T) {
	store := &fakeStore{}
	projectPath := testProjectPath(t)
	controller, _ := newTestController(t, store, projectPath, scriptOptions{
		ignoreTERM:  true,
		stopTimeout: 150 * time.Millisecond,
	})

	pid := startDetachedSleepProcess(t, true)

	if err := os.MkdirAll(controller.paths.PIDDir, 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}
	pidFile := controller.paths.pidFile()
	if err := os.WriteFile(pidFile, []byte(fmt.Sprintf("%d", pid)), 0o644); err != nil {
		t.Fatalf("write controller pid file: %v", err)
	}

	startedAt := time.Now()
	result, err := controller.Stop(context.Background(), StopRequest{
		Project: projectPath,
		Force:   true,
	})
	if err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if elapsed := time.Since(startedAt); elapsed < controller.stopTimeout {
		t.Fatalf("Stop() elapsed = %s, want at least grace period %s", elapsed, controller.stopTimeout)
	}
	if got, want := result.PID, pid; got != want {
		t.Fatalf("result pid = %d, want %d", got, want)
	}
	waitForProcessExit(t, pid)
	if !store.markDaemonStoppedCalled {
		t.Fatal("expected forced stop to mark daemon stopped")
	}
	if _, err := os.Stat(pidFile); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Stat(%q) error = %v, want pid file removed", pidFile, err)
	}
}

func TestLocalControllerCancelHonorsCallerDeadline(t *testing.T) {
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
	if err := os.WriteFile(paths.pidFile(), []byte(fmt.Sprintf("%d", os.Getpid())), 0o644); err != nil {
		t.Fatalf("WriteFile(pidFile) error = %v", err)
	}

	socketPath := paths.socketFile()
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen(%q) error = %v", socketPath, err)
	}
	defer listener.Close()

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		var req rpcRequest
		if err := json.NewDecoder(conn).Decode(&req); err != nil {
			return
		}
		_ = amux.Wait(context.Background(), 200*time.Millisecond)
	}()

	controller, err := NewLocalController(ControllerOptions{
		Store: store,
		Paths: paths,
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	_, err = controller.Cancel(ctx, CancelRequest{
		Project: projectPath,
		Issue:   "LAB-718",
	})
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("Cancel() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(startedAt); elapsed >= time.Second {
		t.Fatalf("Cancel() elapsed = %s, want caller deadline to abort before 1s", elapsed)
	}
}

func TestLocalControllerAssignCancelResumeRPC(t *testing.T) {
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
	if err := os.WriteFile(paths.pidFile(), []byte(fmt.Sprintf("%d", os.Getpid())), 0o644); err != nil {
		t.Fatalf("WriteFile(pidFile) error = %v", err)
	}

	socketPath := paths.socketFile()
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen(%q) error = %v", socketPath, err)
	}
	defer listener.Close()

	requests := make(chan rpcRequest, 3)
	go func() {
		for i := 0; i < 3; i++ {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			var req rpcRequest
			if err := json.NewDecoder(conn).Decode(&req); err == nil {
				requests <- req
				switch req.Method {
				case "assign":
					_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, TaskActionResult{Project: projectPath, Issue: "GH-718", Status: TaskStatusActive, Agent: "claude"}))
				case "cancel":
					_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, TaskActionResult{Project: projectPath, Issue: "GH-720", Status: TaskStatusCancelled, Agent: "codex"}))
				case "resume":
					_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, TaskActionResult{Project: projectPath, Issue: "GH-721", Status: TaskStatusActive, Agent: "codex"}))
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
		Project:    projectPath,
		Issue:      "  #718  ",
		Prompt:     "Implement controller assign.",
		Agent:      "  claude  ",
		CallerPane: "  pane-13  ",
		Title:      "  Assign title  ",
	})
	if err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	if got, want := assignResult.Issue, "GH-718"; got != want {
		t.Fatalf("assign issue = %q, want %q", got, want)
	}

	cancelResult, err := controller.Cancel(context.Background(), CancelRequest{
		Project: projectPath,
		Issue:   "  #720  ",
	})
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if got, want := cancelResult.Issue, "GH-720"; got != want {
		t.Fatalf("cancel issue = %q, want %q", got, want)
	}

	resumeResult, err := controller.Resume(context.Background(), ResumeRequest{
		Project: projectPath,
		Issue:   "  #721  ",
		Prompt:  "  Continue the GitHub-backed task  ",
	})
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if got, want := resumeResult.Issue, "GH-721"; got != want {
		t.Fatalf("resume issue = %q, want %q", got, want)
	}

	assignReq := <-requests
	if got, want := assignReq.Method, "assign"; got != want {
		t.Fatalf("assign method = %q, want %q", got, want)
	}
	var assignParams assignRPCParams
	if err := json.Unmarshal(assignReq.Params, &assignParams); err != nil {
		t.Fatalf("json.Unmarshal(assign params) error = %v", err)
	}
	if got, want := assignParams, (assignRPCParams{
		Project:    projectPath,
		Issue:      "GH-718",
		Prompt:     "Implement controller assign.",
		Agent:      "claude",
		CallerPane: "pane-13",
		Title:      "Assign title",
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("assign params = %#v, want %#v", got, want)
	}

	cancelReq := <-requests
	if got, want := cancelReq.Method, "cancel"; got != want {
		t.Fatalf("cancel method = %q, want %q", got, want)
	}
	var cancelParams cancelRPCParams
	if err := json.Unmarshal(cancelReq.Params, &cancelParams); err != nil {
		t.Fatalf("json.Unmarshal(cancel params) error = %v", err)
	}
	if got, want := cancelParams, (cancelRPCParams{
		Project: projectPath,
		Issue:   "GH-720",
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("cancel params = %#v, want %#v", got, want)
	}

	resumeReq := <-requests
	if got, want := resumeReq.Method, "resume"; got != want {
		t.Fatalf("resume method = %q, want %q", got, want)
	}
	var resumeParams resumeRPCParams
	if err := json.Unmarshal(resumeReq.Params, &resumeParams); err != nil {
		t.Fatalf("json.Unmarshal(resume params) error = %v", err)
	}
	if got, want := resumeParams, (resumeRPCParams{
		Project: projectPath,
		Issue:   "GH-721",
		Prompt:  "Continue the GitHub-backed task",
	}); !reflect.DeepEqual(got, want) {
		t.Fatalf("resume params = %#v, want %#v", got, want)
	}
}

type scriptOptions struct {
	ignoreTERM   bool
	startTimeout time.Duration
	stopTimeout  time.Duration
	stdoutLine   string
	stderrLine   string
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
	if options.stdoutLine != "" {
		script += fmt.Sprintf("printf '%%s\\n' %q\n", options.stdoutLine)
	}
	if options.stderrLine != "" {
		script += fmt.Sprintf("printf '%%s\\n' %q >&2\n", options.stderrLine)
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
		StartTimeout: resolvedStartTimeout(options.startTimeout),
		StopTimeout:  resolvedStopTimeout(options.stopTimeout),
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	return controller, pidOutputPath
}

func resolvedStartTimeout(startTimeout time.Duration) time.Duration {
	if startTimeout > 0 {
		return startTimeout
	}
	return 1500 * time.Millisecond
}

func resolvedStopTimeout(stopTimeout time.Duration) time.Duration {
	if stopTimeout > 0 {
		return stopTimeout
	}
	return time.Second
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

	tempDir := t.TempDir()
	readyPath := filepath.Join(tempDir, "ready")
	scriptPath := filepath.Join(tempDir, "sleep.sh")

	script := "#!/bin/bash\nset -eu\n"
	if ignoreTERM {
		script += "trap '' TERM\n"
	}
	script += fmt.Sprintf("printf ready > %q\n", readyPath)
	if ignoreTERM {
		script += "while true; do\n"
		script += "  sleep 1000 &\n"
		script += "  wait $!\n"
		script += "done\n"
	} else {
		script += "exec sleep 1000\n"
	}

	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write direct sleep script: %v", err)
	}

	cmd := exec.Command("/bin/bash", scriptPath)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start direct sleep process: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(readyPath); err == nil {
			alive, aliveErr := processAlive(cmd.Process.Pid)
			if aliveErr != nil {
				t.Fatalf("processAlive(%d) error = %v", cmd.Process.Pid, aliveErr)
			}
			if alive {
				return cmd
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Stat(%q) error = %v", readyPath, err)
		}
		waitForDuration(t, 10*time.Millisecond)
	}

	t.Fatalf("timed out waiting for direct sleep pid %d to start", cmd.Process.Pid)
	return nil
}

func startDetachedSleepProcess(t *testing.T, ignoreTERM bool) int {
	t.Helper()

	tempDir := t.TempDir()
	readyPath := filepath.Join(tempDir, "ready")
	scriptPath := filepath.Join(tempDir, "sleep.sh")

	script := "#!/bin/bash\nset -eu\n"
	if ignoreTERM {
		script += "trap '' TERM\n"
	}
	script += fmt.Sprintf("printf ready > %q\n", readyPath)
	if ignoreTERM {
		script += "while true; do\n"
		script += "  sleep 1000 &\n"
		script += "  wait $!\n"
		script += "done\n"
	} else {
		script += "exec sleep 1000\n"
	}

	if err := os.WriteFile(scriptPath, []byte(script), 0o755); err != nil {
		t.Fatalf("write detached sleep script: %v", err)
	}

	cmd := exec.Command("/bin/bash", "-lc", fmt.Sprintf("nohup %q >/dev/null 2>&1 </dev/null & echo $!", scriptPath))
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("start detached sleep process: %v", err)
	}

	var pid int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(output)), "%d", &pid); err != nil {
		t.Fatalf("parse detached sleep pid %q: %v", strings.TrimSpace(string(output)), err)
	}

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		alive, err := processAlive(pid)
		if err != nil {
			t.Fatalf("processAlive(%d) error = %v", pid, err)
		}
		if alive {
			if _, statErr := os.Stat(readyPath); statErr == nil {
				return pid
			} else if !errors.Is(statErr, os.ErrNotExist) {
				t.Fatalf("Stat(%q) error = %v", readyPath, statErr)
			}
		}
		if !alive {
			break
		}
		waitForDuration(t, 10*time.Millisecond)
	}

	if _, err := os.Stat(readyPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Fatalf("detached sleep pid %d exited before signaling readiness", pid)
		}
		t.Fatalf("Stat(%q) error = %v", readyPath, err)
	}

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		alive, err := processAlive(pid)
		if err != nil {
			t.Fatalf("processAlive(%d) error = %v", pid, err)
		}
		if alive {
			return pid
		}
		waitForDuration(t, 10*time.Millisecond)
	}

	t.Fatalf("timed out waiting for detached sleep pid %d to start", pid)
	return 0
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
