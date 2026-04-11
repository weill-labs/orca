package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/project"
)

func TestReloadRPCParamsIncludeProjectField(t *testing.T) {
	t.Parallel()

	field, ok := reflect.TypeOf(reloadRPCParams{}).FieldByName("Project")
	if !ok {
		t.Fatal("reloadRPCParams missing Project field")
	}
	if got, want := field.Tag.Get("json"), "project"; got != want {
		t.Fatalf("reloadRPCParams.Project json tag = %q, want %q", got, want)
	}
}

func TestHandleReloadRPCRequest(t *testing.T) {
	t.Parallel()

	request := rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "reload",
		Params: mustJSON(t, reloadRPCParams{Project: "/repo"}),
	}

	var gotParams reloadRPCParams
	response, shouldReload, handled := handleReloadRPCRequest(request, func(params reloadRPCParams) error {
		gotParams = params
		return nil
	})
	if !handled {
		t.Fatal("handleReloadRPCRequest() = not handled, want handled")
	}
	if !shouldReload {
		t.Fatal("handleReloadRPCRequest() shouldReload = false, want true")
	}
	if got, want := gotParams.Project, "/repo"; got != want {
		t.Fatalf("queued project = %q, want %q", got, want)
	}
	if response.Error != nil {
		t.Fatalf("handleReloadRPCRequest() error = %#v, want success", response.Error)
	}

	payload, err := json.Marshal(response.Result)
	if err != nil {
		t.Fatalf("Marshal(result) error = %v", err)
	}
	var result ReloadResult
	if err := json.Unmarshal(payload, &result); err != nil {
		t.Fatalf("Unmarshal(result) error = %v", err)
	}
	if got, want := result.Project, "/repo"; got != want {
		t.Fatalf("result.Project = %q, want %q", got, want)
	}
	if got, want := result.PID, os.Getpid(); got != want {
		t.Fatalf("result.PID = %d, want %d", got, want)
	}
}

func TestHandleReloadRPCRequestRejectsBadParams(t *testing.T) {
	t.Parallel()

	response, shouldReload, handled := handleReloadRPCRequest(rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "reload",
		Params: json.RawMessage(`{"project":`),
	}, func(reloadRPCParams) error {
		t.Fatal("enqueue called for invalid params")
		return nil
	})
	if !handled {
		t.Fatal("handleReloadRPCRequest() = not handled, want handled")
	}
	if shouldReload {
		t.Fatal("handleReloadRPCRequest() shouldReload = true, want false")
	}
	if response.Error == nil || response.Error.Code != -32602 {
		t.Fatalf("response = %#v, want invalid params error", response)
	}
}

func TestHandleReloadRPCRequestRejectsUnavailableReload(t *testing.T) {
	t.Parallel()

	response, shouldReload, handled := handleReloadRPCRequest(rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "reload",
		Params: mustJSON(t, reloadRPCParams{Project: "/repo"}),
	}, nil)
	if !handled {
		t.Fatal("handleReloadRPCRequest() = not handled, want handled")
	}
	if shouldReload {
		t.Fatal("handleReloadRPCRequest() shouldReload = true, want false")
	}
	if response.Error == nil || response.Error.Code != -32000 {
		t.Fatalf("response = %#v, want unavailable reload error", response)
	}
	if got, want := response.Error.Message, "reload unavailable"; got != want {
		t.Fatalf("response.Error.Message = %q, want %q", got, want)
	}
}

func TestHandleReloadRPCRequestReturnsEnqueueError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("reload busy")
	response, shouldReload, handled := handleReloadRPCRequest(rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "reload",
		Params: mustJSON(t, reloadRPCParams{Project: "/repo"}),
	}, func(reloadRPCParams) error {
		return wantErr
	})
	if !handled {
		t.Fatal("handleReloadRPCRequest() = not handled, want handled")
	}
	if shouldReload {
		t.Fatal("handleReloadRPCRequest() shouldReload = true, want false")
	}
	if response.Error == nil || response.Error.Code != -32000 {
		t.Fatalf("response = %#v, want enqueue error", response)
	}
	if got, want := response.Error.Message, wantErr.Error(); got != want {
		t.Fatalf("response.Error.Message = %q, want %q", got, want)
	}
}

func TestLocalControllerReloadRPC(t *testing.T) {
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

	requests := make(chan rpcRequest, 1)
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
		requests <- req
		_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, ReloadResult{
			Project: projectPath,
			PID:     os.Getpid(),
		}))
	}()

	controller, err := NewLocalController(ControllerOptions{
		Store: store,
		Paths: paths,
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	result, err := controller.Reload(context.Background(), ReloadRequest{Project: projectPath})
	if err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	if got, want := result.Project, projectPath; got != want {
		t.Fatalf("result.Project = %q, want %q", got, want)
	}
	if got, want := result.PID, os.Getpid(); got != want {
		t.Fatalf("result.PID = %d, want %d", got, want)
	}

	reloadReq := <-requests
	if got, want := reloadReq.Method, "reload"; got != want {
		t.Fatalf("reload method = %q, want %q", got, want)
	}
	var params reloadRPCParams
	if err := json.Unmarshal(reloadReq.Params, &params); err != nil {
		t.Fatalf("json.Unmarshal(reload params) error = %v", err)
	}
	if got, want := params, (reloadRPCParams{Project: projectPath}); !reflect.DeepEqual(got, want) {
		t.Fatalf("reload params = %#v, want %#v", got, want)
	}
}

func TestLocalControllerReloadErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		request         func(*testing.T) ReloadRequest
		prepare         func(*testing.T, Paths)
		wantErr         error
		wantErrContains string
	}{
		{
			name: "invalid project path",
			request: func(t *testing.T) ReloadRequest {
				return ReloadRequest{Project: t.TempDir()}
			},
			wantErrContains: "is not inside a git repository",
		},
		{
			name: "daemon not running",
			request: func(t *testing.T) ReloadRequest {
				return ReloadRequest{Project: testProjectPath(t)}
			},
			wantErr: ErrDaemonNotRunning,
		},
		{
			name: "rpc failure",
			request: func(t *testing.T) ReloadRequest {
				return ReloadRequest{Project: testProjectPath(t)}
			},
			prepare: func(t *testing.T, paths Paths) {
				t.Helper()

				if err := os.MkdirAll(paths.PIDDir, 0o755); err != nil {
					t.Fatalf("MkdirAll(%q) error = %v", paths.PIDDir, err)
				}
				if err := os.WriteFile(paths.pidFile(), []byte(strconv.Itoa(os.Getpid())), 0o644); err != nil {
					t.Fatalf("WriteFile(%q) error = %v", paths.pidFile(), err)
				}

				listener, err := listenUnixSocket(paths.socketFile())
				if err != nil {
					t.Fatalf("listenUnixSocket(%q) error = %v", paths.socketFile(), err)
				}
				t.Cleanup(func() {
					_ = listener.Close()
				})

				go func() {
					conn, err := listener.Accept()
					if err != nil {
						return
					}
					var req rpcRequest
					_ = json.NewDecoder(conn).Decode(&req)
					_ = conn.Close()
				}()
			},
			wantErrContains: "decode reload response",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tempDir := t.TempDir()
			paths := Paths{
				ConfigDir: tempDir,
				StateDB:   filepath.Join(tempDir, "state.db"),
				PIDDir:    filepath.Join(tempDir, "pids"),
			}

			if tt.prepare != nil {
				tt.prepare(t, paths)
			}

			controller, err := NewLocalController(ControllerOptions{
				Store: &fakeStore{},
				Paths: paths,
			})
			if err != nil {
				t.Fatalf("NewLocalController() error = %v", err)
			}

			_, err = controller.Reload(context.Background(), tt.request(t))
			if tt.wantErr != nil && !errors.Is(err, tt.wantErr) {
				t.Fatalf("Reload() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErrContains != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErrContains)) {
				t.Fatalf("Reload() error = %v, want message containing %q", err, tt.wantErrContains)
			}
		})
	}
}

func TestInheritedReloadListenerFromEnv(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "orca.sock")
	original, err := listenUnixSocket(socketPath)
	if err != nil {
		t.Fatalf("listenUnixSocket(%q) error = %v", socketPath, err)
	}
	defer original.Close()

	listenerFile, err := inheritedListenerFile(original)
	if err != nil {
		t.Fatalf("inheritedListenerFile() error = %v", err)
	}
	defer listenerFile.Close()

	t.Setenv(daemonListenerFDEnvVar, strconv.Itoa(int(listenerFile.Fd())))

	inherited, inheritedOK, err := inheritedListenerFromEnv(socketPath)
	if err != nil {
		t.Fatalf("inheritedListenerFromEnv() error = %v", err)
	}
	if !inheritedOK {
		t.Fatal("inheritedListenerFromEnv() = not inherited, want inherited listener")
	}
	if got := os.Getenv(daemonListenerFDEnvVar); got != "" {
		t.Fatalf("%s = %q, want cleared after restore", daemonListenerFDEnvVar, got)
	}
	defer inherited.Close()

	if unixListener, ok := original.(*net.UnixListener); ok {
		unixListener.SetUnlinkOnClose(false)
	}
	if err := original.Close(); err != nil {
		t.Fatalf("Close(original) error = %v", err)
	}

	accepted := make(chan error, 1)
	go func() {
		conn, err := inherited.Accept()
		if err != nil {
			accepted <- err
			return
		}
		_ = conn.Close()
		accepted <- nil
	}()

	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		t.Fatalf("Dial(%q) error = %v", socketPath, err)
	}
	_ = conn.Close()

	select {
	case err := <-accepted:
		if err != nil {
			t.Fatalf("Accept() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for inherited listener to accept")
	}
}

func TestRunProcessReloadOverUnixSocket(t *testing.T) {
	reloadWaitTimeout := 15 * time.Second

	home := t.TempDir()
	t.Setenv("HOME", home)

	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(filepath.Join(projectDir, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.git) error = %v", err)
	}
	projectPath, err := project.CanonicalPath(projectDir)
	if err != nil {
		t.Fatalf("CanonicalPath(%q) error = %v", projectDir, err)
	}

	configDir, err := os.MkdirTemp("/tmp", "orca-reload-it-")
	if err != nil {
		t.Fatalf("MkdirTemp(/tmp) error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(configDir)
	})
	paths := Paths{
		ConfigDir: configDir,
		StateDB:   filepath.Join(configDir, "state.db"),
		PIDDir:    filepath.Join(configDir, "pids"),
	}
	stateDB := paths.StateDB
	pidFile := paths.pidFile()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errReloadExec := errors.New("reload exec invoked")
	execCalls := make(chan struct {
		executable string
		args       []string
		env        []string
	}, 1)

	errCh := make(chan error, 1)
	go func() {
		errCh <- runProcess(ctx, ServeRequest{
			Session: "reload-session",
			StateDB: stateDB,
			PIDFile: pidFile,
		}, serveDeps{
			detectOrigin: func(_ string) (string, error) {
				return "git@github.com:weill-labs/orca.git", nil
			},
			amux:       &fakeAmux{captures: make(map[string][]string)},
			commands:   newFakeCommands(),
			poolRunner: stubPoolRunner{},
			execProcess: func(executable string, args, env []string) error {
				fdValue := envValue(env, daemonListenerFDEnvVar)
				if strings.TrimSpace(fdValue) == "" {
					return fmt.Errorf("missing %s", daemonListenerFDEnvVar)
				}
				fd, err := strconv.Atoi(fdValue)
				if err != nil {
					return fmt.Errorf("parse %s: %w", daemonListenerFDEnvVar, err)
				}

				file := os.NewFile(uintptr(fd), "reload-listener")
				if file == nil {
					return fmt.Errorf("listener fd %d was not inherited", fd)
				}
				listener, err := net.FileListener(file)
				if err != nil {
					_ = file.Close()
					return fmt.Errorf("rebuild listener from inherited fd: %w", err)
				}
				_ = listener.Close()
				_ = file.Close()

				execCalls <- struct {
					executable string
					args       []string
					env        []string
				}{
					executable: executable,
					args:       append([]string(nil), args...),
					env:        append([]string(nil), env...),
				}
				return errReloadExec
			},
		})
	}()

	store, err := state.OpenSQLite(stateDB)
	if err != nil {
		t.Fatalf("OpenSQLite(%q) error = %v", stateDB, err)
	}
	defer store.Close()

	socketPath := paths.socketFile()
	deadline := time.Now().Add(reloadWaitTimeout)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("runProcess() returned early: %v", err)
		default:
		}

		if _, err := os.Stat(socketPath); err != nil {
			waitForDuration(t, 25*time.Millisecond)
			continue
		}
		status, err := store.ProjectStatus(context.Background(), "")
		if err == nil && status.Daemon != nil && status.Daemon.Status == "running" {
			goto daemonReady
		}
		waitForDuration(t, 25*time.Millisecond)
	}
	t.Fatalf("timed out waiting for daemon socket %s", socketPath)

daemonReady:

	controller, err := NewLocalController(ControllerOptions{
		Store: store,
		Paths: paths,
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	result, err := controller.Reload(context.Background(), ReloadRequest{Project: projectPath})
	if err != nil {
		t.Fatalf("Reload() error = %v", err)
	}
	if got, want := result.Project, projectPath; got != want {
		t.Fatalf("result.Project = %q, want %q", got, want)
	}
	if got, want := result.PID, os.Getpid(); got != want {
		t.Fatalf("result.PID = %d, want %d", got, want)
	}

	select {
	case call := <-execCalls:
		if strings.TrimSpace(call.executable) == "" {
			t.Fatal("exec executable = empty, want current executable path")
		}
		if got, want := call.args[1], "__daemon-serve"; got != want {
			t.Fatalf("exec args[1] = %q, want %q", got, want)
		}
		if !containsArgPair(call.args, "--state-db", stateDB) {
			t.Fatalf("exec args = %#v, want --state-db %q", call.args, stateDB)
		}
		if !containsArgPair(call.args, "--pid-file", pidFile) {
			t.Fatalf("exec args = %#v, want --pid-file %q", call.args, pidFile)
		}
		if !containsArgPair(call.args, "--session", "reload-session") {
			t.Fatalf("exec args = %#v, want --session reload-session", call.args)
		}
		if got := envValue(call.env, daemonListenerFDEnvVar); strings.TrimSpace(got) == "" {
			t.Fatalf("exec env missing %s", daemonListenerFDEnvVar)
		}
	case <-time.After(reloadWaitTimeout):
		t.Fatal("timed out waiting for reload exec call")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, errReloadExec) {
			t.Fatalf("runProcess() error = %v, want %v", err, errReloadExec)
		}
	case <-time.After(reloadWaitTimeout):
		t.Fatal("timed out waiting for runProcess to exit after reload")
	}
}

func TestDaemonStartAllowsCurrentPIDReuseDuringReload(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	d.allowCurrentPIDReuse = true

	if err := os.WriteFile(deps.pidPath, []byte(strconv.Itoa(os.Getpid())+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", deps.pidPath, err)
	}

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
}

func TestInheritedListenerFileErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		listener        net.Listener
		wantErrContains string
	}{
		{
			name:            "listener does not expose File",
			listener:        &reloadTestListener{},
			wantErrContains: "daemon listener does not expose File",
		},
		{
			name: "listener File returns error",
			listener: &reloadTestFileListener{
				fileErr: errors.New("duplicate failed"),
			},
			wantErrContains: "duplicate daemon listener: duplicate failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := inheritedListenerFile(tt.listener)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Fatalf("inheritedListenerFile() error = %v, want message containing %q", err, tt.wantErrContains)
			}
		})
	}
}

func TestCloseListenerForReloadError(t *testing.T) {
	t.Parallel()

	err := closeListenerForReload(&reloadTestListener{
		closeErr: errors.New("close failed"),
	})
	if err == nil || !strings.Contains(err.Error(), "close daemon listener for reload: close failed") {
		t.Fatalf("closeListenerForReload() error = %v, want close failure", err)
	}
}

func TestReloadProcessPropagatesSetupErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		listener        func(*testing.T) net.Listener
		reloadFile      func(*testing.T) *os.File
		wantErrContains string
	}{
		{
			name: "missing reload listener file",
			listener: func(*testing.T) net.Listener {
				return &reloadTestListener{}
			},
			wantErrContains: "missing reload listener file",
		},
		{
			name: "listener close fails",
			listener: func(t *testing.T) net.Listener {
				t.Helper()

				file, err := os.CreateTemp(t.TempDir(), "reload-listener-*")
				if err != nil {
					t.Fatalf("CreateTemp() error = %v", err)
				}
				t.Cleanup(func() {
					_ = file.Close()
				})
				return &reloadTestFileListener{
					reloadTestListener: reloadTestListener{closeErr: errors.New("close failed")},
					file:               file,
				}
			},
			reloadFile: func(t *testing.T) *os.File {
				t.Helper()

				file, err := os.CreateTemp(t.TempDir(), "reload-inherited-*")
				if err != nil {
					t.Fatalf("CreateTemp() error = %v", err)
				}
				t.Cleanup(func() {
					_ = file.Close()
				})
				return file
			},
			wantErrContains: "close daemon listener for reload: close failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var reloadFile *os.File
			if tt.reloadFile != nil {
				reloadFile = tt.reloadFile(t)
			}
			err := reloadProcess(ServeRequest{}, tt.listener(t), reloadFile, nil, nil)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Fatalf("reloadProcess() error = %v, want message containing %q", err, tt.wantErrContains)
			}
		})
	}
}

func TestReloadProcessDoesNotWaitForDaemonShutdown(t *testing.T) {
	t.Parallel()

	file, err := os.CreateTemp(t.TempDir(), "reload-listener-*")
	if err != nil {
		t.Fatalf("CreateTemp() error = %v", err)
	}
	t.Cleanup(func() {
		_ = file.Close()
	})

	blockedDone := make(chan struct{})
	t.Cleanup(func() {
		close(blockedDone)
	})

	stopCalled := make(chan struct{}, 1)
	instance := &Daemon{
		stopCancel: func() {
			select {
			case stopCalled <- struct{}{}:
			default:
			}
		},
		loopDone: blockedDone,
	}

	errExec := errors.New("reload exec invoked")
	execCalled := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		errCh <- reloadProcess(ServeRequest{}, &reloadTestFileListener{file: file}, file, instance, func(string, []string, []string) error {
			execCalled <- struct{}{}
			return errExec
		})
	}()

	select {
	case <-stopCalled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reloadProcess to cancel daemon")
	}

	select {
	case <-execCalled:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reloadProcess to call execProcess without waiting for daemon shutdown")
	}

	select {
	case err := <-errCh:
		if !errors.Is(err, errExec) {
			t.Fatalf("reloadProcess() error = %v, want %v", err, errExec)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reloadProcess to return exec error")
	}
}

func containsArgPair(args []string, flag, value string) bool {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag && args[i+1] == value {
			return true
		}
	}
	return false
}

func envValue(env []string, key string) string {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

type reloadTestListener struct {
	closeErr error
}

func (l *reloadTestListener) Accept() (net.Conn, error) { return nil, net.ErrClosed }

func (l *reloadTestListener) Close() error { return l.closeErr }

func (l *reloadTestListener) Addr() net.Addr { return reloadTestAddr("reload-test") }

type reloadTestFileListener struct {
	reloadTestListener
	file    *os.File
	fileErr error
}

func (l *reloadTestFileListener) File() (*os.File, error) {
	if l.fileErr != nil {
		return nil, l.fileErr
	}
	if l.file == nil {
		return nil, errors.New("missing listener file")
	}

	fd, err := syscall.Dup(int(l.file.Fd()))
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), l.file.Name()), nil
}

type reloadTestAddr string

func (a reloadTestAddr) Network() string { return "unix" }

func (a reloadTestAddr) String() string { return string(a) }
