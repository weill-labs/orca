package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	"github.com/weill-labs/orca/internal/config"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/project"
)

const orcaPoolSubdir = ".orca/pool"
const daemonListenerFDEnvVar = "ORCA_DAEMON_LISTENER_FD"

var (
	openSQLiteDaemonStore = func(path string) (daemonStateStore, error) {
		return state.OpenSQLite(path)
	}
	openPostgresDaemonStore = func(dsn string) (daemonStateStore, error) {
		return state.OpenPostgres(dsn)
	}
)

type ServeRequest struct {
	Session     string
	StateDB     string
	PIDFile     string
	BuildCommit string
}

type serveDeps struct {
	detectOrigin func(projectDir string) (string, error)
	amux         AmuxClient
	commands     CommandRunner
	events       EventSink
	poolRunner   pool.Runner
	socketPath   string
	execProcess  func(string, []string, []string) error
}

type daemonLifecycle interface {
	Start(context.Context) error
	Stop(context.Context) error
}

func RunProcess(ctx context.Context, req ServeRequest) error {
	return runProcess(ctx, req, serveDeps{})
}

func runProcess(ctx context.Context, req ServeRequest, deps serveDeps) error {
	store, err := openDaemonStore(req.StateDB)
	if err != nil {
		return err
	}
	defer store.Close()

	detectOrigin := deps.detectOrigin
	if detectOrigin == nil {
		detectOrigin = config.DetectOrigin
	}

	amuxClient := deps.amux
	if amuxClient == nil {
		amuxClient = amux.NewClient(amux.Config{
			Session: req.Session,
		})
	}

	commandRunner := deps.commands
	if commandRunner == nil {
		commandRunner = execCommandRunner{}
	}
	relayURL := strings.TrimSpace(os.Getenv("ORCA_RELAY_URL"))
	relayToken := strings.TrimSpace(os.Getenv("ORCA_RELAY_TOKEN"))
	hostname, _ := os.Hostname()

	issueTracker, err := newLinearIssueTrackerFromEnv()
	if err != nil {
		return fmt.Errorf("configure Linear issue tracker: %w", err)
	}

	socketPath := deps.socketPath
	if socketPath == "" {
		socketPath = socketFile(filepath.Dir(req.StateDB))
	}

	listener, inheritedListener, err := inheritedListenerFromEnv(socketPath)
	if err != nil {
		return err
	}
	if listener == nil {
		listener, err = listenUnixSocket(socketPath)
		if err != nil {
			return err
		}
	}
	defer func() {
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}()

	daemonState := newSQLiteStateAdapter(store)
	daemonPool := newMultiProjectPool(store, detectOrigin, amuxClient, deps.poolRunner)
	startedAt := time.Now().UTC()
	statusWriter := newSQLiteDaemonStatusWriter(store, "", req.Session, os.Getpid(), startedAt)
	instance, err := New(Options{
		Project:              "",
		Session:              req.Session,
		PIDPath:              req.PIDFile,
		AllowCurrentPIDReuse: inheritedListener,
		Config:               builtinConfigProvider{},
		State:                daemonState,
		Pool:                 daemonPool,
		Amux:                 amuxClient,
		IssueTracker:         issueTracker,
		Commands:             commandRunner,
		Events:               deps.events,
		NewWatchdogTicker: func(interval time.Duration) Ticker {
			return realTicker{Ticker: time.NewTicker(interval)}
		},
		MergeGracePeriod:   defaultMergeGracePeriod,
		DaemonStatusWriter: statusWriter,
		Logf:               log.Printf,
		RelayURL:           relayURL,
		RelayToken:         relayToken,
		Hostname:           strings.TrimSpace(hostname),
		DetectOrigin:       detectOrigin,
	})
	if err != nil {
		return fmt.Errorf("create daemon: %w", err)
	}

	if err := startDaemonLifecycle(ctx, instance, statusWriter, startedAt, func(stopCtx context.Context, updatedAt time.Time) error {
		return store.MarkDaemonStopped(stopCtx, "", updatedAt)
	}); err != nil {
		return err
	}

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- serveRPC(ctx, listener, req, instance, store, req.BuildCommit, deps.execProcess)
	}()

	var serverErr error
	select {
	case <-ctx.Done():
		_ = listener.Close()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
	}

	stopErr := instance.Stop(context.Background())
	markStoppedErr := store.MarkDaemonStopped(context.Background(), "", time.Now().UTC())

	if serverErr != nil && !errors.Is(serverErr, net.ErrClosed) {
		return errors.Join(serverErr, stopErr, markStoppedErr)
	}
	return errors.Join(stopErr, markStoppedErr)
}

func startDaemonLifecycle(ctx context.Context, instance daemonLifecycle, statusWriter daemonStatusWriter, startedAt time.Time, markStopped func(context.Context, time.Time) error) error {
	if err := instance.Start(ctx); err != nil {
		if markStopped != nil {
			_ = markStopped(context.Background(), time.Now().UTC())
		}
		return err
	}

	if statusWriter == nil {
		return nil
	}
	if err := statusWriter.Update(ctx, daemonStatusRunning, startedAt); err != nil {
		stopErr := instance.Stop(context.Background())
		var markStoppedErr error
		if markStopped != nil {
			markStoppedErr = markStopped(context.Background(), time.Now().UTC())
		}
		return errors.Join(err, stopErr, markStoppedErr)
	}

	return nil
}

func listenUnixSocket(socketPath string) (net.Listener, error) {
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		return nil, fmt.Errorf("create daemon socket directory: %w", err)
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove stale daemon socket: %w", err)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("listen on daemon socket %s: %w", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("chmod daemon socket %s: %w", socketPath, err)
	}
	return listener, nil
}

func inheritedListenerFromEnv(socketPath string) (net.Listener, bool, error) {
	rawFD := strings.TrimSpace(os.Getenv(daemonListenerFDEnvVar))
	if rawFD == "" {
		return nil, false, nil
	}
	if err := os.Unsetenv(daemonListenerFDEnvVar); err != nil {
		return nil, false, fmt.Errorf("clear %s: %w", daemonListenerFDEnvVar, err)
	}

	fd, err := strconv.Atoi(rawFD)
	if err != nil {
		return nil, false, fmt.Errorf("parse %s: %w", daemonListenerFDEnvVar, err)
	}

	file := os.NewFile(uintptr(fd), socketPath)
	if file == nil {
		return nil, false, fmt.Errorf("open inherited daemon listener fd %d", fd)
	}
	defer file.Close()

	listener, err := net.FileListener(file)
	if err != nil {
		return nil, false, fmt.Errorf("restore inherited daemon listener: %w", err)
	}
	return listener, true, nil
}

func inheritedListenerFile(listener net.Listener) (*os.File, error) {
	fileListener, ok := listener.(interface {
		File() (*os.File, error)
	})
	if !ok {
		return nil, fmt.Errorf("daemon listener does not expose File")
	}

	file, err := fileListener.File()
	if err != nil {
		return nil, fmt.Errorf("duplicate daemon listener: %w", err)
	}

	// net.UnixListener.File already duplicates the socket fd, but the returned
	// *os.File owns that duplicate. Take one more dup so the inherited descriptor
	// lifetime is independent of the temporary File wrapper and survives exec.
	name := file.Name()
	fd, err := syscall.Dup(int(file.Fd()))
	_ = file.Close()
	if err != nil {
		return nil, fmt.Errorf("duplicate inherited daemon listener: %w", err)
	}
	return os.NewFile(uintptr(fd), name), nil
}

func reloadProcess(req ServeRequest, listener net.Listener, instance *Daemon, execProcess func(string, []string, []string) error) error {
	reloadListenerFile, err := inheritedListenerFile(listener)
	if err != nil {
		return err
	}
	defer reloadListenerFile.Close()
	if err := closeListenerForReload(listener); err != nil {
		return err
	}

	stopDaemonForReload(instance)

	executable, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve current executable: %w", err)
	}
	if execProcess == nil {
		execProcess = syscall.Exec
	}

	env := append(os.Environ(), fmt.Sprintf("%s=%d", daemonListenerFDEnvVar, reloadListenerFile.Fd()))
	return execProcess(executable, daemonServeArgs(executable, req), env)
}

func closeListenerForReload(listener net.Listener) error {
	if unixListener, ok := listener.(*net.UnixListener); ok {
		unixListener.SetUnlinkOnClose(false)
	}
	if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("close daemon listener for reload: %w", err)
	}
	return nil
}

func stopDaemonForReload(instance *Daemon) {
	if instance == nil {
		return
	}
	// Reload must hand off to exec promptly after the RPC response is sent.
	// Waiting for daemon goroutines to drain here can stall hot reload behind a
	// slow background loop. On exec failure, runProcess still falls back to the
	// normal Stop path, which waits for full teardown before returning.
	if instance.stopCancel != nil {
		instance.stopCancel()
	}
}

func daemonServeArgs(executable string, req ServeRequest) []string {
	return []string{
		executable,
		"__daemon-serve",
		"--session", req.Session,
		"--state-db", req.StateDB,
		"--pid-file", req.PIDFile,
	}
}

func serveRPC(ctx context.Context, listener net.Listener, req ServeRequest, instance *Daemon, store daemonStateStore, buildCommit string, execProcess func(string, []string, []string) error) error {
	reloadReserved := false
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept daemon socket connection: %w", err)
		}

		request, response, err := readRPCRequest(conn)
		if err != nil {
			_ = json.NewEncoder(conn).Encode(response)
			_ = conn.Close()
			continue
		}

		reserveReload := func(reloadRPCParams) error {
			if reloadReserved {
				return errors.New("daemon reload already in progress")
			}
			reloadReserved = true
			return nil
		}
		if response, shouldReload, handled := handleReloadRPCRequest(request, reserveReload); handled {
			if err := json.NewEncoder(conn).Encode(response); err != nil {
				reloadReserved = false
				_ = conn.Close()
				continue
			}
			_ = conn.Close()
			if shouldReload {
				return reloadProcess(req, listener, instance, execProcess)
			}
			continue
		}

		go handleRPCRequest(ctx, conn, request, instance, store, buildCommit)
	}
}

func handleRPCConn(ctx context.Context, conn net.Conn, instance *Daemon, store daemonStateStore, buildCommit string, defaultProject ...string) {
	defer conn.Close()
	request, response, err := readRPCRequest(conn)
	if err != nil {
		_ = json.NewEncoder(conn).Encode(response)
		return
	}

	response = dispatchRPCRequest(ctx, request, instance, store, buildCommit, defaultProject...)
	_ = json.NewEncoder(conn).Encode(response)
}

func handleRPCRequest(ctx context.Context, conn net.Conn, request rpcRequest, instance *Daemon, store daemonStateStore, buildCommit string, defaultProject ...string) {
	defer conn.Close()

	response := dispatchRPCRequest(ctx, request, instance, store, buildCommit, defaultProject...)
	_ = json.NewEncoder(conn).Encode(response)
}

func readRPCRequest(conn net.Conn) (rpcRequest, rpcResponse, error) {
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return rpcRequest{}, rpcFailure(nil, -32000, fmt.Errorf("set read deadline: %w", err)), err
	}

	var request rpcRequest
	if err := json.NewDecoder(conn).Decode(&request); err != nil {
		return rpcRequest{}, rpcFailure(nil, -32700, fmt.Errorf("decode request: %w", err)), err
	}
	return request, rpcResponse{}, nil
}

func dispatchRPCRequest(ctx context.Context, request rpcRequest, instance *Daemon, store daemonStateStore, buildCommit string, defaultProject ...string) rpcResponse {
	switch request.Method {
	case "assign":
		var params assignRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode assign params: %w", err))
		}
		projectPath, ok, failure := requireRPCProject(request.ID, params.Project, defaultProject...)
		if !ok {
			return failure
		}
		issue := normalizeIssueIdentifier(params.Issue)
		if err := instance.assign(ctx, projectPath, issue, params.Prompt, params.Agent, params.CallerPane, params.Title); err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		result, err := taskActionResultForIssue(ctx, store, projectPath, issue)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, result)
	case "batch":
		var params batchRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode batch params: %w", err))
		}
		projectPath, ok, failure := requireRPCProject(request.ID, params.Project, defaultProject...)
		if !ok {
			return failure
		}
		delay := time.Duration(0)
		if raw := strings.TrimSpace(params.Delay); raw != "" {
			parsedDelay, err := time.ParseDuration(raw)
			if err != nil {
				return rpcFailure(request.ID, -32602, fmt.Errorf("parse batch delay: %w", err))
			}
			delay = parsedDelay
		}

		result, err := instance.Batch(ctx, BatchRequest{
			Project:    projectPath,
			Entries:    params.Entries,
			Delay:      delay,
			CallerPane: params.CallerPane,
		})
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, result)
	case "cancel":
		var params cancelRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode cancel params: %w", err))
		}
		projectPath, ok, failure := requireRPCProject(request.ID, params.Project, defaultProject...)
		if !ok {
			return failure
		}
		issue := normalizeIssueIdentifier(params.Issue)
		if err := instance.cancel(ctx, projectPath, issue); err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		result, err := taskActionResultForIssue(ctx, store, projectPath, issue)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, result)
	case "resume":
		var params resumeRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode resume params: %w", err))
		}
		projectPath, ok, failure := requireRPCProject(request.ID, params.Project, defaultProject...)
		if !ok {
			return failure
		}
		issue := normalizeIssueIdentifier(params.Issue)
		if err := instance.resume(ctx, projectPath, issue, params.Prompt); err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		result, err := taskActionResultForIssue(ctx, store, projectPath, issue)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, result)
	case "enqueue":
		var params enqueueRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode enqueue params: %w", err))
		}
		projectPath, ok, failure := requireRPCProject(request.ID, params.Project, defaultProject...)
		if !ok {
			return failure
		}

		result, err := instance.enqueue(ctx, projectPath, params.PRNumber)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		return rpcSuccess(request.ID, result)
	case "status":
		var params statusRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode status params: %w", err))
		}
		projectPath, failure := resolveRPCProject(request.ID, params.Project, defaultProject...)
		if failure.Error != nil {
			return failure
		}
		if params.Issue == "" {
			status, err := store.ProjectStatus(ctx, projectPath)
			if err != nil {
				return rpcFailure(request.ID, -32000, err)
			}
			return rpcSuccess(request.ID, ProjectStatusRPCResult{
				ProjectStatus: status,
				BuildCommit:   buildCommit,
			})
		}

		status, err := store.TaskStatus(ctx, projectPath, normalizeIssueIdentifier(params.Issue))
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, status)
	default:
		return rpcFailure(request.ID, -32601, fmt.Errorf("unknown method %q", request.Method))
	}
}

func taskActionResultForIssue(ctx context.Context, store daemonStateStore, projectPath, issue string) (TaskActionResult, error) {
	taskStatus, err := store.TaskStatus(ctx, projectPath, issue)
	if err != nil {
		return TaskActionResult{}, err
	}

	return TaskActionResult{
		Project:   projectPath,
		Issue:     taskStatus.Task.Issue,
		Status:    taskStatus.Task.Status,
		Agent:     taskStatus.Task.Agent,
		UpdatedAt: taskStatus.Task.UpdatedAt,
	}, nil
}

func openDaemonStore(stateDBPath string) (daemonStateStore, error) {
	backend, err := config.ResolveStateBackend(stateDBPath)
	if err != nil {
		return nil, err
	}
	if backend.Kind == config.StateBackendSQLite {
		return openSQLiteDaemonStore(backend.SQLitePath)
	}
	return openPostgresDaemonStore(backend.DSN)
}

func resolveRPCProject(id json.RawMessage, rawProject string, defaultProject ...string) (string, rpcResponse) {
	projectPath := strings.TrimSpace(rawProject)
	if projectPath == "" && len(defaultProject) > 0 {
		return strings.TrimSpace(defaultProject[0]), rpcSuccess(id, nil)
	}
	if projectPath == "" {
		return "", rpcSuccess(id, nil)
	}
	resolvedProject, err := project.CanonicalPath(projectPath)
	if err != nil {
		return "", rpcFailure(id, -32602, fmt.Errorf("resolve project: %w", err))
	}
	return resolvedProject, rpcSuccess(id, nil)
}

func requireRPCProject(id json.RawMessage, rawProject string, defaultProject ...string) (string, bool, rpcResponse) {
	projectPath, response := resolveRPCProject(id, rawProject, defaultProject...)
	if response.Error != nil {
		return "", false, response
	}
	if projectPath == "" {
		return "", false, rpcFailure(id, -32602, fmt.Errorf("project is required"))
	}
	return projectPath, true, rpcResponse{}
}
