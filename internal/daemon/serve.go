package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	"github.com/weill-labs/orca/internal/config"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/project"
)

const orcaPoolSubdir = ".orca/pool"

type ServeRequest struct {
	Session  string
	Project  string
	LeadPane string
	StateDB  string
	PIDFile  string
}

type serveDeps struct {
	detectOrigin func(projectDir string) (string, error)
	amux         AmuxClient
	commands     CommandRunner
	events       EventSink
	poolRunner   pool.Runner
	socketPath   string
}

func RunProcess(ctx context.Context, req ServeRequest) error {
	return runProcess(ctx, req, serveDeps{})
}

func runProcess(ctx context.Context, req ServeRequest, deps serveDeps) error {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return err
	}

	store, err := state.OpenSQLite(req.StateDB)
	if err != nil {
		return err
	}
	defer store.Close()

	detectOrigin := deps.detectOrigin
	if detectOrigin == nil {
		detectOrigin = config.DetectOrigin
	}

	origin, err := detectOrigin(projectPath)
	if err != nil {
		return fmt.Errorf("detect origin: %w", err)
	}

	poolDir := filepath.Join(projectPath, orcaPoolSubdir)
	if err := os.MkdirAll(poolDir, 0o755); err != nil {
		return fmt.Errorf("create pool directory: %w", err)
	}

	amuxClient := deps.amux
	if amuxClient == nil {
		amuxClient = amux.NewClient(amux.Config{
			Session: req.Session,
		})
	}

	managerOptions := []pool.Option{
		pool.WithCWDUsageChecker(amuxCWDUsageChecker{amux: amuxClient}),
	}
	if deps.poolRunner != nil {
		managerOptions = append(managerOptions, pool.WithRunner(deps.poolRunner))
	}
	poolCfg := internalPoolConfig{poolDir: poolDir, origin: origin}
	manager, err := pool.New(projectPath, poolCfg, store, managerOptions...)
	if err != nil {
		return fmt.Errorf("create pool manager: %w", err)
	}

	commandRunner := deps.commands
	if commandRunner == nil {
		commandRunner = execCommandRunner{}
	}

	issueTracker, err := newLinearIssueTrackerFromEnv()
	if err != nil {
		return fmt.Errorf("configure Linear issue tracker: %w", err)
	}

	socketPath := deps.socketPath
	if socketPath == "" {
		socketPath = socketFileForProject(filepath.Dir(req.StateDB), projectPath)
	}

	listener, err := listenUnixSocket(socketPath)
	if err != nil {
		return err
	}
	defer func() {
		_ = listener.Close()
		_ = os.Remove(socketPath)
	}()

	daemonState := newSQLiteStateAdapter(store)
	instance, err := New(Options{
		Project:          projectPath,
		Session:          req.Session,
		LeadPane:         req.LeadPane,
		PIDPath:          req.PIDFile,
		Config:           builtinConfigProvider{},
		State:            daemonState,
		Pool:             pool.NewAdapter(manager),
		Amux:             amuxClient,
		IssueTracker:     issueTracker,
		Commands:         commandRunner,
		Events:           deps.events,
		MergeGracePeriod: defaultMergeGracePeriod,
	})
	if err != nil {
		return fmt.Errorf("create daemon: %w", err)
	}

	if err := instance.Start(ctx); err != nil {
		return err
	}

	startedAt := time.Now().UTC()
	if err := store.UpsertDaemon(ctx, projectPath, state.DaemonStatus{
		Session:   req.Session,
		PID:       os.Getpid(),
		Status:    "running",
		StartedAt: startedAt,
		UpdatedAt: startedAt,
	}); err != nil {
		_ = instance.Stop(context.Background())
		return err
	}

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- serveRPC(ctx, listener, instance, store, projectPath)
	}()

	var serverErr error
	select {
	case <-ctx.Done():
		_ = listener.Close()
		serverErr = <-serverErrCh
	case serverErr = <-serverErrCh:
	}

	stopErr := instance.Stop(context.Background())
	markStoppedErr := store.MarkDaemonStopped(context.Background(), projectPath, time.Now().UTC())

	if serverErr != nil && !errors.Is(serverErr, net.ErrClosed) {
		return errors.Join(serverErr, stopErr, markStoppedErr)
	}
	return errors.Join(stopErr, markStoppedErr)
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

func serveRPC(ctx context.Context, listener net.Listener, instance *Daemon, store *state.SQLiteStore, projectPath string) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept daemon socket connection: %w", err)
		}

		go handleRPCConn(ctx, conn, instance, store, projectPath)
	}
}

func handleRPCConn(ctx context.Context, conn net.Conn, instance *Daemon, store *state.SQLiteStore, projectPath string) {
	defer conn.Close()
	if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		_ = json.NewEncoder(conn).Encode(rpcFailure(nil, -32000, fmt.Errorf("set read deadline: %w", err)))
		return
	}

	var request rpcRequest
	if err := json.NewDecoder(conn).Decode(&request); err != nil {
		_ = json.NewEncoder(conn).Encode(rpcFailure(nil, -32700, fmt.Errorf("decode request: %w", err)))
		return
	}

	response := dispatchRPCRequest(ctx, request, instance, store, projectPath)
	_ = json.NewEncoder(conn).Encode(response)
}

func dispatchRPCRequest(ctx context.Context, request rpcRequest, instance *Daemon, store *state.SQLiteStore, projectPath string) rpcResponse {
	switch request.Method {
	case "assign":
		var params assignRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode assign params: %w", err))
		}
		if err := instance.Assign(ctx, params.Issue, params.Prompt, params.Agent, params.Title); err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		result, err := taskActionResultForIssue(ctx, store, projectPath, params.Issue)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, result)
	case "cancel":
		var params cancelRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode cancel params: %w", err))
		}
		if err := instance.Cancel(ctx, params.Issue); err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		result, err := taskActionResultForIssue(ctx, store, projectPath, params.Issue)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, result)
	case "resume":
		var params resumeRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode resume params: %w", err))
		}
		if err := instance.Resume(ctx, params.Issue); err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		result, err := taskActionResultForIssue(ctx, store, projectPath, params.Issue)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, result)
	case "enqueue":
		var params enqueueRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode enqueue params: %w", err))
		}

		result, err := instance.Enqueue(ctx, params.PRNumber)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		return rpcSuccess(request.ID, result)
	case "status":
		var params statusRPCParams
		if err := decodeRPCParams(request.Params, &params); err != nil {
			return rpcFailure(request.ID, -32602, fmt.Errorf("decode status params: %w", err))
		}
		if params.Issue == "" {
			status, err := store.ProjectStatus(ctx, projectPath)
			if err != nil {
				return rpcFailure(request.ID, -32000, err)
			}
			return rpcSuccess(request.ID, status)
		}

		status, err := store.TaskStatus(ctx, projectPath, params.Issue)
		if err != nil {
			return rpcFailure(request.ID, -32000, err)
		}
		return rpcSuccess(request.ID, status)
	default:
		return rpcFailure(request.ID, -32601, fmt.Errorf("unknown method %q", request.Method))
	}
}

func taskActionResultForIssue(ctx context.Context, store *state.SQLiteStore, projectPath, issue string) (TaskActionResult, error) {
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
