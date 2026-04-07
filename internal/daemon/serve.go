package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
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
	store, err := state.OpenSQLite(req.StateDB)
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

	issueTracker, err := newLinearIssueTrackerFromEnv()
	if err != nil {
		return fmt.Errorf("configure Linear issue tracker: %w", err)
	}

	socketPath := deps.socketPath
	if socketPath == "" {
		socketPath = socketFileForProject(filepath.Dir(req.StateDB), "")
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
	daemonPool := newMultiProjectPool(store, detectOrigin, amuxClient, deps.poolRunner)
	instance, err := New(Options{
		Project:          "",
		Session:          req.Session,
		LeadPane:         req.LeadPane,
		PIDPath:          req.PIDFile,
		Config:           builtinConfigProvider{},
		State:            daemonState,
		Pool:             daemonPool,
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
	if err := store.UpsertDaemon(ctx, "", state.DaemonStatus{
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
		serverErrCh <- serveRPC(ctx, listener, instance, store)
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

func serveRPC(ctx context.Context, listener net.Listener, instance *Daemon, store *state.SQLiteStore) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return fmt.Errorf("accept daemon socket connection: %w", err)
		}

		go handleRPCConn(ctx, conn, instance, store)
	}
}

func handleRPCConn(ctx context.Context, conn net.Conn, instance *Daemon, store *state.SQLiteStore, defaultProject ...string) {
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

	response := dispatchRPCRequest(ctx, request, instance, store, defaultProject...)
	_ = json.NewEncoder(conn).Encode(response)
}

func dispatchRPCRequest(ctx context.Context, request rpcRequest, instance *Daemon, store *state.SQLiteStore, defaultProject ...string) rpcResponse {
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
		if err := instance.assign(ctx, projectPath, params.Issue, params.Prompt, params.Agent, params.Title); err != nil {
			return rpcFailure(request.ID, -32000, err)
		}

		result, err := taskActionResultForIssue(ctx, store, projectPath, params.Issue)
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
			Project: projectPath,
			Entries: params.Entries,
			Delay:   delay,
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
		if err := instance.cancel(ctx, projectPath, params.Issue); err != nil {
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
		projectPath, ok, failure := requireRPCProject(request.ID, params.Project, defaultProject...)
		if !ok {
			return failure
		}
		if err := instance.resume(ctx, projectPath, params.Issue, params.Prompt); err != nil {
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
