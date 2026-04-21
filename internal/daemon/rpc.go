package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

const jsonRPCVersion = "2.0"

const (
	// Darwin rejects AF_UNIX paths above 103 bytes, so keep the derived socket
	// name under that ceiling and fall back to a shorter stable location when
	// config dirs are deeply nested.
	unixSocketPathMax = 103
)

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type assignRPCParams struct {
	Project    string `json:"project"`
	Issue      string `json:"issue"`
	Prompt     string `json:"prompt"`
	Agent      string `json:"agent"`
	CallerPane string `json:"caller_pane"`
	Title      string `json:"title"`
}

type cancelRPCParams struct {
	Project string `json:"project"`
	Issue   string `json:"issue"`
}

type reloadRPCParams struct {
	Project string `json:"project"`
}

type resumeRPCParams struct {
	Project string `json:"project"`
	Issue   string `json:"issue"`
	Prompt  string `json:"prompt"`
}

type enqueueRPCParams struct {
	Project  string `json:"project"`
	PRNumber int    `json:"pr_number"`
}

type statusRPCParams struct {
	Project string `json:"project"`
	Issue   string `json:"issue,omitempty"`
}

type ProjectStatusRPCResult struct {
	state.ProjectStatus
	BuildCommit string `json:"build_commit,omitempty"`
}

func socketFile(configDir string) string {
	candidates := []string{
		filepath.Join(configDir, "orca.sock"),
		filepath.Join(os.TempDir(), "orca.sock"),
	}
	for _, candidate := range candidates {
		if len(candidate) <= unixSocketPathMax {
			return candidate
		}
	}
	return candidates[len(candidates)-1]
}

func callRPC(ctx context.Context, socketPath, method string, params any, result any) error {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			return ErrDaemonNotRunning
		}
		return fmt.Errorf("dial daemon socket %s: %w", socketPath, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return fmt.Errorf("set daemon socket deadline: %w", err)
		}
	}

	encodedParams, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("marshal %s params: %w", method, err)
	}

	requestID := json.RawMessage(`1`)
	if err := json.NewEncoder(conn).Encode(rpcRequest{
		JSONRPC: jsonRPCVersion,
		ID:      requestID,
		Method:  method,
		Params:  encodedParams,
	}); err != nil {
		return fmt.Errorf("send %s request: %w", method, err)
	}

	var response rpcResponse
	if err := json.NewDecoder(conn).Decode(&response); err != nil {
		return fmt.Errorf("decode %s response: %w", method, err)
	}
	if response.Error != nil {
		return fmt.Errorf("%s rpc failed: %s", method, response.Error.Message)
	}
	if result == nil {
		return nil
	}

	payload, err := json.Marshal(response.Result)
	if err != nil {
		return fmt.Errorf("marshal %s result: %w", method, err)
	}
	if err := json.Unmarshal(payload, result); err != nil {
		return fmt.Errorf("decode %s result: %w", method, err)
	}

	return nil
}

func ProjectStatusRPC(ctx context.Context, paths Paths, projectPath string) (ProjectStatusRPCResult, error) {
	var result ProjectStatusRPCResult
	if err := callRPC(ctx, paths.socketFile(), "status", statusRPCParams{Project: projectPath}, &result); err != nil {
		return ProjectStatusRPCResult{}, err
	}
	return result, nil
}

func handleReloadRPCRequest(request rpcRequest, enqueue func(reloadRPCParams) error) (rpcResponse, bool, bool) {
	if request.Method != "reload" {
		return rpcResponse{}, false, false
	}

	var params reloadRPCParams
	if err := decodeRPCParams(request.Params, &params); err != nil {
		return rpcFailure(request.ID, -32602, fmt.Errorf("decode reload params: %w", err)), false, true
	}
	if enqueue == nil {
		return rpcFailure(request.ID, -32000, errors.New("reload unavailable")), false, true
	}

	if err := enqueue(params); err != nil {
		return rpcFailure(request.ID, -32000, err), false, true
	}

	return rpcSuccess(request.ID, ReloadResult{
		Project: params.Project,
		PID:     os.Getpid(),
	}), true, true
}

func rpcSuccess(id json.RawMessage, result any) rpcResponse {
	return rpcResponse{
		JSONRPC: jsonRPCVersion,
		ID:      id,
		Result:  result,
	}
}

func rpcFailure(id json.RawMessage, code int, err error) rpcResponse {
	return rpcResponse{
		JSONRPC: jsonRPCVersion,
		ID:      id,
		Error: &rpcError{
			Code:    code,
			Message: err.Error(),
		},
	}
}

func decodeRPCParams(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, target)
}

func contextWithOptionalTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}
