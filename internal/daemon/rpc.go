package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"
)

const jsonRPCVersion = "2.0"

const (
	// Darwin rejects AF_UNIX paths above 103 bytes, so keep the derived socket
	// name under that ceiling and fall back to a shorter stable location when
	// config dirs are deeply nested.
	unixSocketPathMax   = 103
	socketHashByteCount = 16
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
	Issue  string `json:"issue"`
	Prompt string `json:"prompt"`
	Agent  string `json:"agent"`
}

type cancelRPCParams struct {
	Issue string `json:"issue"`
}

type enqueueRPCParams struct {
	PRNumber int `json:"pr_number"`
}

type statusRPCParams struct {
	Issue string `json:"issue,omitempty"`
}

func projectHash(projectPath string) string {
	sum := sha256.Sum256([]byte(projectPath))
	return hex.EncodeToString(sum[:])
}

func socketFileForProject(configDir, projectPath string) string {
	candidates := []string{
		filepath.Join(configDir, fmt.Sprintf("orca-%s.sock", projectHash(projectPath))),
		filepath.Join(configDir, fmt.Sprintf("orca-%s.sock", shortProjectHash(projectPath, socketHashByteCount))),
		filepath.Join(os.TempDir(), fmt.Sprintf("orca-%s.sock", shortProjectHash(projectPath, socketHashByteCount))),
	}
	for _, candidate := range candidates {
		if len(candidate) <= unixSocketPathMax {
			return candidate
		}
	}
	return candidates[len(candidates)-1]
}

func shortProjectHash(projectPath string, bytes int) string {
	sum := sha256.Sum256([]byte(projectPath))
	if bytes <= 0 || bytes > len(sum) {
		bytes = len(sum)
	}
	return hex.EncodeToString(sum[:bytes])
}

func callRPC(ctx context.Context, socketPath, method string, params any, result any) error {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "unix", socketPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
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
