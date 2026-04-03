package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestCallRPCAndHelpers(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(t.TempDir(), "orca.sock")
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
		_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, TaskActionResult{
			Project: "/repo",
			Issue:   "LAB-718",
			Status:  TaskStatusActive,
		}))
	}()

	var result TaskActionResult
	if err := callRPC(context.Background(), socketPath, "assign", assignRPCParams{Issue: "LAB-718"}, &result); err != nil {
		t.Fatalf("callRPC() error = %v", err)
	}
	if got, want := result.Status, TaskStatusActive; got != want {
		t.Fatalf("result.Status = %q, want %q", got, want)
	}

	if err := callRPC(context.Background(), filepath.Join(t.TempDir(), "missing.sock"), "assign", assignRPCParams{}, &result); !errors.Is(err, ErrDaemonNotRunning) {
		t.Fatalf("callRPC() missing socket error = %v, want ErrDaemonNotRunning", err)
	}

	ctx, cancel := contextWithOptionalTimeout(context.Background(), 25*time.Millisecond)
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("contextWithOptionalTimeout() missing deadline")
	}
	cancel()
	ctx, cancel = contextWithOptionalTimeout(context.Background(), 0)
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("contextWithOptionalTimeout() unexpected deadline")
	}
	cancel()

	if err := decodeRPCParams(nil, &assignRPCParams{}); err != nil {
		t.Fatalf("decodeRPCParams(nil) error = %v", err)
	}
	response := rpcFailure(json.RawMessage(`1`), -32000, errors.New("boom"))
	if response.Error == nil || response.Error.Message != "boom" {
		t.Fatalf("rpcFailure() = %#v", response)
	}
}

func TestHandleRPCConnAndDispatchStatusBranches(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer client.Close()
	go handleRPCConn(context.Background(), server, nil, nil, "/repo")

	if _, err := client.Write([]byte("not-json\n")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	var response rpcResponse
	if err := json.NewDecoder(client).Decode(&response); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if response.Error == nil || response.Error.Code != -32700 {
		t.Fatalf("parse error response = %#v", response)
	}

	store := openDaemonStateStore(t)
	project := "/repo"
	if err := store.UpsertTask(context.Background(), project, state.Task{
		Issue:     "LAB-718",
		Status:    "active",
		Agent:     "codex",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	projectStatus := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "status",
	}, nil, store, project)
	if projectStatus.Error != nil {
		t.Fatalf("dispatch status project error = %#v", projectStatus.Error)
	}

	taskStatus := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "status",
		Params: mustJSON(t, statusRPCParams{Issue: "LAB-718"}),
	}, nil, store, project)
	if taskStatus.Error != nil {
		t.Fatalf("dispatch status issue error = %#v", taskStatus.Error)
	}

	assignErr := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "assign",
		Params: mustJSON(t, assignRPCParams{Issue: "LAB-718", Agent: "codex"}),
	}, &Daemon{}, store, project)
	if assignErr.Error == nil {
		t.Fatalf("dispatch assign error response = %#v, want error", assignErr)
	}

	badParams := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "assign",
		Params: json.RawMessage(`{"issue":`),
	}, &Daemon{}, store, project)
	if badParams.Error == nil || badParams.Error.Code != -32602 {
		t.Fatalf("dispatch bad params response = %#v", badParams)
	}

	unknown := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "missing",
	}, nil, store, project)
	if unknown.Error == nil || unknown.Error.Code != -32601 {
		t.Fatalf("dispatch unknown response = %#v", unknown)
	}
}

func TestListenUnixSocketRemovesStaleFile(t *testing.T) {
	t.Parallel()

	socketDir, err := os.MkdirTemp("/tmp", "orca-rpc-")
	if err != nil {
		t.Fatalf("MkdirTemp(/tmp) error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(socketDir)
	})
	socketPath := filepath.Join(socketDir, "orca.sock")
	if err := os.WriteFile(socketPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", socketPath, err)
	}

	listener, err := listenUnixSocket(socketPath)
	if err != nil {
		t.Fatalf("listenUnixSocket() error = %v", err)
	}
	defer listener.Close()

	if _, err := os.Stat(socketPath); err != nil {
		t.Fatalf("Stat(%q) error = %v", socketPath, err)
	}
}

func mustJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return data
}
