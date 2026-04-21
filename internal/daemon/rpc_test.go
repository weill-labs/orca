package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
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
		_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, MergeQueueActionResult{
			Project:  "/repo",
			PRNumber: 42,
			Status:   "queued",
			Position: 1,
		}))
	}()

	var result MergeQueueActionResult
	if err := callRPC(context.Background(), socketPath, "enqueue", enqueueRPCParams{PRNumber: 42}, &result); err != nil {
		t.Fatalf("callRPC() error = %v", err)
	}
	if got, want := result.Status, "queued"; got != want {
		t.Fatalf("result.Status = %q, want %q", got, want)
	}

	if err := callRPC(context.Background(), filepath.Join(t.TempDir(), "missing.sock"), "enqueue", enqueueRPCParams{}, &result); !errors.Is(err, ErrDaemonNotRunning) {
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

func TestCallRPCTreatsConnectionRefusedAsDaemonNotRunning(t *testing.T) {
	t.Parallel()

	socketPath := filepath.Join(t.TempDir(), "orca.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("Listen(%q) error = %v", socketPath, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	var result MergeQueueActionResult
	err = callRPC(context.Background(), socketPath, "enqueue", enqueueRPCParams{PRNumber: 42}, &result)
	if !errors.Is(err, ErrDaemonNotRunning) {
		t.Fatalf("callRPC() connection refused error = %v, want ErrDaemonNotRunning", err)
	}
}

func TestProjectStatusRPC(t *testing.T) {
	t.Parallel()

	projectPath := filepath.Join(t.TempDir(), "project")
	paths := Paths{ConfigDir: t.TempDir()}
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
		_ = json.NewEncoder(conn).Encode(rpcSuccess(req.ID, ProjectStatusRPCResult{
			ProjectStatus: state.ProjectStatus{
				Project: projectPath,
				Daemon:  &state.DaemonStatus{Status: "running"},
			},
			BuildCommit: "abc1234",
		}))
	}()

	result, err := ProjectStatusRPC(context.Background(), paths, projectPath)
	if err != nil {
		t.Fatalf("ProjectStatusRPC() error = %v", err)
	}
	if got, want := result.Project, projectPath; got != want {
		t.Fatalf("result.Project = %q, want %q", got, want)
	}
	if got, want := result.BuildCommit, "abc1234"; got != want {
		t.Fatalf("result.BuildCommit = %q, want %q", got, want)
	}
}

func TestAssignTypesIncludeTitleField(t *testing.T) {
	t.Parallel()

	assignRequestField, ok := reflect.TypeOf(AssignRequest{}).FieldByName("Title")
	if !ok {
		t.Fatal("AssignRequest missing Title field")
	}
	if got, want := assignRequestField.Type.Kind(), reflect.String; got != want {
		t.Fatalf("AssignRequest.Title kind = %v, want %v", got, want)
	}

	assignRPCField, ok := reflect.TypeOf(assignRPCParams{}).FieldByName("Title")
	if !ok {
		t.Fatal("assignRPCParams missing Title field")
	}
	if got, want := assignRPCField.Tag.Get("json"), "title"; got != want {
		t.Fatalf("assignRPCParams.Title json tag = %q, want %q", got, want)
	}
}

func TestAssignTypesIncludeCallerPaneField(t *testing.T) {
	t.Parallel()

	assignRequestField, ok := reflect.TypeOf(AssignRequest{}).FieldByName("CallerPane")
	if !ok {
		t.Fatal("AssignRequest missing CallerPane field")
	}
	if got, want := assignRequestField.Type.Kind(), reflect.String; got != want {
		t.Fatalf("AssignRequest.CallerPane kind = %v, want %v", got, want)
	}

	assignRPCField, ok := reflect.TypeOf(assignRPCParams{}).FieldByName("CallerPane")
	if !ok {
		t.Fatal("assignRPCParams missing CallerPane field")
	}
	if got, want := assignRPCField.Tag.Get("json"), "caller_pane"; got != want {
		t.Fatalf("assignRPCParams.CallerPane json tag = %q, want %q", got, want)
	}
}

func TestResumeTypesIncludePromptField(t *testing.T) {
	t.Parallel()

	resumeRequestField, ok := reflect.TypeOf(ResumeRequest{}).FieldByName("Prompt")
	if !ok {
		t.Fatal("ResumeRequest missing Prompt field")
	}
	if got, want := resumeRequestField.Type.Kind(), reflect.String; got != want {
		t.Fatalf("ResumeRequest.Prompt kind = %v, want %v", got, want)
	}

	resumeRPCField, ok := reflect.TypeOf(resumeRPCParams{}).FieldByName("Prompt")
	if !ok {
		t.Fatal("resumeRPCParams missing Prompt field")
	}
	if got, want := resumeRPCField.Tag.Get("json"), "prompt"; got != want {
		t.Fatalf("resumeRPCParams.Prompt json tag = %q, want %q", got, want)
	}
}

func TestRPCParamsIncludeProjectField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		value    any
		typeName string
	}{
		{name: "assign", value: assignRPCParams{}, typeName: "assignRPCParams"},
		{name: "cancel", value: cancelRPCParams{}, typeName: "cancelRPCParams"},
		{name: "resume", value: resumeRPCParams{}, typeName: "resumeRPCParams"},
		{name: "enqueue", value: enqueueRPCParams{}, typeName: "enqueueRPCParams"},
		{name: "status", value: statusRPCParams{}, typeName: "statusRPCParams"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			field, ok := reflect.TypeOf(tt.value).FieldByName("Project")
			if !ok {
				t.Fatalf("%s missing Project field", tt.typeName)
			}
			if got, want := field.Tag.Get("json"), "project"; got != want {
				t.Fatalf("%s.Project json tag = %q, want %q", tt.typeName, got, want)
			}
		})
	}
}

func TestHandleRPCConnAndDispatchStatusBranches(t *testing.T) {
	t.Parallel()

	client, server := net.Pipe()
	defer client.Close()
	go handleRPCConn(context.Background(), server, nil, nil, "", "/repo")

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
	if err := store.UpsertTask(context.Background(), project, state.Task{
		Issue:     "GH-718",
		Status:    "active",
		Agent:     "codex",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("UpsertTask(GH-718) error = %v", err)
	}

	projectStatus := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "status",
	}, nil, store, "build-804", project)
	if projectStatus.Error != nil {
		t.Fatalf("dispatch status project error = %#v", projectStatus.Error)
	}
	projectStatusPayload, err := json.Marshal(projectStatus.Result)
	if err != nil {
		t.Fatalf("Marshal(project status result) error = %v", err)
	}
	var projectStatusResult map[string]any
	if err := json.Unmarshal(projectStatusPayload, &projectStatusResult); err != nil {
		t.Fatalf("Unmarshal(project status result) error = %v", err)
	}
	if got, want := projectStatusResult["build_commit"], "build-804"; got != want {
		t.Fatalf("project status build_commit = %v, want %q", got, want)
	}

	taskStatus := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "status",
		Params: mustJSON(t, statusRPCParams{Issue: "LAB-718"}),
	}, nil, store, "build-804", project)
	if taskStatus.Error != nil {
		t.Fatalf("dispatch status issue error = %#v", taskStatus.Error)
	}
	taskStatusAlias := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "status",
		Params: mustJSON(t, statusRPCParams{Issue: "#718"}),
	}, nil, store, "build-804", project)
	if taskStatusAlias.Error != nil {
		t.Fatalf("dispatch status github alias error = %#v", taskStatusAlias.Error)
	}
	taskStatusAliasPayload, err := json.Marshal(taskStatusAlias.Result)
	if err != nil {
		t.Fatalf("Marshal(task status github alias result) error = %v", err)
	}
	var taskStatusAliasResult state.TaskStatus
	if err := json.Unmarshal(taskStatusAliasPayload, &taskStatusAliasResult); err != nil {
		t.Fatalf("Unmarshal(task status github alias result) error = %v", err)
	}
	if got, want := taskStatusAliasResult.Task.Issue, "GH-718"; got != want {
		t.Fatalf("status github alias issue = %q, want %q", got, want)
	}

	assignErr := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "assign",
		Params: mustJSON(t, assignRPCParams{Issue: "LAB-718", Agent: "codex"}),
	}, &Daemon{}, store, "build-804", project)
	if assignErr.Error == nil {
		t.Fatalf("dispatch assign error response = %#v, want error", assignErr)
	}

	badParams := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "assign",
		Params: json.RawMessage(`{"issue":`),
	}, &Daemon{}, store, "build-804", project)
	if badParams.Error == nil || badParams.Error.Code != -32602 {
		t.Fatalf("dispatch bad params response = %#v", badParams)
	}

	enqueueErr := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "enqueue",
		Params: mustJSON(t, enqueueRPCParams{PRNumber: 42}),
	}, &Daemon{}, store, "build-804", project)
	if enqueueErr.Error == nil {
		t.Fatalf("dispatch enqueue error response = %#v, want error", enqueueErr)
	}

	unknown := dispatchRPCRequest(context.Background(), rpcRequest{
		ID:     json.RawMessage(`1`),
		Method: "missing",
	}, nil, store, "build-804", project)
	if unknown.Error == nil || unknown.Error.Code != -32601 {
		t.Fatalf("dispatch unknown response = %#v", unknown)
	}
}

func TestHandleRPCConnSetsReadDeadline(t *testing.T) {
	t.Parallel()

	conn := &deadlineConn{}
	start := time.Now()
	handleRPCConn(context.Background(), conn, nil, nil, "", "/repo")

	if conn.readDeadline.IsZero() {
		t.Fatal("read deadline was not set")
	}
	if conn.readDeadline.Before(start.Add(9*time.Second)) || conn.readDeadline.After(start.Add(11*time.Second)) {
		t.Fatalf("read deadline = %v, want about 10s from %v", conn.readDeadline, start)
	}

	var response rpcResponse
	if err := json.NewDecoder(bytes.NewReader(conn.writes.Bytes())).Decode(&response); err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if response.Error == nil || response.Error.Code != -32700 {
		t.Fatalf("response = %#v, want parse error", response)
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

func TestSocketFileFallsBackWhenConfigDirIsTooLong(t *testing.T) {
	t.Parallel()

	configDir := filepath.Join(t.TempDir(), strings.Repeat("a", 48), strings.Repeat("b", 48))

	socketPath := socketFile(configDir)
	if got, want := socketPath, filepath.Join(os.TempDir(), "orca.sock"); got != want {
		t.Fatalf("socketFile() = %q, want %q", got, want)
	}
	if got := len(socketPath); got > unixSocketPathMax {
		t.Fatalf("socket path length = %d, want <= %d (%q)", got, unixSocketPathMax, socketPath)
	}

	listener, err := listenUnixSocket(socketPath)
	if err != nil {
		t.Fatalf("listenUnixSocket(%q) error = %v", socketPath, err)
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

type deadlineConn struct {
	writes       bytes.Buffer
	readDeadline time.Time
}

func (c *deadlineConn) Read(_ []byte) (int, error)        { return 0, io.EOF }
func (c *deadlineConn) Write(p []byte) (int, error)       { return c.writes.Write(p) }
func (c *deadlineConn) Close() error                      { return nil }
func (c *deadlineConn) LocalAddr() net.Addr               { return dummyAddr("local") }
func (c *deadlineConn) RemoteAddr() net.Addr              { return dummyAddr("remote") }
func (c *deadlineConn) SetDeadline(t time.Time) error     { c.readDeadline = t; return nil }
func (c *deadlineConn) SetReadDeadline(t time.Time) error { c.readDeadline = t; return nil }
func (c *deadlineConn) SetWriteDeadline(time.Time) error  { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return "test" }
func (a dummyAddr) String() string  { return string(a) }
