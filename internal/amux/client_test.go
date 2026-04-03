package amux

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

type recordedCommand struct {
	name string
	args []string
}

type fakeRunner struct {
	output []byte
	err    error
	calls  []recordedCommand
}

func (r *fakeRunner) Run(_ context.Context, name string, args []string) ([]byte, error) {
	r.calls = append(r.calls, recordedCommand{
		name: name,
		args: append([]string(nil), args...),
	})
	return r.output, r.err
}

func newTestClient(config Config, runner commandRunner) *CLIClient {
	return &CLIClient{
		binary:  defaultBinary(config.Binary),
		session: config.Session,
		runner:  runner,
	}
}

func TestCLIClientSpawn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   Config
		req      SpawnRequest
		output   string
		runErr   error
		wantCmds []recordedCommand
		wantPane Pane
		wantErr  string
	}{
		{
			name: "spawns pane then sends cd and command via send-keys",
			config: Config{
				Binary:  "/usr/local/bin/amux",
				Session: "orca-dev",
			},
			req: SpawnRequest{
				Session: "override-session",
				CWD:     "/tmp/clone-01",
				Command: "codex --yolo",
			},
			output: "Spawned clone-01 in pane 7\n",
			wantCmds: []recordedCommand{
				{name: "/usr/local/bin/amux", args: []string{"-s", "override-session", "spawn", "--name", "clone-01"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "send-keys", "7", "cd '/tmp/clone-01'"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "send-keys", "7", "Enter"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "wait", "idle", "7", "--settle", "2s", "--timeout", "5s"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "send-keys", "7", "codex --yolo"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "send-keys", "7", "Enter"}},
			},
			wantPane: Pane{ID: "7", Name: "clone-01"},
		},
		{
			name: "defaults binary name and session",
			config: Config{
				Session: "default",
			},
			req: SpawnRequest{
				CWD:     "/tmp/worker-2",
				Command: "claude --dangerously-skip-permissions",
			},
			output: "Spawned worker-2 in pane 12\n",
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "default", "spawn", "--name", "worker-2"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "12", "cd '/tmp/worker-2'"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "12", "Enter"}},
				{name: "amux", args: []string{"-s", "default", "wait", "idle", "12", "--settle", "2s", "--timeout", "5s"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "12", "claude --dangerously-skip-permissions"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "12", "Enter"}},
			},
			wantPane: Pane{ID: "12", Name: "worker-2"},
		},
		{
			name: "splits from parent pane when AtPane is set",
			config: Config{
				Session: "main",
			},
			req: SpawnRequest{
				AtPane:  "lead-pane",
				Name:    "worker-LAB-99",
				CWD:     "/tmp/clone-5",
				Command: "codex --yolo",
			},
			output: "Split horizontal: new pane worker-LAB-99\n",
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "main", "spawn", "--name", "worker-LAB-99", "--at", "lead-pane", "--horizontal"}},
				{name: "amux", args: []string{"-s", "main", "send-keys", "worker-LAB-99", "cd '/tmp/clone-5'"}},
				{name: "amux", args: []string{"-s", "main", "send-keys", "worker-LAB-99", "Enter"}},
				{name: "amux", args: []string{"-s", "main", "wait", "idle", "worker-LAB-99", "--settle", "2s", "--timeout", "5s"}},
				{name: "amux", args: []string{"-s", "main", "send-keys", "worker-LAB-99", "codex --yolo"}},
				{name: "amux", args: []string{"-s", "main", "send-keys", "worker-LAB-99", "Enter"}},
			},
			wantPane: Pane{ID: "worker-LAB-99", Name: "worker-LAB-99"},
		},
		{
			name: "returns runner error on spawn failure",
			config: Config{
				Session: "default",
			},
			req: SpawnRequest{
				CWD: "/tmp/worker-3",
			},
			runErr: errors.New("exit status 1"),
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "default", "spawn", "--name", "worker-3"}},
			},
			wantErr: "exit status 1",
		},
		{
			name: "fails when pane id is missing from output",
			config: Config{
				Session: "default",
			},
			req: SpawnRequest{
				CWD: "/tmp/worker-4",
			},
			output: "Spawned worker-4\n",
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "default", "spawn", "--name", "worker-4"}},
			},
			wantErr: "parse pane id",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{
				output: []byte(tt.output),
				err:    tt.runErr,
			}
			client := newTestClient(tt.config, runner)

			gotPane, err := client.Spawn(context.Background(), tt.req)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Spawn() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Spawn() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, tt.wantCmds) {
				t.Fatalf("Spawn() commands = %#v, want %#v", runner.calls, tt.wantCmds)
			}

			if gotPane != tt.wantPane {
				t.Fatalf("Spawn() pane = %#v, want %#v", gotPane, tt.wantPane)
			}
		})
	}
}

func TestCLIClientSendKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		paneID  string
		text    string
		runErr  error
		wantCmd recordedCommand
		wantErr string
	}{
		{
			name: "builds send-keys command",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "7",
			text:   "git status",
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"send-keys",
					"7",
					"git status",
				},
			},
		},
		{
			name: "returns runner error",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-8",
			text:   "make test",
			runErr: errors.New("exit status 1"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"send-keys",
					"pane-8",
					"make test",
				},
			},
			wantErr: "exit status 1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{err: tt.runErr}
			client := newTestClient(tt.config, runner)

			err := client.SendKeys(context.Background(), tt.paneID, tt.text)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("SendKeys() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("SendKeys() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("SendKeys() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}
		})
	}
}

func TestCLIClientCapture(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		config     Config
		paneID     string
		output     string
		runErr     error
		wantCmd    recordedCommand
		wantOutput string
		wantErr    string
	}{
		{
			name: "parses pane json into screen output",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-9",
			output: `{"id":9,"name":"worker-1","content":["line one","line two"]}`,
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"capture",
					"--format", "json",
					"pane-9",
				},
			},
			wantOutput: "line one\nline two",
		},
		{
			name: "surfaces capture errors embedded in json",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-10",
			output: `{"error":{"code":"not_found","message":"pane missing"}}`,
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"capture",
					"--format", "json",
					"pane-10",
				},
			},
			wantErr: "pane missing",
		},
		{
			name: "returns json parse error",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-11",
			output: `not-json`,
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"capture",
					"--format", "json",
					"pane-11",
				},
			},
			wantErr: "invalid character",
		},
		{
			name: "returns runner error",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "12",
			runErr: errors.New("exit status 1"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"capture",
					"--format", "json",
					"12",
				},
			},
			wantErr: "exit status 1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{
				output: []byte(tt.output),
				err:    tt.runErr,
			}
			client := newTestClient(tt.config, runner)

			gotOutput, err := client.Capture(context.Background(), tt.paneID)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Capture() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Capture() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("Capture() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}

			if gotOutput != tt.wantOutput {
				t.Fatalf("Capture() output = %q, want %q", gotOutput, tt.wantOutput)
			}
		})
	}
}

func TestCLIClientSetMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		paneID  string
		meta    map[string]string
		runErr  error
		wantCmd recordedCommand
		wantErr string
	}{
		{
			name: "builds set-meta command",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-13",
			meta: map[string]string{
				"issue": "LAB-688",
				"task":  "LAB-688",
			},
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"meta", "set",
					"pane-13",
					"issue=LAB-688",
					"task=LAB-688",
				},
			},
		},
		{
			name: "returns runner error",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-14",
			meta: map[string]string{
				"issue": "LAB-688",
			},
			runErr: errors.New("exit status 1"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"meta", "set",
					"pane-14",
					"issue=LAB-688",
				},
			},
			wantErr: "exit status 1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{err: tt.runErr}
			client := newTestClient(tt.config, runner)

			err := client.SetMetadata(context.Background(), tt.paneID, tt.meta)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("SetMetadata() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("SetMetadata() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("SetMetadata() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}
		})
	}
}

func TestCLIClientKillPane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		paneID  string
		runErr  error
		wantCmd recordedCommand
		wantErr string
	}{
		{
			name: "builds kill command",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-15",
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"kill",
					"pane-15",
				},
			},
		},
		{
			name: "returns runner error",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-16",
			runErr: errors.New("exit status 1"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"kill",
					"pane-16",
				},
			},
			wantErr: "exit status 1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{err: tt.runErr}
			client := newTestClient(tt.config, runner)

			err := client.KillPane(context.Background(), tt.paneID)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("KillPane() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("KillPane() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("KillPane() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}
		})
	}
}

func TestCLIClientWaitIdle(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{}
	client := newTestClient(Config{Session: "orca-dev"}, runner)

	if err := client.WaitIdle(context.Background(), "pane-17", 45*time.Second); err != nil {
		t.Fatalf("WaitIdle() error = %v", err)
	}

	want := []recordedCommand{{
		name: "amux",
		args: []string{
			"-s", "orca-dev",
			"wait",
			"idle",
			"pane-17",
			"--settle", "2s",
			"--timeout", "45s",
		},
	}}
	if !reflect.DeepEqual(runner.calls, want) {
		t.Fatalf("WaitIdle() commands = %#v, want %#v", runner.calls, want)
	}
}

func TestMockClientRecordsCalls(t *testing.T) {
	t.Parallel()

	mock := &MockClient{
		SpawnFunc: func(_ context.Context, req SpawnRequest) (Pane, error) {
			return Pane{ID: "20", Name: paneName(req.CWD)}, nil
		},
		CaptureFunc: func(_ context.Context, paneID string) (string, error) {
			return "captured output", nil
		},
	}

	gotPane, err := mock.Spawn(context.Background(), SpawnRequest{CWD: "/tmp/worker-20"})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	if gotPane.ID != "20" {
		t.Fatalf("Spawn() paneID = %q, want %q", gotPane.ID, "20")
	}

	if err := mock.SendKeys(context.Background(), "20", "make test"); err != nil {
		t.Fatalf("SendKeys() error = %v", err)
	}
	gotCapture, err := mock.Capture(context.Background(), "20")
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if gotCapture != "captured output" {
		t.Fatalf("Capture() output = %q, want %q", gotCapture, "captured output")
	}
	if err := mock.SetMetadata(context.Background(), "20", map[string]string{"task": "LAB-688"}); err != nil {
		t.Fatalf("SetMetadata() error = %v", err)
	}
	if err := mock.KillPane(context.Background(), "20"); err != nil {
		t.Fatalf("KillPane() error = %v", err)
	}
	if err := mock.WaitIdle(context.Background(), "20", 10*time.Second); err != nil {
		t.Fatalf("WaitIdle() error = %v", err)
	}

	if !reflect.DeepEqual(mock.SpawnCalls, []SpawnRequest{{CWD: "/tmp/worker-20"}}) {
		t.Fatalf("SpawnCalls = %#v", mock.SpawnCalls)
	}
	if !reflect.DeepEqual(mock.SendKeysCalls, []SendKeysCall{{PaneID: "20", Text: "make test"}}) {
		t.Fatalf("SendKeysCalls = %#v", mock.SendKeysCalls)
	}
	if !reflect.DeepEqual(mock.CaptureCalls, []string{"20"}) {
		t.Fatalf("CaptureCalls = %#v", mock.CaptureCalls)
	}
	if !reflect.DeepEqual(mock.SetMetadataCalls, []MetadataCall{{
		PaneID:   "20",
		Metadata: map[string]string{"task": "LAB-688"},
	}}) {
		t.Fatalf("SetMetadataCalls = %#v", mock.SetMetadataCalls)
	}
	if !reflect.DeepEqual(mock.KillPaneCalls, []string{"20"}) {
		t.Fatalf("KillPaneCalls = %#v", mock.KillPaneCalls)
	}
	if got := len(mock.WaitIdleCalls); got != 1 {
		t.Fatalf("WaitIdleCalls count = %d, want 1", got)
	}
}
