package amux

import (
	"errors"
	"reflect"
	"strings"
	"testing"
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

func (r *fakeRunner) Run(name string, args []string) ([]byte, error) {
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
		name       string
		config     Config
		opts       SpawnOptions
		output     string
		runErr     error
		wantCmd    recordedCommand
		wantPaneID string
		wantErr    string
	}{
		{
			name: "builds spawn command with all supported flags",
			config: Config{
				Binary:  "/usr/local/bin/amux",
				Session: "orca-dev",
			},
			opts: SpawnOptions{
				Name:       "worker-1",
				Host:       "lambda-a100",
				Task:       "LAB-688",
				Color:      "rosewater",
				Background: true,
			},
			output: "Spawned worker-1 in pane 7\n",
			wantCmd: recordedCommand{
				name: "/usr/local/bin/amux",
				args: []string{
					"-s", "orca-dev",
					"spawn",
					"--name", "worker-1",
					"--host", "lambda-a100",
					"--task", "LAB-688",
					"--color", "rosewater",
					"--background",
				},
			},
			wantPaneID: "pane-7",
		},
		{
			name: "defaults binary name and omits empty flags",
			config: Config{
				Session: "default",
			},
			opts: SpawnOptions{
				Name: "worker-2",
			},
			output: "Spawned worker-2 in pane 12\n",
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "default",
					"spawn",
					"--name", "worker-2",
				},
			},
			wantPaneID: "pane-12",
		},
		{
			name: "returns runner error",
			config: Config{
				Session: "default",
			},
			opts: SpawnOptions{
				Name: "worker-3",
			},
			runErr: errors.New("exit status 1"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "default",
					"spawn",
					"--name", "worker-3",
				},
			},
			wantErr: "exit status 1",
		},
		{
			name: "fails when pane id is missing from output",
			config: Config{
				Session: "default",
			},
			opts: SpawnOptions{
				Name: "worker-4",
			},
			output: "Spawned worker-4\n",
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "default",
					"spawn",
					"--name", "worker-4",
				},
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

			gotPaneID, err := client.Spawn(tt.opts)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Spawn() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Spawn() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("Spawn() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}

			if gotPaneID != tt.wantPaneID {
				t.Fatalf("Spawn() paneID = %q, want %q", gotPaneID, tt.wantPaneID)
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
			paneID: "pane-7",
			text:   "git status",
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"send-keys",
					"pane-7",
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

			err := client.SendKeys(tt.paneID, tt.text)
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
			paneID: "pane-12",
			runErr: errors.New("exit status 1"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"capture",
					"--format", "json",
					"pane-12",
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

			gotOutput, err := client.Capture(tt.paneID)
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

func TestCLIClientMeta(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		paneID  string
		key     string
		value   string
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
			key:    "task",
			value:  "LAB-688",
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"set-meta",
					"pane-13",
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
			key:    "issue",
			value:  "LAB-688",
			runErr: errors.New("exit status 1"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"set-meta",
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

			err := client.Meta(tt.paneID, tt.key, tt.value)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Meta() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Meta() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("Meta() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}
		})
	}
}

func TestCLIClientKill(t *testing.T) {
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

			err := client.Kill(tt.paneID)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Kill() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Kill() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("Kill() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}
		})
	}
}

func TestMockClientRecordsCalls(t *testing.T) {
	t.Parallel()

	mock := &MockClient{
		SpawnFunc: func(opts SpawnOptions) (string, error) {
			return "pane-20", nil
		},
		CaptureFunc: func(paneID string) (string, error) {
			return "captured output", nil
		},
	}

	gotPaneID, err := mock.Spawn(SpawnOptions{Name: "worker-20"})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	if gotPaneID != "pane-20" {
		t.Fatalf("Spawn() paneID = %q, want %q", gotPaneID, "pane-20")
	}

	if err := mock.SendKeys("pane-20", "make test"); err != nil {
		t.Fatalf("SendKeys() error = %v", err)
	}
	gotCapture, err := mock.Capture("pane-20")
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if gotCapture != "captured output" {
		t.Fatalf("Capture() output = %q, want %q", gotCapture, "captured output")
	}
	if err := mock.Meta("pane-20", "task", "LAB-688"); err != nil {
		t.Fatalf("Meta() error = %v", err)
	}
	if err := mock.Kill("pane-20"); err != nil {
		t.Fatalf("Kill() error = %v", err)
	}

	if !reflect.DeepEqual(mock.SpawnCalls, []SpawnOptions{{Name: "worker-20"}}) {
		t.Fatalf("SpawnCalls = %#v", mock.SpawnCalls)
	}
	if !reflect.DeepEqual(mock.SendKeysCalls, []SendKeysCall{{PaneID: "pane-20", Text: "make test"}}) {
		t.Fatalf("SendKeysCalls = %#v", mock.SendKeysCalls)
	}
	if !reflect.DeepEqual(mock.CaptureCalls, []string{"pane-20"}) {
		t.Fatalf("CaptureCalls = %#v", mock.CaptureCalls)
	}
	if !reflect.DeepEqual(mock.MetaCalls, []MetaCall{{
		PaneID: "pane-20",
		Key:    "task",
		Value:  "LAB-688",
	}}) {
		t.Fatalf("MetaCalls = %#v", mock.MetaCalls)
	}
	if !reflect.DeepEqual(mock.KillCalls, []string{"pane-20"}) {
		t.Fatalf("KillCalls = %#v", mock.KillCalls)
	}
}
