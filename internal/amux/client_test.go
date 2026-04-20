package amux

import (
	"context"
	"errors"
	"fmt"
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
	queue  []runnerResult
}

type runnerResult struct {
	output []byte
	err    error
}

func (r *fakeRunner) Run(_ context.Context, name string, args []string) ([]byte, error) {
	r.calls = append(r.calls, recordedCommand{
		name: name,
		args: append([]string(nil), args...),
	})
	if len(r.queue) > 0 {
		result := r.queue[0]
		r.queue = r.queue[1:]
		return result.output, result.err
	}
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
		queue    []runnerResult
		wantCmds []recordedCommand
		wantPane Pane
		wantErr  string
	}{
		{
			name: "targets the explicit lead pane",
			config: Config{
				Binary:  "/usr/local/bin/amux",
				Session: "orca-dev",
			},
			req: SpawnRequest{
				Session: "override-session",
				AtPane:  "lead-pane",
				CWD:     "/tmp/clone-01",
				Command: "codex --yolo",
			},
			queue: []runnerResult{
				{output: []byte("Spawned clone-01 in pane 7\n")},
			},
			wantCmds: []recordedCommand{
				{name: "/usr/local/bin/amux", args: []string{"-s", "override-session", "spawn", "--auto", "--name", "clone-01"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "send-keys", "clone-01", "--delay-final", "250ms", "cd '/tmp/clone-01'"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "send-keys", "clone-01", "--delay-final", "250ms", "Enter"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "wait", "idle", "clone-01", "--timeout", "5s"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "send-keys", "clone-01", "--delay-final", "250ms", "codex --yolo"}},
				{name: "/usr/local/bin/amux", args: []string{"-s", "orca-dev", "send-keys", "clone-01", "--delay-final", "250ms", "Enter"}},
			},
			wantPane: Pane{ID: "clone-01", Name: "clone-01"},
		},
		{
			name: "spawns in the lead pane window",
			config: Config{
				Session: "default",
			},
			req: SpawnRequest{
				AtPane:  "lead-pane",
				CWD:     "/tmp/worker-2",
				Command: "claude --dangerously-skip-permissions",
			},
			queue: []runnerResult{
				{output: []byte("Spawned worker-2 in pane 12\n")},
			},
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "default", "spawn", "--auto", "--name", "worker-2"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "worker-2", "--delay-final", "250ms", "cd '/tmp/worker-2'"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "worker-2", "--delay-final", "250ms", "Enter"}},
				{name: "amux", args: []string{"-s", "default", "wait", "idle", "worker-2", "--timeout", "5s"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "worker-2", "--delay-final", "250ms", "claude --dangerously-skip-permissions"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "worker-2", "--delay-final", "250ms", "Enter"}},
			},
			wantPane: Pane{ID: "worker-2", Name: "worker-2"},
		},
		{
			name: "spawns without an explicit lead pane",
			config: Config{
				Session: "default",
			},
			req: SpawnRequest{
				CWD:     "/tmp/worker-implicit",
				Command: "claude --dangerously-skip-permissions",
			},
			queue: []runnerResult{
				{output: []byte("Spawned worker-implicit in pane 14\n")},
			},
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "default", "spawn", "--auto", "--name", "worker-implicit"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "worker-implicit", "--delay-final", "250ms", "cd '/tmp/worker-implicit'"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "worker-implicit", "--delay-final", "250ms", "Enter"}},
				{name: "amux", args: []string{"-s", "default", "wait", "idle", "worker-implicit", "--timeout", "5s"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "worker-implicit", "--delay-final", "250ms", "claude --dangerously-skip-permissions"}},
				{name: "amux", args: []string{"-s", "default", "send-keys", "worker-implicit", "--delay-final", "250ms", "Enter"}},
			},
			wantPane: Pane{ID: "worker-implicit", Name: "worker-implicit"},
		},
		{
			name: "respects explicit worker names",
			config: Config{
				Session: "main",
			},
			req: SpawnRequest{
				AtPane:  "lead-pane",
				Name:    "w-LAB-99",
				CWD:     "/tmp/clone-5",
				Command: "codex --yolo",
			},
			queue: []runnerResult{
				{output: []byte("Split vertical: new pane w-LAB-99\n")},
			},
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "main", "spawn", "--auto", "--name", "w-LAB-99"}},
				{name: "amux", args: []string{"-s", "main", "send-keys", "w-LAB-99", "--delay-final", "250ms", "cd '/tmp/clone-5'"}},
				{name: "amux", args: []string{"-s", "main", "send-keys", "w-LAB-99", "--delay-final", "250ms", "Enter"}},
				{name: "amux", args: []string{"-s", "main", "wait", "idle", "w-LAB-99", "--timeout", "5s"}},
				{name: "amux", args: []string{"-s", "main", "send-keys", "w-LAB-99", "--delay-final", "250ms", "codex --yolo"}},
				{name: "amux", args: []string{"-s", "main", "send-keys", "w-LAB-99", "--delay-final", "250ms", "Enter"}},
			},
			wantPane: Pane{ID: "w-LAB-99", Name: "w-LAB-99"},
		},
		{
			name: "returns runner error on spawn failure",
			config: Config{
				Session: "default",
			},
			req: SpawnRequest{
				AtPane: "lead-pane",
				CWD:    "/tmp/worker-3",
			},
			queue: []runnerResult{
				{err: errors.New("exit status 1")},
			},
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "default", "spawn", "--auto", "--name", "worker-3"}},
			},
			wantErr: "exit status 1",
		},
		{
			name: "fails when pane id is missing from output",
			config: Config{
				Session: "default",
			},
			req: SpawnRequest{
				AtPane: "lead-pane",
				CWD:    "/tmp/worker-4",
			},
			queue: []runnerResult{
				{output: []byte("Spawned worker-4\n")},
			},
			wantCmds: []recordedCommand{
				{name: "amux", args: []string{"-s", "default", "spawn", "--auto", "--name", "worker-4"}},
			},
			wantErr: "parse pane id",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{queue: append([]runnerResult(nil), tt.queue...)}
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

func TestCLIClientSpawnDoesNotInspectSessionLayout(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{
		output: []byte("not-json"),
		queue: []runnerResult{
			{output: []byte("Spawned clone-01 in pane 7\n")},
		},
	}
	client := newTestClient(Config{Session: "orca-dev"}, runner)

	gotPane, err := client.Spawn(context.Background(), SpawnRequest{
		AtPane: "lead-pane",
		CWD:    "/tmp/clone-01",
	})
	if err != nil {
		t.Fatalf("Spawn() error = %v", err)
	}
	if gotPane != (Pane{ID: "clone-01", Name: "clone-01"}) {
		t.Fatalf("Spawn() pane = %#v, want named pane clone-01", gotPane)
	}

	wantCmds := []recordedCommand{
		{name: "amux", args: []string{"-s", "orca-dev", "spawn", "--auto", "--name", "clone-01"}},
		{name: "amux", args: []string{"-s", "orca-dev", "send-keys", "clone-01", "--delay-final", "250ms", "cd '/tmp/clone-01'"}},
		{name: "amux", args: []string{"-s", "orca-dev", "send-keys", "clone-01", "--delay-final", "250ms", "Enter"}},
		{name: "amux", args: []string{"-s", "orca-dev", "wait", "idle", "clone-01", "--timeout", "5s"}},
	}
	if !reflect.DeepEqual(runner.calls, wantCmds) {
		t.Fatalf("Spawn() commands = %#v, want %#v", runner.calls, wantCmds)
	}
}

func TestCLIClientSpawnCleansUpByStablePaneRefOnSetupFailure(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{
		queue: []runnerResult{
			{output: []byte("Spawned w-LAB-901 in pane 9\n")},
			{err: errors.New("exit status 1")},
			{},
		},
	}
	client := newTestClient(Config{Session: "orca-dev"}, runner)

	_, err := client.Spawn(context.Background(), SpawnRequest{
		Name:    "w-LAB-901",
		Command: "codex --yolo",
	})
	if err == nil || !strings.Contains(err.Error(), "send command to pane") {
		t.Fatalf("Spawn() error = %v, want send command failure", err)
	}

	wantCmds := []recordedCommand{
		{name: "amux", args: []string{"-s", "orca-dev", "spawn", "--auto", "--name", "w-LAB-901"}},
		{name: "amux", args: []string{"-s", "orca-dev", "send-keys", "w-LAB-901", "--delay-final", "250ms", "codex --yolo"}},
		{name: "amux", args: []string{"-s", "orca-dev", "kill", "w-LAB-901"}},
	}
	if !reflect.DeepEqual(runner.calls, wantCmds) {
		t.Fatalf("Spawn() commands = %#v, want %#v", runner.calls, wantCmds)
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
					"--delay-final", "250ms",
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
					"--delay-final", "250ms",
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

func TestCLIClientListPanes(t *testing.T) {
	t.Parallel()

	listOutput := strings.Join([]string{
		fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "PANE", "NAME", "HOST", "BRANCH", "IDLE", "WINDOW", "TASK", "META"),
		fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "*1", "w-LAB-711", "local", "LAB-711", "--", "main", "LAB-711", "agent=codex"),
		fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "2", "w-LAB-712", "local", "LAB-712", "--", "main", "LAB-712", "agent=codex"),
		"",
	}, "\n")

	runner := &fakeRunner{
		queue: []runnerResult{
			{output: []byte(listOutput)},
			{output: []byte(`{"id":1,"name":"w-LAB-711","cwd":"/tmp/orca01"}`)},
			{output: []byte(`{"id":2,"name":"w-LAB-712","cwd":"/tmp/orca02"}`)},
		},
	}
	client := newTestClient(Config{Session: "orca-dev"}, runner)

	panes, err := client.ListPanes(context.Background())
	if err != nil {
		t.Fatalf("ListPanes() error = %v", err)
	}

	wantPanes := []Pane{
		{ID: "1", Name: "w-LAB-711", CWD: "/tmp/orca01", Window: "main"},
		{ID: "2", Name: "w-LAB-712", CWD: "/tmp/orca02", Window: "main"},
	}
	if !reflect.DeepEqual(panes, wantPanes) {
		t.Fatalf("ListPanes() = %#v, want %#v", panes, wantPanes)
	}

	wantCmds := []recordedCommand{
		{name: "amux", args: []string{"-s", "orca-dev", "list", "--no-cwd"}},
		{name: "amux", args: []string{"-s", "orca-dev", "capture", "--format", "json", "w-LAB-711"}},
		{name: "amux", args: []string{"-s", "orca-dev", "capture", "--format", "json", "w-LAB-712"}},
	}
	if !reflect.DeepEqual(runner.calls, wantCmds) {
		t.Fatalf("ListPanes() commands = %#v, want %#v", runner.calls, wantCmds)
	}
}

func TestCLIClientListPanesErrorsAndFallbacks(t *testing.T) {
	t.Parallel()

	validHeader := fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "PANE", "NAME", "HOST", "BRANCH", "IDLE", "WINDOW", "TASK", "META")
	validRow := fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "1", "w-LAB-711", "local", "LAB-711", "--", "main", "LAB-711", "agent=codex")

	tests := []struct {
		name      string
		runner    *fakeRunner
		wantPanes []Pane
		wantErr   string
	}{
		{
			name: "returns list runner error",
			runner: &fakeRunner{
				err: errors.New("exit status 1"),
			},
			wantErr: "exit status 1",
		},
		{
			name: "returns parse error for malformed list header",
			runner: &fakeRunner{
				output: []byte("PANE BROKEN\n1 worker\n"),
			},
			wantErr: "parse pane list header",
		},
		{
			name: "returns capture error with pane context",
			runner: &fakeRunner{
				queue: []runnerResult{
					{output: []byte(validHeader + "\n" + validRow + "\n")},
					{output: []byte(`{"error":{"message":"denied"}}`)},
				},
			},
			wantErr: "capture pane w-LAB-711: capture failed: denied",
		},
		{
			name: "skips missing panes",
			runner: &fakeRunner{
				queue: []runnerResult{
					{output: []byte(validHeader + "\n" + validRow + "\n")},
					{output: []byte(`{"error":{"message":"pane missing"}}`)},
				},
			},
			wantPanes: []Pane{
				{ID: "1", Name: "w-LAB-711", Window: "main"},
			},
		},
		{
			name: "skips eof capture failures",
			runner: &fakeRunner{
				queue: []runnerResult{
					{output: []byte(validHeader + "\n" + validRow + "\n")},
					{output: []byte("amux capture: EOF"), err: errors.New("exit status 1")},
				},
			},
			wantPanes: []Pane{
				{ID: "1", Name: "w-LAB-711", Window: "main"},
			},
		},
		{
			name: "preserves list name when capture omits it",
			runner: &fakeRunner{
				queue: []runnerResult{
					{output: []byte(validHeader + "\n" + validRow + "\n")},
					{output: []byte(`{"id":1,"cwd":"/tmp/orca01"}`)},
				},
			},
			wantPanes: []Pane{
				{ID: "1", Name: "w-LAB-711", CWD: "/tmp/orca01", Window: "main"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(Config{Session: "orca-dev"}, tt.runner)
			gotPanes, err := client.ListPanes(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("ListPanes() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("ListPanes() error = %v", err)
			}
			if !reflect.DeepEqual(gotPanes, tt.wantPanes) {
				t.Fatalf("ListPanes() = %#v, want %#v", gotPanes, tt.wantPanes)
			}
		})
	}
}

func TestCaptureUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", want: false},
		{name: "typed pane not found", err: ErrPaneNotFound, want: true},
		{name: "not found code", err: errors.New("capture failed: not_found"), want: true},
		{name: "pane not found", err: errors.New("capture failed: pane not found"), want: true},
		{name: "pane missing", err: errors.New("capture failed: pane missing"), want: true},
		{name: "no such pane", err: errors.New("capture failed: no such pane"), want: true},
		{name: "amux eof", err: errors.New("amux capture: EOF"), want: true},
		{name: "runner eof suffix", err: errors.New("wrapped runner error: EOF"), want: true},
		{name: "other capture error", err: errors.New("capture failed: denied"), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := captureUnavailable(tt.err); got != tt.want {
				t.Fatalf("captureUnavailable(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestCLIClientPaneExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		output    string
		runErr    error
		want      bool
		wantErr   string
		wantErrIs error
	}{
		{
			name:   "returns true for live pane",
			output: `{"id":1,"name":"w-LAB-711","cwd":"/tmp/orca01"}`,
			want:   true,
		},
		{
			name:      "returns ErrPaneNotFound for not_found code",
			output:    `{"error":{"code":"not_found","message":"pane missing"}}`,
			wantErrIs: ErrPaneNotFound,
		},
		{
			name:      "returns ErrPaneNotFound for missing message",
			output:    `{"error":{"message":"pane not found"}}`,
			wantErrIs: ErrPaneNotFound,
		},
		{
			name:    "returns capture error for other error payloads",
			output:  `{"error":{"code":"permission_denied","message":"denied"}}`,
			wantErr: "capture failed: denied",
		},
		{
			name:    "returns parse errors",
			output:  `{`,
			wantErr: "parse capture json",
		},
		{
			name:      "returns ErrPaneNotFound for pane missing command errors",
			output:    `amux capture: pane "pane-1" not found`,
			runErr:    errors.New("exit status 1"),
			wantErr:   "pane \"pane-1\" not found",
			wantErrIs: ErrPaneNotFound,
		},
		{
			name:    "returns command errors",
			runErr:  errors.New("exit status 1"),
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
			client := newTestClient(Config{Session: "orca-dev"}, runner)

			got, err := client.PaneExists(context.Background(), "pane-1")
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("PaneExists() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if tt.wantErrIs == nil && err != nil {
				t.Fatalf("PaneExists() error = %v", err)
			}
			if tt.wantErrIs != nil && !errors.Is(err, tt.wantErrIs) {
				t.Fatalf("PaneExists() error = %v, want errors.Is(_, %v)", err, tt.wantErrIs)
			}
			if got != tt.want {
				t.Fatalf("PaneExists() = %t, want %t", got, tt.want)
			}

			wantCmds := []recordedCommand{
				{name: "amux", args: []string{"-s", "orca-dev", "capture", "--format", "json", "pane-1"}},
			}
			if !reflect.DeepEqual(runner.calls, wantCmds) {
				t.Fatalf("PaneExists() commands = %#v, want %#v", runner.calls, wantCmds)
			}
		})
	}
}

func TestParsePaneList(t *testing.T) {
	t.Parallel()

	header := fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "PANE", "NAME", "HOST", "BRANCH", "IDLE", "WINDOW", "TASK", "META")

	tests := []struct {
		name    string
		output  string
		want    []Pane
		wantErr string
	}{
		{
			name:   "returns nil for no panes banner",
			output: "No panes.\n",
		},
		{
			name:   "returns nil for empty output",
			output: "",
		},
		{
			name:    "rejects malformed header",
			output:  "PANE ONLY\n",
			wantErr: "parse pane list header",
		},
		{
			name: "parses rows and strips active marker",
			output: strings.Join([]string{
				header,
				fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "*7", "pane-7", "local", "main", "--", "orca", "", "lead"),
				fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "8", "w-LAB-712", "local", "LAB-712", "--", "main", "LAB-712", "agent=codex"),
				"",
			}, "\n"),
			want: []Pane{
				{ID: "7", Name: "pane-7", Window: "orca", Lead: true},
				{ID: "8", Name: "w-LAB-712", Window: "main"},
			},
		},
		{
			name: "rejects row without pane id",
			output: strings.Join([]string{
				header,
				fmt.Sprintf("%-6s %-20s %-15s %-30s %-9s %-10s %-12s %s", "", "w-LAB-711", "local", "LAB-711", "--", "main", "LAB-711", "agent=codex"),
				"",
			}, "\n"),
			wantErr: "parse pane id from list row",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePaneList(tt.output)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parsePaneList() error = %v, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parsePaneList() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parsePaneList() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestColumnSlice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		line  string
		start int
		end   int
		want  string
	}{
		{name: "returns empty when start past end of line", line: "orca", start: 10, end: 12, want: ""},
		{name: "caps end at line length", line: "orca", start: 1, end: 10, want: "rca"},
		{name: "returns empty when end before start", line: "orca", start: 3, end: 1, want: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := columnSlice(tt.line, tt.start, tt.end); got != tt.want {
				t.Fatalf("columnSlice(%q, %d, %d) = %q, want %q", tt.line, tt.start, tt.end, got, tt.want)
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

func TestCLIClientCaptureHistory(t *testing.T) {
	t.Parallel()

	runner := &fakeRunner{
		output: []byte(`{"id":9,"name":"worker-1","content":["line one","line two"],"cwd":"/tmp/clone-01","current_command":"codex","child_pids":[4242]}`),
	}
	client := newTestClient(Config{Session: "orca-dev"}, runner)

	got, err := client.CaptureHistory(context.Background(), "pane-9")
	if err != nil {
		t.Fatalf("CaptureHistory() error = %v", err)
	}

	wantCmd := recordedCommand{
		name: "amux",
		args: []string{
			"-s", "orca-dev",
			"capture",
			"--history",
			"--format", "json",
			"pane-9",
		},
	}
	if !reflect.DeepEqual(runner.calls, []recordedCommand{wantCmd}) {
		t.Fatalf("CaptureHistory() commands = %#v, want %#v", runner.calls, []recordedCommand{wantCmd})
	}

	if got.Output() != "line one\nline two" {
		t.Fatalf("CaptureHistory() output = %q, want %q", got.Output(), "line one\nline two")
	}
	if got.CWD != "/tmp/clone-01" {
		t.Fatalf("CaptureHistory() cwd = %q, want %q", got.CWD, "/tmp/clone-01")
	}
	if got.CurrentCommand != "codex" {
		t.Fatalf("CaptureHistory() current command = %q, want %q", got.CurrentCommand, "codex")
	}
	if !reflect.DeepEqual(got.ChildPIDs, []int{4242}) {
		t.Fatalf("CaptureHistory() child pids = %#v, want %#v", got.ChildPIDs, []int{4242})
	}
}

func TestCLIClientCaptureHistorySurfacesEmbeddedErrorVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		output  string
		wantErr string
	}{
		{
			name:    "uses error code when message missing",
			output:  `{"error":{"code":"not_found"}}`,
			wantErr: "capture failed: not_found",
		},
		{
			name:    "falls back to generic capture failed",
			output:  `{"error":{}}`,
			wantErr: "capture failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{output: []byte(tt.output)}
			client := newTestClient(Config{Session: "orca-dev"}, runner)

			_, err := client.CaptureHistory(context.Background(), "pane-9")
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("CaptureHistory() error = %v, want substring %q", err, tt.wantErr)
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

func TestCLIClientMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		config       Config
		paneID       string
		output       string
		runErr       error
		wantMetadata map[string]string
		wantCmd      recordedCommand
		wantErr      string
	}{
		{
			name: "parses pane metadata lines",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-13",
			output: strings.Join([]string{
				"agent_profile=codex",
				"status=escalated",
				`tracked_issues=[{"id":"LAB-850","status":"active"}]`,
				"",
			}, "\n"),
			wantMetadata: map[string]string{
				"agent_profile":  "codex",
				"status":         "escalated",
				"tracked_issues": `[{"id":"LAB-850","status":"active"}]`,
			},
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"meta", "get",
					"pane-13",
				},
			},
		},
		{
			name: "returns empty metadata for empty output",
			config: Config{
				Session: "orca-dev",
			},
			paneID:       "pane-14",
			output:       "\n",
			wantMetadata: map[string]string{},
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"meta", "get",
					"pane-14",
				},
			},
		},
		{
			name: "returns parse error for malformed metadata row",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-15",
			output: "not-a-kv-row\n",
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"meta", "get",
					"pane-15",
				},
			},
			wantErr: "parse pane metadata row",
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
					"meta", "get",
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

			runner := &fakeRunner{output: []byte(tt.output), err: tt.runErr}
			client := newTestClient(tt.config, runner)

			gotMetadata, err := client.Metadata(context.Background(), tt.paneID)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Metadata() error = %v, want substring %q", err, tt.wantErr)
				}
			} else {
				if err != nil {
					t.Fatalf("Metadata() error = %v", err)
				}
				if !reflect.DeepEqual(gotMetadata, tt.wantMetadata) {
					t.Fatalf("Metadata() = %#v, want %#v", gotMetadata, tt.wantMetadata)
				}
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("Metadata() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}
		})
	}
}

func TestCLIClientRemoveMetadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		paneID  string
		keys    []string
		runErr  error
		wantCmd recordedCommand
		wantErr string
	}{
		{
			name: "builds rm-meta command",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-13",
			keys:   []string{"status", "issue"},
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"meta", "rm",
					"pane-13",
					"status",
					"issue",
				},
			},
		},
		{
			name: "returns runner error",
			config: Config{
				Session: "orca-dev",
			},
			paneID: "pane-14",
			keys:   []string{"status"},
			runErr: errors.New("exit status 1"),
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"meta", "rm",
					"pane-14",
					"status",
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

			err := client.RemoveMetadata(context.Background(), tt.paneID, tt.keys...)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("RemoveMetadata() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("RemoveMetadata() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("RemoveMetadata() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
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

	tests := []struct {
		name    string
		wait    func(context.Context, *CLIClient) error
		wantCmd []recordedCommand
	}{
		{
			name: "without settle",
			wait: func(ctx context.Context, client *CLIClient) error {
				return client.WaitIdle(ctx, "pane-17", 45*time.Second)
			},
			wantCmd: []recordedCommand{{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"wait",
					"idle",
					"pane-17",
					"--timeout", "45s",
				},
			}},
		},
		{
			name: "with settle",
			wait: func(ctx context.Context, client *CLIClient) error {
				return client.WaitIdleSettle(ctx, "pane-17", 45*time.Second, 2*time.Second)
			},
			wantCmd: []recordedCommand{{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"wait",
					"idle",
					"pane-17",
					"--settle", "2s",
					"--timeout", "45s",
				},
			}},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runner := &fakeRunner{}
			client := newTestClient(Config{Session: "orca-dev"}, runner)

			if err := tt.wait(context.Background(), client); err != nil {
				t.Fatalf("wait idle error = %v", err)
			}
			if !reflect.DeepEqual(runner.calls, tt.wantCmd) {
				t.Fatalf("wait idle commands = %#v, want %#v", runner.calls, tt.wantCmd)
			}
		})
	}
}

func TestCLIClientWaitContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		output  string
		runErr  error
		wantErr error
		wantCmd recordedCommand
	}{
		{
			name: "waits for pane content",
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"wait",
					"content",
					"pane-17",
					"Do you trust",
					"--timeout", "45s",
				},
			},
		},
		{
			name:    "classifies timeout",
			output:  `amux wait: timeout waiting for "Do you trust" in pane-17`,
			runErr:  errors.New("exit status 1"),
			wantErr: ErrWaitContentTimeout,
			wantCmd: recordedCommand{
				name: "amux",
				args: []string{
					"-s", "orca-dev",
					"wait",
					"content",
					"pane-17",
					"Do you trust",
					"--timeout", "45s",
				},
			},
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
			client := newTestClient(Config{Session: "orca-dev"}, runner)

			err := client.WaitContent(context.Background(), "pane-17", "Do you trust", 45*time.Second)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("WaitContent() error = %v, want %v", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("WaitContent() error = %v", err)
			}

			if !reflect.DeepEqual(runner.calls, []recordedCommand{tt.wantCmd}) {
				t.Fatalf("WaitContent() commands = %#v, want %#v", runner.calls, []recordedCommand{tt.wantCmd})
			}
		})
	}
}

func TestMockClientRecordsCalls(t *testing.T) {
	t.Parallel()

	mock := &MockClient{
		SpawnFunc: func(_ context.Context, req SpawnRequest) (Pane, error) {
			return Pane{ID: "20", Name: paneName(req.CWD)}, nil
		},
		PaneExistsFunc: func(_ context.Context, paneID string) (bool, error) {
			return paneID == "20", nil
		},
		ListPanesFunc: func(_ context.Context) ([]Pane, error) {
			return []Pane{{ID: "20", Name: "worker-20", CWD: "/tmp/worker-20"}}, nil
		},
		EventsFunc: func(_ context.Context, req EventsRequest) (<-chan Event, <-chan error) {
			eventsCh := make(chan Event, 1)
			errCh := make(chan error, 1)
			eventsCh <- Event{Type: strings.Join(req.Filter, ","), PaneName: req.Pane}
			close(eventsCh)
			close(errCh)
			return eventsCh, errCh
		},
		CaptureFunc: func(_ context.Context, paneID string) (string, error) {
			return "captured output", nil
		},
		CaptureHistoryFunc: func(_ context.Context, paneID string) (PaneCapture, error) {
			return PaneCapture{
				Content:        []string{"captured history"},
				CWD:            "/tmp/worker-20",
				CurrentCommand: "codex",
				ChildPIDs:      []int{4242},
			}, nil
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
	exists, err := mock.PaneExists(context.Background(), "20")
	if err != nil {
		t.Fatalf("PaneExists() error = %v", err)
	}
	if !exists {
		t.Fatal("PaneExists() = false, want true")
	}
	gotPanes, err := mock.ListPanes(context.Background())
	if err != nil {
		t.Fatalf("ListPanes() error = %v", err)
	}
	if !reflect.DeepEqual(gotPanes, []Pane{{ID: "20", Name: "worker-20", CWD: "/tmp/worker-20"}}) {
		t.Fatalf("ListPanes() = %#v", gotPanes)
	}
	eventsCh, errCh := mock.Events(context.Background(), EventsRequest{
		Pane:        "20",
		Filter:      []string{"exited"},
		NoReconnect: true,
	})
	deadline := time.After(2 * time.Second)
	for {
		select {
		case event, ok := <-eventsCh:
			if !ok {
				t.Fatal("Events() closed before returning the first event")
			}
			if got, want := event, (Event{Type: "exited", PaneName: "20"}); got != want {
				t.Fatalf("Events() first event = %#v, want %#v", got, want)
			}
			goto eventAssertions
		case err, ok := <-errCh:
			if !ok {
				continue
			}
			t.Fatalf("Events() unexpected error = %v", err)
		case <-deadline:
			t.Fatal("timed out waiting for mock event")
		}
	}

eventAssertions:
	gotCapture, err := mock.Capture(context.Background(), "20")
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	if gotCapture != "captured output" {
		t.Fatalf("Capture() output = %q, want %q", gotCapture, "captured output")
	}
	gotHistory, err := mock.CaptureHistory(context.Background(), "20")
	if err != nil {
		t.Fatalf("CaptureHistory() error = %v", err)
	}
	if gotHistory.Output() != "captured history" {
		t.Fatalf("CaptureHistory() output = %q, want %q", gotHistory.Output(), "captured history")
	}
	if err := mock.SetMetadata(context.Background(), "20", map[string]string{"task": "LAB-688"}); err != nil {
		t.Fatalf("SetMetadata() error = %v", err)
	}
	gotMetadata, err := mock.Metadata(context.Background(), "20")
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if !reflect.DeepEqual(gotMetadata, map[string]string{"task": "LAB-688"}) {
		t.Fatalf("Metadata() = %#v, want %#v", gotMetadata, map[string]string{"task": "LAB-688"})
	}
	if err := mock.KillPane(context.Background(), "20"); err != nil {
		t.Fatalf("KillPane() error = %v", err)
	}
	if err := mock.WaitIdle(context.Background(), "20", 10*time.Second); err != nil {
		t.Fatalf("WaitIdle() error = %v", err)
	}
	if err := mock.WaitIdleSettle(context.Background(), "20", 10*time.Second, 2*time.Second); err != nil {
		t.Fatalf("WaitIdleSettle() error = %v", err)
	}
	if err := mock.WaitContent(context.Background(), "20", "Do you trust", 10*time.Second); err != nil {
		t.Fatalf("WaitContent() error = %v", err)
	}

	if !reflect.DeepEqual(mock.SpawnCalls, []SpawnRequest{{CWD: "/tmp/worker-20"}}) {
		t.Fatalf("SpawnCalls = %#v", mock.SpawnCalls)
	}
	if !reflect.DeepEqual(mock.PaneExistsCalls, []string{"20"}) {
		t.Fatalf("PaneExistsCalls = %#v", mock.PaneExistsCalls)
	}
	if got, want := mock.ListPanesCalls, 1; got != want {
		t.Fatalf("ListPanesCalls = %d, want %d", got, want)
	}
	if got, want := mock.EventsCalls, []EventsRequest{{
		Pane:        "20",
		Filter:      []string{"exited"},
		NoReconnect: true,
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("EventsCalls = %#v, want %#v", got, want)
	}
	if !reflect.DeepEqual(mock.SendKeysCalls, []SendKeysCall{{PaneID: "20", Text: "make test"}}) {
		t.Fatalf("SendKeysCalls = %#v", mock.SendKeysCalls)
	}
	if !reflect.DeepEqual(mock.CaptureCalls, []string{"20"}) {
		t.Fatalf("CaptureCalls = %#v", mock.CaptureCalls)
	}
	if !reflect.DeepEqual(mock.CaptureHistoryCalls, []string{"20"}) {
		t.Fatalf("CaptureHistoryCalls = %#v", mock.CaptureHistoryCalls)
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
	if got, want := mock.WaitIdleCalls, []struct {
		PaneID  string
		Timeout time.Duration
		Settle  time.Duration
	}{
		{PaneID: "20", Timeout: 10 * time.Second},
		{PaneID: "20", Timeout: 10 * time.Second, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("WaitIdleCalls = %#v, want %#v", got, want)
	}
	if got, want := len(mock.WaitContentCalls), 1; got != want {
		t.Fatalf("WaitContentCalls count = %d, want %d", got, want)
	}
}

func TestNewClientConfiguresEventStarter(t *testing.T) {
	t.Parallel()

	client := NewClient(Config{
		Binary:  "/usr/local/bin/amux",
		Session: "orca-dev",
	})

	if got, want := client.binary, "/usr/local/bin/amux"; got != want {
		t.Fatalf("client.binary = %q, want %q", got, want)
	}
	if got, want := client.session, "orca-dev"; got != want {
		t.Fatalf("client.session = %q, want %q", got, want)
	}
	if client.runner == nil {
		t.Fatal("client.runner = nil, want runner")
	}
	if client.eventStarter == nil {
		t.Fatal("client.eventStarter = nil, want event starter")
	}
}

func TestMockClientEventsDefaultsToClosedChannels(t *testing.T) {
	t.Parallel()

	mock := &MockClient{}
	eventsCh, errCh := mock.Events(context.Background(), EventsRequest{
		Filter: []string{"exited"},
	})

	if _, ok := <-eventsCh; ok {
		t.Fatal("eventsCh open, want closed")
	}
	if _, ok := <-errCh; ok {
		t.Fatal("errCh open, want closed")
	}
	if got, want := mock.EventsCalls, []EventsRequest{{Filter: []string{"exited"}}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("EventsCalls = %#v, want %#v", got, want)
	}
}

func TestMockClientWaitContentUsesFunc(t *testing.T) {
	t.Parallel()

	mock := &MockClient{
		WaitContentFunc: func(_ context.Context, paneID, substring string, timeout time.Duration) error {
			if got, want := paneID, "20"; got != want {
				t.Fatalf("paneID = %q, want %q", got, want)
			}
			if got, want := substring, "Do you trust"; got != want {
				t.Fatalf("substring = %q, want %q", got, want)
			}
			if got, want := timeout, 10*time.Second; got != want {
				t.Fatalf("timeout = %v, want %v", got, want)
			}
			return errors.New("wait content failed")
		},
	}

	err := mock.WaitContent(context.Background(), "20", "Do you trust", 10*time.Second)
	if err == nil || !strings.Contains(err.Error(), "wait content failed") {
		t.Fatalf("WaitContent() error = %v, want wait content failure", err)
	}

	if got, want := len(mock.WaitContentCalls), 1; got != want {
		t.Fatalf("WaitContentCalls count = %d, want %d", got, want)
	}
}
