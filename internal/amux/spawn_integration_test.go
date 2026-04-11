package amux

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/creack/pty"
)

func TestSpawnPlacementArgsAgainstRealAmux(t *testing.T) {
	amuxBinary, err := exec.LookPath("amux")
	if err != nil {
		t.Skip("amux not in PATH")
	}

	session := newRealAmuxSession(t, amuxBinary)

	tests := []struct {
		name      string
		leadPane  string
		spawnName string
	}{
		{
			name:      "targets an explicit pane",
			leadPane:  " pane-1 ",
			spawnName: "integration-targeted",
		},
		{
			name:      "uses auto placement when no lead pane is provided",
			leadPane:  " \t ",
			spawnName: "integration-auto",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			args := append(spawnPlacementArgs(tt.leadPane), "--name", tt.spawnName)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			output, err := runRealAmux(ctx, amuxBinary, session.name, "spawn", args...)
			if err != nil {
				t.Fatalf("amux spawn %v: %v\n%s", args, err, output)
			}

			paneID, err := parseSpawnOutput(output)
			if err != nil {
				t.Fatalf("parseSpawnOutput(%q): %v", output, err)
			}

			pane, ok := session.paneByName(t, tt.spawnName)
			if !ok {
				t.Fatalf("spawned pane %q missing from session list after output %q", tt.spawnName, strings.TrimSpace(output))
			}
			if pane.ID != paneID {
				t.Fatalf("spawned pane id = %q, want %q", pane.ID, paneID)
			}
		})
	}
}

type realAmuxSession struct {
	binary string
	name   string
}

func newRealAmuxSession(t *testing.T, binary string) realAmuxSession {
	t.Helper()

	session := realAmuxSession{
		binary: binary,
		name:   fmt.Sprintf("orca-spawn-integration-%d-%d", os.Getpid(), time.Now().UnixNano()),
	}

	if err := session.start(t); err != nil {
		t.Fatalf("start real amux session %q: %v", session.name, err)
	}

	t.Cleanup(func() {
		if err := session.cleanup(); err != nil {
			t.Errorf("cleanup real amux session %q: %v", session.name, err)
		}
	})

	return session
}

func (s realAmuxSession) start(t *testing.T) error {
	t.Helper()

	createArgs, err := realAmuxSessionCreateArgs(s.binary)
	if err != nil {
		return err
	}

	cmd := exec.Command(s.binary, append(createArgs, s.name)...)
	cmd.Env = realAmuxEnv()

	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: 80, Rows: 24})
	if err != nil {
		return fmt.Errorf("start session pty: %w", err)
	}
	defer ptmx.Close()

	var (
		outputMu sync.Mutex
		output   []byte
		waitErr  = make(chan error, 1)
		updates  = make(chan struct{}, 1)
	)

	go func() {
		buf := make([]byte, 4096)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				outputMu.Lock()
				output = append(output, buf[:n]...)
				outputMu.Unlock()
				select {
				case updates <- struct{}{}:
				default:
				}
			}
			if err != nil {
				return
			}
		}
	}()

	go func() {
		waitErr <- cmd.Wait()
	}()

	if !waitForRealAmuxOutput(t, &outputMu, &output, waitErr, updates, []byte("[pane-1]"), 10*time.Second) {
		return fmt.Errorf("timed out waiting for session bootstrap\n%s", string(copyBytes(&outputMu, output)))
	}

	if _, err := ptmx.Write([]byte{1, 'd'}); err != nil {
		return fmt.Errorf("detach session client: %w", err)
	}

	select {
	case err := <-waitErr:
		if err != nil {
			return fmt.Errorf("wait for detached client exit: %w\n%s", err, string(copyBytes(&outputMu, output)))
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		<-waitErr
		return fmt.Errorf("timed out waiting for detached client exit\n%s", string(copyBytes(&outputMu, output)))
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := runRealAmux(ctx, s.binary, s.name, "list", "--no-cwd")
		cancel()
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("session did not become reachable after detach: %w", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s realAmuxSession) cleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	panes, err := s.listPanes(ctx)
	if err != nil {
		if realAmuxSessionMissing(err) {
			return nil
		}
		return err
	}

	for i := len(panes) - 1; i >= 0; i-- {
		if _, err := runRealAmux(ctx, s.binary, s.name, "kill", panes[i].ID); err != nil && !realAmuxSessionMissing(err) {
			return fmt.Errorf("kill pane %s: %w", panes[i].ID, err)
		}
	}

	return nil
}

func (s realAmuxSession) paneByName(t *testing.T, name string) (Pane, bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	panes, err := s.listPanes(ctx)
	if err != nil {
		t.Fatalf("list panes in real amux session %q: %v", s.name, err)
	}

	for _, pane := range panes {
		if pane.Name == name {
			return pane, true
		}
	}
	return Pane{}, false
}

func (s realAmuxSession) listPanes(ctx context.Context) ([]Pane, error) {
	output, err := runRealAmux(ctx, s.binary, s.name, "list", "--no-cwd")
	if err != nil {
		return nil, err
	}
	panes, err := parsePaneList(output)
	if err != nil {
		return nil, fmt.Errorf("parse pane list: %w", err)
	}
	return panes, nil
}

func runRealAmux(ctx context.Context, binary, session, subcommand string, args ...string) (string, error) {
	commandArgs := make([]string, 0, len(args)+3)
	if strings.TrimSpace(session) != "" {
		commandArgs = append(commandArgs, "-s", session)
	}
	commandArgs = append(commandArgs, subcommand)
	commandArgs = append(commandArgs, args...)

	cmd := exec.CommandContext(ctx, binary, commandArgs...)
	cmd.Env = realAmuxEnv()

	output, err := cmd.CombinedOutput()
	if err != nil {
		return string(output), fmt.Errorf("%s %s: %w", binary, strings.Join(commandArgs, " "), err)
	}
	return string(output), nil
}

func realAmuxSessionCreateArgs(binary string) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, binary, "--help")
	cmd.Env = realAmuxEnv()

	output, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("read amux help: %w\n%s", err, string(output))
	}

	help := string(output)
	switch {
	case strings.Contains(help, "new-session"):
		return []string{"new-session"}, nil
	case strings.Contains(help, "new [name]"):
		return []string{"new"}, nil
	default:
		return nil, fmt.Errorf("amux help did not expose a session creation command\n%s", help)
	}
}

func realAmuxEnv() []string {
	blocked := map[string]struct{}{
		"AMUX_PANE":      {},
		"AMUX_SESSION":   {},
		"BASH_ENV":       {},
		"COLORTERM":      {},
		"CLICOLOR":       {},
		"CLICOLOR_FORCE": {},
		"ENV":            {},
		"NO_COLOR":       {},
		"PROMPT_COMMAND": {},
		"SSH_CONNECTION": {},
		"TERM_PROGRAM":   {},
		"TMUX":           {},
	}

	env := make([]string, 0, len(os.Environ())+3)
	for _, entry := range os.Environ() {
		key, _, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, skip := blocked[key]; skip {
			continue
		}
		env = append(env, entry)
	}

	env = upsertRealAmuxEnv(env, "AMUX_NO_WATCH", "1")
	env = upsertRealAmuxEnv(env, "HISTFILE", "/dev/null")
	env = upsertRealAmuxEnv(env, "TERM", "dumb")
	return env
}

func upsertRealAmuxEnv(env []string, key, value string) []string {
	prefix := key + "="
	for i, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			env[i] = prefix + value
			return env
		}
	}
	return append(env, prefix+value)
}

func waitForRealAmuxOutput(t *testing.T, outputMu *sync.Mutex, output *[]byte, waitErr <-chan error, updates <-chan struct{}, want []byte, timeout time.Duration) bool {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		if bytes.Contains(copyBytes(outputMu, *output), want) {
			return true
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		if remaining > 50*time.Millisecond {
			remaining = 50 * time.Millisecond
		}

		select {
		case err := <-waitErr:
			if err != nil {
				t.Fatalf("real amux session exited before bootstrap completed: %v\n%s", err, string(copyBytes(outputMu, *output)))
			}
			return bytes.Contains(copyBytes(outputMu, *output), want)
		case <-updates:
		case <-time.After(remaining):
		}
	}
}

func copyBytes(outputMu *sync.Mutex, output []byte) []byte {
	outputMu.Lock()
	defer outputMu.Unlock()
	return append([]byte(nil), output...)
}

func realAmuxSessionMissing(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "no such file or directory")
}
