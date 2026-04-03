package amux

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var spawnPanePattern = regexp.MustCompile(`\bpane\s+(\S+)`)

// Client wraps the amux CLI for daemon code and tests.
type Client interface {
	Spawn(ctx context.Context, req SpawnRequest) (Pane, error)
	SendKeys(ctx context.Context, paneID string, keys ...string) error
	Capture(ctx context.Context, paneID string) (string, error)
	SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error
	KillPane(ctx context.Context, paneID string) error
	WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error
}

// Config configures the CLI-backed amux client.
type Config struct {
	Binary  string
	Session string
}

// CLIClient shells out to the amux binary.
type CLIClient struct {
	binary  string
	session string
	runner  commandRunner
}

type commandRunner interface {
	Run(ctx context.Context, name string, args []string) ([]byte, error)
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, name string, args []string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
}

type captureCommandError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type capturePane struct {
	Content []string             `json:"content"`
	Error   *captureCommandError `json:"error,omitempty"`
}

var _ Client = (*CLIClient)(nil)

// NewClient returns a CLI-backed amux client.
func NewClient(config Config) *CLIClient {
	return &CLIClient{
		binary:  defaultBinary(config.Binary),
		session: config.Session,
		runner:  execRunner{},
	}
}

// Spawn creates a pane and returns its stable pane reference, e.g. pane-7.
// amux spawn doesn't support --cwd or --command flags, so after creating the
// pane we send-keys to cd into the working directory and start the command.
func (c *CLIClient) Spawn(ctx context.Context, req SpawnRequest) (Pane, error) {
	name := req.Name
	if name == "" {
		name = paneName(req.CWD)
	}
	args := []string{"--name", name}
	if req.AtPane != "" {
		args = append(args, "--at", req.AtPane, "--horizontal")
	}

	session := c.resolveSession(req.Session)
	output, err := c.run(ctx, session, "spawn", args...)
	if err != nil {
		return Pane{}, err
	}
	paneID, err := parseSpawnOutput(string(output))
	if err != nil {
		return Pane{}, err
	}

	if req.CWD != "" {
		if err := c.SendKeys(ctx, paneID, fmt.Sprintf("cd '%s'", req.CWD)); err != nil {
			_ = c.KillPane(ctx, paneID)
			return Pane{}, fmt.Errorf("send cd to pane: %w", err)
		}
		if err := c.SendKeys(ctx, paneID, "Enter"); err != nil {
			_ = c.KillPane(ctx, paneID)
			return Pane{}, fmt.Errorf("send Enter after cd: %w", err)
		}
		if err := c.WaitIdle(ctx, paneID, 5*time.Second); err != nil {
			_ = c.KillPane(ctx, paneID)
			return Pane{}, fmt.Errorf("wait for cd: %w", err)
		}
	}

	if req.Command != "" {
		if err := c.SendKeys(ctx, paneID, req.Command); err != nil {
			_ = c.KillPane(ctx, paneID)
			return Pane{}, fmt.Errorf("send command to pane: %w", err)
		}
		if err := c.SendKeys(ctx, paneID, "Enter"); err != nil {
			_ = c.KillPane(ctx, paneID)
			return Pane{}, fmt.Errorf("send Enter after command: %w", err)
		}
	}

	return Pane{
		ID:   paneID,
		Name: name,
	}, nil
}

// SendKeys forwards text to a pane with amux send-keys.
// Extra keys arguments (e.g. "Enter") are passed as separate args.
func (c *CLIClient) SendKeys(ctx context.Context, paneID string, keys ...string) error {
	args := append([]string{paneID}, keys...)
	_, err := c.run(ctx, c.session, "send-keys", args...)
	return err
}

// Capture returns the visible screen output for one pane.
func (c *CLIClient) Capture(ctx context.Context, paneID string) (string, error) {
	output, err := c.run(ctx, c.session, "capture", "--format", "json", paneID)
	if err != nil {
		return "", err
	}

	var pane capturePane
	if err := json.Unmarshal(output, &pane); err != nil {
		return "", fmt.Errorf("parse capture json: %w", err)
	}
	if pane.Error != nil {
		if pane.Error.Message != "" {
			return "", fmt.Errorf("capture failed: %s", pane.Error.Message)
		}
		if pane.Error.Code != "" {
			return "", fmt.Errorf("capture failed: %s", pane.Error.Code)
		}
		return "", fmt.Errorf("capture failed")
	}

	return strings.Join(pane.Content, "\n"), nil
}

// SetMetadata applies the provided metadata on a pane.
func (c *CLIClient) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	if len(metadata) == 0 {
		return nil
	}

	keys := make([]string, 0, len(metadata))
	for key := range metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	args := make([]string, 0, len(keys)+1)
	args = append(args, paneID)
	for _, key := range keys {
		args = append(args, fmt.Sprintf("%s=%s", key, metadata[key]))
	}

	_, err := c.run(ctx, c.session, "meta", append([]string{"set"}, args...)...)
	return err
}

// KillPane terminates a pane.
func (c *CLIClient) KillPane(ctx context.Context, paneID string) error {
	_, err := c.run(ctx, c.session, "kill", paneID)
	return err
}

// WaitIdle waits for a pane to become idle before returning.
func (c *CLIClient) WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error {
	_, err := c.run(ctx, c.session, "wait", "idle", paneID, "--timeout", timeout.String())
	return err
}

func (c *CLIClient) run(ctx context.Context, session, subcommand string, extraArgs ...string) ([]byte, error) {
	args := c.commandArgs(session, subcommand, extraArgs...)
	output, err := c.runner.Run(ctx, c.binary, args)
	if err != nil {
		return output, commandError(c.binary, args, output, err)
	}
	return output, nil
}

func (c *CLIClient) commandArgs(session, subcommand string, extraArgs ...string) []string {
	args := make([]string, 0, len(extraArgs)+3)
	if session != "" {
		args = append(args, "-s", session)
	}
	args = append(args, subcommand)
	args = append(args, extraArgs...)
	return args
}

func (c *CLIClient) resolveSession(session string) string {
	if strings.TrimSpace(session) != "" {
		return session
	}
	return c.session
}

func defaultBinary(binary string) string {
	if strings.TrimSpace(binary) == "" {
		return "amux"
	}
	return binary
}

func parseSpawnOutput(output string) (string, error) {
	match := spawnPanePattern.FindStringSubmatch(output)
	if len(match) != 2 {
		return "", fmt.Errorf("parse pane id from spawn output: %q", strings.TrimSpace(output))
	}
	return match[1], nil
}

func paneName(cwd string) string {
	base := filepath.Base(strings.TrimSpace(cwd))
	if base == "." || base == string(filepath.Separator) || base == "" {
		return "orca"
	}
	return base
}

func commandString(binary string, args []string) string {
	parts := append([]string{binary}, args...)
	return strings.Join(parts, " ")
}

func commandError(binary string, args []string, output []byte, err error) error {
	if msg := strings.TrimSpace(string(output)); msg != "" {
		return fmt.Errorf("%s: %w: %s", commandString(binary, args), err, msg)
	}
	return fmt.Errorf("%s: %w", commandString(binary, args), err)
}
