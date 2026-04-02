package amux

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

var spawnPanePattern = regexp.MustCompile(`\bpane\s+(\d+)\b`)

// Client wraps the amux CLI for daemon code and tests.
type Client interface {
	Spawn(opts SpawnOptions) (string, error)
	SendKeys(paneID, text string) error
	Capture(paneID string) (string, error)
	Meta(paneID, key, value string) error
	Kill(paneID string) error
}

// Config configures the CLI-backed amux client.
type Config struct {
	Binary  string
	Session string
}

// SpawnOptions mirrors the supported amux spawn flags Orca needs today.
type SpawnOptions struct {
	Name       string
	Host       string
	Task       string
	Color      string
	Background bool
}

// CLIClient shells out to the amux binary.
type CLIClient struct {
	binary  string
	session string
	runner  commandRunner
}

type commandRunner interface {
	Run(name string, args []string) ([]byte, error)
}

type execRunner struct{}

func (execRunner) Run(name string, args []string) ([]byte, error) {
	return exec.Command(name, args...).CombinedOutput()
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
func (c *CLIClient) Spawn(opts SpawnOptions) (string, error) {
	args := []string{
		"--name", opts.Name,
	}
	if opts.Host != "" {
		args = append(args, "--host", opts.Host)
	}
	if opts.Task != "" {
		args = append(args, "--task", opts.Task)
	}
	if opts.Color != "" {
		args = append(args, "--color", opts.Color)
	}
	if opts.Background {
		args = append(args, "--background")
	}

	output, err := c.run("spawn", args...)
	if err != nil {
		return "", err
	}
	return parseSpawnOutput(string(output))
}

// SendKeys forwards text to a pane with amux send-keys.
func (c *CLIClient) SendKeys(paneID, text string) error {
	_, err := c.run("send-keys", paneID, text)
	return err
}

// Capture returns the visible screen output for one pane.
func (c *CLIClient) Capture(paneID string) (string, error) {
	output, err := c.run("capture", "--format", "json", paneID)
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

// Meta sets one metadata key on a pane.
func (c *CLIClient) Meta(paneID, key, value string) error {
	_, err := c.run("set-meta", paneID, fmt.Sprintf("%s=%s", key, value))
	return err
}

// Kill terminates a pane.
func (c *CLIClient) Kill(paneID string) error {
	_, err := c.run("kill", paneID)
	return err
}

func (c *CLIClient) run(subcommand string, extraArgs ...string) ([]byte, error) {
	args := c.commandArgs(subcommand, extraArgs...)
	output, err := c.runner.Run(c.binary, args)
	if err != nil {
		return output, commandError(c.binary, args, output, err)
	}
	return output, nil
}

func (c *CLIClient) commandArgs(subcommand string, extraArgs ...string) []string {
	args := make([]string, 0, len(extraArgs)+3)
	if c.session != "" {
		args = append(args, "-s", c.session)
	}
	args = append(args, subcommand)
	args = append(args, extraArgs...)
	return args
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
	return "pane-" + match[1], nil
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
