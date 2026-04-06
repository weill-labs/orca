package amux

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

var spawnPanePattern = regexp.MustCompile(`\bpane\s+(\S+)`)

var ErrWaitContentTimeout = errors.New("amux wait content timeout")

// Client wraps the amux CLI for daemon code and tests.
type Client interface {
	Spawn(ctx context.Context, req SpawnRequest) (Pane, error)
	PaneExists(ctx context.Context, paneID string) (bool, error)
	ListPanes(ctx context.Context) ([]Pane, error)
	SendKeys(ctx context.Context, paneID string, keys ...string) error
	Capture(ctx context.Context, paneID string) (string, error)
	CapturePane(ctx context.Context, paneID string) (PaneCapture, error)
	CaptureHistory(ctx context.Context, paneID string) (PaneCapture, error)
	SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error
	KillPane(ctx context.Context, paneID string) error
	WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error
	WaitIdleSettle(ctx context.Context, paneID string, timeout, settle time.Duration) error
	WaitContent(ctx context.Context, paneID, substring string, timeout time.Duration) error
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
	Name           string               `json:"name,omitempty"`
	CWD            string               `json:"cwd,omitempty"`
	Content        []string             `json:"content"`
	CurrentCommand string               `json:"current_command,omitempty"`
	ChildPIDs      []int                `json:"child_pids,omitempty"`
	Exited         bool                 `json:"exited,omitempty"`
	Error          *captureCommandError `json:"error,omitempty"`
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

	session := c.resolveSession(req.Session)
	args := spawnPlacementArgs(req.AtPane)
	args = append(args, "--name", name)

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

// ListPanes returns all panes in the session, enriching each pane with its
// exact cwd from per-pane JSON capture.
func (c *CLIClient) ListPanes(ctx context.Context) ([]Pane, error) {
	output, err := c.run(ctx, c.session, "list", "--no-cwd")
	if err != nil {
		return nil, err
	}

	panes, err := parsePaneList(string(output))
	if err != nil {
		return nil, err
	}

	for i := range panes {
		capture, err := c.capturePaneJSON(ctx, panes[i].ID, false)
		if err != nil {
			return nil, fmt.Errorf("capture pane %s: %w", panes[i].ID, err)
		}
		if capture.Name != "" {
			panes[i].Name = capture.Name
		}
		panes[i].CWD = capture.CWD
	}

	return panes, nil
}

// SendKeys forwards text to a pane with amux send-keys.
// Extra keys arguments (e.g. "Enter") are passed as separate args.
func (c *CLIClient) SendKeys(ctx context.Context, paneID string, keys ...string) error {
	args := append([]string{paneID, "--delay-final", "250ms"}, keys...)
	_, err := c.run(ctx, c.session, "send-keys", args...)
	return err
}

// Capture returns the visible screen output for one pane.
func (c *CLIClient) Capture(ctx context.Context, paneID string) (string, error) {
	pane, err := c.capturePaneJSON(ctx, paneID, false)
	if err != nil {
		return "", err
	}
	return pane.toPaneCapture().Output(), nil
}

// CapturePane returns the visible screen output and process metadata for one pane.
func (c *CLIClient) CapturePane(ctx context.Context, paneID string) (PaneCapture, error) {
	pane, err := c.capturePaneJSON(ctx, paneID, false)
	if err != nil {
		return PaneCapture{}, err
	}
	return pane.toPaneCapture(), nil
}

// CaptureHistory returns retained pane history and process metadata.
func (c *CLIClient) CaptureHistory(ctx context.Context, paneID string) (PaneCapture, error) {
	pane, err := c.capturePaneJSON(ctx, paneID, true)
	if err != nil {
		return PaneCapture{}, err
	}
	return pane.toPaneCapture(), nil
}

// PaneExists verifies whether a pane can still be resolved by stable pane ID.
func (c *CLIClient) PaneExists(ctx context.Context, paneID string) (bool, error) {
	pane, err := c.rawCapturePane(ctx, paneID, false)
	if err != nil {
		return false, err
	}
	if pane.Error == nil {
		return true, nil
	}
	if paneMissing(pane.Error) {
		return false, nil
	}
	return false, paneCaptureError(pane.Error)
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
	return c.WaitIdleSettle(ctx, paneID, timeout, 0)
}

// WaitIdleSettle waits for a pane to become idle and optionally remain settled before returning.
func (c *CLIClient) WaitIdleSettle(ctx context.Context, paneID string, timeout, settle time.Duration) error {
	args := []string{"wait", "idle", paneID}
	if settle > 0 {
		args = append(args, "--settle", settle.String())
	}
	args = append(args, "--timeout", timeout.String())
	_, err := c.run(ctx, c.session, args[0], args[1:]...)
	return err
}

// WaitContent waits for pane output to include a substring.
func (c *CLIClient) WaitContent(ctx context.Context, paneID, substring string, timeout time.Duration) error {
	args := c.commandArgs(c.session, "wait", "content", paneID, substring, "--timeout", timeout.String())
	output, err := c.runner.Run(ctx, c.binary, args)
	if err == nil {
		return nil
	}

	commandErr := commandError(c.binary, args, output, err)
	if waitContentTimedOut(output) {
		return fmt.Errorf("%w: %v", ErrWaitContentTimeout, commandErr)
	}
	return commandErr
}

func (c *CLIClient) capturePane(ctx context.Context, paneID string) (capturePane, error) {
	pane, err := c.rawCapturePane(ctx, paneID, false)
	if err != nil {
		return capturePane{}, err
	}
	if pane.Error != nil {
		return capturePane{}, paneCaptureError(pane.Error)
	}
	return pane, nil
}

func (c *CLIClient) capturePaneJSON(ctx context.Context, paneID string, history bool) (capturePane, error) {
	pane, err := c.rawCapturePane(ctx, paneID, history)
	if err != nil {
		return capturePane{}, err
	}
	if pane.Error != nil {
		return capturePane{}, paneCaptureError(pane.Error)
	}
	return pane, nil
}

func (c *CLIClient) rawCapturePane(ctx context.Context, paneID string, history bool) (capturePane, error) {
	args := []string{}
	if history {
		args = append(args, "--history")
	}
	args = append(args, "--format", "json", paneID)

	output, err := c.run(ctx, c.session, "capture", args...)
	if err != nil {
		return capturePane{}, err
	}

	var pane capturePane
	if err := json.Unmarshal(output, &pane); err != nil {
		return capturePane{}, fmt.Errorf("parse capture json: %w", err)
	}
	return pane, nil
}

func (p capturePane) toPaneCapture() PaneCapture {
	return PaneCapture{
		Content:        append([]string(nil), p.Content...),
		CWD:            p.CWD,
		CurrentCommand: p.CurrentCommand,
		ChildPIDs:      append([]int(nil), p.ChildPIDs...),
		Exited:         p.Exited,
	}
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

func paneCaptureError(errInfo *captureCommandError) error {
	if errInfo == nil {
		return fmt.Errorf("capture failed")
	}
	if errInfo.Message != "" {
		return fmt.Errorf("capture failed: %s", errInfo.Message)
	}
	if errInfo.Code != "" {
		return fmt.Errorf("capture failed: %s", errInfo.Code)
	}
	return fmt.Errorf("capture failed")
}

func paneMissing(errInfo *captureCommandError) bool {
	if errInfo == nil {
		return false
	}
	if strings.EqualFold(errInfo.Code, "not_found") {
		return true
	}
	message := strings.ToLower(strings.TrimSpace(errInfo.Message))
	return strings.Contains(message, "not found") || strings.Contains(message, "missing")
}

func waitContentTimedOut(output []byte) bool {
	message := strings.ToLower(strings.TrimSpace(string(output)))
	return strings.Contains(message, "timeout waiting for")
}

func parsePaneList(output string) ([]Pane, error) {
	trimmed := strings.TrimRight(output, "\n")
	if trimmed == "" || trimmed == "No panes." {
		return nil, nil
	}

	lines := strings.Split(trimmed, "\n")
	if len(lines) == 0 {
		return nil, nil
	}

	header := lines[0]
	nameColumn := strings.Index(header, "NAME")
	hostColumn := strings.Index(header, "HOST")
	if nameColumn <= 0 || hostColumn <= nameColumn {
		return nil, fmt.Errorf("parse pane list header: %q", strings.TrimSpace(header))
	}

	panes := make([]Pane, 0, len(lines)-1)
	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			continue
		}

		paneID := strings.TrimSpace(columnSlice(line, 0, nameColumn))
		paneID = strings.TrimPrefix(paneID, "*")
		paneID = strings.TrimSpace(paneID)
		if paneID == "" {
			return nil, fmt.Errorf("parse pane id from list row: %q", strings.TrimSpace(line))
		}

		panes = append(panes, Pane{
			ID:   paneID,
			Name: strings.TrimSpace(columnSlice(line, nameColumn, hostColumn)),
		})
	}

	return panes, nil
}

func columnSlice(line string, start, end int) string {
	if start >= len(line) {
		return ""
	}
	if end > len(line) {
		end = len(line)
	}
	if end < start {
		return ""
	}
	return line[start:end]
}
