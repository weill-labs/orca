package worksource

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
)

const defaultBeadsBin = "bd"

type runner interface {
	run(ctx context.Context, bin string, args ...string) (stdout []byte, stderr []byte, err error)
}

type execRunner struct{}

func (execRunner) run(ctx context.Context, bin string, args ...string) ([]byte, []byte, error) {
	cmd := exec.CommandContext(ctx, bin, args...)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}

// BeadsSource adapts the Go/Dolt bd CLI to Source.
type BeadsSource struct {
	bin    string
	runner runner
}

var _ Source = (*BeadsSource)(nil)

func NewBeadsSource(bin string, r runner) *BeadsSource {
	if bin == "" {
		bin = defaultBeadsBin
	}
	if r == nil {
		r = execRunner{}
	}

	return &BeadsSource{bin: bin, runner: r}
}

func (s *BeadsSource) Ready(ctx context.Context, limit int) ([]WorkItem, error) {
	args := []string{"ready", "--json", "--limit", strconv.Itoa(limit)}
	stdout, stderr, err := s.run(ctx, args...)
	if err != nil {
		return nil, s.commandError(err, stderr, args...)
	}

	var issues []beadsIssue
	if err := decodeStdout(stdout, &issues, s.command(args...)); err != nil {
		return nil, err
	}

	items := make([]WorkItem, 0, len(issues))
	for _, issue := range issues {
		if issue.IssueType == "epic" {
			continue
		}
		items = append(items, issue.workItem())
	}
	return items, nil
}

func (s *BeadsSource) Get(ctx context.Context, id string) (WorkItem, error) {
	args := []string{"show", id, "--json"}
	stdout, stderr, err := s.run(ctx, args...)
	if err != nil {
		return WorkItem{}, s.commandError(err, stderr, args...)
	}

	var issues []beadsIssue
	if err := decodeStdout(stdout, &issues, s.command(args...)); err != nil {
		return WorkItem{}, err
	}
	if len(issues) == 0 {
		return WorkItem{}, ErrNotFound
	}
	return issues[0].workItem(), nil
}

func (s *BeadsSource) Claim(ctx context.Context, id, workerID string) error {
	args := []string{"update", id, "--claim", "--actor", workerID, "--json"}
	stdout, stderr, err := s.run(ctx, args...)
	if err != nil {
		combined := strings.ToLower(string(stdout) + string(stderr))
		if strings.Contains(combined, "already claimed") {
			return fmt.Errorf("%w: %s", ErrAlreadyClaimed, strings.TrimSpace(string(stdout)+string(stderr)))
		}
		return s.commandError(err, stderr, args...)
	}

	return s.decodeIssueArray(stdout, args...)
}

func (s *BeadsSource) Release(ctx context.Context, id, reason string) error {
	return s.release(ctx, id)
}

func (s *BeadsSource) Complete(ctx context.Context, id string, outcome Outcome) error {
	switch outcome {
	case OutcomeMerged:
		args := []string{"close", id, "--suggest-next", "--json"}
		stdout, stderr, err := s.run(ctx, args...)
		if err != nil {
			return s.commandError(err, stderr, args...)
		}

		var result beadsCloseResult
		return decodeStdout(stdout, &result, s.command(args...))
	case OutcomeAbandoned:
		return s.release(ctx, id)
	case OutcomeFailed:
		args := []string{"update", id, "--status", "blocked", "--json"}
		stdout, stderr, err := s.run(ctx, args...)
		if err != nil {
			return s.commandError(err, stderr, args...)
		}
		return s.decodeIssueArray(stdout, args...)
	default:
		return fmt.Errorf("unsupported worksource outcome %d for %s", outcome, id)
	}
}

func (s *BeadsSource) Verify(ctx context.Context) error {
	versionArgs := []string{"version", "--json"}
	stdout, stderr, err := s.run(ctx, versionArgs...)
	if err != nil {
		return fmt.Errorf("%s failed: %w%s", s.command(versionArgs...), err, stderrMessage(stderr))
	}

	var version beadsVersion
	if err := json.Unmarshal(stdout, &version); err != nil || version.Version == "" {
		return fmt.Errorf("%s must return JSON with a version field; configure Go/Dolt bd, not rust br", s.command(versionArgs...))
	}

	doltArgs := []string{"dolt", "--help"}
	_, stderr, err = s.run(ctx, doltArgs...)
	if err != nil {
		return fmt.Errorf("%s failed: configure Go/Dolt bd; rust br does not provide the dolt subcommand: %w%s", s.command(doltArgs...), err, stderrMessage(stderr))
	}

	return nil
}

func (s *BeadsSource) release(ctx context.Context, id string) error {
	args := []string{"update", id, "--status", "open", "--assignee", "", "--json"}
	stdout, stderr, err := s.run(ctx, args...)
	if err != nil {
		return s.commandError(err, stderr, args...)
	}
	return s.decodeIssueArray(stdout, args...)
}

func (s *BeadsSource) decodeIssueArray(stdout []byte, args ...string) error {
	var issues []beadsIssue
	return decodeStdout(stdout, &issues, s.command(args...))
}

func (s *BeadsSource) run(ctx context.Context, args ...string) ([]byte, []byte, error) {
	return s.runner.run(ctx, s.bin, args...)
}

func (s *BeadsSource) commandError(err error, stderr []byte, args ...string) error {
	return fmt.Errorf("%s failed: %w%s", s.command(args...), err, stderrMessage(stderr))
}

func (s *BeadsSource) command(args ...string) string {
	return strings.Join(append([]string{s.bin}, args...), " ")
}

func decodeStdout(stdout []byte, into any, command string) error {
	if err := json.Unmarshal(stdout, into); err != nil {
		return fmt.Errorf("%s returned invalid JSON on stdout: %w", command, err)
	}
	return nil
}

func stderrMessage(stderr []byte) string {
	message := strings.TrimSpace(string(stderr))
	if message == "" {
		return ""
	}
	return ": " + message
}

type beadsIssue struct {
	ID          string   `json:"id"`
	Title       string   `json:"title"`
	Description string   `json:"description"`
	Priority    int      `json:"priority"`
	Labels      []string `json:"labels"`
	IssueType   string   `json:"issue_type"`
}

func (i beadsIssue) workItem() WorkItem {
	return WorkItem{
		ID:       i.ID,
		Title:    i.Title,
		Body:     i.Description,
		Priority: i.Priority,
		Labels:   i.Labels,
	}
}

type beadsCloseResult struct {
	Closed    []string `json:"closed"`
	Unblocked []string `json:"unblocked"`
}

type beadsVersion struct {
	Version string `json:"version"`
}
