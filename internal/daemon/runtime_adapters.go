package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"

	orcacheckconfig "github.com/weill-labs/orca/internal/config"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/linear"
)

type configAdapter struct {
	cfg orcacheckconfig.Config
}

func (a configAdapter) AgentProfile(_ context.Context, name string) (AgentProfile, error) {
	cfgProfile, ok := a.cfg.Agents[name]
	if !ok {
		return AgentProfile{}, fmt.Errorf("agent profile %q not found", name)
	}

	profile := AgentProfile{
		Name:              name,
		StartCommand:      cfgProfile.StartCommand,
		PostmortemEnabled: cfgProfile.PostmortemEnabled,
		StuckTextPatterns: append([]string(nil), cfgProfile.StuckTextPatterns...),
		StuckTimeout:      cfgProfile.StuckTimeout,
		NudgeCommand:      cfgProfile.NudgeCommand,
		MaxNudgeRetries:   cfgProfile.MaxNudgeRetries,
	}

	return applyAgentProfileQuirks(profile), nil
}

func applyAgentProfileQuirks(profile AgentProfile) AgentProfile {
	switch strings.ToLower(profile.Name) {
	case "codex":
		profile.StartCommand = normalizeCodexStartCommand(profile.StartCommand)
		profile.ResumeSequence = []string{
			profile.StartCommand + " resume",
			"Enter",
			".",
		}
	}

	return profile
}

func normalizeCodexStartCommand(command string) string {
	command = strings.TrimSpace(command)
	if command == "" {
		return "codex --yolo"
	}
	if strings.Contains(command, "--yolo") {
		return command
	}
	return command + " --yolo"
}

type sqliteStateAdapter struct {
	store *state.SQLiteStore
}

func newSQLiteStateAdapter(store *state.SQLiteStore) *sqliteStateAdapter {
	return &sqliteStateAdapter{store: store}
}

func (a *sqliteStateAdapter) PutTask(ctx context.Context, task Task) error {
	var prNumber *int
	if task.PRNumber > 0 {
		value := task.PRNumber
		prNumber = &value
	}

	return a.store.UpsertTask(ctx, task.Project, state.Task{
		Issue:     task.Issue,
		Status:    task.Status,
		Agent:     task.AgentProfile,
		WorkerID:  task.PaneID,
		ClonePath: task.ClonePath,
		PRNumber:  prNumber,
		UpdatedAt: task.UpdatedAt,
	})
}

func (a *sqliteStateAdapter) TaskByIssue(ctx context.Context, project, issue string) (Task, error) {
	status, err := a.store.TaskStatus(ctx, project, issue)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return Task{}, ErrTaskNotFound
		}
		return Task{}, err
	}

	task := status.Task
	out := Task{
		Project:      project,
		Issue:        task.Issue,
		Status:       task.Status,
		PaneID:       task.WorkerID,
		ClonePath:    task.ClonePath,
		AgentProfile: task.Agent,
		UpdatedAt:    task.UpdatedAt,
	}
	if task.PRNumber != nil {
		out.PRNumber = *task.PRNumber
	}
	return out, nil
}

func (a *sqliteStateAdapter) PutWorker(ctx context.Context, worker Worker) error {
	return a.store.UpsertWorker(ctx, worker.Project, state.Worker{
		PaneID:    worker.PaneID,
		Agent:     worker.AgentProfile,
		State:     worker.Health,
		Issue:     worker.Issue,
		ClonePath: worker.ClonePath,
		UpdatedAt: worker.UpdatedAt,
	})
}

func (a *sqliteStateAdapter) DeleteWorker(ctx context.Context, project, paneID string) error {
	err := a.store.DeleteWorker(ctx, project, paneID)
	if errors.Is(err, state.ErrNotFound) {
		return ErrWorkerNotFound
	}
	return err
}

func (a *sqliteStateAdapter) RecordEvent(ctx context.Context, event Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	_, err = a.store.AppendEvent(ctx, state.Event{
		Project:   event.Project,
		Kind:      event.Type,
		Issue:     event.Issue,
		Message:   event.Message,
		Payload:   payload,
		CreatedAt: event.Time,
	})
	return err
}

type execCommandRunner struct{}

func (execCommandRunner) Run(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir

	output, err := cmd.CombinedOutput()
	if err != nil {
		if message := strings.TrimSpace(string(output)); message != "" {
			return output, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, message)
		}
		return output, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}

	return output, nil
}

func newLinearIssueTrackerFromEnv() (IssueTracker, error) {
	client, err := linear.New(linear.Options{})
	if err != nil {
		if errors.Is(err, linear.ErrMissingAPIKey) {
			return nil, nil
		}
		return nil, err
	}
	return client, nil
}

type poolConfigAdapter struct {
	cfg orcacheckconfig.Config
}

func (a poolConfigAdapter) PoolPattern() string {
	return a.cfg.Pool.Pattern
}

func (a poolConfigAdapter) BaseBranch() string {
	return ""
}

type amuxCWDUsageChecker struct {
	amux interface {
		ListPanes(ctx context.Context) ([]Pane, error)
	}
}

func (c amuxCWDUsageChecker) ActiveCWDs(ctx context.Context) ([]string, error) {
	panes, err := c.amux.ListPanes(ctx)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(panes))
	for _, pane := range panes {
		if strings.TrimSpace(pane.CWD) == "" {
			continue
		}
		paths = append(paths, pane.CWD)
	}
	return paths, nil
}
