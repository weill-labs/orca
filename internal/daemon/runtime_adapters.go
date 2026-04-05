package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/linear"
)

var builtinProfiles = map[string]AgentProfile{
	"claude": {
		Name:              "claude",
		StartCommand:      "claude",
		PostmortemEnabled: true,
		StuckTimeout:      8 * time.Minute,
		NudgeCommand:      "Enter",
		StuckTextPatterns: []string{"Do you want to proceed?", "approval required"},
		MaxNudgeRetries:   3,
	},
	"codex": {
		Name:              "codex",
		StartCommand:      "codex --yolo",
		PostmortemEnabled: true,
		StuckTimeout:      9 * time.Minute,
	},
	"aider": {
		Name:              "aider",
		StartCommand:      "aider",
		PostmortemEnabled: true,
		StuckTimeout:      10 * time.Minute,
	},
}

type builtinConfigProvider struct{}

func (builtinConfigProvider) AgentProfile(_ context.Context, name string) (AgentProfile, error) {
	profile, ok := builtinProfiles[strings.ToLower(name)]
	if !ok {
		return AgentProfile{}, fmt.Errorf("unknown agent profile %q (available: claude, codex, aider)", name)
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
		Prompt:    task.Prompt,
		WorkerID:  task.PaneID,
		ClonePath: task.ClonePath,
		PRNumber:  prNumber,
		CreatedAt: task.CreatedAt,
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
		Prompt:       task.Prompt,
		PaneID:       task.WorkerID,
		PaneName:     task.WorkerID,
		ClonePath:    task.ClonePath,
		Branch:       task.Issue,
		AgentProfile: task.Agent,
		CreatedAt:    task.CreatedAt,
		UpdatedAt:    task.UpdatedAt,
	}
	if task.ClonePath != "" {
		out.CloneName = filepath.Base(task.ClonePath)
	}
	if task.PRNumber != nil {
		out.PRNumber = *task.PRNumber
	}
	return out, nil
}

func (a *sqliteStateAdapter) ClaimTask(ctx context.Context, task Task) (*Task, error) {
	var prNumber *int
	if task.PRNumber > 0 {
		value := task.PRNumber
		prNumber = &value
	}

	claimed, err := a.store.ClaimTask(ctx, task.Project, state.Task{
		Issue:     task.Issue,
		Status:    task.Status,
		Agent:     task.AgentProfile,
		Prompt:    task.Prompt,
		WorkerID:  task.PaneID,
		ClonePath: task.ClonePath,
		PRNumber:  prNumber,
		CreatedAt: task.CreatedAt,
		UpdatedAt: task.UpdatedAt,
	})
	if err != nil {
		return nil, err
	}
	if claimed == nil {
		return nil, nil
	}

	previous := Task{
		Project:      task.Project,
		Issue:        claimed.Issue,
		Status:       claimed.Status,
		Prompt:       claimed.Prompt,
		PaneID:       claimed.WorkerID,
		ClonePath:    claimed.ClonePath,
		AgentProfile: claimed.Agent,
		CreatedAt:    claimed.CreatedAt,
		UpdatedAt:    claimed.UpdatedAt,
	}
	if claimed.PRNumber != nil {
		previous.PRNumber = *claimed.PRNumber
	}
	return &previous, nil
}

func (a *sqliteStateAdapter) RestoreTask(ctx context.Context, project, issue string, previous *Task) error {
	if previous == nil {
		return a.DeleteTask(ctx, project, issue)
	}
	return a.PutTask(ctx, *previous)
}

func (a *sqliteStateAdapter) DeleteTask(ctx context.Context, project, issue string) error {
	err := a.store.DeleteTask(ctx, project, issue)
	if errors.Is(err, state.ErrNotFound) {
		return ErrTaskNotFound
	}
	return err
}

func (a *sqliteStateAdapter) NonTerminalTasks(ctx context.Context, project string) ([]Task, error) {
	tasks, err := a.store.NonTerminalTasks(ctx, project)
	if err != nil {
		return nil, err
	}

	out := make([]Task, 0, len(tasks))
	for _, task := range tasks {
		converted := Task{
			Project:      project,
			Issue:        task.Issue,
			Status:       task.Status,
			Prompt:       task.Prompt,
			PaneID:       task.WorkerID,
			PaneName:     task.WorkerID,
			ClonePath:    task.ClonePath,
			Branch:       task.Issue,
			AgentProfile: task.Agent,
			CreatedAt:    task.CreatedAt,
			UpdatedAt:    task.UpdatedAt,
		}
		if task.ClonePath != "" {
			converted.CloneName = filepath.Base(task.ClonePath)
		}
		if task.PRNumber != nil {
			converted.PRNumber = *task.PRNumber
		}
		out = append(out, converted)
	}
	return out, nil
}

func (a *sqliteStateAdapter) TasksByPane(ctx context.Context, project, paneID string) ([]Task, error) {
	tasks, err := a.store.TasksByPane(ctx, project, paneID)
	if err != nil {
		return nil, err
	}

	out := make([]Task, 0, len(tasks))
	for _, task := range tasks {
		converted := Task{
			Project:      project,
			Issue:        task.Issue,
			Status:       task.Status,
			Prompt:       task.Prompt,
			PaneID:       task.WorkerID,
			PaneName:     task.WorkerID,
			ClonePath:    task.ClonePath,
			Branch:       task.Issue,
			AgentProfile: task.Agent,
			CreatedAt:    task.CreatedAt,
			UpdatedAt:    task.UpdatedAt,
		}
		if task.ClonePath != "" {
			converted.CloneName = filepath.Base(task.ClonePath)
		}
		if task.PRNumber != nil {
			converted.PRNumber = *task.PRNumber
		}
		out = append(out, converted)
	}
	return out, nil
}

func (a *sqliteStateAdapter) PutWorker(ctx context.Context, worker Worker) error {
	return a.store.UpsertWorker(ctx, worker.Project, state.Worker{
		PaneID:                worker.PaneID,
		Agent:                 worker.AgentProfile,
		State:                 worker.Health,
		Issue:                 worker.Issue,
		ClonePath:             worker.ClonePath,
		LastReviewCount:       worker.LastReviewCount,
		LastIssueCommentCount: worker.LastIssueCommentCount,
		LastCIState:           worker.LastCIState,
		LastMergeableState:    worker.LastMergeableState,
		NudgeCount:            worker.NudgeCount,
		LastCapture:           worker.LastCapture,
		LastActivityAt:        worker.LastActivityAt,
		UpdatedAt:             worker.UpdatedAt,
	})
}

func (a *sqliteStateAdapter) WorkerByPane(ctx context.Context, project, paneID string) (Worker, error) {
	worker, err := a.store.WorkerByPane(ctx, project, paneID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return Worker{}, ErrWorkerNotFound
		}
		return Worker{}, err
	}

	return Worker{
		Project:            project,
		PaneID:             worker.PaneID,
		PaneName:           worker.PaneID,
		Issue:              worker.Issue,
		ClonePath:          worker.ClonePath,
		AgentProfile:       worker.Agent,
		Health:             worker.State,
		LastReviewCount:    worker.LastReviewCount,
		LastCIState:        worker.LastCIState,
		LastMergeableState: worker.LastMergeableState,
		NudgeCount:         worker.NudgeCount,
		LastCapture:        worker.LastCapture,
		LastActivityAt:     worker.LastActivityAt,
		UpdatedAt:          worker.UpdatedAt,
	}, nil
}

func (a *sqliteStateAdapter) DeleteWorker(ctx context.Context, project, paneID string) error {
	err := a.store.DeleteWorker(ctx, project, paneID)
	if errors.Is(err, state.ErrNotFound) {
		return ErrWorkerNotFound
	}
	return err
}

func (a *sqliteStateAdapter) ActiveAssignments(ctx context.Context, project string) ([]ActiveAssignment, error) {
	assignments, err := a.store.ActiveAssignments(ctx, project)
	if err != nil {
		return nil, err
	}

	out := make([]ActiveAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		out = append(out, convertAssignment(project, assignment))
	}
	return out, nil
}

func (a *sqliteStateAdapter) ActiveAssignmentByIssue(ctx context.Context, project, issue string) (ActiveAssignment, error) {
	assignment, err := a.store.ActiveAssignmentByIssue(ctx, project, issue)
	if errors.Is(err, state.ErrNotFound) {
		return ActiveAssignment{}, ErrTaskNotFound
	}
	if err != nil {
		return ActiveAssignment{}, err
	}
	return convertAssignment(project, assignment), nil
}

func (a *sqliteStateAdapter) ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (ActiveAssignment, error) {
	assignment, err := a.store.ActiveAssignmentByPRNumber(ctx, project, prNumber)
	if errors.Is(err, state.ErrNotFound) {
		return ActiveAssignment{}, ErrTaskNotFound
	}
	if err != nil {
		return ActiveAssignment{}, err
	}
	return convertAssignment(project, assignment), nil
}

func (a *sqliteStateAdapter) EnqueueMerge(ctx context.Context, entry MergeQueueEntry) (int, error) {
	return a.store.EnqueueMergeEntry(ctx, state.MergeQueueEntry{
		Project:   entry.Project,
		Issue:     entry.Issue,
		PRNumber:  entry.PRNumber,
		Status:    entry.Status,
		CreatedAt: entry.CreatedAt,
		UpdatedAt: entry.UpdatedAt,
	})
}

func (a *sqliteStateAdapter) NextMergeEntry(ctx context.Context, project string) (*MergeQueueEntry, error) {
	entry, err := a.store.NextMergeEntry(ctx, project)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	return &MergeQueueEntry{
		Project:   entry.Project,
		Issue:     entry.Issue,
		PRNumber:  entry.PRNumber,
		Status:    entry.Status,
		CreatedAt: entry.CreatedAt,
		UpdatedAt: entry.UpdatedAt,
	}, nil
}

func (a *sqliteStateAdapter) MergeEntry(ctx context.Context, project string, prNumber int) (*MergeQueueEntry, error) {
	entry, err := a.store.MergeEntry(ctx, project, prNumber)
	if err != nil {
		return nil, err
	}
	if entry == nil {
		return nil, nil
	}
	return &MergeQueueEntry{
		Project:   entry.Project,
		Issue:     entry.Issue,
		PRNumber:  entry.PRNumber,
		Status:    entry.Status,
		CreatedAt: entry.CreatedAt,
		UpdatedAt: entry.UpdatedAt,
	}, nil
}

func (a *sqliteStateAdapter) UpdateMergeEntry(ctx context.Context, entry MergeQueueEntry) error {
	err := a.store.UpdateMergeEntry(ctx, state.MergeQueueEntry{
		Project:   entry.Project,
		Issue:     entry.Issue,
		PRNumber:  entry.PRNumber,
		Status:    entry.Status,
		CreatedAt: entry.CreatedAt,
		UpdatedAt: entry.UpdatedAt,
	})
	if errors.Is(err, state.ErrNotFound) {
		return ErrTaskNotFound
	}
	return err
}

func (a *sqliteStateAdapter) DeleteMergeEntry(ctx context.Context, project string, prNumber int) error {
	err := a.store.DeleteMergeEntry(ctx, project, prNumber)
	if errors.Is(err, state.ErrNotFound) {
		return ErrTaskNotFound
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

func convertAssignment(project string, assignment state.Assignment) ActiveAssignment {
	task := Task{
		Project:      project,
		Issue:        assignment.Task.Issue,
		Status:       assignment.Task.Status,
		Prompt:       assignment.Task.Prompt,
		PaneID:       assignment.Task.WorkerID,
		PaneName:     assignment.Worker.PaneID,
		ClonePath:    assignment.Task.ClonePath,
		Branch:       assignment.Task.Issue,
		AgentProfile: assignment.Task.Agent,
		CreatedAt:    assignment.Task.CreatedAt,
		UpdatedAt:    assignment.Task.UpdatedAt,
	}
	if assignment.Task.ClonePath != "" {
		task.CloneName = filepath.Base(assignment.Task.ClonePath)
	}
	if assignment.Task.PRNumber != nil {
		task.PRNumber = *assignment.Task.PRNumber
	}

	worker := Worker{
		Project:               project,
		PaneID:                assignment.Worker.PaneID,
		PaneName:              assignment.Worker.PaneID,
		Issue:                 assignment.Worker.Issue,
		ClonePath:             assignment.Worker.ClonePath,
		AgentProfile:          assignment.Worker.Agent,
		Health:                assignment.Worker.State,
		LastReviewCount:       assignment.Worker.LastReviewCount,
		LastIssueCommentCount: assignment.Worker.LastIssueCommentCount,
		LastCIState:           assignment.Worker.LastCIState,
		LastMergeableState:    assignment.Worker.LastMergeableState,
		NudgeCount:            assignment.Worker.NudgeCount,
		LastCapture:           assignment.Worker.LastCapture,
		LastActivityAt:        assignment.Worker.LastActivityAt,
		UpdatedAt:             assignment.Worker.UpdatedAt,
	}

	return ActiveAssignment{
		Task:   task,
		Worker: worker,
	}
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

type internalPoolConfig struct {
	poolDir string
	origin  string
}

func (c internalPoolConfig) PoolDir() string     { return c.poolDir }
func (c internalPoolConfig) CloneOrigin() string { return c.origin }
func (c internalPoolConfig) BaseBranch() string  { return "" }

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
