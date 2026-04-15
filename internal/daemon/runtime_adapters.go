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
		ReadyPattern:      codexReadyPattern,
		PostmortemEnabled: true,
		StuckTimeout:      9 * time.Minute,
	},
}

type builtinConfigProvider struct{}

func (builtinConfigProvider) AgentProfile(_ context.Context, name string) (AgentProfile, error) {
	profile, ok := builtinProfiles[strings.ToLower(name)]
	if !ok {
		return AgentProfile{}, fmt.Errorf("unknown agent profile %q (available: claude, codex)", name)
	}

	return applyAgentProfileQuirks(profile), nil
}

func applyAgentProfileQuirks(profile AgentProfile) AgentProfile {
	switch strings.ToLower(profile.Name) {
	case "codex":
		profile.StartCommand = normalizeCodexStartCommand(profile.StartCommand)
		if strings.TrimSpace(profile.ReadyPattern) == "" {
			profile.ReadyPattern = codexReadyPattern
		}
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
	store state.Backend
}

type sqliteDaemonStatusWriter struct {
	store     state.Store
	project   string
	session   string
	pid       int
	startedAt time.Time
}

func newSQLiteStateAdapter(store state.Backend) *sqliteStateAdapter {
	return &sqliteStateAdapter{store: store}
}

func newSQLiteDaemonStatusWriter(store state.Store, project, session string, pid int, startedAt time.Time) *sqliteDaemonStatusWriter {
	return &sqliteDaemonStatusWriter{
		store:     store,
		project:   project,
		session:   session,
		pid:       pid,
		startedAt: startedAt,
	}
}

func (w *sqliteDaemonStatusWriter) Update(ctx context.Context, status string, heartbeatAt time.Time) error {
	if heartbeatAt.IsZero() {
		heartbeatAt = w.startedAt
	}
	return w.store.UpsertDaemon(ctx, w.project, state.DaemonStatus{
		Session:   w.session,
		PID:       w.pid,
		Status:    status,
		StartedAt: w.startedAt,
		UpdatedAt: heartbeatAt,
	})
}

func convertStateTask(project string, task state.Task) Task {
	if task.Project != "" {
		project = task.Project
	}
	out := Task{
		Project:      project,
		Issue:        task.Issue,
		Status:       task.Status,
		Prompt:       task.Prompt,
		CallerPane:   task.CallerPane,
		WorkerID:     task.WorkerID,
		PaneID:       task.CurrentPaneID,
		PaneName:     workerPaneName(task.Issue, task.WorkerID),
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
	return out
}

func (a *sqliteStateAdapter) PutTask(ctx context.Context, task Task) error {
	var prNumber *int
	if task.PRNumber > 0 {
		value := task.PRNumber
		prNumber = &value
	}

	return a.store.UpsertTask(ctx, task.Project, state.Task{
		Issue:         task.Issue,
		Status:        task.Status,
		Agent:         task.AgentProfile,
		Prompt:        task.Prompt,
		CallerPane:    task.CallerPane,
		WorkerID:      task.WorkerID,
		CurrentPaneID: task.PaneID,
		ClonePath:     task.ClonePath,
		PRNumber:      prNumber,
		CreatedAt:     task.CreatedAt,
		UpdatedAt:     task.UpdatedAt,
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

	return convertStateTask(project, status.Task), nil
}

func (a *sqliteStateAdapter) ClaimTask(ctx context.Context, task Task) (*Task, error) {
	var prNumber *int
	if task.PRNumber > 0 {
		value := task.PRNumber
		prNumber = &value
	}

	claimed, err := a.store.ClaimTask(ctx, task.Project, state.Task{
		Issue:         task.Issue,
		Status:        task.Status,
		Agent:         task.AgentProfile,
		Prompt:        task.Prompt,
		CallerPane:    task.CallerPane,
		WorkerID:      task.WorkerID,
		CurrentPaneID: task.PaneID,
		ClonePath:     task.ClonePath,
		PRNumber:      prNumber,
		CreatedAt:     task.CreatedAt,
		UpdatedAt:     task.UpdatedAt,
	})
	if err != nil {
		return nil, err
	}
	if claimed == nil {
		return nil, nil
	}

	previous := convertStateTask(task.Project, *claimed)
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
		out = append(out, convertStateTask(project, task))
	}
	return out, nil
}

func (a *sqliteStateAdapter) StaleCloneOccupancies(ctx context.Context, project string) ([]CloneOccupancy, error) {
	occupancies, err := a.store.StaleCloneOccupancies(ctx, project)
	if err != nil {
		return nil, err
	}

	out := make([]CloneOccupancy, 0, len(occupancies))
	for _, occupancy := range occupancies {
		out = append(out, CloneOccupancy{
			Project:       firstNonEmpty(occupancy.Project, project),
			Path:          occupancy.Path,
			CurrentBranch: occupancy.CurrentBranch,
			AssignedTask:  occupancy.AssignedTask,
		})
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
		out = append(out, convertStateTask(project, task))
	}
	return out, nil
}

func (a *sqliteStateAdapter) PutWorker(ctx context.Context, worker Worker) error {
	lastSeenAt := worker.LastSeenAt
	if lastSeenAt.IsZero() {
		lastSeenAt = worker.UpdatedAt
	}
	return a.store.UpsertWorker(ctx, worker.Project, state.Worker{
		WorkerID:                     worker.WorkerID,
		CurrentPaneID:                worker.PaneID,
		Agent:                        worker.AgentProfile,
		State:                        worker.Health,
		Issue:                        worker.Issue,
		ClonePath:                    worker.ClonePath,
		LastReviewCount:              worker.LastReviewCount,
		LastInlineReviewCommentCount: worker.LastInlineReviewCommentCount,
		LastIssueCommentCount:        worker.LastIssueCommentCount,
		ReviewNudgeCount:             worker.ReviewNudgeCount,
		LastCIState:                  worker.LastCIState,
		CINudgeCount:                 worker.CINudgeCount,
		CIFailurePollCount:           worker.CIFailurePollCount,
		CIEscalated:                  worker.CIEscalated,
		LastMergeableState:           worker.LastMergeableState,
		NudgeCount:                   worker.NudgeCount,
		RestartCount:                 worker.RestartCount,
		LastCapture:                  worker.LastCapture,
		LastActivityAt:               worker.LastActivityAt,
		LastPRNumber:                 worker.LastPRNumber,
		LastPushAt:                   worker.LastPushAt,
		LastPRPollAt:                 worker.LastPRPollAt,
		FirstCrashAt:                 worker.FirstCrashAt,
		CreatedAt:                    worker.CreatedAt,
		LastSeenAt:                   lastSeenAt,
	})
}

func (a *sqliteStateAdapter) ClaimWorker(ctx context.Context, worker Worker) (Worker, error) {
	lastSeenAt := worker.LastSeenAt
	if lastSeenAt.IsZero() {
		lastSeenAt = worker.UpdatedAt
	}
	claimed, err := a.store.ClaimWorker(ctx, worker.Project, state.Worker{
		WorkerID:                     worker.WorkerID,
		CurrentPaneID:                worker.PaneID,
		Agent:                        worker.AgentProfile,
		State:                        worker.Health,
		Issue:                        worker.Issue,
		ClonePath:                    worker.ClonePath,
		LastReviewCount:              worker.LastReviewCount,
		LastInlineReviewCommentCount: worker.LastInlineReviewCommentCount,
		LastIssueCommentCount:        worker.LastIssueCommentCount,
		ReviewNudgeCount:             worker.ReviewNudgeCount,
		LastCIState:                  worker.LastCIState,
		CINudgeCount:                 worker.CINudgeCount,
		CIFailurePollCount:           worker.CIFailurePollCount,
		CIEscalated:                  worker.CIEscalated,
		LastMergeableState:           worker.LastMergeableState,
		NudgeCount:                   worker.NudgeCount,
		RestartCount:                 worker.RestartCount,
		LastCapture:                  worker.LastCapture,
		LastActivityAt:               worker.LastActivityAt,
		LastPRNumber:                 worker.LastPRNumber,
		LastPushAt:                   worker.LastPushAt,
		LastPRPollAt:                 worker.LastPRPollAt,
		FirstCrashAt:                 worker.FirstCrashAt,
		CreatedAt:                    worker.CreatedAt,
		LastSeenAt:                   lastSeenAt,
	})
	if err != nil {
		return Worker{}, err
	}

	return Worker{
		Project:                      worker.Project,
		PaneID:                       worker.PaneID,
		WorkerID:                     claimed.WorkerID,
		PaneName:                     workerPaneName(claimed.Issue, claimed.WorkerID),
		Issue:                        claimed.Issue,
		ClonePath:                    claimed.ClonePath,
		AgentProfile:                 claimed.Agent,
		Health:                       claimed.State,
		LastReviewCount:              claimed.LastReviewCount,
		LastInlineReviewCommentCount: claimed.LastInlineReviewCommentCount,
		LastIssueCommentCount:        claimed.LastIssueCommentCount,
		ReviewNudgeCount:             claimed.ReviewNudgeCount,
		LastCIState:                  claimed.LastCIState,
		CINudgeCount:                 claimed.CINudgeCount,
		CIFailurePollCount:           claimed.CIFailurePollCount,
		CIEscalated:                  claimed.CIEscalated,
		LastMergeableState:           claimed.LastMergeableState,
		NudgeCount:                   claimed.NudgeCount,
		RestartCount:                 claimed.RestartCount,
		LastCapture:                  claimed.LastCapture,
		LastActivityAt:               claimed.LastActivityAt,
		LastPRNumber:                 claimed.LastPRNumber,
		LastPushAt:                   claimed.LastPushAt,
		LastPRPollAt:                 claimed.LastPRPollAt,
		FirstCrashAt:                 claimed.FirstCrashAt,
		CreatedAt:                    claimed.CreatedAt,
		LastSeenAt:                   claimed.LastSeenAt,
		UpdatedAt:                    claimed.LastSeenAt,
	}, nil
}

func (a *sqliteStateAdapter) WorkerByID(ctx context.Context, project, workerID string) (Worker, error) {
	worker, err := a.store.WorkerByID(ctx, project, workerID)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return Worker{}, ErrWorkerNotFound
		}
		return Worker{}, err
	}

	return Worker{
		Project:                      firstNonEmpty(worker.Project, project),
		WorkerID:                     worker.WorkerID,
		PaneID:                       worker.CurrentPaneID,
		PaneName:                     workerPaneName(worker.Issue, worker.WorkerID),
		Issue:                        worker.Issue,
		ClonePath:                    worker.ClonePath,
		AgentProfile:                 worker.Agent,
		Health:                       worker.State,
		LastReviewCount:              worker.LastReviewCount,
		LastInlineReviewCommentCount: worker.LastInlineReviewCommentCount,
		LastIssueCommentCount:        worker.LastIssueCommentCount,
		ReviewNudgeCount:             worker.ReviewNudgeCount,
		LastCIState:                  worker.LastCIState,
		CINudgeCount:                 worker.CINudgeCount,
		CIFailurePollCount:           worker.CIFailurePollCount,
		CIEscalated:                  worker.CIEscalated,
		LastMergeableState:           worker.LastMergeableState,
		NudgeCount:                   worker.NudgeCount,
		RestartCount:                 worker.RestartCount,
		LastCapture:                  worker.LastCapture,
		LastActivityAt:               worker.LastActivityAt,
		LastPRNumber:                 worker.LastPRNumber,
		LastPushAt:                   worker.LastPushAt,
		LastPRPollAt:                 worker.LastPRPollAt,
		FirstCrashAt:                 worker.FirstCrashAt,
		CreatedAt:                    worker.CreatedAt,
		LastSeenAt:                   worker.LastSeenAt,
		UpdatedAt:                    worker.LastSeenAt,
	}, nil
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
		Project:                      firstNonEmpty(worker.Project, project),
		WorkerID:                     worker.WorkerID,
		PaneID:                       worker.CurrentPaneID,
		PaneName:                     workerPaneName(worker.Issue, worker.WorkerID),
		Issue:                        worker.Issue,
		ClonePath:                    worker.ClonePath,
		AgentProfile:                 worker.Agent,
		Health:                       worker.State,
		LastReviewCount:              worker.LastReviewCount,
		LastInlineReviewCommentCount: worker.LastInlineReviewCommentCount,
		LastIssueCommentCount:        worker.LastIssueCommentCount,
		ReviewNudgeCount:             worker.ReviewNudgeCount,
		LastCIState:                  worker.LastCIState,
		CINudgeCount:                 worker.CINudgeCount,
		CIFailurePollCount:           worker.CIFailurePollCount,
		CIEscalated:                  worker.CIEscalated,
		LastMergeableState:           worker.LastMergeableState,
		NudgeCount:                   worker.NudgeCount,
		RestartCount:                 worker.RestartCount,
		LastCapture:                  worker.LastCapture,
		LastActivityAt:               worker.LastActivityAt,
		LastPRNumber:                 worker.LastPRNumber,
		LastPushAt:                   worker.LastPushAt,
		LastPRPollAt:                 worker.LastPRPollAt,
		FirstCrashAt:                 worker.FirstCrashAt,
		CreatedAt:                    worker.CreatedAt,
		LastSeenAt:                   worker.LastSeenAt,
		UpdatedAt:                    worker.LastSeenAt,
	}, nil
}

func (a *sqliteStateAdapter) DeleteWorker(ctx context.Context, project, workerID string) error {
	err := a.store.DeleteWorker(ctx, project, workerID)
	if errors.Is(err, state.ErrNotFound) {
		worker, lookupErr := a.store.WorkerByPane(ctx, project, workerID)
		if lookupErr == nil {
			err = a.store.DeleteWorker(ctx, project, worker.WorkerID)
		}
	}
	if errors.Is(err, state.ErrNotFound) {
		return ErrWorkerNotFound
	}
	return err
}

func (a *sqliteStateAdapter) ListWorkers(ctx context.Context, project string) ([]Worker, error) {
	workers, err := a.store.ListWorkers(ctx, project)
	if err != nil {
		return nil, err
	}

	out := make([]Worker, 0, len(workers))
	for _, worker := range workers {
		out = append(out, Worker{
			Project:                      firstNonEmpty(worker.Project, project),
			WorkerID:                     worker.WorkerID,
			PaneID:                       worker.CurrentPaneID,
			PaneName:                     workerPaneName(worker.Issue, worker.WorkerID),
			Issue:                        worker.Issue,
			ClonePath:                    worker.ClonePath,
			AgentProfile:                 worker.Agent,
			Health:                       worker.State,
			LastReviewCount:              worker.LastReviewCount,
			LastInlineReviewCommentCount: worker.LastInlineReviewCommentCount,
			LastIssueCommentCount:        worker.LastIssueCommentCount,
			ReviewNudgeCount:             worker.ReviewNudgeCount,
			LastCIState:                  worker.LastCIState,
			CINudgeCount:                 worker.CINudgeCount,
			CIFailurePollCount:           worker.CIFailurePollCount,
			CIEscalated:                  worker.CIEscalated,
			LastMergeableState:           worker.LastMergeableState,
			NudgeCount:                   worker.NudgeCount,
			RestartCount:                 worker.RestartCount,
			LastCapture:                  worker.LastCapture,
			LastActivityAt:               worker.LastActivityAt,
			LastPRNumber:                 worker.LastPRNumber,
			LastPushAt:                   worker.LastPushAt,
			LastPRPollAt:                 worker.LastPRPollAt,
			FirstCrashAt:                 worker.FirstCrashAt,
			CreatedAt:                    worker.CreatedAt,
			LastSeenAt:                   worker.LastSeenAt,
			UpdatedAt:                    worker.LastSeenAt,
		})
	}
	return out, nil
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

func (a *sqliteStateAdapter) MergeEntries(ctx context.Context, project string) ([]MergeQueueEntry, error) {
	entries, err := a.store.MergeEntries(ctx, project)
	if err != nil {
		return nil, err
	}

	out := make([]MergeQueueEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, MergeQueueEntry{
			Project:   entry.Project,
			Issue:     entry.Issue,
			PRNumber:  entry.PRNumber,
			Status:    entry.Status,
			CreatedAt: entry.CreatedAt,
			UpdatedAt: entry.UpdatedAt,
		})
	}
	return out, nil
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
		WorkerID:  event.WorkerID,
		Message:   event.Message,
		Payload:   payload,
		CreatedAt: event.Time,
	})
	return err
}

func convertAssignment(project string, assignment state.Assignment) ActiveAssignment {
	if assignment.Task.Project != "" {
		project = assignment.Task.Project
	}
	task := Task{
		Project:      project,
		Issue:        assignment.Task.Issue,
		Status:       assignment.Task.Status,
		Prompt:       assignment.Task.Prompt,
		CallerPane:   assignment.Task.CallerPane,
		WorkerID:     assignment.Task.WorkerID,
		PaneID:       assignment.Worker.CurrentPaneID,
		PaneName:     workerPaneName(assignment.Task.Issue, assignment.Worker.WorkerID),
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
		Project:                      firstNonEmpty(assignment.Worker.Project, project),
		WorkerID:                     assignment.Worker.WorkerID,
		PaneID:                       assignment.Worker.CurrentPaneID,
		PaneName:                     workerPaneName(assignment.Worker.Issue, assignment.Worker.WorkerID),
		Issue:                        assignment.Worker.Issue,
		ClonePath:                    assignment.Worker.ClonePath,
		AgentProfile:                 assignment.Worker.Agent,
		Health:                       assignment.Worker.State,
		LastReviewCount:              assignment.Worker.LastReviewCount,
		LastInlineReviewCommentCount: assignment.Worker.LastInlineReviewCommentCount,
		LastIssueCommentCount:        assignment.Worker.LastIssueCommentCount,
		ReviewNudgeCount:             assignment.Worker.ReviewNudgeCount,
		LastCIState:                  assignment.Worker.LastCIState,
		CINudgeCount:                 assignment.Worker.CINudgeCount,
		CIFailurePollCount:           assignment.Worker.CIFailurePollCount,
		CIEscalated:                  assignment.Worker.CIEscalated,
		LastMergeableState:           assignment.Worker.LastMergeableState,
		NudgeCount:                   assignment.Worker.NudgeCount,
		RestartCount:                 assignment.Worker.RestartCount,
		LastCapture:                  assignment.Worker.LastCapture,
		LastActivityAt:               assignment.Worker.LastActivityAt,
		LastPRNumber:                 assignment.Worker.LastPRNumber,
		LastPushAt:                   assignment.Worker.LastPushAt,
		LastPRPollAt:                 assignment.Worker.LastPRPollAt,
		FirstCrashAt:                 assignment.Worker.FirstCrashAt,
		CreatedAt:                    assignment.Worker.CreatedAt,
		LastSeenAt:                   assignment.Worker.LastSeenAt,
		UpdatedAt:                    assignment.Worker.LastSeenAt,
	}

	return ActiveAssignment{
		Task:   task,
		Worker: worker,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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
