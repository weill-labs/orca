package orca

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Daemon struct {
	cfg   Config
	amux  AmuxClient
	repos RepositoryManager
	prs   PRTracker
	clock Clock
	store *Store
}

func NewDaemon(cfg Config, deps Dependencies) (*Daemon, error) {
	projectPath, err := expandPath(cfg.ProjectPath)
	if err != nil {
		return nil, fmt.Errorf("expand project path: %w", err)
	}
	if projectPath == "" {
		return nil, fmt.Errorf("orca: project path is required")
	}

	statePath, err := expandPath(cfg.StatePath)
	if err != nil {
		return nil, fmt.Errorf("expand state path: %w", err)
	}
	if statePath == "" {
		return nil, fmt.Errorf("orca: state path is required")
	}

	poolPattern, err := expandPath(cfg.PoolPattern)
	if err != nil {
		return nil, fmt.Errorf("expand pool pattern: %w", err)
	}
	if poolPattern == "" {
		return nil, fmt.Errorf("orca: pool pattern is required")
	}

	if deps.AMUX == nil {
		return nil, fmt.Errorf("orca: amux client is required")
	}
	if deps.Repos == nil {
		return nil, fmt.Errorf("orca: repository manager is required")
	}
	if deps.PRs == nil {
		return nil, fmt.Errorf("orca: PR tracker is required")
	}
	if deps.Clock == nil {
		deps.Clock = realClock{}
	}

	cfg.ProjectPath = projectPath
	cfg.StatePath = statePath
	cfg.PoolPattern = poolPattern
	cfg.Agent = normalizeAgent(cfg.Agent)

	store, err := OpenStore(cfg.StatePath)
	if err != nil {
		return nil, err
	}

	return &Daemon{
		cfg:   cfg,
		amux:  deps.AMUX,
		repos: deps.Repos,
		prs:   deps.PRs,
		clock: deps.Clock,
		store: store,
	}, nil
}

func (d *Daemon) Close() error {
	return d.store.Close()
}

func (d *Daemon) Start(ctx context.Context) error {
	now := d.clock.Now()
	if err := d.store.Init(ctx, d.cfg.ProjectPath, d.cfg.Session, now); err != nil {
		return err
	}

	paths, err := d.discoverClonePaths()
	if err != nil {
		return err
	}
	if len(paths) == 0 {
		return ErrNoEligibleClones
	}

	for _, path := range paths {
		if err := d.store.EnsureClone(ctx, Clone{
			Project:   d.cfg.ProjectPath,
			Path:      path,
			Status:    CloneStatusFree,
			Branch:    "main",
			TaskID:    "",
			UpdatedAt: now,
		}); err != nil {
			return err
		}
	}

	return d.store.LogEvent(ctx, Event{
		Project:   d.cfg.ProjectPath,
		Type:      "daemon_started",
		Message:   fmt.Sprintf("daemon started with %d eligible clones", len(paths)),
		CreatedAt: now,
	})
}

func (d *Daemon) Assign(ctx context.Context, req AssignRequest) (Assignment, error) {
	now := d.clock.Now()
	issueID := strings.TrimSpace(req.IssueID)
	if issueID == "" {
		return Assignment{}, fmt.Errorf("orca: issue ID is required")
	}

	branch := issueID
	clone, err := d.store.ReserveClone(ctx, d.cfg.ProjectPath, branch, issueID, now)
	if err != nil {
		return Assignment{}, err
	}

	releaseClone := true
	defer func() {
		if !releaseClone {
			return
		}
		_ = d.store.PutClone(ctx, Clone{
			Project:   d.cfg.ProjectPath,
			Path:      clone.Path,
			Status:    CloneStatusFree,
			Branch:    "main",
			TaskID:    "",
			UpdatedAt: d.clock.Now(),
		})
	}()

	if err := d.repos.Prepare(ctx, clone.Path, branch); err != nil {
		return Assignment{}, fmt.Errorf("prepare clone %s: %w", clone.Path, err)
	}

	pane, err := d.amux.SpawnPane(ctx, SpawnRequest{
		CWD:     clone.Path,
		Command: d.cfg.Agent.StartCommand,
	})
	if err != nil {
		return Assignment{}, fmt.Errorf("spawn amux pane: %w", err)
	}
	defer func() {
		if !releaseClone {
			return
		}
		_ = d.amux.KillPane(ctx, pane.ID)
	}()

	if err := d.amux.SetMetadata(ctx, pane.ID, map[string]string{
		"task":   issueID,
		"issue":  issueID,
		"branch": branch,
	}); err != nil {
		return Assignment{}, fmt.Errorf("set pane metadata: %w", err)
	}

	prompt := req.Prompt
	if prompt != "" && !strings.HasSuffix(prompt, "\n") {
		prompt += "\n"
	}
	if prompt != "" {
		if err := d.amux.SendKeys(ctx, pane.ID, prompt); err != nil {
			return Assignment{}, fmt.Errorf("send task prompt: %w", err)
		}
	}

	task := Task{
		Project:        d.cfg.ProjectPath,
		IssueID:        issueID,
		Status:         TaskStatusActive,
		Branch:         branch,
		ClonePath:      clone.Path,
		WorkerID:       pane.ID,
		Prompt:         prompt,
		PRNumber:       0,
		NudgeCount:     0,
		LastCapture:    "",
		LastActivityAt: now,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := d.store.PutTask(ctx, task); err != nil {
		return Assignment{}, err
	}

	worker := Worker{
		Project:        d.cfg.ProjectPath,
		PaneID:         pane.ID,
		PaneName:       pane.Name,
		Agent:          d.cfg.Agent.Name,
		TaskID:         issueID,
		ClonePath:      clone.Path,
		State:          WorkerStateRunning,
		LastCapture:    "",
		LastActivityAt: now,
		NudgeCount:     0,
		UpdatedAt:      now,
	}
	if err := d.store.PutWorker(ctx, worker); err != nil {
		return Assignment{}, err
	}

	if err := d.store.LogEvent(ctx, Event{
		Project:   d.cfg.ProjectPath,
		Type:      "task_assigned",
		TaskID:    issueID,
		WorkerID:  pane.ID,
		ClonePath: clone.Path,
		Message:   fmt.Sprintf("%s assigned to %s in %s", issueID, pane.ID, clone.Path),
		CreatedAt: now,
	}); err != nil {
		return Assignment{}, err
	}

	releaseClone = false
	return Assignment{
		IssueID:   issueID,
		Branch:    branch,
		ClonePath: clone.Path,
		PaneID:    pane.ID,
		PaneName:  pane.Name,
	}, nil
}

func (d *Daemon) MonitorOnce(ctx context.Context) error {
	tasks, err := d.store.ActiveTasks(ctx, d.cfg.ProjectPath)
	if err != nil {
		return err
	}

	for _, task := range tasks {
		worker, err := d.store.Worker(ctx, d.cfg.ProjectPath, task.WorkerID)
		if err != nil {
			return err
		}

		screen, err := d.amux.Capture(ctx, worker.PaneID)
		if err != nil {
			return fmt.Errorf("capture pane %s: %w", worker.PaneID, err)
		}

		now := d.clock.Now()
		if screen != task.LastCapture {
			task.LastCapture = screen
			task.LastActivityAt = now
			task.UpdatedAt = now
			worker.LastCapture = screen
			worker.LastActivityAt = now
			worker.UpdatedAt = now
		}

		pr, err := d.prs.BranchStatus(ctx, task.Branch)
		if err != nil {
			return fmt.Errorf("lookup PR status for %s: %w", task.Branch, err)
		}
		if pr.Number != 0 {
			task.PRNumber = pr.Number
		}
		if pr.Merged {
			if err := d.completeMergedTask(ctx, task, worker, now); err != nil {
				return err
			}
			continue
		}

		if d.shouldNudge(task, screen, now) {
			if task.NudgeCount < d.cfg.Agent.MaxNudgeRetries {
				if err := d.amux.SendKeys(ctx, worker.PaneID, d.cfg.Agent.NudgeCommand); err != nil {
					return fmt.Errorf("send nudge to %s: %w", worker.PaneID, err)
				}
				task.NudgeCount++
				task.UpdatedAt = now
				worker.NudgeCount = task.NudgeCount
				worker.State = WorkerStateNudged
				worker.UpdatedAt = now
				if err := d.store.LogEvent(ctx, Event{
					Project:   d.cfg.ProjectPath,
					Type:      "worker_nudged",
					TaskID:    task.IssueID,
					WorkerID:  worker.PaneID,
					ClonePath: task.ClonePath,
					Message:   fmt.Sprintf("%s nudged after stuck detection", task.IssueID),
					CreatedAt: now,
				}); err != nil {
					return err
				}
			} else if worker.State != WorkerStateAttention {
				worker.State = WorkerStateAttention
				worker.UpdatedAt = now
				if err := d.store.LogEvent(ctx, Event{
					Project:   d.cfg.ProjectPath,
					Type:      "worker_escalated",
					TaskID:    task.IssueID,
					WorkerID:  worker.PaneID,
					ClonePath: task.ClonePath,
					Message:   fmt.Sprintf("%s exceeded nudge retries", task.IssueID),
					CreatedAt: now,
				}); err != nil {
					return err
				}
			}
		} else if worker.State != WorkerStateRunning {
			worker.State = WorkerStateRunning
			worker.UpdatedAt = now
		}

		if err := d.store.PutTask(ctx, task); err != nil {
			return err
		}
		if err := d.store.PutWorker(ctx, worker); err != nil {
			return err
		}
	}

	return nil
}

func (d *Daemon) Task(ctx context.Context, issueID string) (Task, error) {
	return d.store.Task(ctx, d.cfg.ProjectPath, issueID)
}

func (d *Daemon) Worker(ctx context.Context, paneID string) (Worker, error) {
	return d.store.Worker(ctx, d.cfg.ProjectPath, paneID)
}

func (d *Daemon) Clone(ctx context.Context, path string) (Clone, error) {
	normalizedPath, err := expandPath(path)
	if err != nil {
		return Clone{}, err
	}
	return d.store.Clone(ctx, d.cfg.ProjectPath, normalizedPath)
}

func (d *Daemon) Clones(ctx context.Context) ([]Clone, error) {
	return d.store.Clones(ctx, d.cfg.ProjectPath)
}

func (d *Daemon) Events(ctx context.Context) ([]Event, error) {
	return d.store.Events(ctx, d.cfg.ProjectPath)
}

func (d *Daemon) completeMergedTask(ctx context.Context, task Task, worker Worker, now time.Time) error {
	if err := d.amux.SendKeys(ctx, worker.PaneID, d.cfg.Agent.WrapUpMessage); err != nil {
		return fmt.Errorf("send merge wrap-up to %s: %w", worker.PaneID, err)
	}
	if err := d.amux.KillPane(ctx, worker.PaneID); err != nil {
		return fmt.Errorf("kill pane %s: %w", worker.PaneID, err)
	}
	if err := d.repos.Cleanup(ctx, task.ClonePath, task.Branch); err != nil {
		return fmt.Errorf("cleanup clone %s: %w", task.ClonePath, err)
	}

	task.Status = TaskStatusDone
	task.UpdatedAt = now
	worker.State = WorkerStateExited
	worker.UpdatedAt = now

	if err := d.store.PutTask(ctx, task); err != nil {
		return err
	}
	if err := d.store.PutWorker(ctx, worker); err != nil {
		return err
	}
	if err := d.store.PutClone(ctx, Clone{
		Project:   d.cfg.ProjectPath,
		Path:      task.ClonePath,
		Status:    CloneStatusFree,
		Branch:    "main",
		TaskID:    "",
		UpdatedAt: now,
	}); err != nil {
		return err
	}

	if err := d.store.LogEvent(ctx, Event{
		Project:   d.cfg.ProjectPath,
		Type:      "pr_merged",
		TaskID:    task.IssueID,
		WorkerID:  worker.PaneID,
		ClonePath: task.ClonePath,
		Message:   fmt.Sprintf("%s merged as PR #%d", task.IssueID, task.PRNumber),
		CreatedAt: now,
	}); err != nil {
		return err
	}
	if err := d.store.LogEvent(ctx, Event{
		Project:   d.cfg.ProjectPath,
		Type:      "clone_cleaned",
		TaskID:    task.IssueID,
		WorkerID:  worker.PaneID,
		ClonePath: task.ClonePath,
		Message:   fmt.Sprintf("%s cleaned after merge", task.ClonePath),
		CreatedAt: now,
	}); err != nil {
		return err
	}

	return nil
}

func (d *Daemon) shouldNudge(task Task, screen string, now time.Time) bool {
	for _, pattern := range d.cfg.Agent.StuckTextPatterns {
		if pattern != "" && strings.Contains(screen, pattern) {
			return true
		}
	}
	if task.PRNumber != 0 {
		return false
	}
	if d.cfg.Agent.StuckTimeout <= 0 {
		return false
	}
	return now.Sub(task.LastActivityAt) >= d.cfg.Agent.StuckTimeout
}

func (d *Daemon) discoverClonePaths() ([]string, error) {
	paths, err := filepath.Glob(d.cfg.PoolPattern)
	if err != nil {
		return nil, fmt.Errorf("glob clone pool: %w", err)
	}

	sort.Strings(paths)

	var eligible []string
	for _, candidate := range paths {
		info, err := os.Stat(candidate)
		if err != nil || !info.IsDir() {
			continue
		}
		marker := filepath.Join(candidate, ".orca-pool")
		if _, err := os.Stat(marker); err != nil {
			continue
		}
		absolute, err := filepath.Abs(candidate)
		if err != nil {
			return nil, fmt.Errorf("resolve clone path %s: %w", candidate, err)
		}
		eligible = append(eligible, absolute)
	}

	return eligible, nil
}

func normalizeAgent(agent AgentProfile) AgentProfile {
	if agent.Name == "" {
		agent.Name = "codex"
	}
	if agent.NudgeCommand == "" {
		agent.NudgeCommand = "\n"
	}
	if agent.MaxNudgeRetries <= 0 {
		agent.MaxNudgeRetries = 1
	}
	if agent.WrapUpMessage == "" {
		agent.WrapUpMessage = "PR merged, wrap up.\n"
	}
	return agent
}

func expandPath(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if strings.HasPrefix(value, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("lookup home directory: %w", err)
		}
		value = filepath.Join(home, strings.TrimPrefix(value, "~/"))
	}
	return filepath.Abs(value)
}
