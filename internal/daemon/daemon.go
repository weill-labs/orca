package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	defaultCaptureInterval       = 5 * time.Second
	defaultAgentHandshakeTimeout = 30 * time.Second
	defaultPollInterval          = 30 * time.Second
	defaultMergeGracePeriod      = 10 * time.Minute
	defaultPostmortemWindow      = 10 * time.Minute
	defaultPostmortemTimeout     = 2 * time.Minute
)

var autonomousBacklogPromptPattern = regexp.MustCompile(`(?i)pick up.*(work|issue|task|ticket)|from.*(backlog|queue|linear)|new work|next (issue|task|ticket)|find.*(issue|task|work).*backlog`)

type Daemon struct {
	project           string
	session           string
	leadPane          string
	pidPath           string
	postmortemDir     string
	config            ConfigProvider
	state             StateStore
	pool              Pool
	amux              AmuxClient
	issueTracker      IssueTracker
	commands          CommandRunner
	github            gitHubClient
	events            EventSink
	now               func() time.Time
	newTicker         func(time.Duration) Ticker
	captureInterval   time.Duration
	pollInterval      time.Duration
	mergeGracePeriod  time.Duration
	postmortemWindow  time.Duration
	postmortemTimeout time.Duration
	sleep             func(context.Context, time.Duration) error
	signalProcess     func(int, syscall.Signal) error

	started     atomic.Bool
	stopContext context.Context
	stopCancel  context.CancelFunc
	loopDone    chan struct{}
}

type realTicker struct {
	*time.Ticker
}

func (t realTicker) C() <-chan time.Time {
	return t.Ticker.C
}

func New(opts Options) (*Daemon, error) {
	if opts.Project == "" {
		return nil, errors.New("project is required")
	}
	if opts.Config == nil {
		return nil, errors.New("config is required")
	}
	if opts.State == nil {
		return nil, errors.New("state is required")
	}
	if opts.Pool == nil {
		return nil, errors.New("pool is required")
	}
	if opts.Amux == nil {
		return nil, errors.New("amux is required")
	}
	if opts.Commands == nil {
		return nil, errors.New("commands are required")
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.NewTicker == nil {
		opts.NewTicker = func(interval time.Duration) Ticker {
			return realTicker{Ticker: time.NewTicker(interval)}
		}
	}
	if opts.CaptureInterval <= 0 {
		opts.CaptureInterval = defaultCaptureInterval
	}
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultPollInterval
	}
	if opts.MergeGracePeriod <= 0 {
		opts.MergeGracePeriod = defaultMergeGracePeriod
	}
	if opts.PostmortemWindow <= 0 {
		opts.PostmortemWindow = defaultPostmortemWindow
	}
	if opts.PostmortemTimeout <= 0 {
		opts.PostmortemTimeout = defaultPostmortemTimeout
	}
	if opts.Sleep == nil {
		opts.Sleep = sleepContext
	}
	if opts.SignalProcess == nil {
		opts.SignalProcess = syscall.Kill
	}
	if opts.PIDPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve home directory: %w", err)
		}
		opts.PIDPath = filepath.Join(home, ".config", "orca", "orca.pid")
	}
	if opts.Session == "" {
		opts.Session = "orca"
	}
	if opts.PostmortemDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, fmt.Errorf("resolve home directory: %w", err)
		}
		opts.PostmortemDir = filepath.Join(home, ".local", "share", "postmortems")
	}

	return &Daemon{
		project:           opts.Project,
		session:           opts.Session,
		leadPane:          opts.LeadPane,
		pidPath:           opts.PIDPath,
		postmortemDir:     opts.PostmortemDir,
		config:            opts.Config,
		state:             opts.State,
		pool:              opts.Pool,
		amux:              opts.Amux,
		issueTracker:      opts.IssueTracker,
		commands:          opts.Commands,
		github:            newDefaultGitHubClient(opts.Project, opts.Commands),
		events:            opts.Events,
		now:               opts.Now,
		newTicker:         opts.NewTicker,
		captureInterval:   opts.CaptureInterval,
		pollInterval:      opts.PollInterval,
		mergeGracePeriod:  opts.MergeGracePeriod,
		postmortemWindow:  opts.PostmortemWindow,
		postmortemTimeout: opts.PostmortemTimeout,
		sleep:             opts.Sleep,
		signalProcess:     opts.SignalProcess,
	}, nil
}

func (d *Daemon) Start(ctx context.Context) error {
	if !d.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	if err := os.MkdirAll(filepath.Dir(d.pidPath), 0o755); err != nil {
		d.started.Store(false)
		return fmt.Errorf("create pid directory: %w", err)
	}
	if _, err := os.Stat(d.pidPath); err == nil {
		d.started.Store(false)
		return fmt.Errorf("pid file already exists: %w", ErrAlreadyStarted)
	} else if !errors.Is(err, os.ErrNotExist) {
		d.started.Store(false)
		return fmt.Errorf("stat pid file: %w", err)
	}
	if err := os.WriteFile(d.pidPath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o644); err != nil {
		d.started.Store(false)
		return fmt.Errorf("write pid file: %w", err)
	}

	d.stopContext, d.stopCancel = context.WithCancel(context.Background())
	d.reconcileActiveAssignments(ctx)
	d.loopDone = make(chan struct{})
	go d.runLoop(d.stopContext, d.loopDone)

	d.emit(ctx, Event{
		Time:    d.now(),
		Type:    EventDaemonStarted,
		Project: d.project,
		Message: "daemon started",
	})
	return nil
}

func (d *Daemon) Stop(ctx context.Context) error {
	if !d.started.CompareAndSwap(true, false) {
		return ErrNotStarted
	}

	if d.stopCancel != nil {
		d.stopCancel()
	}
	if d.loopDone != nil {
		<-d.loopDone
	}

	if err := os.Remove(d.pidPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove pid file: %w", err)
	}

	d.emit(ctx, Event{
		Time:    d.now(),
		Type:    EventDaemonStopped,
		Project: d.project,
		Message: "daemon stopped",
	})
	return nil
}

func (d *Daemon) Assign(ctx context.Context, issue, prompt, agentProfile string) error {
	if err := d.requireStarted(); err != nil {
		return err
	}

	profile, err := d.config.AgentProfile(ctx, agentProfile)
	if err != nil {
		return fmt.Errorf("load agent profile %q: %w", agentProfile, err)
	}
	if profile.Name == "" {
		profile.Name = agentProfile
	}
	profile = enforceLifecycleProfile(profile)

	if err := d.validateAssignment(ctx, issue, prompt); err != nil {
		return err
	}

	now := d.now()
	claimedTask := Task{
		Project:      d.project,
		Issue:        issue,
		Status:       TaskStatusStarting,
		Prompt:       prompt,
		Branch:       issue,
		AgentProfile: profile.Name,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	previousTask, err := d.state.ClaimTask(ctx, claimedTask)
	if err != nil {
		return err
	}
	restoreReservation := func() {
		_ = d.state.RestoreTask(context.WithoutCancel(ctx), d.project, issue, previousTask)
	}

	clone, err := d.pool.Acquire(ctx, d.project, issue)
	if err != nil {
		restoreReservation()
		return fmt.Errorf("acquire clone: %w", err)
	}

	if err := d.prepareClone(ctx, clone.Path, issue); err != nil {
		_ = d.pool.Release(ctx, d.project, clone)
		restoreReservation()
		return fmt.Errorf("prepare clone: %w", err)
	}
	clone.CurrentBranch = issue
	clone.AssignedTask = issue

	pane, err := d.amux.Spawn(ctx, SpawnRequest{
		Session: d.session,
		AtPane:  d.leadPane,
		Name:    "worker-" + issue,
		CWD:     clone.Path,
		Command: profile.StartCommand,
	})
	if err != nil {
		_ = d.cleanupCloneAndRelease(ctx, clone, issue)
		restoreReservation()
		return fmt.Errorf("spawn pane: %w", err)
	}

	if err := d.setPaneMetadata(ctx, pane.ID, assignmentMetadata(profile.Name, issue, issue, 0)); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		restoreReservation()
		return fmt.Errorf("set pane metadata: %w", err)
	}

	if err := d.agentHandshake(ctx, pane.ID, profile); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("agent handshake: %w", err)
	}

	if err := d.amux.SendKeys(ctx, pane.ID, prompt, "Enter"); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.setIssueStatus(ctx, issue, IssueStateInProgress); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		restoreReservation()
		return fmt.Errorf("set issue status: %w", err)
	}

	task := claimedTask
	task.Status = TaskStatusActive
	task.PaneID = pane.ID
	task.PaneName = pane.Name
	task.CloneName = clone.Name
	task.ClonePath = clone.Path
	task.UpdatedAt = now
	worker := Worker{
		Project:        d.project,
		PaneID:         pane.ID,
		PaneName:       pane.Name,
		Issue:          issue,
		ClonePath:      clone.Path,
		AgentProfile:   profile.Name,
		Health:         WorkerHealthHealthy,
		LastActivityAt: now,
		UpdatedAt:      now,
	}
	if err := d.state.PutTask(ctx, task); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		restoreReservation()
		return fmt.Errorf("store task: %w", err)
	}
	if err := d.state.PutWorker(ctx, worker); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		restoreReservation()
		return fmt.Errorf("store worker: %w", err)
	}

	d.emit(ctx, Event{
		Time:         now,
		Type:         EventTaskAssigned,
		Project:      d.project,
		Issue:        issue,
		PaneID:       pane.ID,
		PaneName:     pane.Name,
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       issue,
		AgentProfile: profile.Name,
		Message:      "task assigned",
	})
	return nil
}

func (d *Daemon) validateAssignment(ctx context.Context, issue, prompt string) error {
	if err := validateAssignmentPrompt(prompt); err != nil {
		return err
	}

	existingTask, err := d.state.TaskByIssue(ctx, d.project, issue)
	if err == nil {
		if taskBlocksAssignment(existingTask.Status) {
			return fmt.Errorf("issue %s already assigned", issue)
		}
	} else if !errors.Is(err, ErrTaskNotFound) {
		return fmt.Errorf("load task %s: %w", issue, err)
	}

	prNumber, err := d.lookupOpenPRNumber(ctx, issue)
	if err != nil {
		return fmt.Errorf("check open PRs for %s: %w", issue, err)
	}
	if prNumber > 0 {
		return fmt.Errorf("issue %s already has open PR #%d", issue, prNumber)
	}

	return nil
}

func validateAssignmentPrompt(prompt string) error {
	if autonomousBacklogPromptPattern.MatchString(prompt) {
		return errors.New("assignment prompt cannot ask the worker to pick backlog work autonomously; assign a specific issue instead")
	}
	return nil
}

func (d *Daemon) failPendingAssignment(ctx context.Context, issue string, clone Clone, pane Pane, profile AgentProfile, err error, releaseReservation func()) {
	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventTaskAssignFailed,
		Project:      d.project,
		Issue:        issue,
		PaneID:       pane.ID,
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       issue,
		AgentProfile: profile.Name,
		Message:      err.Error(),
	})
	_ = d.rollbackAssignment(ctx, clone, pane, issue)
	releaseReservation()
}

func (d *Daemon) Cancel(ctx context.Context, issue string) error {
	if err := d.requireStarted(); err != nil {
		return err
	}

	active, err := d.state.ActiveAssignmentByIssue(ctx, d.project, issue)
	if err != nil {
		return err
	}
	return d.finishAssignment(ctx, active, TaskStatusCancelled, EventTaskCancelled, false)
}
