package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultCaptureInterval       = 5 * time.Second
	defaultAgentHandshakeTimeout = 30 * time.Second
	defaultPromptSettleDuration  = 2 * time.Second
	defaultTrustPromptTimeout    = 2 * time.Second
	defaultPollInterval          = 30 * time.Second
	defaultMergeGracePeriod      = 10 * time.Minute
)

var autonomousBacklogPromptPattern = regexp.MustCompile(strings.Join([]string{
	`(?i)\bpick up\b.*\b(?:work|issue|task|ticket)\b`,
	`\bfrom\b.*\b(?:backlog|queue)\b`,
	`\bnew work\b`,
	`\bnext\s+(?:issue|task|ticket)\b`,
	`\bfind\b.*\b(?:issue|task|work)\b.*\bbacklog\b`,
}, "|"))

type Daemon struct {
	project          string
	session          string
	leadPane         string
	pidPath          string
	config           ConfigProvider
	state            StateStore
	pool             Pool
	amux             AmuxClient
	issueTracker     IssueTracker
	commands         CommandRunner
	github           gitHubClient
	events           EventSink
	now              func() time.Time
	newTicker        func(time.Duration) Ticker
	sleep            func(context.Context, time.Duration) error
	captureInterval  time.Duration
	pollInterval     time.Duration
	mergeGracePeriod time.Duration

	started           atomic.Bool
	stopContext       context.Context
	stopCancel        context.CancelFunc
	loopDone          chan struct{}
	mergeQueueInbox   chan ProcessQueue
	mergeQueueUpdates chan MergeQueueUpdate
	mergeQueueDone    chan struct{}
	taskMonitorMu     sync.Mutex
	taskMonitors      map[string]*TaskMonitor
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
	if opts.Sleep == nil {
		opts.Sleep = sleepContext
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

	return &Daemon{
		project:          opts.Project,
		session:          opts.Session,
		leadPane:         opts.LeadPane,
		pidPath:          opts.PIDPath,
		config:           opts.Config,
		state:            opts.State,
		pool:             opts.Pool,
		amux:             opts.Amux,
		issueTracker:     opts.IssueTracker,
		commands:         opts.Commands,
		github:           newDefaultGitHubClient(opts.Project, opts.Commands),
		events:           opts.Events,
		now:              opts.Now,
		newTicker:        opts.NewTicker,
		sleep:            opts.Sleep,
		captureInterval:  opts.CaptureInterval,
		pollInterval:     opts.PollInterval,
		mergeGracePeriod: opts.MergeGracePeriod,
	}, nil
}

func (d *Daemon) Start(ctx context.Context) error {
	if !d.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	if err := d.initializePIDFile(); err != nil {
		d.started.Store(false)
		return err
	}

	d.normalizeLeadPane(ctx)
	d.stopContext, d.stopCancel = context.WithCancel(context.Background())
	d.reconcileNonTerminalAssignments(ctx)
	d.refreshTaskMonitors(ctx)
	d.resetMergeQueueTransientStatuses(ctx)
	d.mergeQueueInbox = make(chan ProcessQueue)
	d.mergeQueueUpdates = make(chan MergeQueueUpdate, 32)
	d.mergeQueueDone = make(chan struct{})
	actor := newMergeQueueActor(d.project, d.commands, d.mergeQueueUpdates)
	go actor.run(d.stopContext, d.mergeQueueInbox, d.mergeQueueDone)
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

func (d *Daemon) normalizeLeadPane(ctx context.Context) {
	leadPane := strings.TrimSpace(d.leadPane)
	if leadPane == "" {
		return
	}

	// Best effort only: if amux is unavailable at startup we keep the caller's
	// configured reference and let later spawn operations surface the error.
	panes, err := d.amux.ListPanes(ctx)
	if err != nil {
		return
	}

	for _, pane := range panes {
		paneID := strings.TrimSpace(pane.ID)
		paneName := strings.TrimSpace(pane.Name)
		if paneName == "" {
			continue
		}
		if leadPane == paneID || leadPane == paneName {
			d.leadPane = paneName
			return
		}
	}
}

func (d *Daemon) initializePIDFile() error {
	if err := os.MkdirAll(filepath.Dir(d.pidPath), 0o755); err != nil {
		return fmt.Errorf("create pid directory: %w", err)
	}
	if err := d.removeStalePIDFile(); err != nil {
		return err
	}
	if err := os.WriteFile(d.pidPath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o644); err != nil {
		return fmt.Errorf("write pid file: %w", err)
	}
	return nil
}

func (d *Daemon) removeStalePIDFile() error {
	return d.removeStalePIDFileWithProcessCheck(processAlive)
}

func (d *Daemon) removeStalePIDFileWithProcessCheck(processCheck func(int) (bool, error)) error {
	pid, err := readPIDFile(d.pidPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read pid file: %w", err)
	}

	alive, err := processCheck(pid)
	if err != nil {
		return fmt.Errorf("check pid file process: %w", err)
	}
	if alive {
		return fmt.Errorf("daemon already running: %w", ErrAlreadyStarted)
	}
	if err := os.Remove(d.pidPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale pid file: %w", err)
	}
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
	if d.mergeQueueDone != nil {
		<-d.mergeQueueDone
	}

	if err := os.Remove(d.pidPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove pid file: %w", err)
	}

	d.mergeQueueInbox = nil
	d.mergeQueueUpdates = nil
	d.mergeQueueDone = nil

	d.emit(ctx, Event{
		Time:    d.now(),
		Type:    EventDaemonStopped,
		Project: d.project,
		Message: "daemon stopped",
	})
	return nil
}

func (d *Daemon) Assign(ctx context.Context, issue, prompt, agentProfile string, title ...string) error {
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

	assignmentBranch := issue
	prNumber, err := d.lookupOpenPRNumber(ctx, assignmentBranch)
	if err != nil {
		return fmt.Errorf("check open PRs for %s: %w", issue, err)
	}
	adoptingOpenPR := prNumber > 0

	now := d.now()
	claimedTask := Task{
		Project:      d.project,
		Issue:        issue,
		Status:       TaskStatusStarting,
		Prompt:       prompt,
		Branch:       assignmentBranch,
		AgentProfile: profile.Name,
		PRNumber:     prNumber,
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

	prepareClone := d.prepareClone
	if adoptingOpenPR {
		prepareClone = d.prepareAdoptedClone
	}
	if err := prepareClone(ctx, clone.Path, assignmentBranch); err != nil {
		_ = d.pool.Release(ctx, d.project, clone)
		restoreReservation()
		return fmt.Errorf("prepare clone: %w", err)
	}
	clone.CurrentBranch = assignmentBranch
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

	metadata, err := d.assignmentPaneMetadata(ctx, pane.ID, profile.Name, assignmentBranch, issue, d.resolveAssignmentTitle(ctx, issue, firstTitle(title)), prNumber)
	if err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		restoreReservation()
		return fmt.Errorf("build pane metadata: %w", err)
	}

	if err := d.setPaneMetadata(ctx, pane.ID, metadata); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		restoreReservation()
		return fmt.Errorf("set pane metadata: %w", err)
	}

	task := claimedTask
	task.PaneID = pane.ID
	task.PaneName = pane.Name
	task.CloneName = clone.Name
	task.ClonePath = clone.Path
	task.UpdatedAt = d.now()
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
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("store pending task: %w", err)
	}
	if err := d.state.PutWorker(ctx, worker); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("store pending worker: %w", err)
	}

	if err := d.agentHandshake(ctx, pane.ID, profile); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("agent handshake: %w", err)
	}

	if err := d.amux.SendKeys(ctx, pane.ID, prompt); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.amux.WaitIdleSettle(ctx, pane.ID, defaultAgentHandshakeTimeout, defaultPromptSettleDuration); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.amux.SendKeys(ctx, pane.ID, "Enter"); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.confirmPromptDelivery(ctx, pane.ID, profile); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.setIssueStatus(ctx, issue, IssueStateInProgress); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("set issue status: %w", err)
	}

	task.Status = TaskStatusActive
	task.UpdatedAt = d.now()
	if err := d.state.PutTask(ctx, task); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, restoreReservation)
		return fmt.Errorf("store task: %w", err)
	}
	d.ensureTaskMonitor(issue)

	d.emit(ctx, Event{
		Time:         now,
		Type:         EventTaskAssigned,
		Project:      d.project,
		Issue:        issue,
		PaneID:       pane.ID,
		PaneName:     pane.Name,
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       assignmentBranch,
		AgentProfile: profile.Name,
		PRNumber:     prNumber,
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
	if deleteErr := d.state.DeleteWorker(context.WithoutCancel(ctx), d.project, pane.ID); deleteErr != nil && !errors.Is(deleteErr, ErrWorkerNotFound) {
		_ = deleteErr
	}
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
