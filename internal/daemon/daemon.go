package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultCaptureInterval       = 5 * time.Second
	defaultAgentHandshakeTimeout = 30 * time.Second
	defaultPollInterval          = 30 * time.Second
	defaultMergeGracePeriod      = 10 * time.Minute
)

var autonomousBacklogPromptPattern = regexp.MustCompile(`(?i)pick up.*(work|issue|task|ticket)|from.*(backlog|queue|linear)|new work|next (issue|task|ticket)|find.*(issue|task|work).*backlog`)

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
	captureInterval  time.Duration
	pollInterval     time.Duration
	mergeGracePeriod time.Duration

	mu          sync.Mutex
	started     bool
	stopContext context.Context
	stopCancel  context.CancelFunc
	assignments map[string]*assignment
	mergeQueue  []int
	mergeQueued map[int]struct{}
	activeMerge int
	mergeBusy   bool
	postmortems map[string]time.Time
	wg          sync.WaitGroup
}

type assignment struct {
	ctx                context.Context
	cancel             context.CancelFunc
	captureTick        Ticker
	pollTick           Ticker
	pending            bool
	profile            AgentProfile
	task               Task
	worker             Worker
	clone              Clone
	pane               Pane
	startedAt          time.Time
	lastOutput         string
	lastActivity       time.Time
	prNumber           int
	lastMergeableState atomic.Value
	lastCIState        string
	lastReviewCount    atomic.Int64
	escalated          bool
	cleanupOnce        sync.Once
}

func (a *assignment) mergeableState() string {
	value := a.lastMergeableState.Load()
	if value == nil {
		return ""
	}
	state, _ := value.(string)
	return state
}

func (a *assignment) setMergeableState(state string) {
	a.lastMergeableState.Store(state)
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
		captureInterval:  opts.CaptureInterval,
		pollInterval:     opts.PollInterval,
		mergeGracePeriod: opts.MergeGracePeriod,
		assignments:      make(map[string]*assignment),
		mergeQueued:      make(map[int]struct{}),
		postmortems:      make(map[string]time.Time),
	}, nil
}

func (d *Daemon) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.started {
		return ErrAlreadyStarted
	}
	if err := os.MkdirAll(filepath.Dir(d.pidPath), 0o755); err != nil {
		return fmt.Errorf("create pid directory: %w", err)
	}
	if _, err := os.Stat(d.pidPath); err == nil {
		return fmt.Errorf("pid file already exists: %w", ErrAlreadyStarted)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("stat pid file: %w", err)
	}
	if err := os.WriteFile(d.pidPath, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0o644); err != nil {
		return fmt.Errorf("write pid file: %w", err)
	}

	d.stopContext, d.stopCancel = context.WithCancel(context.Background())
	d.started = true
	d.emit(ctx, Event{
		Time:    d.now(),
		Type:    EventDaemonStarted,
		Project: d.project,
		Message: "daemon started",
	})
	return nil
}

func (d *Daemon) Stop(ctx context.Context) error {
	d.mu.Lock()
	if !d.started {
		d.mu.Unlock()
		return ErrNotStarted
	}
	assignments := make([]*assignment, 0, len(d.assignments))
	for _, active := range d.assignments {
		assignments = append(assignments, active)
	}
	stopCancel := d.stopCancel
	d.started = false
	d.mu.Unlock()

	if stopCancel != nil {
		stopCancel()
	}
	for _, active := range assignments {
		active.cancel()
	}
	d.wg.Wait()

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

	placeholder := &assignment{
		pending: true,
		task: Task{
			Project: d.project,
			Issue:   issue,
			Branch:  issue,
		},
	}
	d.mu.Lock()
	if _, exists := d.assignments[issue]; exists {
		d.mu.Unlock()
		return fmt.Errorf("issue %s already assigned", issue)
	}
	d.assignments[issue] = placeholder
	d.mu.Unlock()

	releaseReservation := func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		if current, ok := d.assignments[issue]; ok && current == placeholder {
			delete(d.assignments, issue)
		}
	}

	if err := d.validateAssignment(ctx, issue, prompt); err != nil {
		releaseReservation()
		return err
	}

	clone, err := d.pool.Acquire(ctx, d.project, issue)
	if err != nil {
		releaseReservation()
		return fmt.Errorf("acquire clone: %w", err)
	}

	if err := d.prepareClone(ctx, clone.Path, issue); err != nil {
		_ = d.pool.Release(ctx, d.project, clone)
		releaseReservation()
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
		releaseReservation()
		return fmt.Errorf("spawn pane: %w", err)
	}

	if err := d.setPaneMetadata(ctx, pane.ID, assignmentMetadata(profile.Name, issue, issue, 0)); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		releaseReservation()
		return fmt.Errorf("set pane metadata: %w", err)
	}

	if err := d.agentHandshake(ctx, pane.ID, profile); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, releaseReservation)
		return fmt.Errorf("agent handshake: %w", err)
	}

	if err := d.amux.SendKeys(ctx, pane.ID, prompt, "Enter"); err != nil {
		d.failPendingAssignment(ctx, issue, clone, pane, profile, err, releaseReservation)
		return fmt.Errorf("send prompt: %w", err)
	}
	if err := d.setIssueStatus(ctx, issue, IssueStateInProgress); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		releaseReservation()
		return fmt.Errorf("set issue status: %w", err)
	}

	now := d.now()
	task := Task{
		Project:      d.project,
		Issue:        issue,
		Status:       TaskStatusActive,
		PaneID:       pane.ID,
		PaneName:     pane.Name,
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       issue,
		AgentProfile: profile.Name,
		UpdatedAt:    now,
	}
	worker := Worker{
		Project:      d.project,
		PaneID:       pane.ID,
		PaneName:     pane.Name,
		Issue:        issue,
		ClonePath:    clone.Path,
		AgentProfile: profile.Name,
		Health:       WorkerHealthHealthy,
		UpdatedAt:    now,
	}
	if err := d.state.PutTask(ctx, task); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		releaseReservation()
		return fmt.Errorf("store task: %w", err)
	}
	if err := d.state.PutWorker(ctx, worker); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		releaseReservation()
		return fmt.Errorf("store worker: %w", err)
	}

	monitorCtx, monitorCancel := context.WithCancel(d.stopContext)
	active := &assignment{
		ctx:          monitorCtx,
		cancel:       monitorCancel,
		captureTick:  d.newTicker(d.captureInterval),
		pollTick:     d.newTicker(d.pollInterval),
		profile:      profile,
		task:         task,
		worker:       worker,
		clone:        clone,
		pane:         pane,
		startedAt:    now,
		lastActivity: now,
	}

	d.mu.Lock()
	d.assignments[issue] = active
	d.mu.Unlock()

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

	d.wg.Add(1)
	go d.monitorAssignment(active)
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

	active, err := d.assignment(issue)
	if err != nil {
		return err
	}
	return d.finishAssignment(ctx, active, TaskStatusCancelled, EventTaskCancelled, false)
}
