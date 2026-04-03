package daemon

import (
	"context"
	"encoding/json"
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
	defaultPollInterval          = 30 * time.Second
	defaultMergeGracePeriod      = 10 * time.Minute
	mergeQueueChecksIntervalSecs = "10"
	mergedWrapUpPrompt           = "PR merged, wrap up.\n"
	ciStateFail                  = "fail"
	ciStatePending               = "pending"
	ciStatePass                  = "pass"
	ciStateCancel                = "cancel"
	ciStateSkipping              = "skipping"
	postmortemCommand            = "$postmortem"
	conflictNudgePrompt          = "PR has merge conflicts, rebase onto origin/main and push.\n"
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

type prReviewPayload struct {
	ReviewDecision string     `json:"reviewDecision"`
	Reviews        []prReview `json:"reviews"`
}

type prReview struct {
	State  string `json:"state"`
	Body   string `json:"body"`
	Author struct {
		Login string `json:"login"`
	} `json:"author"`
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

func (d *Daemon) agentHandshake(ctx context.Context, paneID string, profile AgentProfile) error {
	if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
		return fmt.Errorf("wait for startup idle: %w", err)
	}

	trustConfirmed := false
	resumed := false

	for {
		output, err := d.amux.Capture(ctx, paneID)
		if err != nil {
			return fmt.Errorf("capture startup output: %w", err)
		}

		switch {
		case !trustConfirmed && hasTrustPrompt(profile, output):
			if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
				return fmt.Errorf("confirm trust prompt: %w", err)
			}
			trustConfirmed = true
		case !resumed && hasResumePrompt(profile, output):
			if err := d.amux.SendKeys(ctx, paneID, profile.ResumeSequence...); err != nil {
				return fmt.Errorf("resume prior session: %w", err)
			}
			resumed = true
		default:
			return nil
		}

		if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
			return fmt.Errorf("wait for post-startup action idle: %w", err)
		}
	}
}

func hasTrustPrompt(profile AgentProfile, output string) bool {
	switch strings.ToLower(profile.Name) {
	case "codex":
		return strings.Contains(strings.ToLower(output), "do you trust")
	default:
		return false
	}
}

func hasResumePrompt(profile AgentProfile, output string) bool {
	if len(profile.ResumeSequence) == 0 {
		return false
	}

	switch strings.ToLower(profile.Name) {
	case "codex":
		return strings.Contains(strings.ToLower(output), "resume")
	default:
		return false
	}
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

func (d *Daemon) Enqueue(ctx context.Context, prNumber int) (MergeQueueActionResult, error) {
	if err := d.requireStarted(); err != nil {
		return MergeQueueActionResult{}, err
	}

	active, err := d.assignmentByPRNumber(prNumber)
	if err != nil {
		return MergeQueueActionResult{}, err
	}

	now := d.now()

	d.mu.Lock()
	if _, exists := d.mergeQueued[prNumber]; exists {
		d.mu.Unlock()
		return MergeQueueActionResult{}, fmt.Errorf("PR #%d is already queued for landing", prNumber)
	}
	d.mergeQueue = append(d.mergeQueue, prNumber)
	d.mergeQueued[prNumber] = struct{}{}
	position := len(d.mergeQueue)
	if d.activeMerge != 0 {
		position++
	}
	shouldStart := !d.mergeBusy
	if shouldStart {
		d.mergeBusy = true
	}
	d.mu.Unlock()

	d.emit(ctx, d.mergeQueueEvent(active, EventPREnqueued, prNumber, "pull request queued for landing", now))

	if shouldStart {
		d.wg.Add(1)
		go d.processMergeQueue()
	}

	return MergeQueueActionResult{
		Project:   d.project,
		PRNumber:  prNumber,
		Status:    "queued",
		Position:  position,
		UpdatedAt: now,
	}, nil
}

func (d *Daemon) monitorAssignment(active *assignment) {
	defer d.wg.Done()
	defer active.captureTick.Stop()
	defer active.pollTick.Stop()

	for {
		select {
		case <-active.ctx.Done():
			return
		case <-active.captureTick.C():
			d.handleCapture(active)
		case <-active.pollTick.C():
			d.handlePRPoll(active)
		}
	}
}

func (d *Daemon) processMergeQueue() {
	defer d.wg.Done()

	for {
		prNumber, active, ok := d.nextQueuedPR()
		if !ok {
			return
		}
		d.processQueuedPR(prNumber, active)
		d.completeQueuedPR(prNumber)
	}
}

func (d *Daemon) nextQueuedPR() (int, *assignment, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if len(d.mergeQueue) == 0 {
		d.mergeBusy = false
		return 0, nil, false
	}

	prNumber := d.mergeQueue[0]
	d.mergeQueue = d.mergeQueue[1:]
	d.activeMerge = prNumber
	return prNumber, d.assignmentByPRNumberLocked(prNumber), true
}

func (d *Daemon) completeQueuedPR(prNumber int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.mergeQueued, prNumber)
	if d.activeMerge == prNumber {
		d.activeMerge = 0
	}
}

func (d *Daemon) processQueuedPR(prNumber int, active *assignment) {
	ctx := d.mergeQueueContext()
	if active == nil {
		if ctx.Err() != nil {
			return
		}
		d.emit(ctx, d.mergeQueueEvent(nil, EventPRLandingFailed, prNumber, fmt.Sprintf("PR #%d is no longer tracked by an active assignment", prNumber), d.now()))
		return
	}

	d.emit(ctx, d.mergeQueueEvent(active, EventPRLandingStarted, prNumber, "processing queued PR landing", d.now()))

	if err := d.rebaseQueuedPR(ctx, prNumber); err != nil {
		d.handleQueuedPRFailure(ctx, active, prNumber, mergeQueueRebaseConflictPrompt(prNumber), err)
		return
	}
	if err := d.waitForQueuedPRChecks(ctx, prNumber); err != nil {
		d.handleQueuedPRFailure(ctx, active, prNumber, mergeQueueChecksFailedPrompt(prNumber), err)
		return
	}
	if err := d.mergeQueuedPR(ctx, prNumber); err != nil {
		d.handleQueuedPRFailure(ctx, active, prNumber, mergeQueueMergeFailedPrompt(prNumber), err)
	}
}

func (d *Daemon) handleQueuedPRFailure(ctx context.Context, active *assignment, prNumber int, prompt string, err error) {
	if ctx.Err() != nil {
		return
	}

	_ = d.amux.SendKeys(ctx, active.pane.ID, ensureTrailingNewline(prompt))
	d.emit(ctx, d.mergeQueueEvent(active, EventPRLandingFailed, prNumber, err.Error(), d.now()))
}

func (d *Daemon) mergeQueueContext() context.Context {
	if d.stopContext != nil {
		return d.stopContext
	}
	return context.Background()
}

func (d *Daemon) handleCapture(active *assignment) {
	output, err := d.amux.Capture(active.ctx, active.pane.ID)
	if err != nil {
		return
	}

	now := d.now()
	changed := output != active.lastOutput
	if changed {
		wasStuck := active.worker.Health == WorkerHealthStuck || active.worker.NudgeCount > 0 || active.escalated
		active.lastOutput = output
		active.lastActivity = now
		active.escalated = false
		active.worker.Health = WorkerHealthHealthy
		active.worker.NudgeCount = 0
		active.worker.UpdatedAt = now
		active.task.UpdatedAt = now
		_ = d.state.PutWorker(active.ctx, active.worker)
		_ = d.state.PutTask(active.ctx, active.task)
		if wasStuck {
			d.emit(active.ctx, Event{
				Time:         now,
				Type:         EventWorkerRecovered,
				Project:      d.project,
				Issue:        active.task.Issue,
				PaneID:       active.pane.ID,
				PaneName:     active.pane.Name,
				CloneName:    active.clone.Name,
				ClonePath:    active.clone.Path,
				Branch:       active.task.Branch,
				AgentProfile: active.profile.Name,
				Message:      "worker output changed",
			})
		}
	}

	if d.matchesStuckPattern(active.profile, output) {
		d.nudgeOrEscalate(active, "matched stuck text pattern")
		return
	}
	if active.profile.StuckTimeout > 0 && now.Sub(active.lastActivity) >= active.profile.StuckTimeout {
		d.nudgeOrEscalate(active, "idle timeout exceeded")
	}
}

func (d *Daemon) handlePRPoll(active *assignment) {
	if active.prNumber == 0 {
		prNumber, err := d.lookupPRNumber(active.ctx, active.task.Branch)
		if err != nil {
			return
		}
		if prNumber > 0 {
			if err := d.setPaneMetadata(active.ctx, active.pane.ID, assignmentMetadata(active.profile.Name, active.task.Branch, active.task.Issue, prNumber)); err != nil {
				return
			}
			active.prNumber = prNumber
			active.task.PRNumber = prNumber
			active.task.UpdatedAt = d.now()
			_ = d.state.PutTask(active.ctx, active.task)
			d.emit(active.ctx, Event{
				Time:         d.now(),
				Type:         EventPRDetected,
				Project:      d.project,
				Issue:        active.task.Issue,
				PaneID:       active.pane.ID,
				PaneName:     active.pane.Name,
				CloneName:    active.clone.Name,
				ClonePath:    active.clone.Path,
				Branch:       active.task.Branch,
				AgentProfile: active.profile.Name,
				PRNumber:     prNumber,
				Message:      "pull request detected",
			})
		}
	}

	if active.prNumber == 0 {
		return
	}

	d.handlePRChecksPoll(active)

	merged, err := d.isPRMerged(active.ctx, active.prNumber)
	if err != nil || !merged {
		d.handlePRMergeablePoll(active)
		d.handlePRReviewPoll(active)
		return
	}

	message := "pull request merged"
	if err := d.setIssueStatus(active.ctx, active.task.Issue, IssueStateDone); err != nil {
		message = fmt.Sprintf("pull request merged (failed to update Linear issue status: %v)", err)
	}

	d.emit(active.ctx, Event{
		Time:         d.now(),
		Type:         EventPRMerged,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		PRNumber:     active.prNumber,
		Message:      message,
	})
	if err := d.finishAssignment(active.ctx, active, TaskStatusDone, EventTaskCompleted, true); err != nil {
		d.emit(active.ctx, Event{
			Time:         d.now(),
			Type:         EventTaskCompletionFailed,
			Project:      d.project,
			Issue:        active.task.Issue,
			PaneID:       active.pane.ID,
			PaneName:     active.pane.Name,
			CloneName:    active.clone.Name,
			ClonePath:    active.clone.Path,
			Branch:       active.task.Branch,
			AgentProfile: active.profile.Name,
			PRNumber:     active.prNumber,
			Message:      err.Error(),
		})
	}
}

func (d *Daemon) handlePRReviewPoll(active *assignment) {
	payload, ok, err := d.lookupPRReviews(active.ctx, active.prNumber)
	if err != nil || !ok {
		return
	}

	previousCount := int(active.lastReviewCount.Load())
	if previousCount > len(payload.Reviews) {
		active.lastReviewCount.Store(int64(len(payload.Reviews)))
		return
	}
	if previousCount == len(payload.Reviews) {
		return
	}

	newReviews := payload.Reviews[previousCount:]
	blocking := blockingReviews(payload.ReviewDecision, newReviews)
	if len(blocking) == 0 {
		active.lastReviewCount.Store(int64(len(payload.Reviews)))
		return
	}

	feedback := formatBlockingReviewFeedback(active.prNumber, blocking)
	if err := d.amux.SendKeys(active.ctx, active.pane.ID, ensureTrailingNewline(feedback)); err != nil {
		return
	}

	active.lastReviewCount.Store(int64(len(payload.Reviews)))
	d.emit(active.ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedReview,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		PRNumber:     active.prNumber,
		Message:      fmt.Sprintf("sent %d new blocking review(s) to worker", len(blocking)),
	})
}

func (d *Daemon) handlePRChecksPoll(active *assignment) {
	ciState, err := d.lookupPRChecksState(active.ctx, active.prNumber)
	if err != nil {
		return
	}

	previous := active.lastCIState
	if ciState != ciStateFail {
		active.lastCIState = ciState
		return
	}
	if previous == ciStateFail {
		return
	}
	if d.nudgeForCIFailure(active) {
		active.lastCIState = ciStateFail
	}
}

func (d *Daemon) handlePRMergeablePoll(active *assignment) {
	state, ok, err := d.lookupPRMergeableState(active.ctx, active.prNumber)
	if err != nil || !ok {
		return
	}

	previousState := active.mergeableState()
	if previousState == "CONFLICTING" || state != "CONFLICTING" {
		active.setMergeableState(state)
		return
	}

	if err := d.amux.SendKeys(active.ctx, active.pane.ID, conflictNudgePrompt); err != nil {
		return
	}

	active.setMergeableState(state)

	d.emit(active.ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedConflict,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		PRNumber:     active.prNumber,
		Message:      strings.TrimSpace(conflictNudgePrompt),
	})
}

func (d *Daemon) nudgeOrEscalate(active *assignment, reason string) {
	now := d.now()
	active.worker.Health = WorkerHealthStuck
	active.worker.UpdatedAt = now

	if active.worker.NudgeCount < active.profile.MaxNudgeRetries {
		if err := d.amux.SendKeys(active.ctx, active.pane.ID, active.profile.NudgeCommand); err != nil {
			return
		}
		active.worker.NudgeCount++
		_ = d.state.PutWorker(active.ctx, active.worker)
		d.emit(active.ctx, Event{
			Time:         now,
			Type:         EventWorkerNudged,
			Project:      d.project,
			Issue:        active.task.Issue,
			PaneID:       active.pane.ID,
			PaneName:     active.pane.Name,
			CloneName:    active.clone.Name,
			ClonePath:    active.clone.Path,
			Branch:       active.task.Branch,
			AgentProfile: active.profile.Name,
			Retry:        active.worker.NudgeCount,
			Message:      reason,
		})
		return
	}

	if active.escalated {
		return
	}
	active.escalated = true
	_ = d.state.PutWorker(active.ctx, active.worker)
	d.emit(active.ctx, Event{
		Time:         now,
		Type:         EventWorkerEscalated,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		Retry:        active.worker.NudgeCount,
		Message:      reason,
	})
}

func (d *Daemon) nudgeForCIFailure(active *assignment) bool {
	if active.profile.NudgeCommand == "" {
		return false
	}
	if err := d.amux.SendKeys(active.ctx, active.pane.ID, active.profile.NudgeCommand); err != nil {
		return false
	}

	d.emit(active.ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedCI,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		PRNumber:     active.prNumber,
		Message:      "pull request checks failing",
	})
	return true
}

func (d *Daemon) finishAssignment(ctx context.Context, active *assignment, status, eventType string, merged bool) error {
	if err := d.ensurePostmortem(ctx, active); err != nil {
		return err
	}

	var result error
	active.cleanupOnce.Do(func() {
		cleanupCtx := context.WithoutCancel(ctx)
		active.cancel()

		if merged {
			if err := d.amux.SendKeys(cleanupCtx, active.pane.ID, mergedWrapUpPrompt); err != nil {
				result = errors.Join(result, err)
			}
			if err := d.amux.WaitIdle(cleanupCtx, active.pane.ID, d.mergeGracePeriod); err != nil {
				result = errors.Join(result, d.amux.KillPane(cleanupCtx, active.pane.ID))
			}
		} else {
			result = errors.Join(result, d.amux.KillPane(cleanupCtx, active.pane.ID))
		}

		result = errors.Join(result, d.cleanupCloneAndRelease(cleanupCtx, active.clone, active.task.Branch))

		active.task.Status = status
		active.task.PRNumber = active.prNumber
		active.task.UpdatedAt = d.now()
		result = errors.Join(result, d.state.PutTask(cleanupCtx, active.task))
		result = errors.Join(result, d.state.DeleteWorker(cleanupCtx, d.project, active.pane.ID))

		d.mu.Lock()
		delete(d.assignments, active.task.Issue)
		d.mu.Unlock()

		message := "task finished"
		if status == TaskStatusCancelled {
			message = "task cancelled"
		}
		d.emit(cleanupCtx, Event{
			Time:         d.now(),
			Type:         eventType,
			Project:      d.project,
			Issue:        active.task.Issue,
			PaneID:       active.pane.ID,
			PaneName:     active.pane.Name,
			CloneName:    active.clone.Name,
			ClonePath:    active.clone.Path,
			Branch:       active.task.Branch,
			AgentProfile: active.profile.Name,
			PRNumber:     active.prNumber,
			Message:      message,
		})
	})
	return result
}

func (d *Daemon) rollbackAssignment(ctx context.Context, clone Clone, pane Pane, branch string) error {
	result := d.amux.KillPane(ctx, pane.ID)
	result = errors.Join(result, d.cleanupCloneAndRelease(ctx, clone, branch))
	return result
}

func (d *Daemon) cleanupCloneAndRelease(ctx context.Context, clone Clone, branch string) error {
	if clone.CurrentBranch == "" {
		clone.CurrentBranch = branch
	}
	if clone.AssignedTask == "" {
		clone.AssignedTask = branch
	}
	return d.pool.Release(ctx, d.project, clone)
}

func (d *Daemon) prepareClone(ctx context.Context, clonePath, branch string) error {
	commands := [][]string{
		{"checkout", "main"},
		{"pull"},
		{"checkout", "-B", branch},
	}
	for _, args := range commands {
		if _, err := d.commands.Run(ctx, clonePath, "git", args...); err != nil {
			return err
		}
	}
	return nil
}

func (d *Daemon) rebaseQueuedPR(ctx context.Context, prNumber int) error {
	_, err := d.commands.Run(ctx, d.project, "gh", "pr", "update-branch", fmt.Sprintf("%d", prNumber), "--rebase")
	return err
}

func (d *Daemon) waitForQueuedPRChecks(ctx context.Context, prNumber int) error {
	_, err := d.commands.Run(ctx, d.project, "gh", "pr", "checks", fmt.Sprintf("%d", prNumber), "--required", "--watch", "--fail-fast", "--interval", mergeQueueChecksIntervalSecs)
	return err
}

func (d *Daemon) mergeQueuedPR(ctx context.Context, prNumber int) error {
	_, err := d.commands.Run(ctx, d.project, "gh", "pr", "merge", fmt.Sprintf("%d", prNumber), "--squash")
	return err
}
func (d *Daemon) lookupPRNumber(ctx context.Context, branch string) (int, error) {
	return d.github.lookupPRNumber(ctx, branch)
}

func (d *Daemon) lookupOpenPRNumber(ctx context.Context, branch string) (int, error) {
	return d.github.lookupOpenPRNumber(ctx, branch)
}

func taskBlocksAssignment(status string) bool {
	switch status {
	case "", TaskStatusDone, TaskStatusCancelled, TaskStatusFailed:
		return false
	default:
		return true
	}
}

func assignmentMetadata(agentProfile, branch, issue string, prNumber int) map[string]string {
	metadata := map[string]string{
		"agent_profile": agentProfile,
		"branch":        branch,
		"issue":         issue,
		"task":          issue,
	}
	if prNumber > 0 {
		metadata["pr"] = fmt.Sprintf("%d", prNumber)
	}
	return metadata
}

func (d *Daemon) lookupPRChecksState(ctx context.Context, prNumber int) (string, error) {
	output, err := d.commands.Run(ctx, d.project, "gh", "pr", "checks", fmt.Sprintf("%d", prNumber), "--json", "bucket")
	if err != nil {
		return "", err
	}
	if len(output) == 0 {
		return "", nil
	}

	var checks []struct {
		Bucket string `json:"bucket"`
	}
	if err := json.Unmarshal(output, &checks); err != nil {
		return "", err
	}

	bestState := ""
	bestRank := 0
	for _, check := range checks {
		rank := ciStateRank(check.Bucket)
		if rank > bestRank {
			bestState = check.Bucket
			bestRank = rank
		}
	}
	return bestState, nil
}

func (d *Daemon) isPRMerged(ctx context.Context, prNumber int) (bool, error) {
	return d.github.isPRMerged(ctx, prNumber)
}

func (d *Daemon) lookupPRMergeableState(ctx context.Context, prNumber int) (string, bool, error) {
	output, err := d.commands.Run(ctx, d.project, "gh", "pr", "view", fmt.Sprintf("%d", prNumber), "--json", "mergeable")
	if err != nil {
		return "", false, err
	}
	if len(output) == 0 {
		return "", false, nil
	}

	var payload struct {
		Mergeable string `json:"mergeable"`
	}
	if err := json.Unmarshal(output, &payload); err != nil {
		return "", false, err
	}
	if payload.Mergeable == "" {
		return "", false, nil
	}
	return payload.Mergeable, true, nil
}

func (d *Daemon) lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error) {
	return d.github.lookupPRReviews(ctx, prNumber)
}

func (d *Daemon) setPaneMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	return d.amux.SetMetadata(ctx, paneID, metadata)
}

func (d *Daemon) setIssueStatus(ctx context.Context, issue, state string) error {
	if d.issueTracker == nil {
		return nil
	}
	return d.issueTracker.SetIssueStatus(ctx, issue, state)
}

func (d *Daemon) assignment(issue string) (*assignment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	active, ok := d.assignments[issue]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if active.pending {
		return nil, fmt.Errorf("issue %s assignment is still starting", issue)
	}
	return active, nil
}

func (d *Daemon) assignmentByPRNumber(prNumber int) (*assignment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	active := d.assignmentByPRNumberLocked(prNumber)
	if active == nil {
		return nil, fmt.Errorf("PR #%d is not associated with an active assignment", prNumber)
	}
	return active, nil
}

func (d *Daemon) assignmentByPRNumberLocked(prNumber int) *assignment {
	for _, active := range d.assignments {
		if active.pending {
			continue
		}
		if active.prNumber == prNumber {
			return active
		}
	}
	return nil
}

func (d *Daemon) mergeQueueEvent(active *assignment, eventType string, prNumber int, message string, at time.Time) Event {
	event := Event{
		Time:     at,
		Type:     eventType,
		Project:  d.project,
		PRNumber: prNumber,
		Message:  message,
	}
	if active == nil {
		return event
	}

	event.Issue = active.task.Issue
	event.PaneID = active.pane.ID
	event.PaneName = active.pane.Name
	event.CloneName = active.clone.Name
	event.ClonePath = active.clone.Path
	event.Branch = active.task.Branch
	event.AgentProfile = active.profile.Name
	return event
}

func (d *Daemon) requireStarted() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.started {
		return ErrNotStarted
	}
	return nil
}

func (d *Daemon) matchesStuckPattern(profile AgentProfile, output string) bool {
	lower := strings.ToLower(output)
	for _, pattern := range profile.StuckTextPatterns {
		if strings.Contains(lower, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}

func ciStateRank(state string) int {
	switch state {
	case ciStateFail:
		return 5
	case ciStatePending:
		return 4
	case ciStatePass:
		return 3
	case ciStateCancel:
		return 2
	case ciStateSkipping:
		return 1
	default:
		return 0
	}
}

func (d *Daemon) emit(ctx context.Context, event Event) {
	if err := d.state.RecordEvent(ctx, event); err != nil {
		_ = err
	}
	if d.events != nil {
		if err := d.events.Emit(ctx, event); err != nil {
			_ = err
		}
	}
}

func ensureTrailingNewline(input string) string {
	if strings.HasSuffix(input, "\n") {
		return input
	}
	return input + "\n"
}

func blockingReviews(reviewDecision string, reviews []prReview) []prReview {
	if reviewDecision != "CHANGES_REQUESTED" {
		return nil
	}

	blocking := make([]prReview, 0, len(reviews))
	for _, review := range reviews {
		if review.State == "CHANGES_REQUESTED" {
			blocking = append(blocking, review)
		}
	}
	return blocking
}

func formatBlockingReviewFeedback(prNumber int, reviews []prReview) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "New blocking PR review feedback on #%d:\n", prNumber)
	for _, review := range reviews {
		author := strings.TrimSpace(review.Author.Login)
		if author == "" {
			author = "reviewer"
		}
		body := normalizeReviewBody(review.Body)
		fmt.Fprintf(&builder, "- %s: %s\n", author, body)
	}
	builder.WriteString("\nAddress the feedback in the PR review and push an update.\n")
	return builder.String()
}

func normalizeReviewBody(body string) string {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return "requested changes without a review body."
	}
	return strings.Join(strings.Fields(trimmed), " ")
}

func mergeQueueRebaseConflictPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not rebase PR #%d onto main. Resolve the conflicts, push an update, and re-run `orca enqueue %d` when ready.\n", prNumber, prNumber)
}

func mergeQueueChecksFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue rebased PR #%d onto main, but required checks did not pass. Fix the branch, push an update, and re-run `orca enqueue %d` when ready.\n", prNumber, prNumber)
}

func mergeQueueMergeFailedPrompt(prNumber int) string {
	return fmt.Sprintf("Merge queue could not land PR #%d after verification. Check the PR state, push an update if needed, and re-run `orca enqueue %d` when ready.\n", prNumber, prNumber)
}
