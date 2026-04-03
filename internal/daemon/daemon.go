package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultCaptureInterval  = 5 * time.Second
	defaultPollInterval     = 30 * time.Second
	defaultMergeGracePeriod = 10 * time.Minute
	mergedWrapUpPrompt      = "PR merged, wrap up.\n"
)

type Daemon struct {
	project          string
	session          string
	leadPane         string
	pidPath          string
	config           ConfigProvider
	state            StateStore
	pool             Pool
	amux             AmuxClient
	commands         CommandRunner
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
	wg          sync.WaitGroup
}

type assignment struct {
	ctx             context.Context
	cancel          context.CancelFunc
	captureTick     Ticker
	pollTick        Ticker
	pending         bool
	profile         AgentProfile
	task            Task
	worker          Worker
	clone           Clone
	pane            Pane
	lastOutput      string
	lastActivity    time.Time
	prNumber        int
	lastReviewCount atomic.Int64
	escalated       bool
	cleanupOnce     sync.Once
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
		commands:         opts.Commands,
		events:           opts.Events,
		now:              opts.Now,
		newTicker:        opts.NewTicker,
		captureInterval:  opts.CaptureInterval,
		pollInterval:     opts.PollInterval,
		mergeGracePeriod: opts.MergeGracePeriod,
		assignments:      make(map[string]*assignment),
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

	if err := d.amux.SetMetadata(ctx, pane.ID, map[string]string{
		"agent_profile": profile.Name,
		"branch":        issue,
		"issue":         issue,
		"task":          issue,
	}); err != nil {
		_ = d.rollbackAssignment(ctx, clone, pane, issue)
		releaseReservation()
		return fmt.Errorf("set pane metadata: %w", err)
	}

	if err := d.amux.SendKeys(ctx, pane.ID, ensureTrailingNewline(prompt)); err != nil {
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
		return fmt.Errorf("send prompt: %w", err)
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

	merged, err := d.isPRMerged(active.ctx, active.prNumber)
	if err != nil || !merged {
		d.handlePRReviewPoll(active)
		return
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
		Message:      "pull request merged",
	})
	_ = d.finishAssignment(active.ctx, active, TaskStatusDone, EventTaskCompleted, true)
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

func (d *Daemon) finishAssignment(ctx context.Context, active *assignment, status, eventType string, merged bool) error {
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
	result := d.cleanupClone(ctx, clone.Path, branch)
	result = errors.Join(result, d.pool.Release(ctx, d.project, clone))
	return result
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

func (d *Daemon) cleanupClone(ctx context.Context, clonePath, branch string) error {
	commands := [][]string{
		{"reset", "--hard"},
		{"checkout", "main"},
		{"pull"},
		{"clean", "-fdx", "--exclude=.orca-pool"},
		{"branch", "-D", branch},
	}
	var result error
	for _, args := range commands {
		_, err := d.commands.Run(ctx, clonePath, "git", args...)
		result = errors.Join(result, err)
	}
	return result
}

func (d *Daemon) lookupPRNumber(ctx context.Context, branch string) (int, error) {
	output, err := d.commands.Run(ctx, d.project, "gh", "pr", "list", "--head", branch, "--json", "number")
	if err != nil {
		return 0, err
	}
	var prs []struct {
		Number int `json:"number"`
	}
	if len(output) == 0 {
		return 0, nil
	}
	if err := json.Unmarshal(output, &prs); err != nil {
		return 0, err
	}
	if len(prs) == 0 {
		return 0, nil
	}
	return prs[0].Number, nil
}

func (d *Daemon) isPRMerged(ctx context.Context, prNumber int) (bool, error) {
	output, err := d.commands.Run(ctx, d.project, "gh", "pr", "view", fmt.Sprintf("%d", prNumber), "--json", "mergedAt")
	if err != nil {
		return false, err
	}
	if len(output) == 0 {
		return false, nil
	}
	var payload struct {
		MergedAt *string `json:"mergedAt"`
	}
	if err := json.Unmarshal(output, &payload); err != nil {
		return false, err
	}
	return payload.MergedAt != nil && *payload.MergedAt != "", nil
}

func (d *Daemon) lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error) {
	output, err := d.commands.Run(ctx, d.project, "gh", "pr", "view", fmt.Sprintf("%d", prNumber), "--json", "reviews,reviewDecision")
	if err != nil {
		return prReviewPayload{}, false, err
	}
	if len(output) == 0 {
		return prReviewPayload{}, false, nil
	}

	var payload prReviewPayload
	if err := json.Unmarshal(output, &payload); err != nil {
		return prReviewPayload{}, false, err
	}
	return payload, true, nil
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
