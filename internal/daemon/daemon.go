package daemon

import (
	"context"
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
	defaultCaptureInterval       = 5 * time.Second
	defaultAgentHandshakeTimeout = 30 * time.Second
	defaultPromptSettleDuration  = 2 * time.Second
	defaultTrustPromptTimeout    = 2 * time.Second
	defaultPollInterval          = 30 * time.Second
	defaultMergeGracePeriod      = 10 * time.Minute
)

type Daemon struct {
	project              string
	session              string
	leadPane             string
	pidPath              string
	config               ConfigProvider
	state                StateStore
	pool                 Pool
	amux                 AmuxClient
	issueTracker         IssueTracker
	commands             CommandRunner
	github               gitHubClient
	githubMu             sync.Mutex
	githubClients        map[string]gitHubClient
	events               EventSink
	now                  func() time.Time
	newTicker            func(time.Duration) Ticker
	newWatchdogTicker    func(time.Duration) Ticker
	sleep                func(context.Context, time.Duration) error
	captureInterval      time.Duration
	pollInterval         time.Duration
	mergeGracePeriod     time.Duration
	statusWriter         daemonStatusWriter
	logf                 func(string, ...any)
	monitorAmuxCircuit   *CircuitBreaker
	monitorGitHubCircuit *CircuitBreaker

	started           atomic.Bool
	lastHeartbeat     atomic.Int64
	stopContext       context.Context
	stopCancel        context.CancelFunc
	loopDone          chan struct{}
	eventStreamDone   chan struct{}
	watchdogDone      chan struct{}
	mergeQueueInbox   chan ProcessQueue
	mergeQueueUpdates chan MergeQueueUpdate
	mergeQueueDone    chan struct{}
	monitorRuns       sync.WaitGroup
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
		project:           opts.Project,
		session:           opts.Session,
		leadPane:          opts.LeadPane,
		pidPath:           opts.PIDPath,
		config:            opts.Config,
		state:             opts.State,
		pool:              opts.Pool,
		amux:              opts.Amux,
		issueTracker:      opts.IssueTracker,
		commands:          opts.Commands,
		github:            newDefaultGitHubClient(opts.Project, opts.Commands),
		githubClients:     make(map[string]gitHubClient),
		events:            opts.Events,
		now:               opts.Now,
		newTicker:         opts.NewTicker,
		newWatchdogTicker: opts.NewWatchdogTicker,
		sleep:             opts.Sleep,
		captureInterval:   opts.CaptureInterval,
		pollInterval:      opts.PollInterval,
		mergeGracePeriod:  opts.MergeGracePeriod,
		statusWriter:      opts.DaemonStatusWriter,
		logf:              opts.Logf,
		// Monitor circuits are daemon-wide by design so broad amux/GitHub
		// outages pause all monitor traffic instead of retrying per task.
		monitorAmuxCircuit:   NewCircuitBreakerWithHooks(opts.Now, defaultCircuitBreakerFailureThreshold, defaultCircuitBreakerCooldown, daemonCircuitHooks(opts.Project, opts.Now, opts.State, opts.Events, "monitor amux")),
		monitorGitHubCircuit: NewCircuitBreakerWithHooks(opts.Now, defaultCircuitBreakerFailureThreshold, defaultCircuitBreakerCooldown, daemonCircuitHooks(opts.Project, opts.Now, opts.State, opts.Events, "monitor github")),
	}, nil
}

func daemonCircuitHooks(project string, now func() time.Time, state StateStore, events EventSink, name string) CircuitBreakerHooks {
	emit := func(eventType, message string) {
		event := Event{
			Time:    now(),
			Type:    eventType,
			Project: project,
			Message: message,
		}
		if state != nil {
			_ = state.RecordEvent(context.Background(), event)
		}
		if events != nil {
			_ = events.Emit(context.Background(), event)
		}
	}

	return CircuitBreakerHooks{
		OnOpen: func() {
			emit(EventDaemonCircuitOpened, name+" circuit opened after 3 consecutive failures")
		},
		OnClose: func() {
			emit(EventDaemonCircuitClosed, name+" circuit closed after cooldown")
		},
	}
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
	d.lastHeartbeat.Store(d.now().UnixMilli())
	d.reconcileNonTerminalAssignments(ctx)
	d.refreshTaskMonitors(ctx)
	d.resetMergeQueueTransientStatuses(ctx)
	d.mergeQueueInbox = make(chan ProcessQueue)
	d.mergeQueueUpdates = make(chan MergeQueueUpdate, 32)
	d.mergeQueueDone = make(chan struct{})
	actor := newMergeQueueActor(d.project, d.commands, d.mergeQueueUpdates)
	go actor.run(d.stopContext, d.mergeQueueInbox, d.mergeQueueDone)
	d.eventStreamDone = make(chan struct{})
	go d.runExitedEventLoop(d.stopContext, d.eventStreamDone)
	d.loopDone = make(chan struct{})
	go d.runLoop(d.stopContext, d.loopDone)
	if d.newWatchdogTicker != nil {
		d.watchdogDone = make(chan struct{})
		go d.runWatchdog(d.stopContext, d.watchdogDone)
	}

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
	if d.eventStreamDone != nil {
		<-d.eventStreamDone
	}
	if d.watchdogDone != nil {
		<-d.watchdogDone
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
	d.eventStreamDone = nil
	d.watchdogDone = nil

	d.emit(ctx, Event{
		Time:    d.now(),
		Type:    EventDaemonStopped,
		Project: d.project,
		Message: "daemon stopped",
	})
	return nil
}
func (d *Daemon) Cancel(ctx context.Context, issue string) error {
	return d.cancel(ctx, d.project, issue)
}

func (d *Daemon) cancel(ctx context.Context, projectPath, issue string) error {
	if err := d.requireStarted(); err != nil {
		return err
	}

	active, err := d.state.ActiveAssignmentByIssue(ctx, projectPath, issue)
	if err != nil {
		return err
	}
	return d.finishAssignment(ctx, active, TaskStatusCancelled, EventTaskCancelled, false)
}
