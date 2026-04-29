package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/amux"
)

func TestDaemonSkipsBufferedPollTickWhilePollCycleIsRunning(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedTaskMonitorAssignment(t, deps, "LAB-890", "pane-1", 42)

	checkArgs := []string{"pr", "checks", "42", "--json", "bucket"}
	firstChecks := deps.commands.block("gh", checkArgs)
	deps.commands.queue("gh", checkArgs, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var releaseFirst sync.Once
	var releaseSecond sync.Once
	var secondChecks commandBlock
	t.Cleanup(func() {
		releaseFirst.Do(func() {
			close(firstChecks.release)
		})
		releaseSecond.Do(func() {
			close(secondChecks.release)
		})
		_ = d.Stop(context.Background())
	})

	pollTicker.tick(deps.clock.Now())
	waitForTaskMonitorBlocks(t, firstChecks.started)

	secondChecks = deps.commands.block("gh", checkArgs)
	pollTicker.tick(deps.clock.Now())

	releaseFirst.Do(func() {
		close(firstChecks.release)
	})
	waitFor(t, "first poll cycle completion", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCIState == ciStatePending
	})

	select {
	case <-secondChecks.started:
		releaseSecond.Do(func() {
			close(secondChecks.release)
		})
		t.Fatal("second poll cycle started after the first cycle finished, want buffered tick skipped")
	case <-time.After(200 * time.Millisecond):
	}

	if got, want := deps.commands.countCalls("gh", checkArgs), 1; got != want {
		t.Fatalf("gh pr checks call count = %d, want %d", got, want)
	}
}

func TestDaemonStartEmitsPRPollTraceWithinConfiguredPollIntervalWhenRelayHealthy(t *testing.T) {
	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1476", "pane-1", 0)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1476", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", issueIDPRSearchArgs("LAB-1476"), `[]`, nil)

	const pollInterval = 250 * time.Millisecond
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.NewTicker = nil
		opts.PollInterval = pollInterval
		opts.CaptureInterval = 25 * time.Millisecond
	})
	d.relayHealthy.Store(true)

	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deadline := time.After(2 * pollInterval)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		if deps.events.countType(EventPRPollTrace) > 0 {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("pr.poll_trace event count = 0, want at least 1 within %s after Start", 2*pollInterval)
		case <-ticker.C:
		}
	}
}

func TestAdaptivePRPollInterval(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		lastPushAt time.Time
		want       time.Duration
	}{
		{name: "no push uses default interval", want: 30 * time.Second},
		{name: "fast window", lastPushAt: now.Add(-9 * time.Minute), want: 5 * time.Second},
		{name: "warm window", lastPushAt: now.Add(-20 * time.Minute), want: 15 * time.Second},
		{name: "steady state", lastPushAt: now.Add(-31 * time.Minute), want: 30 * time.Second},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := adaptivePRPollInterval(now, Worker{LastPushAt: tt.lastPushAt}, 30*time.Second); got != tt.want {
				t.Fatalf("adaptivePRPollInterval() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestPRPollSchedulerTickInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input time.Duration
		want  time.Duration
	}{
		{name: "non-positive falls back to fast interval", input: 0, want: adaptivePRFastPollInterval},
		{name: "larger than slow interval is preserved", input: 5 * time.Minute, want: 5 * time.Minute},
		{name: "smaller than fast interval is preserved", input: time.Second, want: time.Second},
		{name: "default thirty second interval schedules fast ticks", input: adaptivePRSlowPollInterval, want: adaptivePRFastPollInterval},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := prPollSchedulerTickInterval(tt.input); got != tt.want {
				t.Fatalf("prPollSchedulerTickInterval(%v) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestShouldPollAssignmentForAdaptivePRIntervals(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		worker Worker
		want   bool
	}{
		{
			name: "never polled is due immediately",
			worker: Worker{
				LastPushAt: now,
			},
			want: true,
		},
		{
			name: "fast window waits five seconds",
			worker: Worker{
				LastPushAt:   now.Add(-time.Minute),
				LastPRPollAt: now.Add(-4 * time.Second),
			},
			want: false,
		},
		{
			name: "warm window waits fifteen seconds",
			worker: Worker{
				LastPushAt:   now.Add(-20 * time.Minute),
				LastPRPollAt: now.Add(-14 * time.Second),
			},
			want: false,
		},
		{
			name: "steady state waits thirty seconds",
			worker: Worker{
				LastPushAt:   now.Add(-40 * time.Minute),
				LastPRPollAt: now.Add(-30 * time.Second),
			},
			want: true,
		},
		{
			name: "no push uses default interval",
			worker: Worker{
				LastPRPollAt: now.Add(-29 * time.Second),
			},
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := shouldPollAssignmentForPR(now, ActiveAssignment{Worker: tt.worker}, 30*time.Second); got != tt.want {
				t.Fatalf("shouldPollAssignmentForPR() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestShouldPollAssignmentImmediatelyWhenPRNumberChanges(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC)
	active := ActiveAssignment{
		Task: Task{
			PRNumber: 42,
		},
		Worker: Worker{
			LastPRNumber: 41,
			LastPushAt:   now.Add(-40 * time.Minute),
			LastPRPollAt: now,
		},
	}

	if !shouldPollAssignmentForPR(now, active, 30*time.Second) {
		t.Fatal("shouldPollAssignmentForPR() = false, want true when PR number changes")
	}
}

func TestDaemonCaptureMonitorKeepsPollingAfterAmuxFailures(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-924", "pane-1", 0)
	deps.amux.capturePaneErr = errors.New("amux capture pane-1: exit status 1: connection refused")

	d := deps.newDaemon(t)
	ctx := context.Background()
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	for i := 0; i < 3; i++ {
		d.runCaptureTick(ctx)
		if got, want := deps.amux.captureCount("pane-1"), i+1; got != want {
			t.Fatalf("capture count after failure %d = %d, want %d", i+1, got, want)
		}
	}

	d.runCaptureTick(ctx)
	if got, want := deps.amux.captureCount("pane-1"), 4; got != want {
		t.Fatalf("capture count after fourth failure = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventDaemonCircuitOpened), 0; got != want {
		t.Fatalf("amux circuit events = %d, want %d", got, want)
	}

	deps.amux.capturePaneErr = nil
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{paneCaptureFromOutput("worker output")})

	d.runCaptureTick(ctx)
	if got, want := deps.amux.captureCount("pane-1"), 5; got != want {
		t.Fatalf("capture count after recovery = %d, want %d", got, want)
	}
}

func TestDaemonCaptureMonitorDoesNotOpenAmuxCircuitForPaneNotFound(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1253", "w-LAB-1033", 0)
	deps.amux.capturePaneErr = errors.New(`amux -s main capture --format json w-LAB-1033: exit status 1: amux capture: pane "w-LAB-1033" not found`)

	d := deps.newDaemon(t)
	ctx := context.Background()
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	for i := 0; i < 3; i++ {
		d.runCaptureTick(ctx)
		if got, want := deps.amux.captureCount("w-LAB-1033"), i+1; got != want {
			t.Fatalf("capture count after pane-not-found attempt %d = %d, want %d", i+1, got, want)
		}
	}

	if got := deps.events.countType(EventDaemonCircuitOpened); got != 0 {
		t.Fatalf("circuit opened event count = %d, want 0", got)
	}
}

func TestDaemonPollMonitorOpensGitHubCircuitAfterThreeFailures(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-924", "pane-1", 0)

	lookupArgs := []string{"pr", "list", "--head", "LAB-924", "--json", "number"}
	for i := 0; i < 3; i++ {
		deps.commands.queue("gh", lookupArgs, ``, errors.New("gh pr list --head LAB-924 --json number: exit status 1: github unavailable"))
	}
	deps.commands.queue("gh", lookupArgs, `[]`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	for i := 0; i < 3; i++ {
		d.runPollTick(ctx)
		if got, want := deps.commands.countCalls("gh", lookupArgs), i+1; got != want {
			t.Fatalf("github call count after failure %d = %d, want %d", i+1, got, want)
		}
		deps.clock.Advance(30 * time.Second)
	}
	if err := d.monitorGitHubCircuit.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("github circuit Allow() error = %v, want %v", err, ErrCircuitBreakerOpen)
	}
	if got, want := deps.events.countType(EventDaemonCircuitOpened), 1; got != want {
		t.Fatalf("circuit opened event count = %d, want %d", got, want)
	}
	if got, want := deps.events.lastMessage(EventDaemonCircuitOpened), "monitor github circuit opened after 3 consecutive failures: gh pr list --head LAB-924 --json number: exit status 1: github unavailable"; got != want {
		t.Fatalf("opened event message = %q, want %q", got, want)
	}

	d.runPollTick(ctx)
	if got, want := deps.commands.countCalls("gh", lookupArgs), 3; got != want {
		t.Fatalf("github call count with open circuit = %d, want %d", got, want)
	}

	deps.clock.Advance(60 * time.Second)
	d.runPollTick(ctx)
	if got, want := deps.commands.countCalls("gh", lookupArgs), 4; got != want {
		t.Fatalf("github call count after cooldown = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventDaemonCircuitClosed), 1; got != want {
		t.Fatalf("circuit closed event count = %d, want %d", got, want)
	}
}

func TestDaemonCaptureTickEscalatesMissingTrackedPaneBeforeCapture(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-988", "pane-1", 0)
	deps.amux.paneExists = map[string]bool{"pane-1": false}

	d := deps.newDaemon(t)
	d.runCaptureTick(context.Background())

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after capture reconciliation")
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got := deps.amux.captureCount("pane-1"); got != 0 {
		t.Fatalf("capture count = %d, want 0", got)
	}
	event, ok := deps.events.lastEventOfType(EventWorkerEscalated)
	if !ok {
		t.Fatal("worker escalation event missing")
	}
	if got, want := event.Message, "worker pane missing during monitor reconciliation"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

func TestDaemonPollTickSkipsMissingTrackedPaneBeforePRPoll(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-988", "pane-1", 42)
	deps.amux.paneExists = map[string]bool{"pane-1": false}

	d := deps.newDaemon(t)
	d.runPollTick(context.Background())

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after poll reconciliation")
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got := deps.commands.countCalls("gh", []string{"pr", "checks", "42", "--json", "bucket"}); got != 0 {
		t.Fatalf("gh pr checks calls = %d, want 0", got)
	}
	event, ok := deps.events.lastEventOfType(EventWorkerEscalated)
	if !ok {
		t.Fatal("worker escalation event missing")
	}
	if got, want := event.Message, "worker pane missing during monitor reconciliation"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

type prPollErrorState struct {
	*fakeState
	nonTerminalErr error
}

func (s *prPollErrorState) NonTerminalTasks(ctx context.Context, project string) ([]Task, error) {
	if s.nonTerminalErr != nil {
		return nil, s.nonTerminalErr
	}
	return s.fakeState.NonTerminalTasks(ctx, project)
}

func TestPRPollAssignmentsEmitsTraceWhenTaskQueryFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	stateErr := errors.New("postgres unavailable")
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.State = &prPollErrorState{fakeState: deps.state, nonTerminalErr: stateErr}
	})

	assignments, err := d.prPollAssignments(context.Background())
	if err == nil || !errors.Is(err, stateErr) {
		t.Fatalf("prPollAssignments() error = %v, want %v", err, stateErr)
	}
	if got := len(assignments); got != 0 {
		t.Fatalf("len(assignments) = %d, want 0", got)
	}
	event, ok := deps.events.lastEventOfType(EventPRPollTrace)
	if !ok {
		t.Fatal("pr poll trace event missing")
	}
	if got, want := event.Message, `pr poll trace: action=list_non_terminal_tasks_error error="postgres unavailable"`; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

func TestPRPollAssignmentsEmitsTraceWhenWorkerCannotResume(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1415",
		Status:       TaskStatusActive,
		State:        TaskStateAssigned,
		WorkerID:     "worker-01",
		PaneID:       "pane-1",
		PaneName:     "w-LAB-1415",
		ClonePath:    deps.pool.clone.Path,
		Branch:       "LAB-1415",
		AgentProfile: "codex",
		CreatedAt:    deps.clock.Now(),
		UpdatedAt:    deps.clock.Now(),
	})

	d := deps.newDaemon(t)
	assignments, err := d.prPollAssignments(context.Background())
	if err != nil {
		t.Fatalf("prPollAssignments() error = %v", err)
	}
	if got := len(assignments); got != 0 {
		t.Fatalf("len(assignments) = %d, want 0", got)
	}
	event, ok := deps.events.lastEventOfType(EventPRPollTrace)
	if !ok {
		t.Fatal("pr poll trace event missing")
	}
	if got, want := event.Message, "pr poll trace: issue=LAB-1415 pr_number=0 action=resume_worker_missing"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

func TestReconcileTrackedPanesEmitsTraceOnPaneExistsError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1415", "pane-1", 0)
	deps.amux.paneExistsErr = errors.New("amux unavailable")

	d := deps.newDaemon(t)
	assignments := []ActiveAssignment{activeTaskMonitorAssignment(t, deps, "LAB-1415")}

	reconciled := d.reconcileTrackedPanes(context.Background(), assignments)
	if got, want := len(reconciled), 1; got != want {
		t.Fatalf("len(reconciled) = %d, want %d", got, want)
	}

	assignments, err := deps.state.ActiveAssignments(context.Background(), "/tmp/project")
	if err != nil {
		t.Fatalf("ActiveAssignments() error = %v", err)
	}
	if got, want := len(assignments), 1; got != want {
		t.Fatalf("len(assignments) = %d, want %d", got, want)
	}
	reconciled = d.reconcileTrackedPanes(context.Background(), assignments)
	if got, want := len(reconciled), 1; got != want {
		t.Fatalf("len(reconciled) after repeat = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventPRPollTrace), 2; got != want {
		t.Fatalf("pr poll trace event count = %d, want %d", got, want)
	}
	event, ok := deps.events.lastEventOfType(EventPRPollTrace)
	if !ok {
		t.Fatal("pr poll trace event missing")
	}
	if got, want := event.Message, `pr poll trace: issue=LAB-1415 pr_number=0 action=pane_exists_error error="amux unavailable"`; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

func TestReconcileTrackedPanesEscalatesAndDropsErrPaneNotFoundOnce(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1420", "w-LAB-1033", 0)
	deps.amux.paneExistsErr = amux.ErrPaneNotFound

	d := deps.newDaemon(t)
	assignments := []ActiveAssignment{activeTaskMonitorAssignment(t, deps, "LAB-1420")}

	reconciled := d.reconcileTrackedPanes(context.Background(), assignments)
	if got := len(reconciled); got != 0 {
		t.Fatalf("len(reconciled) = %d, want 0", got)
	}
	if got, want := deps.events.countType(EventPRPollTrace), 1; got != want {
		t.Fatalf("pr poll trace event count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerEscalated), 1; got != want {
		t.Fatalf("worker escalated event count = %d, want %d", got, want)
	}

	active, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1420")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if got, want := active.Worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}

	reconciled = d.reconcileTrackedPanes(context.Background(), []ActiveAssignment{active})
	if got := len(reconciled); got != 0 {
		t.Fatalf("len(reconciled) after repeat = %d, want 0", got)
	}
	if got, want := deps.events.countType(EventPRPollTrace), 1; got != want {
		t.Fatalf("pr poll trace event count after repeat = %d, want %d", got, want)
	}
	event, ok := deps.events.lastEventOfType(EventPRPollTrace)
	if !ok {
		t.Fatal("pr poll trace event missing")
	}
	if got, want := event.Message, `pr poll trace: issue=LAB-1420 pr_number=0 action=pane_exists_error error="amux pane not found"`; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}
