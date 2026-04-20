package daemon

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

const taskMonitorBlockTimeout = 200 * time.Millisecond

func TestTaskMonitorSpawnsOnAssignmentAndStopsOnCompletion(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-814", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-814", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-814", "Implement per-task task monitors", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task monitor spawn", func() bool {
		return d.taskMonitorCount() == 1
	})

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "task completion", func() bool {
		task, ok := deps.state.task("LAB-814")
		return ok && task.Status == TaskStatusDone
	})
	waitFor(t, "task monitor stop", func() bool {
		return d.taskMonitorCount() == 0
	})
}

func TestTaskMonitorPollsAssignmentsInParallel(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedTaskMonitorAssignment(t, deps, "LAB-901", "pane-1", 41)
	seedTaskMonitorAssignment(t, deps, "LAB-902", "pane-2", 42)

	check41 := []string{"pr", "checks", "41", "--json", "bucket"}
	check42 := []string{"pr", "checks", "42", "--json", "bucket"}
	block41 := deps.commands.block("gh", check41)
	block42 := deps.commands.block("gh", check42)
	deps.commands.queue("gh", check41, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", check42, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", prReviewJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	d.ensureTaskMonitor("LAB-901")
	d.ensureTaskMonitor("LAB-902")

	pollTicker.tick(deps.clock.Now())
	waitForTaskMonitorBlocks(t, block41.started, block42.started)
	close(block41.release)
	close(block42.release)

	waitFor(t, "parallel pr poll completion", func() bool {
		worker1, ok1 := deps.state.worker("pane-1")
		worker2, ok2 := deps.state.worker("pane-2")
		return ok1 && ok2 &&
			worker1.LastCIState == ciStatePending &&
			worker2.LastCIState == ciStatePending
	})
}

func TestTaskMonitorUsesProjectScopedKeys(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	assignments := []ActiveAssignment{
		{
			Task: Task{
				Project: "/repo-a",
				Issue:   "LAB-901",
				Status:  TaskStatusActive,
				PaneID:  "pane-a",
			},
			Worker: Worker{Project: "/repo-a", PaneID: "pane-a", Issue: "LAB-901"},
		},
		{
			Task: Task{
				Project: "/repo-b",
				Issue:   "LAB-901",
				Status:  TaskStatusActive,
				PaneID:  "pane-b",
			},
			Worker: Worker{Project: "/repo-b", PaneID: "pane-b", Issue: "LAB-901"},
		},
	}

	monitors := d.syncTaskMonitors(assignments)
	if got, want := len(monitors), 2; got != want {
		t.Fatalf("len(monitors) = %d, want %d", got, want)
	}
	if got, want := d.taskMonitorCount(), 2; got != want {
		t.Fatalf("taskMonitorCount() = %d, want %d", got, want)
	}
}

func TestTaskMonitorStaleResultIsDropped(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-903", "pane-3", 43)

	check43 := []string{"pr", "checks", "43", "--json", "bucket"}
	block43 := deps.commands.block("gh", check43)
	deps.commands.queue("gh", check43, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "43", "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "43", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "43", "--json", prReviewJSONFields}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()

	originalMonitor := d.ensureTaskMonitor("LAB-903")
	response := originalMonitor.dispatch(ctx, taskMonitorCheckPRPoll, activeTaskMonitorAssignment(t, deps, "LAB-903"))
	if response == nil {
		t.Fatal("dispatch() = nil, want response channel")
	}

	waitForTaskMonitorBlocks(t, block43.started)
	d.stopTaskMonitor("LAB-903")
	replacementMonitor := d.ensureTaskMonitor("LAB-903")
	if replacementMonitor == originalMonitor {
		t.Fatal("replacement monitor = original monitor, want new instance")
	}

	close(block43.release)

	result := <-response
	d.applyTaskMonitorResults(ctx, []taskMonitorResult{result})

	worker, ok := deps.state.worker("pane-3")
	if !ok {
		t.Fatal("worker missing after stale result application")
	}
	if got := worker.LastCIState; got != "" {
		t.Fatalf("worker.LastCIState = %q, want stale result dropped", got)
	}
	event, ok := deps.events.lastEventOfType(EventPRPollTrace)
	if !ok {
		t.Fatal("pr poll trace event missing")
	}
	if got, want := event.Message, "pr poll trace: issue=LAB-903 pr_number=43 action=stale_result_dropped"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

func TestDispatchTaskMonitorChecksEmitsTraceWhenStoppedPRPollMonitorDropsDispatch(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-904", "pane-4", 0)

	d := deps.newDaemon(t)
	active := activeTaskMonitorAssignment(t, deps, "LAB-904")
	key := taskMonitorKey(active.Task.Project, active.Task.Issue)

	stopped := newTaskMonitor(d, key)
	stopped.stop()
	stopped.wait()

	d.taskMonitorMu.Lock()
	d.taskMonitors = map[string]*TaskMonitor{key: stopped}
	d.taskMonitorMu.Unlock()

	results := d.dispatchTaskMonitorChecks(context.Background(), []ActiveAssignment{active}, taskMonitorCheckPRPoll)
	if got := len(results); got != 0 {
		t.Fatalf("len(results) = %d, want 0", got)
	}

	event, ok := deps.events.lastEventOfType(EventPRPollTrace)
	if !ok {
		t.Fatal("pr poll trace event missing")
	}
	if got, want := event.Message, "pr poll trace: issue=LAB-904 pr_number=0 action=monitor_dispatch_dropped"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

func TestApplyTaskStateUpdateDoesNotPersistUnflaggedWorkerFieldsFromMergedUpdate(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1051", "pane-1", 42)

	d := deps.newDaemon(t)
	active := activeTaskMonitorAssignment(t, deps, "LAB-1051")

	base := TaskStateUpdate{
		Active:        active,
		WorkerChanged: true,
	}
	base.Active.Worker.LastMergeableState = "MERGEABLE"
	base.Active.Worker.LastSeenAt = deps.clock.Now()

	next := TaskStateUpdate{Active: base.Active}
	next.Active.Worker.LastIssueCommentCount = 2

	d.applyTaskStateUpdate(context.Background(), mergeTaskStateUpdates(base, next))

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after applyTaskStateUpdate()")
	}
	if got, want := worker.LastMergeableState, "MERGEABLE"; got != want {
		t.Fatalf("worker.LastMergeableState = %q, want %q", got, want)
	}
	if got, want := worker.LastIssueCommentCount, 0; got != want {
		t.Fatalf("worker.LastIssueCommentCount = %d, want %d", got, want)
	}
}

func TestApplyTaskStateUpdateRefreshesPollScheduleWhenPRDetected(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1318", "pane-1", 0)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.PollInterval = 10 * time.Minute
	})
	d.pollIntervalCh = make(chan time.Duration, 1)
	d.relayHealthy.Store(true)

	active := activeTaskMonitorAssignment(t, deps, "LAB-1318")
	now := deps.clock.Now()
	update := TaskStateUpdate{
		Active:        active,
		TaskChanged:   true,
		WorkerChanged: true,
	}
	update.Active.Task.PRNumber = 42
	update.Active.Task.State = TaskStatePRDetected
	update.Active.Task.UpdatedAt = now
	update.Active.Worker.LastPRNumber = 42
	update.Active.Worker.LastPushAt = now

	d.applyTaskStateUpdate(context.Background(), update)

	select {
	case got := <-d.pollIntervalCh:
		if got != openPRPollIntervalCap {
			t.Fatalf("queued poll interval = %v, want %v", got, openPRPollIntervalCap)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for poll interval refresh")
	}
}

func TestTaskMonitorPollRunsConflictNudgesWithBoundedConcurrency(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	assignments := seedConflictNudgeAssignments(t, deps, 5)
	waitIdle := newBlockedWaitIdle()
	deps.amux.waitIdleHook = waitIdle.hook
	t.Cleanup(waitIdle.releaseAll)

	resultCh := make(chan []taskMonitorResult, 1)
	go func() {
		resultCh <- d.dispatchTaskMonitorChecks(context.Background(), assignments, taskMonitorCheckPRPoll)
	}()

	waitIdle.waitForStarted(t, 4)
	waitIdle.requireStartedCount(t, 4)

	waitIdle.releaseOne()
	waitIdle.waitForStarted(t, 5)
	waitIdle.releaseAll()

	results := <-resultCh
	d.applyTaskMonitorResults(context.Background(), results)

	for i := 0; i < 5; i++ {
		paneID := fmt.Sprintf("pane-%d", i+1)
		worker, ok := deps.state.worker(paneID)
		if !ok {
			t.Fatalf("worker %q missing after conflict nudges", paneID)
		}
		if got, want := worker.LastMergeableState, "CONFLICTING"; got != want {
			t.Fatalf("worker %q last mergeable state = %q, want %q", paneID, got, want)
		}
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 5; got != want {
		t.Fatalf("conflict nudge event count = %d, want %d", got, want)
	}
}

func TestTaskMonitorPollRunsReviewNudgesWithBoundedConcurrency(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	assignments := seedReviewNudgeAssignments(t, deps, 5)
	deps.clock.Advance(time.Minute)
	assignments = reloadTaskMonitorAssignments(t, deps, assignments)

	waitIdle := newBlockedWaitIdle()
	deps.amux.waitIdleHook = waitIdle.hook
	t.Cleanup(waitIdle.releaseAll)

	resultCh := make(chan []taskMonitorResult, 1)
	go func() {
		resultCh <- d.dispatchTaskMonitorChecks(context.Background(), assignments, taskMonitorCheckPRPoll)
	}()

	waitIdle.waitForStarted(t, 4)
	waitIdle.requireStartedCount(t, 4)

	waitIdle.releaseOne()
	waitIdle.waitForStarted(t, 5)
	waitIdle.releaseAll()

	results := <-resultCh
	d.applyTaskMonitorResults(context.Background(), results)

	for i := 0; i < 5; i++ {
		paneID := fmt.Sprintf("pane-%d", i+1)
		worker, ok := deps.state.worker(paneID)
		if !ok {
			t.Fatalf("worker %q missing after review nudges", paneID)
		}
		if got, want := worker.ReviewNudgeCount, 1; got != want {
			t.Fatalf("worker %q review nudge count = %d, want %d", paneID, got, want)
		}
		if got, want := worker.LastReviewCount, 1; got != want {
			t.Fatalf("worker %q last review count = %d, want %d", paneID, got, want)
		}
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 5; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func seedTaskMonitorAssignment(t *testing.T, deps *testDeps, issue, paneID string, prNumber int) {
	t.Helper()

	now := deps.clock.Now()
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        issue,
		Status:       TaskStatusActive,
		State:        initialTaskState(prNumber),
		Prompt:       "Monitor active task",
		PaneID:       paneID,
		PaneName:     paneID,
		CloneName:    "clone-" + issue,
		ClonePath:    "/tmp/" + issue,
		Branch:       issue,
		AgentProfile: "codex",
		PRNumber:     prNumber,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	worker := Worker{
		Project:        "/tmp/project",
		PaneID:         paneID,
		PaneName:       paneID,
		Issue:          issue,
		ClonePath:      "/tmp/" + issue,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastCapture:    defaultCodexReadyOutput(),
		LastActivityAt: now,
		UpdatedAt:      now,
	}
	if prNumber > 0 {
		worker.LastPRNumber = prNumber
		worker.LastPushAt = now
	}
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
}

func activeTaskMonitorAssignment(t *testing.T, deps *testDeps, issue string) ActiveAssignment {
	t.Helper()

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatalf("task %q not found", issue)
	}

	worker, ok := deps.state.worker(task.PaneID)
	if !ok {
		t.Fatalf("worker %q not found", task.PaneID)
	}

	return ActiveAssignment{
		Task:   task,
		Worker: worker,
	}
}

func waitForTaskMonitorBlocks(t *testing.T, started ...<-chan struct{}) {
	t.Helper()

	for _, ch := range started {
		select {
		case <-time.After(taskMonitorBlockTimeout):
			t.Fatal("timed out waiting for parallel task monitor polls")
		case <-ch:
		}
	}
}

func seedConflictNudgeAssignments(t *testing.T, deps *testDeps, count int) []ActiveAssignment {
	t.Helper()

	assignments := make([]ActiveAssignment, 0, count)
	for i := 0; i < count; i++ {
		issue := fmt.Sprintf("LAB-%03d", 910+i)
		paneID := fmt.Sprintf("pane-%d", i+1)
		prNumber := 100 + i
		seedTaskMonitorAssignment(t, deps, issue, paneID, prNumber)
		deps.commands.queue("gh", []string{"pr", "checks", fmt.Sprintf("%d", prNumber), "--json", "bucket"}, ``, nil)
		deps.commands.queue("gh", []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
		deps.commands.queue("gh", []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prMergeableJSONFields}, `{"mergeable":"CONFLICTING"}`, nil)
		deps.commands.queue("gh", []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prReviewJSONFields}, ``, nil)
		assignments = append(assignments, activeTaskMonitorAssignment(t, deps, issue))
	}
	return assignments
}

func seedReviewNudgeAssignments(t *testing.T, deps *testDeps, count int) []ActiveAssignment {
	t.Helper()

	assignments := make([]ActiveAssignment, 0, count)
	for i := 0; i < count; i++ {
		issue := fmt.Sprintf("LAB-%03d", 920+i)
		paneID := fmt.Sprintf("pane-%d", i+1)
		prNumber := 200 + i
		seedTaskMonitorAssignment(t, deps, issue, paneID, prNumber)
		deps.commands.queue("gh", []string{"pr", "checks", fmt.Sprintf("%d", prNumber), "--json", "bucket"}, ``, nil)
		deps.commands.queue("gh", []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
		deps.commands.queue("gh", []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prMergeableJSONFields}, `{"mergeable":"MERGEABLE"}`, nil)
		deps.commands.queue("gh", []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prReviewJSONFields}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
			testReview(fmt.Sprintf("reviewer-%d", i+1), "CHANGES_REQUESTED", "Please add tests."),
		}, nil), nil)
		assignments = append(assignments, activeTaskMonitorAssignment(t, deps, issue))
	}
	return assignments
}

func reloadTaskMonitorAssignments(t *testing.T, deps *testDeps, assignments []ActiveAssignment) []ActiveAssignment {
	t.Helper()

	reloaded := make([]ActiveAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		reloaded = append(reloaded, activeTaskMonitorAssignment(t, deps, assignment.Task.Issue))
	}
	return reloaded
}

type blockedWaitIdle struct {
	mu       sync.Mutex
	started  []string
	release  map[string]chan struct{}
	released int
}

func newBlockedWaitIdle() *blockedWaitIdle {
	return &blockedWaitIdle{
		release: make(map[string]chan struct{}),
	}
}

func (b *blockedWaitIdle) hook(paneID string, _ time.Duration, _ time.Duration) {
	b.mu.Lock()
	if _, ok := b.release[paneID]; !ok {
		b.started = append(b.started, paneID)
		b.release[paneID] = make(chan struct{})
	}
	release := b.release[paneID]
	b.mu.Unlock()

	<-release
}

func (b *blockedWaitIdle) waitForStarted(t *testing.T, want int) {
	t.Helper()

	waitFor(t, fmt.Sprintf("%d blocked nudges", want), func() bool {
		return b.startedCount() >= want
	})
}

func (b *blockedWaitIdle) startedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.started)
}

func (b *blockedWaitIdle) requireStartedCount(t *testing.T, want int) {
	t.Helper()

	waitForDuration(t, 50*time.Millisecond)
	if got := b.startedCount(); got != want {
		t.Fatalf("blocked wait-idle count = %d, want %d", got, want)
	}
}

func (b *blockedWaitIdle) releaseOne() {
	b.mu.Lock()
	paneID := b.started[b.released]
	release := b.release[paneID]
	b.released++
	b.mu.Unlock()

	close(release)
}

func (b *blockedWaitIdle) releaseAll() {
	b.mu.Lock()
	channels := make([]chan struct{}, 0, len(b.started)-b.released)
	for _, paneID := range b.started[b.released:] {
		channels = append(channels, b.release[paneID])
	}
	b.released = len(b.started)
	b.mu.Unlock()

	for _, release := range channels {
		close(release)
	}
}
