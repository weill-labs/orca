package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDaemonStartEscalatesMissingPaneAssignmentsAndKeepsClone(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedActiveAssignment(t, deps, "LAB-721", "pane-1")
	deps.amux.paneExists = map[string]bool{"pane-1": false}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "startup reconciliation", func() bool {
		task, ok := deps.state.task("LAB-721")
		if !ok || task.Status != TaskStatusActive {
			return false
		}
		worker, workerOK := deps.state.worker(task.WorkerID)
		return workerOK && worker.Health == WorkerHealthEscalated
	})

	task, ok := deps.state.task("LAB-721")
	if !ok {
		t.Fatal("task missing after startup reconciliation")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	worker, ok := deps.state.worker(task.WorkerID)
	if !ok {
		t.Fatal("worker missing after missing-pane reconciliation")
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got, want := worker.PaneID, "pane-1"; got != want {
		t.Fatalf("worker.PaneID = %q, want %q", got, want)
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventWorkerEscalated)
	event, ok := deps.events.lastEventOfType(EventWorkerEscalated)
	if !ok {
		t.Fatal("worker escalation event missing")
	}
	if got, want := event.Message, "worker pane missing on daemon startup"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
}

func TestDaemonStartResumesMonitoringLiveAssignments(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedActiveAssignment(t, deps, "LAB-722", "pane-1")
	deps.amux.captureSequence("pane-1", []string{"updated output"})

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "capture resume", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCapture == "updated output"
	})

	task, ok := deps.state.task("LAB-722")
	if !ok {
		t.Fatal("task missing after startup")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := deps.amux.captureCount("pane-1"), 2; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
}

func TestDaemonStartReleasesStaleOccupiedClones(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedActiveAssignment(t, deps, "LAB-722", "pane-1")
	deps.amux.captureSequence("pane-1", []string{"updated output"})

	staleClonePath := filepath.Join(t.TempDir(), "clone-stale")
	deps.state.cloneOccupancies = []CloneOccupancy{
		{
			Project:       "/tmp/project",
			Path:          staleClonePath,
			CurrentBranch: "LAB-721",
			AssignedTask:  "LAB-721",
		},
		{
			Project:       "/tmp/project",
			Path:          deps.pool.clone.Path,
			CurrentBranch: "LAB-722",
			AssignedTask:  "LAB-722",
		},
	}
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-721",
		Status:       TaskStatusDone,
		Prompt:       "Wrap up stale task",
		WorkerID:     "worker-stale",
		CloneName:    filepath.Base(staleClonePath),
		ClonePath:    staleClonePath,
		Branch:       "LAB-721",
		AgentProfile: "codex",
		CreatedAt:    deps.clock.Now().Add(-time.Hour),
		UpdatedAt:    deps.clock.Now().Add(-time.Minute),
	})

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "stale clone release", func() bool {
		return len(deps.pool.releasedClones()) == 1
	})

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          filepath.Base(staleClonePath),
		Path:          staleClonePath,
		CurrentBranch: "LAB-721",
		AssignedTask:  "LAB-721",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
}

func TestDaemonStartReconcilesStrandedMergedTasks(t *testing.T) {
	t.Parallel()

	const (
		issue    = "LAB-1320"
		prNumber = 1320
	)

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	seedActiveAssignment(t, deps, issue, "pane-1")
	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatalf("task %q missing before startup", issue)
	}
	task.State = TaskStateMerged
	task.PRNumber = prNumber
	deps.state.putTaskForTest(task)
	seedMergeEntryForTest(t, deps, issue, prNumber)

	workerID := task.WorkerID
	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "stranded merged task cleanup", func() bool {
		task, ok := deps.state.task(issue)
		return ok && task.Status == TaskStatusDone && task.State == TaskStateDone
	})

	task, ok = deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after startup reconciliation")
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := task.State, TaskStateDone; got != want {
		t.Fatalf("task.State = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker(workerID)
	if !ok {
		t.Fatalf("worker %q missing after startup reconciliation", workerID)
	}
	if worker.Issue != "" || worker.PaneID != "" {
		t.Fatalf("worker claim = issue %q pane %q, want released", worker.Issue, worker.PaneID)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: issue,
		AssignedTask:  issue,
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", prNumber)
	if err != nil {
		t.Fatalf("MergeEntry() error = %v", err)
	}
	if entry != nil {
		t.Fatalf("MergeEntry() = %#v, want nil after startup cleanup", entry)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		mergedWrapUpPrompt + "\n",
		postmortemCommand + "\n",
	})
	deps.events.requireTypes(t, EventWorkerPostmortem, EventTaskCompleted)

	wrapUpPromptCount := deps.amux.countKey("pane-1", mergedWrapUpPrompt+"\n")
	postmortemPromptCount := deps.amux.countKey("pane-1", postmortemCommand+"\n")
	completedEventCount := deps.events.countType(EventTaskCompleted)
	d.reconcileStrandedMergedTasks(ctx)
	if got := deps.amux.countKey("pane-1", mergedWrapUpPrompt+"\n"); got != wrapUpPromptCount {
		t.Fatalf("merged wrap-up prompt count after second recovery = %d, want %d", got, wrapUpPromptCount)
	}
	if got := deps.amux.countKey("pane-1", postmortemCommand+"\n"); got != postmortemPromptCount {
		t.Fatalf("postmortem prompt count after second recovery = %d, want %d", got, postmortemPromptCount)
	}
	if got := deps.events.countType(EventTaskCompleted); got != completedEventCount {
		t.Fatalf("task completed event count after second recovery = %d, want %d", got, completedEventCount)
	}
}

func TestIsStrandedMergedTaskOnlyMatchesNonTerminalMergedState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		task Task
		want bool
	}{
		{
			name: "active merged",
			task: Task{Status: TaskStatusActive, State: TaskStateMerged},
			want: true,
		},
		{
			name: "active done",
			task: Task{Status: TaskStatusActive, State: TaskStateDone},
			want: false,
		},
		{
			name: "active assigned",
			task: Task{Status: TaskStatusActive, State: TaskStateAssigned},
			want: false,
		},
		{
			name: "terminal merged",
			task: Task{Status: TaskStatusDone, State: TaskStateMerged},
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isStrandedMergedTask(tt.task); got != tt.want {
				t.Fatalf("isStrandedMergedTask(%#v) = %t, want %t", tt.task, got, tt.want)
			}
		})
	}
}

func TestGlobalDaemonStartUsesTaskProjectForWorkerLookup(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedActiveAssignment(t, deps, "LAB-900", "pane-1")
	deps.amux.captureSequence("pane-1", []string{"updated output"})

	d, err := New(Options{
		Project:          "",
		Session:          "test-session",
		PIDPath:          deps.pidPath,
		Config:           deps.config,
		State:            deps.state,
		Pool:             deps.pool,
		Amux:             deps.amux,
		IssueTracker:     deps.issueTracker,
		Commands:         deps.commands,
		Events:           deps.events,
		Now:              deps.clock.Now,
		NewTicker:        deps.tickers.NewTicker,
		Sleep:            deps.sleep,
		CaptureInterval:  5 * time.Second,
		PollInterval:     30 * time.Second,
		MergeGracePeriod: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	d.github = newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "",
		commands:    deps.commands,
		now:         deps.clock.Now,
		sleep:       noSleep,
		maxAttempts: 1,
	})

	task, ok := deps.state.task("LAB-900")
	if !ok {
		t.Fatal("task missing before startup reconciliation")
	}

	d.reconcileTaskOnStartup(context.Background(), task)

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after global startup reconciliation")
	}
	if got, want := worker.LastCapture, defaultCodexReadyOutput(); got != want {
		t.Fatalf("worker.LastCapture = %q, want %q without monitor tick", got, want)
	}

	task, ok = deps.state.task("LAB-900")
	if !ok {
		t.Fatal("task missing after startup reconciliation")
	}
	if _, ok := deps.state.worker("pane-1"); !ok {
		t.Fatal("worker missing after startup reconciliation")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := deps.amux.captureCount("pane-1"), 1; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
}

func TestDaemonStartNormalizesLegacyNumericPaneRefs(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedActiveAssignment(t, deps, "LAB-854", "7")

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	task, ok := deps.state.task("LAB-854")
	if !ok {
		t.Fatal("task missing after startup")
	}
	if got, want := task.PaneID, "7"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	if got, want := task.WorkerID, "worker-01"; got != want {
		t.Fatalf("task.WorkerID = %q, want %q", got, want)
	}
	if got, want := task.PaneName, "w-LAB-854"; got != want {
		t.Fatalf("task.PaneName = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after startup reconciliation")
	}
	if got, want := worker.PaneID, "7"; got != want {
		t.Fatalf("worker.PaneID = %q, want %q", got, want)
	}
	if got, want := worker.PaneName, "w-LAB-854"; got != want {
		t.Fatalf("worker.PaneName = %q, want %q", got, want)
	}

	if got, want := deps.amux.paneExistsCalls, []string{"7"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pane exists calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.captureCount("7"), 1; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
}

func TestDaemonStartEscalatesWhenPaneRefNormalizationFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedActiveAssignment(t, deps, "LAB-856", "7")
	task, ok := deps.state.task("LAB-856")
	if !ok {
		t.Fatal("seeded task missing")
	}
	worker, ok := deps.state.worker(task.WorkerID)
	if !ok {
		t.Fatal("seeded worker missing")
	}
	worker.PaneName = ""
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	state := &resumeStateStub{
		fakeState:    deps.state,
		putWorkerErr: errors.New("put worker failed"),
	}

	d := newResumeCoverageDaemon(t, deps, state, deps.amux)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.events.requireTypes(t, EventDaemonStarted, EventWorkerEscalated)
	event, ok := deps.events.lastEventOfType(EventWorkerEscalated)
	if !ok {
		t.Fatal("worker escalation event missing")
	}
	if !strings.Contains(event.Message, "worker pane normalization failed on daemon startup") {
		t.Fatalf("event.Message = %q, want normalization failure context", event.Message)
	}
	if !strings.Contains(event.Message, "put worker failed") {
		t.Fatalf("event.Message = %q, want put worker failure", event.Message)
	}
	if got := len(deps.amux.paneExistsCalls); got != 0 {
		t.Fatalf("pane exists calls = %d, want 0 after normalization failure", got)
	}
}

func TestDaemonStartMissingPaneRecoveryIsIdempotentAcrossRestart(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedActiveAssignment(t, deps, "LAB-723", "pane-1")
	deps.amux.paneExists = map[string]bool{"pane-1": false}

	first := deps.newDaemon(t)
	ctx := context.Background()
	if err := first.Start(ctx); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}
	waitFor(t, "first startup reconciliation", func() bool {
		task, ok := deps.state.task("LAB-723")
		if !ok {
			return false
		}
		worker, workerOK := deps.state.worker(task.WorkerID)
		return workerOK && task.Status == TaskStatusActive && worker.Health == WorkerHealthEscalated
	})
	if err := first.Stop(ctx); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}

	second := deps.newDaemon(t)
	if err := second.Start(ctx); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = second.Stop(context.Background())
	})

	task, ok := deps.state.task("LAB-723")
	if !ok {
		t.Fatal("task missing after second startup reconciliation")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	worker, ok := deps.state.worker(task.WorkerID)
	if !ok {
		t.Fatal("worker missing after second startup reconciliation")
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	if got, want := deps.events.countType(EventTaskFailed), 0; got != want {
		t.Fatalf("task failed events = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerEscalated), 1; got != want {
		t.Fatalf("worker escalated events = %d, want %d", got, want)
	}
}

func seedActiveAssignment(t *testing.T, deps *testDeps, issue, paneID string) {
	t.Helper()

	now := deps.clock.Now()
	workerID := nextTestWorkerID(deps)
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        issue,
		Status:       TaskStatusActive,
		Prompt:       "Implement startup recovery",
		WorkerID:     workerID,
		PaneID:       paneID,
		PaneName:     workerID,
		CloneName:    deps.pool.clone.Name,
		ClonePath:    deps.pool.clone.Path,
		Branch:       issue,
		AgentProfile: "codex",
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        "/tmp/project",
		WorkerID:       workerID,
		PaneID:         paneID,
		PaneName:       workerID,
		Issue:          issue,
		ClonePath:      deps.pool.clone.Path,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastCapture:    defaultCodexReadyOutput(),
		LastActivityAt: now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	deps.pool.mu.Lock()
	if deps.pool.acquired == nil {
		deps.pool.acquired = make(map[string]bool)
	}
	deps.pool.acquired[deps.pool.clone.Path] = true
	deps.pool.mu.Unlock()
}
