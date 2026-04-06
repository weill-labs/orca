package daemon

import (
	"context"
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
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

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
	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", "mergeable"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", "reviews,reviewDecision,comments"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

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

func TestTaskMonitorStaleResultIsDropped(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-903", "pane-3", 43)

	check43 := []string{"pr", "checks", "43", "--json", "bucket"}
	block43 := deps.commands.block("gh", check43)
	deps.commands.queue("gh", check43, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "43", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "43", "--json", "mergeable"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "43", "--json", "reviews,reviewDecision,comments"}, ``, nil)

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
}

func seedTaskMonitorAssignment(t *testing.T, deps *testDeps, issue, paneID string, prNumber int) {
	t.Helper()

	now := deps.clock.Now()
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        issue,
		Status:       TaskStatusActive,
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
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        "/tmp/project",
		PaneID:         paneID,
		PaneName:       paneID,
		Issue:          issue,
		ClonePath:      "/tmp/" + issue,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastActivityAt: now,
		UpdatedAt:      now,
	}); err != nil {
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
