package daemon

import (
	"context"
	"testing"
	"time"
)

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

func waitForTaskMonitorBlocks(t *testing.T, started ...<-chan struct{}) {
	t.Helper()

	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	for _, ch := range started {
		select {
		case <-timer.C:
			t.Fatal("timed out waiting for parallel task monitor polls")
		case <-ch:
		}
	}
}
