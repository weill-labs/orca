package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPRMergePollingSendsWrapUpAndCleansClone(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	const paneTitle = "Merge queue title"

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex", paneTitle); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task completion after merge", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusDone
	})

	task, _ := deps.state.task("LAB-689")
	if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-689",
		AssignedTask:  "LAB-689",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-689", State: "In Progress"},
		{Issue: "LAB-689", State: "Done"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none", got)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{
		wrappedCodexPrompt("LAB-689", "Implement daemon core") + "\n",
		"PR merged, wrap up.\n",
		"$postmortem\n",
	})
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventPRMerged, EventWorkerPostmortem, EventTaskCompleted)
	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"status":         "done",
		"task":           "\x1b[2m\x1b[9m" + paneTitle + "\x1b[29m\x1b[22m",
		"tracked_issues": `[{"id":"LAB-689","status":"completed"}]`,
		"tracked_prs":    `[{"number":42,"status":"completed"}]`,
	})
	if got := deps.events.lastMessage(EventWorkerPostmortem); !strings.Contains(got, "sent") {
		t.Fatalf("postmortem event message = %q, want sent status", got)
	}
}

func TestQueuedPRMergePollingCompletesTaskWithoutExtraTick(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	task, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("LAB-689 task missing from state")
	}
	task.PRNumber = 42
	deps.state.putTaskForTest(task)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "queued task completion after merge", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusDone
	})

	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker still present after queued PR merge cleanup")
	}
	entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
	if err != nil {
		t.Fatalf("MergeEntry() error = %v", err)
	}
	if entry != nil {
		t.Fatalf("MergeEntry() = %#v, want nil after queued PR merge cleanup", entry)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-689",
		AssignedTask:  "LAB-689",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-689", State: IssueStateInProgress},
		{Issue: "LAB-689", State: IssueStateDone},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if got := deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}); got != 0 {
		t.Fatalf("rebase call count = %d, want 0 for already-merged queued PR", got)
	}
	if got := deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}); got != 0 {
		t.Fatalf("merge call count = %d, want 0 for already-merged queued PR", got)
	}
	if got := deps.events.countType(EventPRLandingStarted); got != 0 {
		t.Fatalf("landing started event count = %d, want 0", got)
	}
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPREnqueued, EventPRMerged, EventWorkerPostmortem, EventTaskCompleted)
}

func TestPRMergeCleanupContinuesWhenIssueTrackerDoneUpdateFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		ResumeSequence:    []string{"codex --yolo resume", "Enter", "."},
		PostmortemEnabled: false,
		StuckTimeout:      5 * time.Minute,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   3,
	}
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
	deps.issueTracker.errors = map[string]error{
		IssueStateDone: errors.New("linear unavailable"),
	}
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task completion after merge despite tracker failure", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusDone
	})

	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker still present after merge cleanup")
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-689",
		AssignedTask:  "LAB-689",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-689", State: IssueStateInProgress},
		{Issue: "LAB-689", State: IssueStateDone},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if event, ok := deps.events.lastEventOfType(EventPRMerged); !ok {
		t.Fatal("missing PR merged event")
	} else if !strings.Contains(event.Message, "failed to update Linear issue status") {
		t.Fatalf("PR merged event message = %q, want tracker failure context", event.Message)
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none", got)
	}
}

func TestPRMergePollingStillSendsPostmortemAfterWrapUpError(t *testing.T) {
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	deps.amux.sendKeysResults = []error{errors.New("wrap up failed")}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task completion after merge error", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusDone
	})

	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none", got)
	}
	if got, want := deps.amux.countKey("pane-1", "$postmortem\n"), 1; got != want {
		t.Fatalf("postmortem prompt count = %d, want %d", got, want)
	}
	if got := deps.events.lastMessage(EventWorkerPostmortem); !strings.Contains(got, "sent") {
		t.Fatalf("postmortem event message = %q, want sent status", got)
	}
	deps.events.requireTypes(t, EventTaskCompletionFailed)
}

func TestPRCloseWithoutMergePollingCancelsTaskAndStopsFollowUpPolls(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1323", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1323", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"state":"CLOSED","mergedAt":null,"closedAt":"2026-04-16T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1323", "Handle closed PRs", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task cancellation after closed PR", func() bool {
		task, ok := deps.state.task("LAB-1323")
		return ok && task.Status == TaskStatusCancelled
	})

	task, ok := deps.state.task("LAB-1323")
	if !ok {
		t.Fatal("task missing after closed PR cleanup")
	}
	if got, want := task.State, TaskStateDone; got != want {
		t.Fatalf("task.State = %q, want %q", got, want)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-1323", State: IssueStateInProgress},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "checks", "42", "--json", "bucket"}), 0; got != want {
		t.Fatalf("gh pr checks call count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 0; got != want {
		t.Fatalf("mergeable poll call count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}), 0; got != want {
		t.Fatalf("review poll call count = %d, want %d", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", "PR merged, wrap up."), 0; got != want {
		t.Fatalf("merged wrap-up prompt count = %d, want %d", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", "$postmortem\n"), 1; got != want {
		t.Fatalf("postmortem prompt count = %d, want %d", got, want)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got, want := deps.events.countType(EventPRClosedWithoutMerge), 1; got != want {
		t.Fatalf("closed-without-merge event count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventPRMerged), 0; got != want {
		t.Fatalf("merged event count = %d, want %d", got, want)
	}
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventPRClosedWithoutMerge, EventWorkerPostmortem, EventTaskCancelled)
}

func TestPRDetectionSyncsPaneMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "pr metadata sync", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.PRNumber == 42
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after PR detection")
	}
	if got, want := worker.LastPushAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("worker.LastPushAt = %v, want %v", got, want)
	}
	if got, want := worker.LastPRNumber, 42; got != want {
		t.Fatalf("worker.LastPRNumber = %d, want %d", got, want)
	}
	if got, want := worker.LastPRPollAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("worker.LastPRPollAt = %v, want %v", got, want)
	}

	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"task":           "LAB-689",
		"tracked_issues": `[{"id":"LAB-689","status":"active"}]`,
		"tracked_prs":    `[{"number":42,"status":"active"}]`,
	})
}

func TestPRDetectionUsesAdaptiveIntervalsAfterPRDiscovery(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	detectedAt := tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "pr detection cycle completion")
	waitFor(t, "pr detection", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.PRNumber == 42
	})

	if got, want := deps.commands.countCalls("gh", []string{"pr", "checks", "42", "--json", "bucket"}), 1; got != want {
		t.Fatalf("gh pr checks calls on detection poll = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, 4*time.Second, "pre-fast-window poll cycle completion")
	if got, want := deps.commands.countCalls("gh", []string{"pr", "checks", "42", "--json", "bucket"}), 1; got != want {
		t.Fatalf("gh pr checks calls before 5s = %d, want %d", got, want)
	}

	followUpAt := tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "fast follow-up poll cycle completion")
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after fast follow-up poll")
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "checks", "42", "--json", "bucket"}), 2; got != want {
		t.Fatalf("gh pr checks calls after 5s = %d, want %d", got, want)
	}
	if got, want := worker.LastPushAt, detectedAt; !got.Equal(want) {
		t.Fatalf("worker.LastPushAt = %v, want %v", got, want)
	}
	if got, want := worker.LastPRPollAt, followUpAt; !got.Equal(want) {
		t.Fatalf("worker.LastPRPollAt = %v, want %v", got, want)
	}
}

func TestPRPollResetsAdaptiveWindowWhenTaskPRNumberChanges(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	seedTaskMonitorAssignment(t, deps, "LAB-689", "pane-1", 42)
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after seed")
	}
	worker.LastPRNumber = 41
	worker.LastPushAt = deps.clock.Now().Add(-40 * time.Minute)
	worker.LastPRPollAt = deps.clock.Now()
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	resetAt := tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "pr change poll cycle completion")
	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after PR change poll")
	}
	if got, want := worker.LastPRNumber, 42; got != want {
		t.Fatalf("worker.LastPRNumber = %d, want %d", got, want)
	}
	if got, want := worker.LastPushAt, resetAt; !got.Equal(want) {
		t.Fatalf("worker.LastPushAt = %v, want %v", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, 4*time.Second, "pre-reset-window poll cycle completion")
	if got, want := deps.commands.countCalls("gh", []string{"pr", "checks", "42", "--json", "bucket"}), 1; got != want {
		t.Fatalf("gh pr checks calls before reset window elapses = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "reset-window follow-up poll cycle completion")
	if got, want := deps.commands.countCalls("gh", []string{"pr", "checks", "42", "--json", "bucket"}), 2; got != want {
		t.Fatalf("gh pr checks calls after reset window elapses = %d, want %d", got, want)
	}
}

func TestPRPollingLogsGitHubRateLimitWarnings(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, "HTTP 429: API rate limit exceeded\nRetry-After: 120\n", errors.New("gh: HTTP 429"))

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "github rate limit warning event", func() bool {
		return deps.events.countType("pr.rate_limited") == 1
	})

	event, ok := deps.events.lastEventOfType("pr.rate_limited")
	if !ok {
		t.Fatal("missing GitHub rate limit event")
	}
	if got, want := event.Message, "github: rate limited until 09:02"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
	if task, ok := deps.state.task("LAB-689"); !ok {
		t.Fatal("task missing after poll")
	} else if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
}

func TestPRPollingLogsGitHubRateLimitWarningsDuringPRDiscovery(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, "HTTP 429: API rate limit exceeded\nRetry-After: 120\n", errors.New("gh: HTTP 429"))

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "discovery rate limit warning event", func() bool {
		return deps.events.countType(EventPRRateLimited) == 1
	})

	if task, ok := deps.state.task("LAB-689"); !ok {
		t.Fatal("task missing after discovery poll")
	} else if got := task.PRNumber; got != 0 {
		t.Fatalf("task.PRNumber = %d, want 0", got)
	}
}

func TestPRPollingContinuesFollowUpPollsAfterNonRateLimitMergeLookupError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, ``, errors.New("gh failed"))
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"MERGEABLE"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "follow-up polls after merge lookup failure", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastMergeableState == "MERGEABLE" &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}) == 1
	})

	if got, want := deps.events.countType(EventPRRateLimited), 0; got != want {
		t.Fatalf("GitHub rate limit event count = %d, want %d", got, want)
	}
}

func TestPRPollingSkipsDiscoveryWhenAssignmentAlreadyTracksPR(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "pr checks poll", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCIState == ciStatePending
	})

	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}), 0; got != want {
		t.Fatalf("gh pr discovery calls = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventPRDetected), 0; got != want {
		t.Fatalf("PR detected event count = %d, want %d", got, want)
	}
	if task, ok := deps.state.task("LAB-689"); !ok {
		t.Fatal("task not stored in fake state")
	} else if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
}

func TestPRPollFallsBackToIssueIDSearchAndPersistsObservedBranch(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		ResumeSequence:    []string{"codex --yolo resume", "Enter", "."},
		PostmortemEnabled: false,
		StuckTimeout:      5 * time.Minute,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   3,
	}
	seedTaskMonitorAssignment(t, deps, "LAB-123", "pane-1", 0)
	task, ok := deps.state.task("LAB-123")
	if !ok {
		t.Fatal("task missing after seed")
	}
	task.Branch = "LAB-123"
	deps.state.putTaskForTest(task)

	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-123", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--search", "LAB-123 in:title", "--state", "all", "--json", "number,state,headRefName,title", "--limit", "5"}, `[
		{"number":455,"state":"MERGED","headRefName":"lab-1230-renamed","title":"LAB-1230: sibling issue"},
		{"number":456,"state":"MERGED","headRefName":"lab-123-renamed","title":"LAB-123: recover renamed branch"}
	]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "456", "--json", "bucket"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "456", "--json", prTerminalStateJSONFields}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-123"))
	d.applyTaskStateUpdate(context.Background(), update)

	task, ok = deps.state.task("LAB-123")
	if !ok {
		t.Fatal("task missing after poll")
	}
	if got, want := task.PRNumber, 456; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	if got, want := task.Branch, "lab-123-renamed"; got != want {
		t.Fatalf("task.Branch = %q, want %q", got, want)
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventPRDetected), 1; got != want {
		t.Fatalf("PR detected event count = %d, want %d", got, want)
	}
}

func TestPRPollDoesNotGuessFromAmbiguousIssueIDSearch(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-123", "pane-1", 0)

	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-123", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--search", "LAB-123 in:title", "--state", "all", "--json", "number,state,headRefName,title", "--limit", "5"}, `[
		{"number":456,"state":"OPEN","headRefName":"lab-123-renamed","title":"LAB-123: current task"},
		{"number":457,"state":"MERGED","headRefName":"follow-up-lab-123","title":"follow-up for LAB-123"}
	]`, nil)

	var logs strings.Builder
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = func(format string, args ...any) {
			fmt.Fprintf(&logs, format, args...)
		}
	})

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-123"))
	d.applyTaskStateUpdate(context.Background(), update)

	task, ok := deps.state.task("LAB-123")
	if !ok {
		t.Fatal("task missing after poll")
	}
	if got := task.PRNumber; got != 0 {
		t.Fatalf("task.PRNumber = %d, want 0", got)
	}
	if got, want := task.Branch, "LAB-123"; got != want {
		t.Fatalf("task.Branch = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventPRDetected), 0; got != want {
		t.Fatalf("PR detected event count = %d, want %d", got, want)
	}
	if got := logs.String(); !strings.Contains(got, "LAB-123") || !strings.Contains(got, "multiple pull requests") {
		t.Fatalf("logs = %q, want ambiguous search message", got)
	}
}

func TestPRPollSkipsSingleFalsePositiveIssueIDSearchResult(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-123", "pane-1", 0)

	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-123", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--search", "LAB-123 in:title", "--state", "all", "--json", "number,state,headRefName,title", "--limit", "5"}, `[{"number":456,"state":"MERGED","headRefName":"lab-1230-renamed","title":"LAB-1230: sibling issue"}]`, nil)

	d := deps.newDaemon(t)

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-123"))
	d.applyTaskStateUpdate(context.Background(), update)

	task, ok := deps.state.task("LAB-123")
	if !ok {
		t.Fatal("task missing after poll")
	}
	if got := task.PRNumber; got != 0 {
		t.Fatalf("task.PRNumber = %d, want 0", got)
	}
	if got, want := task.Branch, "LAB-123"; got != want {
		t.Fatalf("task.Branch = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventPRDetected), 0; got != want {
		t.Fatalf("PR detected event count = %d, want %d", got, want)
	}
}

func TestPRDetectionPreservesTrackedHistoryForReusedPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	deps.state.tasks["LAB-688"] = Task{
		Project:      d.project,
		Issue:        "LAB-688",
		Status:       TaskStatusDone,
		PaneID:       deps.amux.spawnPane.ID,
		ClonePath:    deps.pool.clone.Path,
		Branch:       "LAB-688",
		AgentProfile: "codex",
		PRNumber:     41,
		CreatedAt:    deps.clock.Now().Add(-2 * time.Hour),
		UpdatedAt:    deps.clock.Now().Add(-time.Hour),
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "pr metadata sync", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.PRNumber == 42
	})

	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"task":           "LAB-689",
		"tracked_issues": `[{"id":"LAB-688","status":"completed"},{"id":"LAB-689","status":"active"}]`,
		"tracked_prs":    `[{"number":41,"status":"completed"},{"number":42,"status":"active"}]`,
	})
}

func TestPRDetectionContinuesForEscalatedWorkers(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-984", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-984", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-984", "Keep polling PRs after escalation", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after assignment")
	}
	worker.Health = WorkerHealthEscalated
	if err := deps.state.PutWorker(ctx, worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "pr detection for escalated worker", func() bool {
		task, ok := deps.state.task("LAB-984")
		return ok && task.PRNumber == 42
	})

	task, ok := deps.state.task("LAB-984")
	if !ok {
		t.Fatal("task missing after poll")
	}
	if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}

	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after poll")
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventPRDetected), 1; got != want {
		t.Fatalf("PR detected event count = %d, want %d", got, want)
	}
}

func TestPRDetectionContinuesForEscalatedStartingWorkers(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-985", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	now := deps.clock.Now()
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-985",
		Status:       TaskStatusStarting,
		Prompt:       "Resume after startup escalation",
		WorkerID:     "worker-01",
		PaneID:       "pane-1",
		PaneName:     "pane-1",
		CloneName:    "clone-LAB-985",
		ClonePath:    "/tmp/LAB-985",
		Branch:       "LAB-985",
		AgentProfile: "codex",
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-01",
		PaneID:       "pane-1",
		PaneName:     "pane-1",
		Issue:        "LAB-985",
		ClonePath:    "/tmp/LAB-985",
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
		CreatedAt:    now,
		LastSeenAt:   now,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	deps.amux.capturePaneErr = errors.New("capture failed on startup")

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "startup escalation", func() bool {
		task, ok := deps.state.task("LAB-985")
		if !ok || task.Status != TaskStatusStarting {
			return false
		}
		worker, ok := deps.state.worker("worker-01")
		return ok && worker.Health == WorkerHealthEscalated
	})

	deps.amux.capturePaneErr = nil

	prTicker.tick(deps.clock.Now())
	waitFor(t, "pr detection for escalated starting worker", func() bool {
		task, ok := deps.state.task("LAB-985")
		return ok && task.PRNumber == 42
	})

	task, ok := deps.state.task("LAB-985")
	if !ok {
		t.Fatal("task missing after poll")
	}
	if got, want := task.Status, TaskStatusStarting; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
}

func TestPRMergeablePollingNudgesWorkerOnConflictTransitions(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"MERGEABLE"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "initial conflict poll cycle completion")
	waitFor(t, "initial conflict nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", conflictNudgePrompt+"\n") == 1 &&
			worker.LastMergeableState == "CONFLICTING"
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "second conflict poll cycle completion")
	waitFor(t, "conflicting state re-polled", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}) == 2
	})
	if got, want := deps.amux.countKey("pane-1", conflictNudgePrompt+"\n"), 1; got != want {
		t.Fatalf("conflict nudge count after repeated conflicting poll = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 1; got != want {
		t.Fatalf("conflict nudge event count after repeated conflicting poll = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "mergeable recovery poll cycle completion")
	waitFor(t, "mergeable state recovery", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}) == 3 &&
			worker.LastMergeableState == "MERGEABLE"
	})
	if got, want := deps.amux.countKey("pane-1", conflictNudgePrompt+"\n"), 1; got != want {
		t.Fatalf("conflict nudge count after recovery = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "conflict renudge poll cycle completion")
	waitFor(t, "conflict nudge after recovery", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", conflictNudgePrompt+"\n") == 2 &&
			worker.LastMergeableState == "CONFLICTING"
	})

	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 4; got != want {
		t.Fatalf("mergeable poll count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 2; got != want {
		t.Fatalf("conflict nudge event count = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		wrappedCodexPrompt("LAB-689", "Implement daemon core") + "\n",
		conflictNudgePrompt + "\n",
		conflictNudgePrompt + "\n",
	})
}

func TestPRMergeablePollingRetriesUnknownUntilConflict(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1293", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"UNKNOWN","mergeStateStatus":"CLEAN"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"CONFLICTING","mergeStateStatus":"DIRTY"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	var sleeps []time.Duration
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Sleep = func(_ context.Context, delay time.Duration) error {
			sleeps = append(sleeps, delay)
			return nil
		}
	})

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-1293"))
	update.runNudges(context.Background(), d)
	d.applyTaskStateUpdate(context.Background(), update)

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after unknown mergeable retry")
	}
	if got, want := worker.LastMergeableState, "CONFLICTING"; got != want {
		t.Fatalf("worker.LastMergeableState = %q, want %q", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", conflictNudgePrompt+"\n"), 1; got != want {
		t.Fatalf("conflict nudge count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 1; got != want {
		t.Fatalf("conflict nudge event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 2; got != want {
		t.Fatalf("mergeable poll count = %d, want %d", got, want)
	}
	if got, want := sleeps, []time.Duration{mergeableUnknownRetryDelay}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep delays = %#v, want %#v", got, want)
	}
}

func TestPRMergeablePollingTreatsPersistentUnknownDirtyStateAsConflict(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1293", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	for i := 0; i < 4; i++ {
		deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"UNKNOWN","mergeStateStatus":"DIRTY"}`, nil)
	}
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	var sleeps []time.Duration
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Sleep = func(_ context.Context, delay time.Duration) error {
			sleeps = append(sleeps, delay)
			return nil
		}
	})

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-1293"))
	update.runNudges(context.Background(), d)
	d.applyTaskStateUpdate(context.Background(), update)

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after dirty fallback mergeable poll")
	}
	if got, want := worker.LastMergeableState, "CONFLICTING"; got != want {
		t.Fatalf("worker.LastMergeableState = %q, want %q", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", conflictNudgePrompt+"\n"), 1; got != want {
		t.Fatalf("conflict nudge count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 1; got != want {
		t.Fatalf("conflict nudge event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 4; got != want {
		t.Fatalf("mergeable poll count = %d, want %d", got, want)
	}
	if got, want := sleeps, []time.Duration{mergeableUnknownRetryDelay, mergeableUnknownRetryDelay, mergeableUnknownRetryDelay}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep delays = %#v, want %#v", got, want)
	}
}

func TestPRMergeablePollingLeavesPreviousStateOnPersistentUnknownCleanState(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1293", "pane-1", 42)
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after seed")
	}
	worker.LastMergeableState = "MERGEABLE"
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	for i := 0; i < 4; i++ {
		deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"UNKNOWN","mergeStateStatus":"CLEAN"}`, nil)
	}
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	var sleeps []time.Duration
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Sleep = func(_ context.Context, delay time.Duration) error {
			sleeps = append(sleeps, delay)
			return nil
		}
	})

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-1293"))
	update.runNudges(context.Background(), d)
	d.applyTaskStateUpdate(context.Background(), update)

	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after clean unknown mergeable poll")
	}
	if got, want := worker.LastMergeableState, "MERGEABLE"; got != want {
		t.Fatalf("worker.LastMergeableState = %q, want %q", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", conflictNudgePrompt+"\n"), 0; got != want {
		t.Fatalf("conflict nudge count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 0; got != want {
		t.Fatalf("conflict nudge event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 4; got != want {
		t.Fatalf("mergeable poll count = %d, want %d", got, want)
	}
	if got, want := sleeps, []time.Duration{mergeableUnknownRetryDelay, mergeableUnknownRetryDelay, mergeableUnknownRetryDelay}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep delays = %#v, want %#v", got, want)
	}
}

func TestPRMergeablePollingRetriesConflictNudgeAfterWaitIdleFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)
	waitIdleCalls := 0
	deps.amux.waitIdleHook = func(_ string, _ time.Duration, _ time.Duration) {
		waitIdleCalls++
		if waitIdleCalls == 3 {
			deps.amux.waitIdleErr = errors.New("wait idle failed")
			return
		}
		deps.amux.waitIdleErr = nil
	}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "failed conflict nudge cycle completion")
	waitFor(t, "failed conflict nudge attempt", func() bool {
		return deps.amux.countKey("pane-1", conflictNudgePrompt) == 1
	})

	if worker, ok := deps.state.worker("pane-1"); !ok {
		t.Fatal("worker missing after failed conflict nudge")
	} else if worker.LastMergeableState != "" {
		t.Fatalf("worker.LastMergeableState = %q, want empty after failed wait idle", worker.LastMergeableState)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 0; got != want {
		t.Fatalf("conflict nudge event count after failed wait idle = %d, want %d", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", "\n"), 0; got != want {
		t.Fatalf("enter key count after failed wait idle = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "retried conflict nudge cycle completion")
	waitFor(t, "retried conflict nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", conflictNudgePrompt) == 1 &&
			deps.amux.countKey("pane-1", conflictNudgePrompt+"\n") == 1 &&
			deps.events.countType(EventWorkerNudgedConflict) == 1 &&
			worker.LastMergeableState == "CONFLICTING"
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{
		wrappedCodexPrompt("LAB-689", "Implement daemon core") + "\n",
		conflictNudgePrompt,
		conflictNudgePrompt + "\n",
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
}

func TestEnqueueProcessesQueuedPRLandingsInParallel(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.pool.clones = []Clone{
		deps.pool.clone,
		{Name: "clone-02", Path: filepath.Join(t.TempDir(), "clone-02")},
	}
	firstCaptureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	secondCaptureTicker := newFakeTicker()
	secondPollTicker := newFakeTicker()
	deps.tickers.enqueue(firstCaptureTicker, pollTicker, secondCaptureTicker, secondPollTicker)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign(LAB-689) error = %v", err)
	}
	if err := d.Assign(ctx, "LAB-690", "Implement merge queue", "codex"); err != nil {
		t.Fatalf("Assign(LAB-690) error = %v", err)
	}

	firstTask, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("LAB-689 task missing from state")
	}
	firstTask.PRNumber = 42
	deps.state.putTaskForTest(firstTask)

	secondTask, ok := deps.state.task("LAB-690")
	if !ok {
		t.Fatal("LAB-690 task missing from state")
	}
	secondTask.PRNumber = 43
	deps.state.putTaskForTest(secondTask)

	firstRebase := deps.commands.block("gh", []string{"pr", "update-branch", "42", "--rebase"})
	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	firstMerge := deps.commands.block("gh", []string{"pr", "merge", "42", "--squash"})
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)
	secondRebase := deps.commands.block("gh", []string{"pr", "update-branch", "43", "--rebase"})
	deps.commands.queue("gh", []string{"pr", "update-branch", "43", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "43", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	secondMerge := deps.commands.block("gh", []string{"pr", "merge", "43", "--squash"})
	deps.commands.queue("gh", []string{"pr", "merge", "43", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	if _, err := d.Enqueue(ctx, 43); err != nil {
		t.Fatalf("Enqueue(43) error = %v", err)
	}

	pollTicker.tick(deps.clock.Now())
	select {
	case <-firstRebase.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PR 42 rebase to start")
	}
	select {
	case <-secondRebase.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PR 43 rebase to start")
	}

	if got := deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}); got != 0 {
		t.Fatalf("merge started before rebase completed for PR 42: %d", got)
	}
	if got := deps.commands.countCalls("gh", []string{"pr", "merge", "43", "--squash"}); got != 0 {
		t.Fatalf("merge started before rebase completed for PR 43: %d", got)
	}

	close(firstRebase.release)
	close(secondRebase.release)
	waitFor(t, "queued PRs awaiting checks", func() bool {
		firstEntry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
		if err != nil || firstEntry == nil || firstEntry.Status != MergeQueueStatusAwaitingChecks {
			return false
		}
		secondEntry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 43)
		return err == nil && secondEntry != nil && secondEntry.Status == MergeQueueStatusAwaitingChecks
	})

	pollTicker.tick(deps.clock.Now())
	select {
	case <-firstMerge.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PR 42 merge to start")
	}
	select {
	case <-secondMerge.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for PR 43 merge to start")
	}

	close(firstMerge.release)
	close(secondMerge.release)

	if got, want := deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}), 1; got != want {
		t.Fatalf("rebase count for PR 42 = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}), 1; got != want {
		t.Fatalf("merge count for PR 42 = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "update-branch", "43", "--rebase"}), 1; got != want {
		t.Fatalf("rebase count for PR 43 = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "merge", "43", "--squash"}), 1; got != want {
		t.Fatalf("merge count for PR 43 = %d, want %d", got, want)
	}
}

func TestEnqueueNotifiesWorkerWhenRebaseFailsAndAllowsRequeue(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	task, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("LAB-689 task missing from state")
	}
	task.PRNumber = 42
	deps.state.putTaskForTest(task)

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, errors.New("rebase conflict"))

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	pollTicker.tick(deps.clock.Now())
	conflictNotice := "Merge queue could not rebase PR #42 onto main. Resolve the conflicts, push an update, and re-run `orca enqueue 42` when ready."
	waitFor(t, "merge queue conflict notice", func() bool {
		return deps.amux.countKey("pane-1", conflictNotice+"\n") == 1
	})
	if got, want := deps.amux.countKey("pane-1", "\n"), 0; got != want {
		t.Fatalf("merge queue enter count = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}

	if got := deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}); got != 0 {
		t.Fatalf("unexpected merge attempt after rebase failure: %d", got)
	}

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) after conflict error = %v", err)
	}

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "merge queue re-enqueue rebase", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}) == 2
	})

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "merge queue re-enqueue merge", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}) == 1
	})
}

func TestEnqueueSkipsDuplicateChecksWhileCIPollInFlight(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	task, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("LAB-689 task missing from state")
	}
	task.PRNumber = 42
	deps.state.putTaskForTest(task)

	checkArgs := []string{"pr", "checks", "42", "--json", "bucket"}
	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	firstChecks := deps.commands.block("gh", checkArgs)
	deps.commands.queue("gh", checkArgs, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "queued PR awaiting checks", func() bool {
		entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
		return err == nil && entry != nil && entry.Status == MergeQueueStatusAwaitingChecks
	})

	pollTicker.tick(deps.clock.Now())
	select {
	case <-firstChecks.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for CI check poll to start")
	}
	waitFor(t, "queued PR checking CI", func() bool {
		entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
		return err == nil && entry != nil && entry.Status == MergeQueueStatusCheckingCI
	})

	secondChecks := deps.commands.block("gh", checkArgs)
	pollTicker.tick(deps.clock.Now())
	select {
	case <-secondChecks.started:
		t.Fatal("second CI poll started while first CI poll was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(firstChecks.release)
	waitFor(t, "queued PR merged after CI completes", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}) == 1
	})

	if got, want := deps.commands.countCalls("gh", checkArgs), 1; got != want {
		t.Fatalf("checks call count = %d, want %d", got, want)
	}
}

func TestEnqueueMergesQueuedPRAfterPendingChecksPassOnLaterTick(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	task, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("LAB-689 task missing from state")
	}
	task.PRNumber = 42
	deps.state.putTaskForTest(task)

	checkArgs := []string{"pr", "checks", "42", "--json", "bucket"}
	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	firstChecks := deps.commands.block("gh", checkArgs)
	deps.commands.queue("gh", checkArgs, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", checkArgs, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "queued PR awaiting checks", func() bool {
		entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
		return err == nil && entry != nil && entry.Status == MergeQueueStatusAwaitingChecks
	})

	pollTicker.tick(deps.clock.Now())
	select {
	case <-firstChecks.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for pending CI poll to start")
	}
	waitFor(t, "queued PR checking CI", func() bool {
		entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
		return err == nil && entry != nil && entry.Status == MergeQueueStatusCheckingCI
	})

	close(firstChecks.release)
	waitFor(t, "queued PR returns to awaiting checks after pending CI", func() bool {
		entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
		return err == nil && entry != nil && entry.Status == MergeQueueStatusAwaitingChecks
	})
	if got := deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}); got != 0 {
		t.Fatalf("merge count after pending CI = %d, want 0", got)
	}

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "queued PR merged after passing CI", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}) == 1
	})

	if got, want := deps.commands.countCalls("gh", checkArgs), 2; got != want {
		t.Fatalf("checks call count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}), 1; got != want {
		t.Fatalf("rebase call count = %d, want %d", got, want)
	}
}

func TestEnqueueSurvivesRestartUsingPersistedQueueState(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	firstCaptureTicker := newFakeTicker()
	firstPollTicker := newFakeTicker()
	secondCaptureTicker := newFakeTicker()
	secondPollTicker := newFakeTicker()
	deps.tickers.enqueue(firstCaptureTicker, firstPollTicker, secondCaptureTicker, secondPollTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)

	first := deps.newDaemon(t)
	ctx := context.Background()
	if err := first.Start(ctx); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}

	if err := first.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	task, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("task not stored in fake state")
	}
	task.PRNumber = 42
	deps.state.putTaskForTest(task)

	if _, err := first.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	if err := first.Stop(context.Background()); err != nil {
		t.Fatalf("first Stop() error = %v", err)
	}

	second := deps.newDaemon(t)
	if err := second.Start(ctx); err != nil {
		t.Fatalf("second Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = second.Stop(context.Background())
	})

	secondPollTicker.tick(deps.clock.Now())
	waitFor(t, "persisted queued rebase", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}) == 1
	})

	secondPollTicker.tick(deps.clock.Now())
	waitFor(t, "persisted queued merge", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}) == 1
	})
}

func TestEnqueueRejectsPRsThatAreNotTrackedByActiveAssignments(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if _, err := d.Enqueue(ctx, 42); err == nil {
		t.Fatal("Enqueue(42) succeeded, want error")
	} else if !strings.Contains(err.Error(), "active assignment") {
		t.Fatalf("Enqueue(42) error = %v, want active assignment error", err)
	}
}
