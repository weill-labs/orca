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

func TestPRMergePollingSendsWrapUpAndCleansClone(t *testing.T) {
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		for _, key := range keys {
			if key == "$postmortem" {
				writePostmortemLog(t, deps.postmortemDir, "LAB-689", deps.clock.Now().Add(time.Minute))
			}
		}
	}
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

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
		"Implement daemon core\n",
		"PR merged, wrap up.",
		"\n",
		"$postmortem\n",
	})
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventPRMerged, EventWorkerPostmortem, EventTaskCompleted)
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
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

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

func TestPRMergePollingSkipsPostmortemTriggerAfterWrapUpError(t *testing.T) {
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

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
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", "$postmortem\n"), 0; got != want {
		t.Fatalf("postmortem prompt count = %d, want %d", got, want)
	}
	if got := deps.events.lastMessage(EventWorkerPostmortem); !strings.Contains(got, "skipped") {
		t.Fatalf("postmortem event message = %q, want skipped status", got)
	}
	deps.events.requireTypes(t, EventTaskCompletionFailed)
}

func TestPRDetectionSyncsPaneMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)

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

	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile": "codex",
		"branch":        "LAB-689",
		"issue":         "LAB-689",
		"pr":            "42",
		"task":          "LAB-689",
	})
}

func TestPRMergeablePollingNudgesWorkerOnConflictTransitions(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, `{"mergeable":"MERGEABLE"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)

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
	waitFor(t, "initial conflict nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", conflictNudgePrompt) == 1 &&
			worker.LastMergeableState == "CONFLICTING"
	})

	prTicker.tick(deps.clock.Now())
	waitFor(t, "conflicting state re-polled", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "mergeable"}) == 2
	})
	if got, want := deps.amux.countKey("pane-1", conflictNudgePrompt), 1; got != want {
		t.Fatalf("conflict nudge count after repeated conflicting poll = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 1; got != want {
		t.Fatalf("conflict nudge event count after repeated conflicting poll = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "mergeable state recovery", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "mergeable"}) == 3 &&
			worker.LastMergeableState == "MERGEABLE"
	})
	if got, want := deps.amux.countKey("pane-1", conflictNudgePrompt), 1; got != want {
		t.Fatalf("conflict nudge count after recovery = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "conflict nudge after recovery", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", conflictNudgePrompt) == 2 &&
			worker.LastMergeableState == "CONFLICTING"
	})

	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "mergeable"}), 4; got != want {
		t.Fatalf("mergeable poll count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 2; got != want {
		t.Fatalf("conflict nudge event count = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		conflictNudgePrompt,
		"\n",
		conflictNudgePrompt,
		"\n",
	})
}

func TestPRMergeablePollingRetriesConflictNudgeAfterWaitIdleFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, `{"mergeable":"CONFLICTING"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision"}, ``, nil)
	waitIdleCalls := 0
	deps.amux.waitIdleHook = func(_ string, _ time.Duration) {
		waitIdleCalls++
		if waitIdleCalls == 2 {
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

	prTicker.tick(deps.clock.Now())
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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "retried conflict nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", conflictNudgePrompt) == 2 &&
			deps.events.countType(EventWorkerNudgedConflict) == 1 &&
			worker.LastMergeableState == "CONFLICTING"
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		conflictNudgePrompt,
		conflictNudgePrompt,
		"\n",
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
}

func TestEnqueueSerializesQueuedPRLandingsFIFO(t *testing.T) {
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

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "update-branch", "43", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "43", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "43", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	if _, err := d.Enqueue(ctx, 43); err != nil {
		t.Fatalf("Enqueue(43) error = %v", err)
	}

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "first queued PR rebase", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}) == 1
	})

	if got := deps.commands.countCalls("gh", []string{"pr", "update-branch", "43", "--rebase"}); got != 0 {
		t.Fatalf("second queued PR started before first completed, update-branch calls = %d", got)
	}

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "first queued PR merge", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}) == 1
	})

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "second queued PR rebase", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "update-branch", "43", "--rebase"}) == 1
	})

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "second queued PR merge", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "43", "--squash"}) == 1
	})

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
		return deps.amux.countKey("pane-1", conflictNotice) == 1
	})
	if got, want := deps.amux.countKey("pane-1", "\n"), 1; got != want {
		t.Fatalf("merge queue enter count = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
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
