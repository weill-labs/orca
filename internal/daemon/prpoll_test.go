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
	home := t.TempDir()
	t.Setenv("HOME", home)

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	postmortemDir := filepath.Join(home, "sync", "postmortems")
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		for _, key := range keys {
			if key == "$postmortem" {
				writePostmortemLog(t, postmortemDir, "LAB-689", deps.clock.Now().Add(time.Minute))
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
	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		"$postmortem\n",
		"PR merged, wrap up.\n",
	})
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventPRMerged, EventTaskCompleted)
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
	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial conflict nudge", func() bool {
		return deps.amux.countKey("pane-1", conflictNudgePrompt) == 1 &&
			active.mergeableState() == "CONFLICTING"
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
		return deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "mergeable"}) == 3 &&
			active.mergeableState() == "MERGEABLE"
	})
	if got, want := deps.amux.countKey("pane-1", conflictNudgePrompt), 1; got != want {
		t.Fatalf("conflict nudge count after recovery = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "conflict nudge after recovery", func() bool {
		return deps.amux.countKey("pane-1", conflictNudgePrompt) == 2 &&
			active.mergeableState() == "CONFLICTING"
	})

	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", "mergeable"}), 4; got != want {
		t.Fatalf("mergeable poll count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 2; got != want {
		t.Fatalf("conflict nudge event count = %d, want %d", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		conflictNudgePrompt,
		conflictNudgePrompt,
	})
}

func TestEnqueueSerializesQueuedPRLandingsFIFO(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.pool.clones = []Clone{
		deps.pool.clone,
		{Name: "clone-02", Path: filepath.Join(t.TempDir(), "clone-02")},
	}
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker(), newFakeTicker(), newFakeTicker())
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

	first, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment(LAB-689) error = %v", err)
	}
	second, err := d.assignment("LAB-690")
	if err != nil {
		t.Fatalf("assignment(LAB-690) error = %v", err)
	}

	first.prNumber = 42
	first.task.PRNumber = 42
	second.prNumber = 43
	second.task.PRNumber = 43

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	firstChecks := deps.commands.block("gh", []string{"pr", "checks", "42", "--required", "--watch", "--fail-fast", "--interval", "10"})
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--required", "--watch", "--fail-fast", "--interval", "10"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "update-branch", "43", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "43", "--required", "--watch", "--fail-fast", "--interval", "10"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "43", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	select {
	case <-firstChecks.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first queued PR checks to start")
	}

	if _, err := d.Enqueue(ctx, 43); err != nil {
		t.Fatalf("Enqueue(43) error = %v", err)
	}

	if got := deps.commands.countCalls("gh", []string{"pr", "update-branch", "43", "--rebase"}); got != 0 {
		t.Fatalf("second queued PR started before first completed, update-branch calls = %d", got)
	}

	close(firstChecks.release)

	waitFor(t, "second queued PR merge", func() bool {
		return deps.commands.countCalls("gh", []string{"pr", "merge", "43", "--squash"}) == 1
	})

	if got, want := deps.commands.callsByName("gh"), []commandCall{
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "list", "--head", "LAB-690", "--state", "open", "--json", "number"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "update-branch", "42", "--rebase"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "checks", "42", "--required", "--watch", "--fail-fast", "--interval", "10"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "merge", "42", "--squash"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "update-branch", "43", "--rebase"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "checks", "43", "--required", "--watch", "--fail-fast", "--interval", "10"}},
		{Dir: "/tmp/project", Name: "gh", Args: []string{"pr", "merge", "43", "--squash"}},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("gh calls = %#v, want %#v", got, want)
	}
}

func TestEnqueueNotifiesWorkerWhenRebaseFailsAndAllowsRequeue(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
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

	active, err := d.assignment("LAB-689")
	if err != nil {
		t.Fatalf("assignment() error = %v", err)
	}
	active.prNumber = 42
	active.task.PRNumber = 42

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, errors.New("rebase conflict"))

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) error = %v", err)
	}

	conflictNotice := "Merge queue could not rebase PR #42 onto main. Resolve the conflicts, push an update, and re-run `orca enqueue 42` when ready.\n"
	waitFor(t, "merge queue conflict notice", func() bool {
		return deps.amux.countKey("pane-1", conflictNotice) == 1
	})

	if got := deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}); got != 0 {
		t.Fatalf("unexpected merge attempt after rebase failure: %d", got)
	}

	deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--required", "--watch", "--fail-fast", "--interval", "10"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)

	if _, err := d.Enqueue(ctx, 42); err != nil {
		t.Fatalf("Enqueue(42) after conflict error = %v", err)
	}

	waitFor(t, "merge queue re-enqueue merge", func() bool {
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
