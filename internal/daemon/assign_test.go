package daemon

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func TestAssignEnforcesCodexYoloFlag(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex",
		PostmortemEnabled: true,
		StuckTimeout:      5 * time.Minute,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   3,
	}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-696", "Implement lifecycle enforcement", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "spawn request", func() bool {
		return len(deps.amux.spawnRequests) == 1
	})

	if got, want := deps.amux.spawnRequests[0].Command, "codex --yolo"; got != want {
		t.Fatalf("spawn.Command = %q, want %q", got, want)
	}
}

func TestAssignResolvesPaneTitle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		issue        string
		title        string
		linearTitle  string
		linearErr    error
		wantTask     string
		wantLookups  []string
	}{
		{
			name:        "uses provided title as-is",
			issue:       "LAB-844",
			title:       "Keep the explicit title",
			linearTitle: "Ignored Linear title",
			wantTask:    "Keep the explicit title",
		},
		{
			name:        "uses Linear title when omitted",
			issue:       "LAB-844",
			linearTitle: "Default pane title to Linear issue title when --title is omitted",
			wantTask:    "Default pane title to Linear issue title when --title is omitted",
			wantLookups: []string{"LAB-844"},
		},
		{
			name:        "falls back to issue when Linear lookup fails",
			issue:       "LAB-844",
			linearErr:   errors.New("linear unavailable"),
			wantTask:    "LAB-844",
			wantLookups: []string{"LAB-844"},
		},
		{
			name:     "falls back to issue when identifier is not Linear",
			issue:    "fix-resume-sequence",
			wantTask: "fix-resume-sequence",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
			deps.issueTracker.titles = map[string]string{tt.issue: tt.linearTitle}
			deps.issueTracker.titleErrors = map[string]error{tt.issue: tt.linearErr}
			d := deps.newDaemon(t)
			ctx := context.Background()

			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			var err error
			if tt.title == "" {
				err = d.Assign(ctx, tt.issue, "Implement daemon core", "codex")
			} else {
				err = d.Assign(ctx, tt.issue, "Implement daemon core", "codex", tt.title)
			}
			if err != nil {
				t.Fatalf("Assign() error = %v", err)
			}

			waitFor(t, "task registration", func() bool {
				task, ok := deps.state.task(tt.issue)
				return ok && task.Status == TaskStatusActive
			})

			deps.amux.requireMetadata(t, "pane-1", map[string]string{
				"agent_profile":  "codex",
				"branch":         tt.issue,
				"task":           tt.wantTask,
				"tracked_issues": `[{"id":"` + tt.issue + `","status":"active"}]`,
			})
			if got := deps.issueTracker.lookups(); !slices.Equal(got, tt.wantLookups) {
				t.Fatalf("issue title lookups = %#v, want %#v", got, tt.wantLookups)
			}
		})
	}
}

func TestAssignRollsBackOnPromptSendFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.sendKeysErr = errors.New("send failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err == nil {
		t.Fatal("Assign() succeeded, want error")
	}

	if _, ok := deps.state.task("LAB-689"); ok {
		t.Fatal("task stored despite rollback")
	}
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker stored despite rollback")
	}

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-689",
		AssignedTask:  "LAB-689",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}

	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "-B", "LAB-689"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantGit) {
		t.Fatalf("git calls = %#v, want %#v", got, wantGit)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssignFailed)
}

func TestAssignRetriesCodexPromptUntilWorkingAppears(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		nil,
	}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-898", "Verify prompt delivery", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-898")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Verify prompt delivery",
		"Enter",
		"Enter",
		"Enter",
	})
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout, Settle: defaultPromptSettleDuration},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignRollsBackWhenCodexPromptNeverShowsWorking(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
	}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-898", "Verify prompt delivery", "codex"); err == nil {
		t.Fatal("Assign() succeeded, want error")
	} else if !strings.Contains(err.Error(), "send prompt") {
		t.Fatalf("Assign() error = %v, want send prompt context", err)
	}

	if _, ok := deps.state.task("LAB-898"); ok {
		t.Fatal("task stored despite prompt delivery rollback")
	}
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker stored despite prompt delivery rollback")
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-898",
		AssignedTask:  "LAB-898",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.countKey("pane-1", "\n"), 10; got != want {
		t.Fatalf("retry enter count = %d, want %d", got, want)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssignFailed)
}

func TestAssignRollsBackOnIssueStatusFailureAfterPersistingStartingState(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.issueTracker.errors = map[string]error{
		IssueStateInProgress: errors.New("linear unavailable"),
	}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-743", "Implement restart recovery", "codex"); err == nil {
		t.Fatal("Assign() succeeded, want error")
	} else if !strings.Contains(err.Error(), "set issue status") {
		t.Fatalf("Assign() error = %v, want issue status context", err)
	}

	if _, ok := deps.state.task("LAB-743"); ok {
		t.Fatal("task stored despite issue status rollback")
	}
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker stored despite issue status rollback")
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-743",
		AssignedTask:  "LAB-743",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssignFailed)
}

func TestAssignRejectsConcurrentDuplicateIssueBeforeCloneAcquire(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker(), newFakeTicker(), newFakeTicker())
	deps.pool.acquireStarted = make(chan struct{}, 1)
	deps.pool.acquireRelease = make(chan struct{})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	firstErr := make(chan error, 1)
	go func() {
		firstErr <- d.Assign(ctx, "LAB-689", "Implement daemon core", "codex")
	}()

	select {
	case <-deps.pool.acquireStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first clone acquisition")
	}

	secondErr := d.Assign(ctx, "LAB-689", "Implement daemon core again", "codex")
	if secondErr == nil {
		t.Fatal("second Assign() succeeded, want duplicate assignment error")
	}
	if !strings.Contains(secondErr.Error(), "already assigned") {
		t.Fatalf("second Assign() error = %v, want duplicate assignment error", secondErr)
	}
	if got, want := deps.pool.acquireCallCount(), 1; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}

	close(deps.pool.acquireRelease)
	if err := <-firstErr; err != nil {
		t.Fatalf("first Assign() error = %v", err)
	}
}

func TestAssignRejectsIssueAlreadyActiveInStateBeforeCloneAcquire(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()
	deps.state.tasks["LAB-689"] = Task{
		Project:      "/tmp/project",
		Issue:        "LAB-689",
		Status:       TaskStatusActive,
		PaneID:       "pane-existing",
		ClonePath:    "/tmp/existing-clone",
		Branch:       "LAB-689",
		AgentProfile: "codex",
	}
	deps.state.workers["pane-existing"] = Worker{
		Project:      "/tmp/project",
		PaneID:       "pane-existing",
		Issue:        "LAB-689",
		ClonePath:    "/tmp/existing-clone",
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex")
	if err == nil {
		t.Fatal("Assign() succeeded, want duplicate assignment error")
	}
	if !strings.Contains(err.Error(), "already assigned") {
		t.Fatalf("Assign() error = %v, want duplicate assignment error", err)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 0; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
}

func TestAssignRejectsAutonomousBacklogPickingPromptBeforePRLookupOrCloneAcquire(t *testing.T) {
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

	err := d.Assign(ctx, "LAB-689", "Pick up the next issue from the backlog and start working.", "codex")
	if err == nil {
		t.Fatal("Assign() succeeded, want prompt validation error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "backlog") {
		t.Fatalf("Assign() error = %v, want backlog-picking context", err)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 0; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
}

func TestValidateAssignmentPrompt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prompt  string
		wantErr bool
	}{
		{
			name:    "rejects autonomous backlog pickup",
			prompt:  "Pick up the next issue from the backlog and start working.",
			wantErr: true,
		},
		{
			name:    "rejects standalone new work phrasing",
			prompt:  "After you land the fix, pick up new work from the queue.",
			wantErr: true,
		},
		{
			name:    "allows mentioning the linear issue as context",
			prompt:  "Follow up from CHANGES_REQUESTED. See the Linear issue for context.",
			wantErr: false,
		},
		{
			name:    "allows mentioning the new worker",
			prompt:  "Fix the new worker startup flow so it resumes cleanly after restart.",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateAssignmentPrompt(tt.prompt)
			if tt.wantErr && err == nil {
				t.Fatalf("validateAssignmentPrompt(%q) error = nil, want error", tt.prompt)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validateAssignmentPrompt(%q) error = %v, want nil", tt.prompt, err)
			}
		})
	}
}

func TestAssignAdoptsOpenPRAndPrepopulatesTask(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[{"number":42}]`, nil)
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

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusActive
	})

	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 1; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 1; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 1; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}

	task, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("task not stored in fake state")
	}
	if got, want := task.Branch, "LAB-689"; got != want {
		t.Fatalf("task.Branch = %q, want %q", got, want)
	}
	if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"fetch", "origin"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "-B", "LAB-689", "origin/LAB-689"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantGit) {
		t.Fatalf("git calls = %#v, want %#v", got, wantGit)
	}

	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"task":           "LAB-689",
		"tracked_issues": `[{"id":"LAB-689","status":"active"}]`,
		"tracked_prs":    `[{"number":42,"status":"active"}]`,
	})
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-689", State: IssueStateInProgress},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}

	if event, ok := deps.events.lastEventOfType(EventTaskAssigned); !ok {
		t.Fatal("missing task assigned event")
	} else if got, want := event.PRNumber, 42; got != want {
		t.Fatalf("assigned event PRNumber = %d, want %d", got, want)
	}
}

func TestAssignAllowsReassigningInactiveStoredIssueWhenNoOpenPRExists(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.state.tasks["LAB-689"] = Task{
		Project: "/tmp/project",
		Issue:   "LAB-689",
		Status:  TaskStatusCancelled,
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

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusActive
	})
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 1; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
}

func TestAssignPreservesTrackedHistoryForReusedPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
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

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-689",
		"task":           "LAB-689",
		"tracked_issues": `[{"id":"LAB-688","status":"completed"},{"id":"LAB-689","status":"active"}]`,
		"tracked_prs":    `[{"number":41,"status":"completed"}]`,
	})
}
