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

func TestAssignCreatesStableWorkerIdentityAndCloneGitIdentity(t *testing.T) {
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

	if err := d.Assign(ctx, "LAB-894", "Implement stable workers", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-894")
		return ok && task.Status == TaskStatusActive
	})

	task, ok := deps.state.task("LAB-894")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got, want := task.WorkerID, "worker-01"; got != want {
		t.Fatalf("task.WorkerID = %q, want %q", got, want)
	}
	if got, want := task.PaneID, "pane-1"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	if got, want := task.PaneName, "w-LAB-894"; got != want {
		t.Fatalf("task.PaneName = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker not stored in state")
	}
	if got, want := worker.WorkerID, "worker-01"; got != want {
		t.Fatalf("worker.WorkerID = %q, want %q", got, want)
	}
	if got, want := worker.PaneID, "pane-1"; got != want {
		t.Fatalf("worker.PaneID = %q, want %q", got, want)
	}
	if got, want := worker.PaneName, "w-LAB-894"; got != want {
		t.Fatalf("worker.PaneName = %q, want %q", got, want)
	}

	if got, want := len(deps.amux.spawnRequests), 1; got != want {
		t.Fatalf("len(spawnRequests) = %d, want %d", got, want)
	}
	if got, want := deps.amux.spawnRequests[0].Name, "w-LAB-894"; got != want {
		t.Fatalf("spawn.Name = %q, want %q", got, want)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.name", "Orca worker-01"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.email", "worker-01@orca.local"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "-B", "LAB-894"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantGit) {
		t.Fatalf("git calls = %#v, want %#v", got, wantGit)
	}
}

func TestAssignReusesIdleWorkerIdentity(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-01",
		PaneName:     "worker-01",
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
		CreatedAt:    deps.clock.Now().Add(-time.Hour),
		LastSeenAt:   deps.clock.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-895", "Reuse stable workers", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-895")
		return ok && task.Status == TaskStatusActive
	})

	task, ok := deps.state.task("LAB-895")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got, want := task.WorkerID, "worker-01"; got != want {
		t.Fatalf("task.WorkerID = %q, want %q", got, want)
	}
	if got, want := task.PaneName, "w-LAB-895"; got != want {
		t.Fatalf("task.PaneName = %q, want %q", got, want)
	}
	if got, want := deps.amux.spawnRequests[0].Name, "w-LAB-895"; got != want {
		t.Fatalf("spawn.Name = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker not stored in state")
	}
	if got, want := worker.PaneName, "w-LAB-895"; got != want {
		t.Fatalf("worker.PaneName = %q, want %q", got, want)
	}
}

func TestAssignResolvesPaneTitle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		issue       string
		title       string
		linearTitle string
		linearErr   error
		wantTask    string
		wantLookups []string
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

func TestAssignUsesGitHubIssueContextAndSkipsLinear(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.commands.queue("gh", []string{"issue", "view", "702", "--json", "title,body"}, `{"title":"Assign should accept GitHub issue numbers","body":"## Problem\n\nGitHub issues should be first-class assign targets."}`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "GH-702", "--state", "open", "--json", "number"}, `[]`, nil)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "#702", "Implement the GitHub issue flow", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "github-backed task registration", func() bool {
		task, ok := deps.state.task("GH-702")
		return ok && task.Status == TaskStatusActive
	})

	task, ok := deps.state.task("GH-702")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got, want := task.Branch, "GH-702"; got != want {
		t.Fatalf("task.Branch = %q, want %q", got, want)
	}

	deps.amux.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile":  "codex",
		"branch":         "GH-702",
		"task":           "Assign should accept GitHub issue numbers",
		"tracked_issues": `[{"id":"GH-702","status":"active"}]`,
	})
	deps.amux.requireSentKeys(t, "pane-1", []string{
		wrappedCodexPrompt("GH-702", strings.Join([]string{
			"GitHub issue GH-702",
			"Title: Assign should accept GitHub issue numbers",
			"",
			"Body:",
			"## Problem",
			"",
			"GitHub issues should be first-class assign targets.",
			"",
			"Task:",
			"Implement the GitHub issue flow",
		}, "\n")) + "\n",
	})
	if got := deps.issueTracker.lookups(); len(got) != 0 {
		t.Fatalf("issue title lookups = %#v, want none", got)
	}
	if got := deps.issueTracker.statuses(); len(got) != 0 {
		t.Fatalf("issue tracker statuses = %#v, want none", got)
	}
	if got, want := deps.commands.countCalls("gh", []string{"issue", "view", "702", "--json", "title,body"}), 1; got != want {
		t.Fatalf("gh issue view calls = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "GH-702", "--state", "open", "--json", "number"}), 1; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
}

func TestAssignFailsWhenGitHubIssueLookupFailsBeforePRLookupOrCloneAcquire(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.commands.queue("gh", []string{"issue", "view", "702", "--json", "title,body"}, ``, errors.New("gh: issue not found"))
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	err := d.Assign(ctx, "GH-702", "Implement the GitHub issue flow", "codex")
	if err == nil {
		t.Fatal("Assign() succeeded, want GitHub issue lookup error")
	}
	if !strings.Contains(err.Error(), "lookup GitHub issue GH-702") {
		t.Fatalf("Assign() error = %v, want GitHub issue lookup context", err)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "GH-702", "--state", "open", "--json", "number"}), 0; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
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
	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after rollback")
	}
	if got := worker.PaneID; got != "" {
		t.Fatalf("worker.PaneID = %q, want empty after rollback", got)
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
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.name", "Orca worker-01"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.email", "worker-01@orca.local"}},
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
		wrappedCodexPrompt("LAB-898", "Verify prompt delivery") + "\n",
		"\n",
	})
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Substring: "Working", Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout, Settle: defaultPromptSettleDuration},
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignRetriesCodexPromptDeliveryAfterPaneReturnsToShell(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	sleep := &sleepRecorder{}
	deps.sleep = sleep.Sleep
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.spawnPanes = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
	}
	deps.amux.captureSequence("pane-1", []string{"OpenAI Codex\n›"})
	deps.amux.captureSequence("pane-2", []string{"OpenAI Codex\n›"})
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content:        []string{"bash-5.2$", "codex exited after startup"},
		CurrentCommand: "bash",
	}})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1330", "Retry prompt delivery on a fresh pane", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-1330")
		return ok && task.Status == TaskStatusActive && task.PaneID == "pane-2"
	})

	task, ok := deps.state.task("LAB-1330")
	if !ok {
		t.Fatal("task missing after prompt-delivery retry")
	}
	if got, want := task.PaneID, "pane-2"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after prompt-delivery retry")
	}
	if got, want := worker.PaneID, "pane-2"; got != want {
		t.Fatalf("worker.PaneID = %q, want %q", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{wrappedCodexPrompt("LAB-1330", "Retry prompt delivery on a fresh pane") + "\n"})
	deps.amux.requireSentKeys(t, "pane-2", []string{wrappedCodexPrompt("LAB-1330", "Retry prompt delivery on a fresh pane") + "\n"})
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got, want := sleep.snapshot(), []time.Duration{2 * time.Second}; !equalDurations(got, want) {
		t.Fatalf("sleep calls = %#v, want %#v", got, want)
	}
}

func TestAssignRollsBackWhenPromptDeliveryCheckReturnsHardError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
	}
	deps.amux.capturePaneErr = errors.New("capture failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-899", "Fail prompt delivery classification", "codex"); err == nil {
		t.Fatal("Assign() succeeded, want error")
	} else if !strings.Contains(err.Error(), `send prompt: capture pane while waiting for "Working" after prompt: capture failed`) {
		t.Fatalf("Assign() error = %v, want prompt delivery capture failure", err)
	}

	if _, ok := deps.state.task("LAB-899"); ok {
		t.Fatal("task stored despite prompt delivery classification failure")
	}
	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after prompt delivery classification failure")
	}
	if got := worker.PaneID; got != "" {
		t.Fatalf("worker.PaneID = %q, want empty after rollback", got)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
}

func TestAssignRollsBackWhenCodexPromptNeverShowsWorking(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.spawnPanes = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
		{ID: "pane-3", Name: "worker-3"},
	}
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
		amuxapi.ErrWaitContentTimeout,
	}
	deps.amux.captureSequence("pane-1", []string{"OpenAI Codex\n›"})
	deps.amux.captureSequence("pane-2", []string{"OpenAI Codex\n›"})
	deps.amux.captureSequence("pane-3", []string{"OpenAI Codex\n›"})
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content:        []string{"bash-5.2$", "codex exited on first attempt"},
		CurrentCommand: "bash",
	}})
	deps.amux.capturePaneSequence("pane-2", []PaneCapture{{
		Content:        []string{"bash-5.2$", "codex exited on second attempt"},
		CurrentCommand: "bash",
	}})
	deps.amux.capturePaneSequence("pane-3", []PaneCapture{{
		Content:        []string{"bash-5.2$", "codex exited on third attempt"},
		CurrentCommand: "bash",
	}})
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
	} else if !strings.Contains(err.Error(), "prompt delivery failed after 3 attempts") {
		t.Fatalf("Assign() error = %v, want prompt retry exhaustion", err)
	}

	if _, ok := deps.state.task("LAB-898"); ok {
		t.Fatal("task stored despite prompt delivery rollback")
	}
	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after prompt delivery rollback")
	}
	if got := worker.PaneID; got != "" {
		t.Fatalf("worker.PaneID = %q, want empty after rollback", got)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-898",
		AssignedTask:  "LAB-898",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1", "pane-2", "pane-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: "Working", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-2", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-2", Substring: "Working", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-3", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-3", Substring: "Working", Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got := deps.amux.countKey("pane-1", "\n") + deps.amux.countKey("pane-2", "\n") + deps.amux.countKey("pane-3", "\n"); got != 0 {
		t.Fatalf("retry enter count = %d, want 0", got)
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
	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after issue status rollback")
	}
	if got := worker.PaneID; got != "" {
		t.Fatalf("worker.PaneID = %q, want empty after rollback", got)
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

func waitContentTimeouts(count int) []error {
	timeouts := make([]error, count)
	for i := range timeouts {
		timeouts[i] = amuxapi.ErrWaitContentTimeout
	}
	return timeouts
}

func TestAssignRejectsIssueAlreadyActiveInStateBeforeCloneAcquire(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-689",
		Status:       TaskStatusActive,
		WorkerID:     "worker-01",
		PaneID:       "pane-existing",
		PaneName:     "worker-01",
		ClonePath:    "/tmp/existing-clone",
		Branch:       "LAB-689",
		AgentProfile: "codex",
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-01",
		PaneID:       "pane-existing",
		PaneName:     "worker-01",
		Issue:        "LAB-689",
		ClonePath:    "/tmp/existing-clone",
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
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

func TestAssignAllowsPromptReferencingAssignedIssueFromBacklog(t *testing.T) {
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

	if err := d.Assign(ctx, "LAB-689", "Address LAB-689 from the backlog and start working.", "codex"); err != nil {
		t.Fatalf("Assign() error = %v, want success", err)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}), 1; got != want {
		t.Fatalf("gh pr list calls = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 1; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
}

func TestAssignLogsFailureWhenCloneAcquireFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.pool.acquired = map[string]bool{deps.pool.clone.Path: true}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex")
	if err == nil {
		t.Fatal("Assign() succeeded, want clone acquire error")
	}
	if !strings.Contains(err.Error(), "acquire clone") {
		t.Fatalf("Assign() error = %v, want clone acquire context", err)
	}
	if got, want := deps.events.countType(EventTaskAssignFailed), 1; got != want {
		t.Fatalf("assign failure events = %d, want %d", got, want)
	}
	if message := deps.events.lastMessage(EventTaskAssignFailed); !strings.Contains(message, "acquire clone: clone already acquired") {
		t.Fatalf("assign failure message = %q, want clone acquire context", message)
	}
}

func TestAssignLogsFailureWhenSpawnFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.spawnErr = errors.New("spawn failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex")
	if err == nil {
		t.Fatal("Assign() succeeded, want spawn error")
	}
	if !strings.Contains(err.Error(), "spawn pane") {
		t.Fatalf("Assign() error = %v, want spawn context", err)
	}
	if got, want := deps.events.countType(EventTaskAssignFailed), 1; got != want {
		t.Fatalf("assign failure events = %d, want %d", got, want)
	}
	if message := deps.events.lastMessage(EventTaskAssignFailed); !strings.Contains(message, "spawn pane: spawn failed") {
		t.Fatalf("assign failure message = %q, want spawn context", message)
	}
}

func TestValidateAssignmentPrompt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		issue   string
		prompt  string
		wantErr bool
	}{
		{
			name:    "rejects autonomous backlog pickup",
			prompt:  "Pick up the next issue from the backlog and start working.",
			wantErr: true,
		},
		{
			name:    "allows assigned issue from backlog context",
			issue:   "LAB-689",
			prompt:  "Address LAB-689 from the backlog and start working.",
			wantErr: false,
		},
		{
			name:    "rejects different issue from backlog context",
			issue:   "LAB-689",
			prompt:  "Address LAB-690 from the backlog and start working.",
			wantErr: true,
		},
		{
			name:    "rejects generic next issue even when assigned issue is mentioned",
			issue:   "LAB-689",
			prompt:  "Address LAB-689, then pick up the next issue from the backlog.",
			wantErr: true,
		},
		{
			name:    "matches assigned issue exactly instead of by substring",
			issue:   "LAB-68",
			prompt:  "Address LAB-689 from the backlog and start working.",
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
			name:    "allows github issue context alongside assigned linear issue",
			issue:   "LAB-689",
			prompt:  "Address LAB-689 from the backlog; see context in #702.",
			wantErr: false,
		},
		{
			name:    "allows github issue identifier context alongside assigned linear issue",
			issue:   "LAB-689",
			prompt:  "Address LAB-689 from the backlog; see gh-702 for context.",
			wantErr: false,
		},
		{
			name:    "allows assigned github issue alias from backlog context",
			issue:   "GH-702",
			prompt:  "Address #702 from the backlog and start working.",
			wantErr: false,
		},
		{
			name:    "allows mixed case github identifier from backlog context",
			issue:   "GH-702",
			prompt:  "Address gh-702 from the backlog and start working.",
			wantErr: false,
		},
		{
			name:    "rejects different github issue alias from backlog context",
			issue:   "GH-702",
			prompt:  "Address #703 from the backlog and start working.",
			wantErr: true,
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

			err := validateAssignmentPrompt(tt.issue, tt.prompt)
			if tt.wantErr && err == nil {
				t.Fatalf("validateAssignmentPrompt(%q, %q) error = nil, want error", tt.issue, tt.prompt)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("validateAssignmentPrompt(%q, %q) error = %v, want nil", tt.issue, tt.prompt, err)
			}
		})
	}
}

func TestWithGitHubIssueContextOmitsEmptyBody(t *testing.T) {
	t.Parallel()

	got := withGitHubIssueContext("GH-702", gitHubIssueDetails{
		Title: "Assign should accept GitHub issue numbers",
	}, "Implement the GitHub issue flow")

	if strings.Contains(got, "\nBody:\n") {
		t.Fatalf("withGitHubIssueContext() = %q, want no body section", got)
	}
	want := strings.Join([]string{
		"GitHub issue GH-702",
		"Title: Assign should accept GitHub issue numbers",
		"",
		"Task:",
		"Implement the GitHub issue flow",
	}, "\n")
	if got != want {
		t.Fatalf("withGitHubIssueContext() = %q, want %q", got, want)
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

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not stored in fake state")
	}
	if got, want := worker.LastPushAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("worker.LastPushAt = %v, want %v", got, want)
	}
	if got, want := worker.LastPRNumber, 42; got != want {
		t.Fatalf("worker.LastPRNumber = %d, want %d", got, want)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"fetch", "origin"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.name", "Orca worker-01"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.email", "worker-01@orca.local"}},
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

func TestAssignClearsClaimedWorkerStalePaneRefBeforeSpawn(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	now := deps.clock.Now()
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-01",
		PaneID:       "pane-stale",
		PaneName:     "w-LAB-old",
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
		CreatedAt:    now.Add(-time.Hour),
		LastSeenAt:   now.Add(-time.Minute),
		UpdatedAt:    now.Add(-time.Minute),
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	deps.amux.paneExists = map[string]bool{"pane-stale": false}

	var spawnWorker Worker
	deps.amux.spawnHook = func(SpawnRequest) {
		worker, ok := deps.state.worker("worker-01")
		if !ok {
			t.Fatal("claimed worker missing during spawn")
		}
		spawnWorker = worker
	}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-988", "Fix pane reconciliation", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	if got := spawnWorker.PaneID; got != "" {
		t.Fatalf("worker.PaneID during spawn = %q, want empty", got)
	}
	if got, want := spawnWorker.PaneName, "worker-01"; got != want {
		t.Fatalf("worker.PaneName during spawn = %q, want %q", got, want)
	}
	if got, want := deps.amux.paneExistsCalls[0], "pane-stale"; got != want {
		t.Fatalf("first pane exists call = %q, want %q", got, want)
	}
}
