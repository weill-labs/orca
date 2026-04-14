package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func TestAssignConfirmsCodexTrustPromptBeforeSendingPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitContentResults = []error{
		nil,
		amuxapi.ErrWaitContentTimeout,
	}
	deps.amux.captureSequence("pane-1", []string{"OpenAI Codex\n›"})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-720", "Implement handshake", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-720")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{"Enter", wrappedCodexPrompt("Implement handshake"), "Enter"})
	if got, want := deps.amux.captureCount("pane-1"), 1; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignDoesNotBlindlyConfirmWhenTrustPromptNotPresent(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitContentErr = amuxapi.ErrWaitContentTimeout
	deps.amux.captureSequence("pane-1", []string{"OpenAI Codex\n›"})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-720", "Implement handshake", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-720")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{wrappedCodexPrompt("Implement handshake") + "\n"})
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignResumesCodexBeforeSendingPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		nil,
		amuxapi.ErrWaitContentTimeout,
	}
	deps.amux.captureSequence("pane-1", []string{
		"Resume your previous session",
		"OpenAI Codex\n›",
	})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-720", "Implement resume flow", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-720")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"codex --yolo resume\n",
		".",
		wrappedCodexPrompt("Implement resume flow") + "\n",
	})
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: "›", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout, Settle: 2 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignRollsBackOnAgentHandshakeFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitIdleErr = errors.New("wait idle failed")
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-720", "Implement handshake", "codex"); err == nil {
		t.Fatal("Assign() succeeded, want error")
	} else if !strings.Contains(err.Error(), "agent handshake") {
		t.Fatalf("Assign() error = %v, want handshake context", err)
	}

	if _, ok := deps.state.task("LAB-720"); ok {
		t.Fatal("task stored despite handshake rollback")
	}
	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after handshake rollback")
	}
	if got := worker.PaneID; got != "" {
		t.Fatalf("worker.PaneID = %q, want empty after rollback", got)
	}

	deps.amux.requireSentKeys(t, "pane-1", nil)
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if exists, err := deps.amux.PaneExists(ctx, "pane-1"); err != nil {
		t.Fatalf("PaneExists() error = %v", err)
	} else if exists {
		t.Fatal("PaneExists() = true, want pane removed after rollback")
	}
	if panes, err := deps.amux.ListPanes(ctx); err != nil {
		t.Fatalf("ListPanes() error = %v", err)
	} else if len(panes) != 0 {
		t.Fatalf("ListPanes() = %#v, want no panes after rollback", panes)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.name", "Orca worker-01"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"config", "user.email", "worker-01@orca.local"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "-B", "LAB-720"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantGit) {
		t.Fatalf("git calls = %#v, want %#v", got, wantGit)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssignFailed)
}

func TestAssignWaitsForCodexReadyPatternBeforeSendingPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		nil,
	}
	deps.amux.captureSequence("pane-1", []string{"bash-5.2$"})
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		{Content: []string{"OpenAI Codex", codexReadyPattern}, CurrentCommand: "codex"},
	})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1191", "Wait for codex readiness", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-1191")
		return ok && task.Status == TaskStatusActive
	})

	deps.amux.requireSentKeys(t, "pane-1", []string{wrappedCodexPrompt("Wait for codex readiness") + "\n"})
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: codexReadyPattern, Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout, Settle: defaultPromptSettleDuration},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.captureCount("pane-1"), 2; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
}

func TestAssignRetriesCodexStartupAfterBareBashHandshakeFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	sleep := &sleepRecorder{}
	deps.sleep = sleep.Sleep
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.disableAutomaticReadyCapture = true
	deps.amux.spawnPanes = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		nil,
		amuxapi.ErrWaitContentTimeout,
	}
	deps.amux.captureSequence("pane-1", []string{"bash-5.2$"})
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content:        []string{"bash-5.2$", "codex exited: missing API key"},
		CurrentCommand: "bash",
	}})
	deps.amux.captureSequence("pane-2", []string{"OpenAI Codex\n›"})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1209", "Retry codex startup during assign", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-1209")
		return ok && task.Status == TaskStatusActive && task.PaneID == "pane-2"
	})

	task, ok := deps.state.task("LAB-1209")
	if !ok {
		t.Fatal("task missing after retry success")
	}
	if got, want := task.PaneID, "pane-2"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after retry success")
	}
	if got, want := worker.PaneID, "pane-2"; got != want {
		t.Fatalf("worker.PaneID = %q, want %q", got, want)
	}
	if got, want := worker.LastCapture, defaultCodexReadyOutput(); got != want {
		t.Fatalf("worker.LastCapture = %q, want %q", got, want)
	}

	deps.amux.requireSentKeys(t, "pane-1", nil)
	deps.amux.requireSentKeys(t, "pane-2", []string{wrappedCodexPrompt("Retry codex startup during assign") + "\n"})
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}
	if got, want := sleep.snapshot(), []time.Duration{assignHandshakeRetryDelay}; !equalDurations(got, want) {
		t.Fatalf("sleep calls = %#v, want %#v", got, want)
	}

	got := deps.events.eventsByType(EventWorkerHandshakeRetry)
	if len(got) != 1 {
		t.Fatalf("handshake retry events = %d, want 1", len(got))
	}
	if got[0].Retry != 1 {
		t.Fatalf("retry event Retry = %d, want 1", got[0].Retry)
	}
	if !strings.Contains(got[0].Message, "attempt 1/3") || !strings.Contains(got[0].Message, "retrying in 2s") {
		t.Fatalf("retry event message = %q, want attempt/retry context", got[0].Message)
	}
	if got[0].PaneID != "pane-1" {
		t.Fatalf("retry event PaneID = %q, want %q", got[0].PaneID, "pane-1")
	}
	if want := []string{"bash-5.2$", "codex exited: missing API key"}; !reflect.DeepEqual(got[0].Scrollback, want) {
		t.Fatalf("retry event scrollback = %#v, want %#v", got[0].Scrollback, want)
	}
}

func TestAssignRollsBackWhenCodexReadyPatternNeverAppearsAfterIdle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	sleep := &sleepRecorder{}
	deps.sleep = sleep.Sleep
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.spawnPanes = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
		{ID: "pane-3", Name: "worker-3"},
	}
	deps.amux.disableAutomaticReadyCapture = true
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		nil,
		amuxapi.ErrWaitContentTimeout,
		nil,
		amuxapi.ErrWaitContentTimeout,
		nil,
	}
	deps.amux.captureSequence("pane-1", []string{"bash-5.2$"})
	deps.amux.captureSequence("pane-2", []string{"bash-5.2$"})
	deps.amux.captureSequence("pane-3", []string{"bash-5.2$"})
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

	if err := d.Assign(ctx, "LAB-1191", "Wait for codex readiness", "codex"); err == nil {
		t.Fatal("Assign() succeeded, want error")
	} else if !strings.Contains(err.Error(), "agent handshake failed after 3 attempts") {
		t.Fatalf("Assign() error = %v, want retry exhaustion", err)
	}

	if _, ok := deps.state.task("LAB-1191"); ok {
		t.Fatal("task stored despite handshake rollback")
	}
	worker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after handshake rollback")
	}
	if got := worker.PaneID; got != "" {
		t.Fatalf("worker.PaneID = %q, want empty after rollback", got)
	}

	deps.amux.requireSentKeys(t, "pane-1", nil)
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-1", Substring: codexReadyPattern, Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-2", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-2", Substring: codexReadyPattern, Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-3", Substring: "do you trust", Timeout: defaultTrustPromptTimeout},
		{PaneID: "pane-3", Substring: codexReadyPattern, Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-2", Timeout: defaultAgentHandshakeTimeout},
		{PaneID: "pane-3", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got, want := sleep.snapshot(), []time.Duration{assignHandshakeRetryDelay, assignHandshakeRetryDelay}; !equalDurations(got, want) {
		t.Fatalf("sleep calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.killCalls, []string{"pane-1", "pane-2", "pane-3"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}

	got := deps.events.eventsByType(EventWorkerHandshakeRetry)
	if len(got) != 3 {
		t.Fatalf("handshake retry events = %d, want 3", len(got))
	}
	for i, wantRetry := range []int{1, 2, 3} {
		if got[i].Retry != wantRetry {
			t.Fatalf("retry event %d Retry = %d, want %d", i, got[i].Retry, wantRetry)
		}
	}
	if !strings.Contains(got[2].Message, "no retries remaining") {
		t.Fatalf("final retry event message = %q, want no-retries context", got[2].Message)
	}
}

func TestResumeAgentInPaneReturnsNilWithoutResumeSequence(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	err := d.resumeAgentInPane(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err != nil {
		t.Fatalf("resumeAgentInPane() error = %v", err)
	}

	deps.amux.requireSentKeys(t, "pane-1", nil)
	if got, want := len(deps.amux.waitContentCalls), 0; got != want {
		t.Fatalf("waitContent calls = %d, want %d", got, want)
	}
}

func TestResumeAgentInPaneFallsBackToDirectSendForNonCodex(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	profile := AgentProfile{
		Name:           "claude",
		ResumeSequence: []string{"claude --resume", "Enter"},
	}

	err := d.resumeAgentInPane(context.Background(), "pane-1", profile)
	if err != nil {
		t.Fatalf("resumeAgentInPane() error = %v", err)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{"claude --resume\n"})
	if got, want := len(deps.amux.waitContentCalls), 0; got != want {
		t.Fatalf("waitContent calls = %d, want %d", got, want)
	}
}

func TestResumeAgentInPaneReturnsInitialSendError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.sendKeysResults = []error{errors.New("send failed")}
	d := deps.newDaemon(t)
	profile := AgentProfile{
		Name:           "codex",
		ResumeSequence: []string{"codex --yolo resume", "Enter", "."},
	}

	err := d.resumeAgentInPane(context.Background(), "pane-1", profile)
	if err == nil || !strings.Contains(err.Error(), "send failed") {
		t.Fatalf("resumeAgentInPane() error = %v, want send failure", err)
	}

	if got, want := len(deps.amux.waitContentCalls), 0; got != want {
		t.Fatalf("waitContent calls = %d, want %d", got, want)
	}
}

func TestResumeAgentInPaneReturnsWaitContentError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentErr = errors.New("prompt not ready")
	d := deps.newDaemon(t)
	profile := AgentProfile{
		Name:           "codex",
		ResumeSequence: []string{"codex --yolo resume", "Enter", "."},
	}

	err := d.resumeAgentInPane(context.Background(), "pane-1", profile)
	if err == nil || !strings.Contains(err.Error(), "prompt not ready") {
		t.Fatalf("resumeAgentInPane() error = %v, want wait content failure", err)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{"codex --yolo resume\n"})
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: "›", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
}

func TestConfirmTrustPromptIfPresentIgnoresProfilesWithoutTrustPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	confirmed, err := d.confirmTrustPromptIfPresent(context.Background(), "pane-1", AgentProfile{Name: "claude"})
	if err != nil {
		t.Fatalf("confirmTrustPromptIfPresent() error = %v", err)
	}
	if confirmed {
		t.Fatal("confirmTrustPromptIfPresent() = true, want false")
	}
	if got, want := len(deps.amux.waitContentCalls), 0; got != want {
		t.Fatalf("waitContent calls = %d, want %d", got, want)
	}
}

func TestConfirmTrustPromptIfPresentReturnsWaitContentError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentErr = errors.New("wait failed")
	d := deps.newDaemon(t)

	confirmed, err := d.confirmTrustPromptIfPresent(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if confirmed {
		t.Fatal("confirmTrustPromptIfPresent() = true, want false")
	}
	if err == nil || !strings.Contains(err.Error(), "wait for trust prompt: wait failed") {
		t.Fatalf("confirmTrustPromptIfPresent() error = %v, want wrapped wait-content failure", err)
	}
}

func TestConfirmTrustPromptIfPresentReturnsSendKeysError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentResults = []error{nil}
	deps.amux.sendKeysErr = errors.New("send failed")
	d := deps.newDaemon(t)

	confirmed, err := d.confirmTrustPromptIfPresent(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if confirmed {
		t.Fatal("confirmTrustPromptIfPresent() = true, want false")
	}
	if err == nil || !strings.Contains(err.Error(), "confirm trust prompt: send failed") {
		t.Fatalf("confirmTrustPromptIfPresent() error = %v, want wrapped send failure", err)
	}
}

func TestConfirmTrustPromptIfPresentReturnsWaitIdleErrorAfterConfirmation(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentResults = []error{nil}
	deps.amux.waitIdleErr = errors.New("idle failed")
	d := deps.newDaemon(t)

	confirmed, err := d.confirmTrustPromptIfPresent(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if confirmed {
		t.Fatal("confirmTrustPromptIfPresent() = true, want false")
	}
	if err == nil || !strings.Contains(err.Error(), "wait for post-startup action idle: idle failed") {
		t.Fatalf("confirmTrustPromptIfPresent() error = %v, want wrapped idle failure", err)
	}
}

func TestEmitHandshakeEventUsesWorkerMetadataBeforeTaskFallback(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := deps.state.PutWorker(ctx, Worker{
		Project:      d.project,
		PaneID:       "pane-1",
		PaneName:     "worker-pane",
		Issue:        "LAB-WORKER",
		AgentProfile: "codex",
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	if err := deps.state.PutTask(ctx, Task{
		Project:      d.project,
		Issue:        "LAB-TASK",
		PaneID:       "pane-1",
		PaneName:     "task-pane",
		CloneName:    "clone-01",
		ClonePath:    "/tmp/clone-01",
		Branch:       "LAB-TASK",
		AgentProfile: "aider",
		CreatedAt:    deps.clock.Now(),
		UpdatedAt:    deps.clock.Now(),
	}); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}

	d.emitHandshakeEvent(ctx, "pane-1", AgentProfile{}, handshakeStepWait)

	got := deps.events.eventsByType(EventWorkerHandshake)
	if len(got) != 1 {
		t.Fatalf("handshake events = %d, want 1", len(got))
	}
	event := got[0]
	if event.Issue != "LAB-WORKER" {
		t.Fatalf("event.Issue = %q, want %q", event.Issue, "LAB-WORKER")
	}
	if event.PaneName != "worker-pane" {
		t.Fatalf("event.PaneName = %q, want %q", event.PaneName, "worker-pane")
	}
	if event.AgentProfile != "codex" {
		t.Fatalf("event.AgentProfile = %q, want %q", event.AgentProfile, "codex")
	}
	if event.CloneName != "clone-01" || event.ClonePath != "/tmp/clone-01" || event.Branch != "LAB-TASK" {
		t.Fatalf("event clone metadata = %#v, want task clone metadata", event)
	}
}

func TestEmitHandshakeEventFallsBackToTaskMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := deps.state.PutTask(ctx, Task{
		Project:      d.project,
		Issue:        "LAB-TASK",
		PaneID:       "pane-1",
		PaneName:     "task-pane",
		CloneName:    "clone-01",
		ClonePath:    "/tmp/clone-01",
		Branch:       "LAB-TASK",
		AgentProfile: "codex",
		CreatedAt:    deps.clock.Now(),
		UpdatedAt:    deps.clock.Now(),
	}); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}

	d.emitHandshakeEvent(ctx, "pane-1", AgentProfile{}, handshakeStepWaitTrustContent)

	got := deps.events.eventsByType(EventWorkerHandshake)
	if len(got) != 1 {
		t.Fatalf("handshake events = %d, want 1", len(got))
	}
	event := got[0]
	if event.Issue != "LAB-TASK" {
		t.Fatalf("event.Issue = %q, want %q", event.Issue, "LAB-TASK")
	}
	if event.PaneName != "task-pane" {
		t.Fatalf("event.PaneName = %q, want %q", event.PaneName, "task-pane")
	}
	if event.AgentProfile != "codex" {
		t.Fatalf("event.AgentProfile = %q, want %q", event.AgentProfile, "codex")
	}
	if event.CloneName != "clone-01" || event.ClonePath != "/tmp/clone-01" || event.Branch != "LAB-TASK" {
		t.Fatalf("event clone metadata = %#v, want task clone metadata", event)
	}
}

func TestHandshakePromptDetectionHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		profile    AgentProfile
		output     string
		wantTrust  bool
		wantResume bool
		wantReady  bool
	}{
		{
			name:       "codex detects trust prompt and resume prompt",
			profile:    AgentProfile{Name: "codex", ResumeSequence: []string{"codex --yolo resume", "Enter", "."}},
			output:     "Do you trust this folder? Resume your previous session ›",
			wantTrust:  true,
			wantResume: true,
			wantReady:  true,
		},
		{
			name:       "codex without resume sequence ignores resume prompt",
			profile:    AgentProfile{Name: "codex"},
			output:     "Resume your previous session",
			wantTrust:  false,
			wantResume: false,
			wantReady:  false,
		},
		{
			name:       "non codex ignores both prompts",
			profile:    AgentProfile{Name: "claude", ResumeSequence: []string{"claude --resume", "Enter"}},
			output:     "Do you trust this folder? Resume your previous session",
			wantTrust:  false,
			wantResume: false,
			wantReady:  false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := hasTrustPrompt(tt.profile, tt.output); got != tt.wantTrust {
				t.Fatalf("hasTrustPrompt() = %v, want %v", got, tt.wantTrust)
			}
			if got := hasResumePrompt(tt.profile, tt.output); got != tt.wantResume {
				t.Fatalf("hasResumePrompt() = %v, want %v", got, tt.wantResume)
			}
			if got := hasReadyPattern(tt.profile, tt.output); got != tt.wantReady {
				t.Fatalf("hasReadyPattern() = %v, want %v", got, tt.wantReady)
			}
		})
	}
}

func TestAssignEmitsCodexHandshakeDiagnostics(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.waitContentResults = []error{
		nil,
		amuxapi.ErrWaitContentTimeout,
	}
	deps.amux.captureSequence("pane-1", []string{"OpenAI Codex\n›"})
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-729", "Fix handshake diagnostics", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "handshake diagnostics", func() bool {
		return len(deps.events.eventsByType(EventWorkerHandshake)) == 8
	})

	got := deps.events.eventsByType(EventWorkerHandshake)
	wantMessages := []string{
		handshakeStepWait,
		handshakeStepWaitTrustContent,
		handshakeStepTrustDetected,
		handshakeStepTrustEnter,
		handshakeStepWait,
		handshakeStepWaitTrustContent,
		handshakeStepCapture,
		handshakeStepReadyValidated,
	}
	for i, event := range got {
		if event.Message != wantMessages[i] {
			t.Fatalf("event[%d].Message = %q, want %q", i, event.Message, wantMessages[i])
		}
		if gotIssue, wantIssue := event.Issue, "LAB-729"; gotIssue != wantIssue {
			t.Fatalf("event[%d].Issue = %q, want %q", i, gotIssue, wantIssue)
		}
		if gotPane, wantPane := event.PaneID, "pane-1"; gotPane != wantPane {
			t.Fatalf("event[%d].PaneID = %q, want %q", i, gotPane, wantPane)
		}
		if gotProfile, wantProfile := event.AgentProfile, "codex"; gotProfile != wantProfile {
			t.Fatalf("event[%d].AgentProfile = %q, want %q", i, gotProfile, wantProfile)
		}
	}
}

func TestAssignAllowsConcurrentTrustPromptWaitsAcrossPanes(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())

	secondClonePath := filepath.Join(t.TempDir(), "clone-02")
	if err := os.MkdirAll(secondClonePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", secondClonePath, err)
	}
	deps.pool.clones = []Clone{
		deps.pool.clone,
		{Name: "clone-02", Path: secondClonePath},
	}
	deps.amux.spawnPanes = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}
	deps.amux.captureSequence("pane-1", []string{"OpenAI Codex\n›"})
	deps.amux.captureSequence("pane-2", []string{"OpenAI Codex\n›"})
	firstWaitContentEntered := make(chan struct{})
	releaseFirstWaitContent := make(chan struct{})
	secondWaitContentEntered := make(chan struct{}, 1)
	var firstWaitContent sync.Once
	var secondWaitContent sync.Once
	deps.amux.waitContentHook = func(paneID, substring string, timeout time.Duration) {
		switch substring {
		case "do you trust":
			if timeout != defaultTrustPromptTimeout {
				t.Errorf("wait content timeout = %v, want %v", timeout, defaultTrustPromptTimeout)
			}
		case codexWorkingText:
			if timeout != defaultAgentHandshakeTimeout {
				t.Errorf("wait content timeout = %v, want %v", timeout, defaultAgentHandshakeTimeout)
			}
			return
		default:
			t.Errorf("wait content substring = %q, want trust prompt or %q", substring, codexWorkingText)
			return
		}
		switch paneID {
		case "pane-1":
			firstWaitContent.Do(func() {
				close(firstWaitContentEntered)
				<-releaseFirstWaitContent
			})
		case "pane-2":
			secondWaitContent.Do(func() {
				secondWaitContentEntered <- struct{}{}
			})
		}
	}

	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	firstErr := make(chan error, 1)
	secondErr := make(chan error, 1)
	go func() {
		firstErr <- d.Assign(ctx, "LAB-729-1", "Implement handshake one", "codex")
	}()

	select {
	case <-firstWaitContentEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for first trust prompt wait")
	}

	go func() {
		secondErr <- d.Assign(ctx, "LAB-729-2", "Implement handshake two", "codex")
	}()

	waitFor(t, "second spawn", func() bool {
		return deps.amux.spawnCount() == 2
	})

	select {
	case <-secondWaitContentEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("second trust prompt wait did not start while the first was blocked")
	}

	close(releaseFirstWaitContent)

	if err := <-firstErr; err != nil {
		t.Fatalf("first Assign() error = %v", err)
	}
	if err := <-secondErr; err != nil {
		t.Fatalf("second Assign() error = %v", err)
	}

	if got, want := deps.amux.waitContentCount("pane-1"), 2; got != want {
		t.Fatalf("pane-1 waitContent count = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitContentCount("pane-2"), 2; got != want {
		t.Fatalf("pane-2 waitContent count = %d, want %d", got, want)
	}
}

func TestAgentHandshakeFailsWhenReadyValidationStillLooksLikeBash(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.disableAutomaticReadyCapture = true
	deps.amux.waitContentResults = []error{
		amuxapi.ErrWaitContentTimeout,
		nil,
	}
	deps.amux.captureSequence("pane-1", []string{"bash-5.2$"})
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{
		{Content: []string{"bash-5.2$"}, CurrentCommand: "bash"},
		{Content: []string{"bash-5.2$"}, CurrentCommand: "bash"},
	})
	d := deps.newDaemon(t)

	_, err := d.agentHandshake(context.Background(), "pane-1", deps.config.profiles["codex"])
	if err == nil {
		t.Fatal("agentHandshake() succeeded, want error")
	}
	if !strings.Contains(err.Error(), `validate ready pattern "›"`) {
		t.Fatalf("agentHandshake() error = %v, want ready-pattern validation failure", err)
	}
	if !strings.Contains(err.Error(), `current command "bash"`) {
		t.Fatalf("agentHandshake() error = %v, want bash current-command context", err)
	}
}
