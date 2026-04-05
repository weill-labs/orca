package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAssignConfirmsCodexTrustPromptBeforeSendingPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.captureSequence("pane-1", []string{"Do you trust this folder?"})
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

	deps.amux.requireSentKeys(t, "pane-1", []string{"Enter", "Implement handshake", "Enter"})
	if got, want := deps.amux.captureCount("pane-1"), 2; got != want {
		t.Fatalf("capture count = %d, want %d", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignDoesNotBlindlyConfirmWhenTrustPromptNotPresent(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.captureSequence("pane-1", []string{"Codex is ready."})
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

	deps.amux.requireSentKeys(t, "pane-1", []string{"Implement handshake\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
}

func TestAssignResumesCodexBeforeSendingPrompt(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.captureSequence("pane-1", []string{"Resume your previous session"})
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
		"Implement resume flow\n",
	})
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Content: "›", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 30 * time.Second},
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
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker stored despite handshake rollback")
	}

	deps.amux.requireSentKeys(t, "pane-1", nil)
	if got, want := deps.amux.killCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("kill calls = %#v, want %#v", got, want)
	}

	wantGit := []commandCall{
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "main"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"pull"}},
		{Dir: deps.pool.clone.Path, Name: "git", Args: []string{"checkout", "-B", "LAB-720"}},
	}
	if got := deps.commands.callsByName("git"); !reflect.DeepEqual(got, wantGit) {
		t.Fatalf("git calls = %#v, want %#v", got, wantGit)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssignFailed)
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
		{PaneID: "pane-1", Content: "›", Timeout: 30 * time.Second},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
}
