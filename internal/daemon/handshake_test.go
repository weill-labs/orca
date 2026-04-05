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

func TestAssignEmitsCodexHandshakeDiagnostics(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.amux.captureSequence("pane-1", []string{
		"Do you trust this folder?",
		"Codex is ready.",
	})
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
		return len(deps.events.eventsByType(EventWorkerHandshake)) == 6
	})

	got := deps.events.eventsByType(EventWorkerHandshake)
	wantMessages := []string{
		handshakeStepWait,
		handshakeStepCapture,
		handshakeStepTrustDetected,
		handshakeStepTrustEnter,
		handshakeStepWait,
		handshakeStepCapture,
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

func TestAssignSerializesCodexHandshakeAcrossConcurrentAssigns(t *testing.T) {
	tests := []struct {
		name          string
		pane1Captures []string
		pane2Captures []string
		wantPane1Keys []string
		wantPane2Keys []string
	}{
		{
			name:          "trust prompt on both panes",
			pane1Captures: []string{"Do you trust this folder?", "Codex is ready."},
			pane2Captures: []string{"Do you trust this folder?", "Codex is ready."},
			wantPane1Keys: []string{"Enter", "Implement handshake one", "Enter"},
			wantPane2Keys: []string{"Enter", "Implement handshake two", "Enter"},
		},
		{
			name:          "second pane waits even without trust prompt",
			pane1Captures: []string{"Do you trust this folder?", "Codex is ready."},
			pane2Captures: []string{"Codex is ready."},
			wantPane1Keys: []string{"Enter", "Implement handshake one", "Enter"},
			wantPane2Keys: []string{"Implement handshake two", "Enter"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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
			deps.amux.captureSequence("pane-1", tt.pane1Captures)
			deps.amux.captureSequence("pane-2", tt.pane2Captures)

			firstHandshakeEntered := make(chan struct{})
			releaseFirstHandshake := make(chan struct{})
			secondHandshakeWait := make(chan struct{}, 1)
			var firstWait sync.Once
			var secondWait sync.Once
			deps.amux.waitIdleHook = func(paneID string, timeout time.Duration) {
				if timeout != defaultAgentHandshakeTimeout {
					t.Fatalf("wait idle timeout = %v, want %v", timeout, defaultAgentHandshakeTimeout)
				}
				switch paneID {
				case "pane-1":
					firstWait.Do(func() {
						close(firstHandshakeEntered)
						<-releaseFirstHandshake
					})
				case "pane-2":
					secondWait.Do(func() {
						secondHandshakeWait <- struct{}{}
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
			case <-firstHandshakeEntered:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for first handshake to start")
			}

			go func() {
				secondErr <- d.Assign(ctx, "LAB-729-2", "Implement handshake two", "codex")
			}()

			waitFor(t, "second spawn", func() bool {
				return deps.amux.spawnCount() == 2
			})

			select {
			case <-secondHandshakeWait:
				t.Fatal("second handshake reached wait idle before first handshake completed")
			case <-time.After(150 * time.Millisecond):
			}

			close(releaseFirstHandshake)

			if err := <-firstErr; err != nil {
				t.Fatalf("first Assign() error = %v", err)
			}
			if err := <-secondErr; err != nil {
				t.Fatalf("second Assign() error = %v", err)
			}

			waitFor(t, "second handshake wait idle", func() bool {
				return deps.amux.waitIdleCount("pane-2") > 0
			})

			deps.amux.requireSentKeys(t, "pane-1", tt.wantPane1Keys)
			deps.amux.requireSentKeys(t, "pane-2", tt.wantPane2Keys)

			events := deps.events.eventsByType(EventWorkerHandshake)
			firstPane2 := -1
			lastPane1 := -1
			for i, event := range events {
				switch event.PaneID {
				case "pane-1":
					lastPane1 = i
				case "pane-2":
					if firstPane2 == -1 {
						firstPane2 = i
					}
				}
			}
			if firstPane2 == -1 {
				t.Fatal("missing pane-2 handshake events")
			}
			if lastPane1 == -1 {
				t.Fatal("missing pane-1 handshake events")
			}
			if firstPane2 <= lastPane1 {
				t.Fatalf("pane-2 handshake started before pane-1 completed: events=%#v", events)
			}
		})
	}
}
