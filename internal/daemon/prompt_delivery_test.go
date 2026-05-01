package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func TestConfirmPromptDeliverySkipsNonCodexProfiles(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	if err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "claude"}); err != nil {
		t.Fatalf("confirmPromptDelivery() error = %v", err)
	}
	if got, want := len(deps.amux.waitContentCalls), 0; got != want {
		t.Fatalf("waitContent calls = %d, want %d", got, want)
	}
}

func TestPaneRunsCodex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		set  func(*testDeps)
		want bool
	}{
		{
			name: "history output identifies codex",
			set: func(deps *testDeps) {
				deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
					Content:        []string{"OpenAI Codex", "›"},
					CurrentCommand: "bash",
				}})
			},
			want: true,
		},
		{
			name: "command basename identifies codex",
			set: func(deps *testDeps) {
				deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
					Content:        []string{"ready"},
					CurrentCommand: "/usr/local/bin/codex --yolo",
				}})
			},
			want: true,
		},
		{
			name: "non codex pane returns false",
			set: func(deps *testDeps) {
				deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
					Content:        []string{"bash-5.2$"},
					CurrentCommand: "bash",
				}})
			},
			want: false,
		},
		{
			name: "capture failure returns false",
			set: func(deps *testDeps) {
				deps.amux.captureHistoryErrors("pane-1", []error{ErrPaneGone})
				deps.amux.capturePaneErr = ErrPaneGone
			},
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			tt.set(deps)
			d := deps.newDaemon(t)

			if got := d.paneRunsCodex(context.Background(), "pane-1"); got != tt.want {
				t.Fatalf("paneRunsCodex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConfirmPromptDeliveryReturnsWaitContentError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	deps.amux.waitContentResults = []error{errors.New("wait failed")}
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil || !strings.Contains(err.Error(), `wait for "Working" after prompt: wait failed`) {
		t.Fatalf("confirmPromptDelivery() error = %v, want wrapped wait-content failure", err)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
}

func TestConfirmPromptDeliveryReturnsSendKeysErrorOnRetry(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{Content: []string{"OpenAI Codex", "›"}, CurrentCommand: "codex"}})
	deps.amux.sendKeysErr = errors.New("send failed")
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil || !strings.Contains(err.Error(), "retry prompt delivery: send failed") {
		t.Fatalf("confirmPromptDelivery() error = %v, want wrapped send failure", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func TestConfirmPromptDeliverySucceedsWhenWorkingAppearsAfterShortProbe(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	d := deps.newDaemon(t)

	if err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"}); err != nil {
		t.Fatalf("confirmPromptDelivery() error = %v", err)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func TestConfirmPromptDeliveryFailsFastWhenCodexReturnsToShell(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	deps.amux.waitContentResults = []error{amuxapi.ErrWaitContentTimeout}
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content:        []string{"bash-5.2$", "codex exited"},
		CurrentCommand: "bash",
	}})
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil {
		t.Fatal("confirmPromptDelivery() error = nil, want prompt delivery failure")
	}
	if !strings.Contains(err.Error(), `current command "bash"`) {
		t.Fatalf("confirmPromptDelivery() error = %v, want bash prompt context", err)
	}
	if !strings.Contains(err.Error(), "codex exited") {
		t.Fatalf("confirmPromptDelivery() error = %v, want capture context", err)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func TestConfirmPromptDeliveryWrapsCodexUpdateRequiredOnNpmPermissionError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	deps.amux.waitContentResults = []error{amuxapi.ErrWaitContentTimeout}
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content: []string{
			"npm ERR! code EACCES",
			"npm ERR! Error: EACCES: permission denied, mkdir '/usr/lib/node_modules/@openai'",
		},
		CurrentCommand: "bash",
	}})
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil {
		t.Fatal("confirmPromptDelivery() error = nil, want codex update remediation")
	}
	if !errors.Is(err, ErrCodexUpdateRequired) {
		t.Fatalf("confirmPromptDelivery() error = %v, want ErrCodexUpdateRequired", err)
	}
	for _, want := range []string{
		CodexRefreshCommand,
		"/usr/lib/node_modules",
		"codex self-update failed",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("confirmPromptDelivery() error = %v, want substring %q", err, want)
		}
	}
}

func TestConfirmPromptDeliveryStopsRetryingWhenIdlePaneShowsCodexUpdateError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 1)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content: []string{
			"Codex global npm install failed while checking for updates",
			"npm ERR! code EACCES",
		},
		CurrentCommand: "codex",
	}})
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil {
		t.Fatal("confirmPromptDelivery() error = nil, want codex update remediation")
	}
	if !errors.Is(err, ErrCodexUpdateRequired) {
		t.Fatalf("confirmPromptDelivery() error = %v, want ErrCodexUpdateRequired", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
	if got, want := len(deps.amux.waitContentCalls), 0; got != want {
		t.Fatalf("waitContent calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.waitIdleCalls), 1; got != want {
		t.Fatalf("waitIdle calls = %d, want %d", got, want)
	}
}

func TestConfirmPromptDeliveryReturnsExitedPaneError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	deps.amux.waitContentResults = []error{amuxapi.ErrWaitContentTimeout}
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content:        []string{"codex exited"},
		CurrentCommand: "codex",
		Exited:         true,
	}})
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil || !strings.Contains(err.Error(), "after prompt and pane exited") {
		t.Fatalf("confirmPromptDelivery() error = %v, want exited-pane context", err)
	}
}

func TestConfirmPromptDeliveryReturnsPaneGoneError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	deps.amux.waitContentResults = []error{amuxapi.ErrWaitContentTimeout}
	deps.amux.capturePaneErr = ErrPaneGone
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil || !strings.Contains(err.Error(), `pane disappeared while waiting for "Working" after prompt`) {
		t.Fatalf("confirmPromptDelivery() error = %v, want pane-gone context", err)
	}
}

func TestConfirmPromptDeliveryReturnsCapturePaneError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	deps.amux.waitContentResults = []error{amuxapi.ErrWaitContentTimeout}
	deps.amux.capturePaneErr = errors.New("capture failed")
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil || !strings.Contains(err.Error(), `capture pane while waiting for "Working" after prompt: capture failed`) {
		t.Fatalf("confirmPromptDelivery() error = %v, want capture failure", err)
	}
}

func TestConfirmPromptDeliveryReturnsPromptDeliveryErrorWhenPaneStaysIdle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitIdleHook = func(_ string, timeout, _ time.Duration) {
		if timeout == codexPromptRetryIdleProbeTime {
			deps.amux.waitIdleErr = nil
		}
	}
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{Content: []string{"OpenAI Codex", "›"}, CurrentCommand: "codex"}})
	d := deps.newDaemon(t)

	err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"})
	if err == nil || !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
		t.Fatalf("confirmPromptDelivery() error = %v, want ErrPromptDeliveryNotConfirmed", err)
	}
}

func TestConfirmPromptDeliveryRetriesPasteCollapseUntilPaneTurnsActive(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	simulateCodexSubmitPasteCollapse(deps, 3)
	d := deps.newDaemon(t)

	if err := d.confirmPromptDelivery(context.Background(), "pane-1", AgentProfile{Name: "codex"}); err != nil {
		t.Fatalf("confirmPromptDelivery() error = %v", err)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		"\n",
		"\n",
		"\n",
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
}

func TestAssignmentPromptInjectionIsIdempotentForTaskToken(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	task := Task{
		Project:   "/tmp/project",
		Issue:     "LAB-1555",
		WorkerID:  "worker-01",
		CreatedAt: deps.clock.Now(),
	}

	if err := d.sendAssignmentPromptAndEnter(context.Background(), "pane-1", task, "Fix prompt delivery"); err != nil {
		t.Fatalf("first sendAssignmentPromptAndEnter() error = %v", err)
	}
	if err := d.sendAssignmentPromptAndEnter(context.Background(), "pane-1", task, "Fix prompt delivery"); err != nil {
		t.Fatalf("second sendAssignmentPromptAndEnter() error = %v", err)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{"Fix prompt delivery\n"})
}

func TestAssignmentPromptInjectionRetrySendsOnlyMissingEnter(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.sendKeysResults = []error{nil, errors.New("enter failed")}
	d := deps.newDaemon(t)
	task := Task{
		Project:   "/tmp/project",
		Issue:     "LAB-1555",
		WorkerID:  "worker-01",
		CreatedAt: deps.clock.Now(),
	}

	err := d.sendAssignmentPromptAndEnter(context.Background(), "pane-1", task, "Fix prompt delivery")
	if err == nil || !strings.Contains(err.Error(), "enter failed") {
		t.Fatalf("first sendAssignmentPromptAndEnter() error = %v, want enter failure", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"Fix prompt delivery"})

	if err := d.sendAssignmentPromptAndEnter(context.Background(), "pane-1", task, "Fix prompt delivery"); err != nil {
		t.Fatalf("second sendAssignmentPromptAndEnter() error = %v", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"Fix prompt delivery\n"})
}

func TestSendAndConfirmWorkingTreatsStaleWorkingAsUnconfirmed(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	var sleeps []time.Duration
	deps.sleep = recordSleep(&sleeps, deps.clock)
	stale := PaneCapture{
		Content:        []string{"OpenAI Codex", "Working (5m 12s • esc to interrupt)", "› $postmortem"},
		CurrentCommand: "codex",
	}
	deps.amux.captureHistorySequence("pane-1", repeatPaneCaptures(stale, 32))
	d := deps.newDaemon(t)

	err := d.sendAndConfirmWorking(context.Background(), "pane-1", "$postmortem")
	if !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
		t.Fatalf("sendAndConfirmWorking() error = %v, want ErrPromptDeliveryNotConfirmed", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"$postmortem\n", "\n", "\n", "\n", "\n", "\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got := deps.amux.captureHistoryCount("pane-1"); got == 0 {
		t.Fatal("capture history count = 0, want stale-scrollback probe")
	}
	if len(sleeps) == 0 {
		t.Fatal("sleep calls = 0, want freshness polling between stale captures")
	}
}

func TestSendAndConfirmWorkingRetriesWhenFreshWorkingAppearsAfterStaleScrollback(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	var sleeps []time.Duration
	deps.sleep = recordSleep(&sleeps, deps.clock)
	stale := PaneCapture{
		Content:        []string{"OpenAI Codex", "Working (5m 12s • esc to interrupt)", "› $postmortem"},
		CurrentCommand: "codex",
	}
	fresh := PaneCapture{
		Content:        []string{"OpenAI Codex", "Working (5m 12s • esc to interrupt)", "$postmortem", "• Working (0s • esc to interrupt)"},
		CurrentCommand: "codex",
	}
	deps.amux.captureHistorySequence("pane-1", repeatPaneCaptures(stale, 16))
	deps.amux.sendKeysHook = func(paneID string, keys []string) {
		if len(keys) == 1 && keys[0] == "Enter" {
			deps.amux.captureHistorySequence(paneID, []PaneCapture{fresh})
		}
	}
	d := deps.newDaemon(t)

	if err := d.sendAndConfirmWorking(context.Background(), "pane-1", "$postmortem"); err != nil {
		t.Fatalf("sendAndConfirmWorking() error = %v", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"$postmortem\n", "\n"})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: defaultAgentHandshakeTimeout},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got := deps.amux.captureHistoryCount("pane-1"); got == 0 {
		t.Fatal("capture history count = 0, want stale-scrollback probe")
	}
	if len(sleeps) == 0 {
		t.Fatal("sleep calls = 0, want freshness polling before retry")
	}
}

func TestSendAndConfirmWorkingRetriesPasteCollapseUntilPaneTurnsActive(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	simulateCodexSubmitPasteCollapse(deps, 3)
	d := deps.newDaemon(t)

	prompt := strings.Repeat("Address the blocking review comments and push an update. ", 20)
	if err := d.sendAndConfirmWorking(context.Background(), "pane-1", prompt); err != nil {
		t.Fatalf("sendAndConfirmWorking() error = %v", err)
	}

	deps.amux.requireSentKeys(t, "pane-1", []string{
		prompt + "\n",
		"\n",
		"\n",
		"\n",
	})
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
		{PaneID: "pane-1", Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitIdle calls = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitContentCalls, []waitContentCall{
		{PaneID: "pane-1", Substring: codexWorkingText, Timeout: codexPromptRetryIdleProbeTime},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("waitContent calls = %#v, want %#v", got, want)
	}
}

func TestWaitForPromptDeliveryIdleOrWorkingReturnsTimedOutWhenPaneStaysIdle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content:        []string{"OpenAI Codex", "›"},
		CurrentCommand: "codex",
	}})
	d := deps.newDaemon(t)

	state, err := d.waitForPromptDeliveryIdleOrWorking(context.Background(), "pane-1", AgentProfile{Name: "codex"}, "after prompt")
	if got, want := state, promptDeliveryWaitTimedOut; got != want {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), "pane remained idle after prompt") {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() error = %v, want idle timeout context", err)
	}
	if got := deps.amux.waitContentCalls; len(got) != 0 {
		t.Fatalf("waitContent calls = %#v, want none", got)
	}
}

func TestWaitForPromptDeliveryIdleOrWorkingReturnsPaneGoneError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistoryErrors("pane-1", []error{ErrPaneGone})
	deps.amux.capturePaneErr = ErrPaneGone
	d := deps.newDaemon(t)

	state, err := d.waitForPromptDeliveryIdleOrWorking(context.Background(), "pane-1", AgentProfile{Name: "codex"}, "after prompt")
	if got, want := state, promptDeliveryWaitAgentGone; got != want {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), "pane disappeared while waiting for idle after prompt") {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() error = %v, want pane-gone context", err)
	}
	if got := deps.amux.waitContentCalls; len(got) != 0 {
		t.Fatalf("waitContent calls = %#v, want none", got)
	}
}

func TestWaitForPromptDeliveryIdleOrWorkingReturnsCapturePaneError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.capturePaneErr = errors.New("capture failed")
	d := deps.newDaemon(t)

	state, err := d.waitForPromptDeliveryIdleOrWorking(context.Background(), "pane-1", AgentProfile{Name: "codex"}, "after prompt")
	if got, want := state, promptDeliveryWaitError; got != want {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), "capture prompt delivery state while waiting for idle after prompt: capture failed") {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() error = %v, want capture-pane context", err)
	}
	if got := deps.amux.waitContentCalls; len(got) != 0 {
		t.Fatalf("waitContent calls = %#v, want none", got)
	}
}

func TestWaitForPromptDeliveryIdleOrWorkingReturnsExitedPaneError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content:        []string{"codex exited"},
		CurrentCommand: "codex",
		Exited:         true,
	}})
	d := deps.newDaemon(t)

	state, err := d.waitForPromptDeliveryIdleOrWorking(context.Background(), "pane-1", AgentProfile{Name: "codex"}, "after prompt")
	if got, want := state, promptDeliveryWaitAgentGone; got != want {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), "after prompt and pane exited") {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() error = %v, want exited-pane context", err)
	}
	if got := deps.amux.waitContentCalls; len(got) != 0 {
		t.Fatalf("waitContent calls = %#v, want none", got)
	}
}

func TestWaitForPromptDeliveryIdleOrWorkingReturnsShellError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content:        []string{"bash-5.2$", "codex exited"},
		CurrentCommand: "bash",
	}})
	d := deps.newDaemon(t)

	state, err := d.waitForPromptDeliveryIdleOrWorking(context.Background(), "pane-1", AgentProfile{Name: "codex"}, "after prompt")
	if got, want := state, promptDeliveryWaitAgentGone; got != want {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), "after prompt and codex returned to shell") {
		t.Fatalf("waitForPromptDeliveryIdleOrWorking() error = %v, want shell-return context", err)
	}
	if got := deps.amux.waitContentCalls; len(got) != 0 {
		t.Fatalf("waitContent calls = %#v, want none", got)
	}
}

func TestSendAndConfirmWorkingReturnsIdleProbeErrorWhenStaleWorkingPresent(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content:        []string{"OpenAI Codex", "Working (5m 12s • esc to interrupt)"},
		CurrentCommand: "codex",
	}})
	deps.amux.waitIdleErr = errors.New("idle failed")
	d := deps.newDaemon(t)

	err := d.sendAndConfirmWorking(context.Background(), "pane-1", "$postmortem")
	if err == nil || !strings.Contains(err.Error(), "wait for idle before prompt delivery: idle failed") {
		t.Fatalf("sendAndConfirmWorking() error = %v, want idle probe failure", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func TestWaitForFreshPromptDeliveryMarkerReturnsShellError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content:        []string{"bash-5.2$", "codex exited"},
		CurrentCommand: "bash",
	}})
	d := deps.newDaemon(t)

	state, err := d.waitForFreshPromptDeliveryMarker(
		context.Background(),
		"pane-1",
		AgentProfile{Name: "codex"},
		codexWorkingFreshnessProbeTimeout,
		"after prompt",
		promptDeliveryBaseline{output: strings.ToLower("Working (5m 12s • esc to interrupt)"), hasWorking: true},
	)
	if got, want := state, promptDeliveryWaitAgentGone; got != want {
		t.Fatalf("waitForFreshPromptDeliveryMarker() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), "after prompt and codex returned to shell") {
		t.Fatalf("waitForFreshPromptDeliveryMarker() error = %v, want shell-return context", err)
	}
}

func TestWaitForFreshPromptDeliveryMarkerReturnsExitedPaneError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content:        []string{"codex exited"},
		CurrentCommand: "codex",
		Exited:         true,
	}})
	d := deps.newDaemon(t)

	state, err := d.waitForFreshPromptDeliveryMarker(
		context.Background(),
		"pane-1",
		AgentProfile{Name: "codex"},
		codexWorkingFreshnessProbeTimeout,
		"after prompt",
		promptDeliveryBaseline{output: strings.ToLower("Working (5m 12s • esc to interrupt)"), hasWorking: true},
	)
	if got, want := state, promptDeliveryWaitAgentGone; got != want {
		t.Fatalf("waitForFreshPromptDeliveryMarker() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), "after prompt and pane exited") {
		t.Fatalf("waitForFreshPromptDeliveryMarker() error = %v, want exited-pane context", err)
	}
}

func TestWaitForFreshPromptDeliveryMarkerReturnsPaneGoneError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistoryErrors("pane-1", []error{ErrPaneGone})
	deps.amux.capturePaneErr = ErrPaneGone
	d := deps.newDaemon(t)

	state, err := d.waitForFreshPromptDeliveryMarker(
		context.Background(),
		"pane-1",
		AgentProfile{Name: "codex"},
		codexWorkingFreshnessProbeTimeout,
		"after prompt",
		promptDeliveryBaseline{output: strings.ToLower("Working (5m 12s • esc to interrupt)"), hasWorking: true},
	)
	if got, want := state, promptDeliveryWaitAgentGone; got != want {
		t.Fatalf("waitForFreshPromptDeliveryMarker() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), `pane disappeared while waiting for fresh "Working" after prompt`) {
		t.Fatalf("waitForFreshPromptDeliveryMarker() error = %v, want pane-gone context", err)
	}
}

func TestWaitForFreshPromptDeliveryMarkerReturnsSleepError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", repeatPaneCaptures(PaneCapture{
		Content:        []string{"OpenAI Codex", "› $postmortem"},
		CurrentCommand: "codex",
	}, 4))
	deps.sleep = func(context.Context, time.Duration) error {
		return errors.New("sleep failed")
	}
	d := deps.newDaemon(t)

	state, err := d.waitForFreshPromptDeliveryMarker(
		context.Background(),
		"pane-1",
		AgentProfile{Name: "codex"},
		codexWorkingFreshnessProbeTimeout,
		"after prompt",
		promptDeliveryBaseline{
			output:     strings.ToLower("OpenAI Codex\nWorking (5m 12s • esc to interrupt)\n› $postmortem"),
			hasWorking: true,
		},
	)
	if got, want := state, promptDeliveryWaitError; got != want {
		t.Fatalf("waitForFreshPromptDeliveryMarker() state = %v, want %v", got, want)
	}
	if err == nil || !strings.Contains(err.Error(), `wait for fresh "Working" after prompt: sleep failed`) {
		t.Fatalf("waitForFreshPromptDeliveryMarker() error = %v, want sleep failure", err)
	}
}

func TestCapturePromptDeliverySnapshotFallsBackToPaneCaptureWhenHistoryIsEmpty(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	want := PaneCapture{
		Content:        []string{"OpenAI Codex", "›"},
		CurrentCommand: "codex",
	}
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{want})
	d := deps.newDaemon(t)

	got, err := d.capturePromptDeliverySnapshot(context.Background(), "pane-1")
	if err != nil {
		t.Fatalf("capturePromptDeliverySnapshot() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("capturePromptDeliverySnapshot() = %#v, want %#v", got, want)
	}
	if got := deps.amux.captureHistoryCount("pane-1"); got != 1 {
		t.Fatalf("capture history count = %d, want 1", got)
	}
	if got := deps.amux.captureCount("pane-1"); got != 1 {
		t.Fatalf("capture count = %d, want 1", got)
	}
}

func TestCapturePromptDeliverySnapshotReturnsCombinedError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistoryErrors("pane-1", []error{errors.New("history failed")})
	deps.amux.capturePaneErr = errors.New("pane failed")
	d := deps.newDaemon(t)

	_, err := d.capturePromptDeliverySnapshot(context.Background(), "pane-1")
	if err == nil || !strings.Contains(err.Error(), "capture history: history failed; capture pane: pane failed") {
		t.Fatalf("capturePromptDeliverySnapshot() error = %v, want combined capture failure", err)
	}
}

func TestPromptDeliveryHasFreshWorking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		baseline promptDeliveryBaseline
		snapshot PaneCapture
		want     bool
	}{
		{
			name:     "prefix append adds new working marker",
			baseline: promptDeliveryBaseline{output: strings.ToLower("OpenAI Codex\nWorking (5m 12s • esc to interrupt)"), hasWorking: true},
			snapshot: PaneCapture{Content: []string{"OpenAI Codex", "Working (5m 12s • esc to interrupt)", "• Working (0s • esc to interrupt)"}},
			want:     true,
		},
		{
			name:     "count increase without prefix still counts as fresh",
			baseline: promptDeliveryBaseline{output: strings.ToLower("Working (5m 12s • esc to interrupt)"), hasWorking: true},
			snapshot: PaneCapture{Content: []string{"header", "Working (5m 12s • esc to interrupt)", "middle", "Working (0s • esc to interrupt)"}},
			want:     true,
		},
		{
			name:     "same content is stale",
			baseline: promptDeliveryBaseline{output: strings.ToLower("Working (5m 12s • esc to interrupt)"), hasWorking: true},
			snapshot: PaneCapture{Content: []string{"Working (5m 12s • esc to interrupt)"}},
			want:     false,
		},
		{
			name:     "missing working marker is not fresh",
			baseline: promptDeliveryBaseline{output: strings.ToLower("Working (5m 12s • esc to interrupt)"), hasWorking: true},
			snapshot: PaneCapture{Content: []string{"OpenAI Codex", "›"}},
			want:     false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := promptDeliveryHasFreshWorking(tt.baseline, tt.snapshot); got != tt.want {
				t.Fatalf("promptDeliveryHasFreshWorking() = %v, want %v", got, tt.want)
			}
		})
	}
}

func repeatPaneCaptures(capture PaneCapture, count int) []PaneCapture {
	if count <= 0 {
		return nil
	}

	repeated := make([]PaneCapture, count)
	for i := range repeated {
		repeated[i] = clonePaneCapture(capture)
	}
	return repeated
}
