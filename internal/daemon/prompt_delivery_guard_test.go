package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func TestCodexPromptTargetGuardHelpers(t *testing.T) {
	t.Parallel()

	exitedShell := PaneCapture{
		Content:        []string{"OpenAI Codex", "bash-5.2$"},
		CurrentCommand: "bash",
		Exited:         true,
	}
	if codexPromptTargetRunning(exitedShell) {
		t.Fatal("codexPromptTargetRunning(exited shell) = true, want false")
	}
	if codexPromptTargetSafe(exitedShell, false) {
		t.Fatal("codexPromptTargetSafe(exited shell) = true, want false")
	}
	if codexPromptTargetSafe(exitedShell, true) {
		t.Fatal("codexPromptTargetSafe(exited shell, trusted) = true, want false")
	}
	err := codexPromptTargetError("before prompt delivery", exitedShell, false)
	if !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
		t.Fatalf("codexPromptTargetError(exited shell) error = %v, want ErrPromptDeliveryNotConfirmed", err)
	}
	if !strings.Contains(err.Error(), "before prompt delivery and pane exited") {
		t.Fatalf("codexPromptTargetError(exited shell) error = %v, want pane exited context", err)
	}
	if strings.Contains(err.Error(), "returned to shell") {
		t.Fatalf("codexPromptTargetError(exited shell) error = %v, want exited message before shell message", err)
	}

	shellOutput := PaneCapture{Content: []string{"bash: Verify: command not found"}}
	if codexPromptTargetRunning(shellOutput) {
		t.Fatal("codexPromptTargetRunning(shell output) = true, want false")
	}

	bashLeaks := []PaneCapture{
		{
			Content:        []string{"OpenAI Codex", "bash: Verify: command not found"},
			CurrentCommand: "bash",
		},
		{
			Content:        []string{"OpenAI Codex", "bash: syntax error near unexpected token `('"},
			CurrentCommand: "bash",
		},
	}
	for _, snapshot := range bashLeaks {
		if codexPromptTargetSafe(snapshot, false) {
			t.Fatalf("codexPromptTargetSafe(%q) = true, want false", snapshot.Output())
		}
		if codexPromptTargetSafe(snapshot, true) {
			t.Fatalf("codexPromptTargetSafe(%q, trusted) = true, want false (bash-leak protection must remain)", snapshot.Output())
		}
		err := codexPromptTargetError("before prompt delivery", snapshot, false)
		if !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
			t.Fatalf("codexPromptTargetError(%q) error = %v, want ErrPromptDeliveryNotConfirmed", snapshot.Output(), err)
		}
	}

	spawnHandshake := PaneCapture{}
	if !codexPromptTargetSafe(spawnHandshake, false) {
		t.Fatal("codexPromptTargetSafe(empty current command) = false, want true")
	}

	commands := []struct {
		command   string
		wantRuns  bool
		wantKnown bool
	}{
		{command: "/usr/local/bin/codex --yolo", wantRuns: true, wantKnown: true},
		{command: "node", wantRuns: false, wantKnown: false},
		{command: "/usr/bin/nodejs", wantRuns: false, wantKnown: false},
		{command: "npx @openai/codex", wantRuns: false, wantKnown: false},
		{command: "sh /usr/local/bin/codex-wrapper", wantRuns: false, wantKnown: false},
		{command: "", wantRuns: false, wantKnown: false},
	}
	for _, tt := range commands {
		runs, known := codexCommandRunsCodex(tt.command)
		if runs != tt.wantRuns || known != tt.wantKnown {
			t.Fatalf("codexCommandRunsCodex(%q) = (%t, %t), want (%t, %t)", tt.command, runs, known, tt.wantRuns, tt.wantKnown)
		}
	}

	otherCommand := PaneCapture{
		Content:        []string{"python prompt"},
		CurrentCommand: "python",
	}
	err = codexPromptTargetError("before prompt delivery", otherCommand, false)
	if !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
		t.Fatalf("codexPromptTargetError(other command, untrusted) error = %v, want ErrPromptDeliveryNotConfirmed", err)
	}
	if !strings.Contains(err.Error(), "before prompt delivery and codex is not running") {
		t.Fatalf("codexPromptTargetError(other command) error = %v, want not-running context", err)
	}

	// Long-lived codex pane: current command appears as "node" because codex is
	// a Node.js app, and the startup "OpenAI Codex" banner has scrolled out of
	// the visible snapshot. Untrusted callers must reject (initial handshake
	// has no prior proof). Trusted callers must accept (orca verified codex at
	// handshake time; demanding the banner again would fail every merge-notify
	// after a worker has been running for ~10 minutes).
	longLivedNode := PaneCapture{
		Content:        []string{"gpt-5.5 xhigh · ~/clone-01 · LAB-1815 · Context 37% used"},
		CurrentCommand: "node",
	}
	if codexPromptTargetSafe(longLivedNode, false) {
		t.Fatal("codexPromptTargetSafe(long-lived node, untrusted) = true, want false")
	}
	if !codexPromptTargetSafe(longLivedNode, true) {
		t.Fatal("codexPromptTargetSafe(long-lived node, trusted) = false, want true")
	}
	if err := codexPromptTargetError("before prompt delivery", longLivedNode, true); err != nil {
		t.Fatalf("codexPromptTargetError(long-lived node, trusted) error = %v, want nil", err)
	}
}

func TestEnsureCodexPromptTargetReturnsGuardCaptureErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantPrompt bool
		wantText   string
	}{
		{
			name:       "pane gone",
			err:        ErrPaneGone,
			wantPrompt: true,
			wantText:   "pane disappeared before prompt delivery",
		},
		{
			name:     "history failure",
			err:      errors.New("history failed"),
			wantText: "capture prompt delivery state before prompt delivery: capture history: history failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.amux.captureHistoryErrors("pane-1", []error{tt.err})
			d := deps.newDaemon(t)

			err := d.ensureCodexPromptTarget(context.Background(), "pane-1", "before prompt delivery", false)
			if tt.wantPrompt && !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
				t.Fatalf("ensureCodexPromptTarget() error = %v, want ErrPromptDeliveryNotConfirmed", err)
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantText) {
				t.Fatalf("ensureCodexPromptTarget() error = %v, want %q", err, tt.wantText)
			}
		})
	}
}

func TestAssignmentPromptInjectionRefusesShellBeforeRetryEnter(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	token := "token"
	if err := deps.amux.SetMetadata(context.Background(), "pane-1", map[string]string{
		assignmentPromptInjectionTokenKey: token,
		assignmentPromptInjectionStageKey: assignmentPromptInjectionStagePromptSent,
	}); err != nil {
		t.Fatalf("SetMetadata() error = %v", err)
	}
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Content:        []string{"OpenAI Codex", "bash-5.2$"},
		CurrentCommand: "bash",
	}})

	err := d.sendIdempotentAssignmentPromptCommand(context.Background(), "pane-1", token, "Fix prompt delivery", "Enter")
	if !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
		t.Fatalf("sendIdempotentAssignmentPromptCommand() error = %v, want ErrPromptDeliveryNotConfirmed", err)
	}
	if !strings.Contains(err.Error(), "before assignment prompt enter") {
		t.Fatalf("sendIdempotentAssignmentPromptCommand() error = %v, want enter guard context", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func TestSendAndConfirmWorkingRefusesShellAfterWaitingForIdle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{
		{
			Content:        []string{"OpenAI Codex", "Working (5m 12s - esc to interrupt)", "> $postmortem"},
			CurrentCommand: "codex",
		},
		{
			Content:        []string{"OpenAI Codex", "bash-5.2$"},
			CurrentCommand: "bash",
		},
	})
	d := deps.newDaemon(t)

	err := d.sendAndConfirmWorking(context.Background(), "pane-1", "$postmortem")
	if !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
		t.Fatalf("sendAndConfirmWorking() error = %v, want ErrPromptDeliveryNotConfirmed", err)
	}
	if !strings.Contains(err.Error(), "before prompt delivery after idle") {
		t.Fatalf("sendAndConfirmWorking() error = %v, want after-idle guard context", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func TestSendAndConfirmWorkingRefusesShellBeforeRetryEnter(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.waitContentResults = []error{amuxapi.ErrWaitContentTimeout}
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{
		{
			Content:        []string{"OpenAI Codex", ">"},
			CurrentCommand: "codex",
		},
		{
			Content:        []string{"OpenAI Codex", "bash-5.2$"},
			CurrentCommand: "bash",
		},
	})
	d := deps.newDaemon(t)

	err := d.sendAndConfirmWorking(context.Background(), "pane-1", "$postmortem")
	if !errors.Is(err, ErrPromptDeliveryNotConfirmed) {
		t.Fatalf("sendAndConfirmWorking() error = %v, want ErrPromptDeliveryNotConfirmed", err)
	}
	if !strings.Contains(err.Error(), "before retry enter") {
		t.Fatalf("sendAndConfirmWorking() error = %v, want retry-enter guard context", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"$postmortem\n"})
}
