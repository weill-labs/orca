package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

const (
	codexWorkingText                   = "Working"
	codexWorkingConfirmationAttempts   = 6
	codexPromptRetryIdleProbeTime      = 5 * time.Second
	codexWorkingFreshnessProbeTimeout  = 500 * time.Millisecond
	codexWorkingFreshnessPollInterval  = 50 * time.Millisecond
)

var ErrPromptDeliveryNotConfirmed = errors.New("prompt delivery not confirmed")

type promptDeliveryWaitState int

type promptDeliveryBaseline struct {
	output     string
	hasWorking bool
}

const (
	promptDeliveryWaitError promptDeliveryWaitState = iota
	promptDeliveryWaitObserved
	promptDeliveryWaitTimedOut
	promptDeliveryWaitAgentGone
)

// Codex can occasionally miss the initial prompt delivery or exit back to a
// bare shell immediately after startup. Confirm prompt delivery in two stages:
// an initial wait, then one retry command plus a longer wait before the caller
// decides whether to respawn on a fresh pane.
func (d *Daemon) confirmPromptDelivery(ctx context.Context, paneID string, profile AgentProfile) error {
	if !strings.EqualFold(profile.Name, "codex") {
		return nil
	}
	return d.submitToCodex(ctx, paneID, "")
}

// Post-start lifecycle prompts can race with Codex input buffering. Send the
// prompt once, then retry only Enter until Codex transitions to Working or the
// bounded retry budget is exhausted.
func (d *Daemon) sendAndConfirmWorking(ctx context.Context, paneID, prompt string) error {
	return d.submitToCodex(ctx, paneID, prompt)
}

// submitToCodex submits prompt+Enter to a codex pane and retries Enter while
// the pane keeps returning to idle. An empty prompt means the caller already
// sent the initial Enter and only needs confirmation/retries.
func (d *Daemon) submitToCodex(ctx context.Context, paneID, prompt string) error {
	baseline := promptDeliveryBaseline{}
	if strings.TrimSpace(prompt) != "" {
		var err error
		baseline, err = d.capturePromptDeliveryBaseline(ctx, paneID)
		if err != nil {
			return err
		}
		if baseline.hasWorking {
			if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
				return fmt.Errorf("wait for idle before prompt delivery: %w", err)
			}
		}
	}

	if strings.TrimSpace(prompt) != "" {
		deliveryPrompt, err := normalizePromptForDelivery(prompt)
		if err != nil {
			return err
		}
		if err := d.amux.SendKeys(ctx, paneID, deliveryPrompt, "Enter"); err != nil {
			return err
		}
	}

	profile := AgentProfile{Name: "codex"}
	phase := "after prompt"
	lastErr := fmt.Errorf("%w: working confirmation exhausted", ErrPromptDeliveryNotConfirmed)
	for attempt := 1; attempt <= codexWorkingConfirmationAttempts; attempt++ {
		state, err := d.waitForPromptDeliveryConfirmation(ctx, paneID, profile, phase, baseline)
		switch state {
		case promptDeliveryWaitObserved:
			return nil
		case promptDeliveryWaitError, promptDeliveryWaitAgentGone:
			return err
		case promptDeliveryWaitTimedOut:
			lastErr = err
		}
		if attempt == codexWorkingConfirmationAttempts {
			return lastErr
		}
		if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
			return fmt.Errorf("retry prompt delivery: %w", err)
		}
		phase = "after retry enter"
	}

	return lastErr
}

func (d *Daemon) paneRunsCodex(ctx context.Context, paneID string) bool {
	snapshot, err := d.capturePromptDeliverySnapshot(ctx, paneID)
	if err != nil {
		return false
	}
	if containsFold(snapshot.Output(), "OpenAI Codex") {
		return true
	}

	command := strings.TrimSpace(snapshot.CurrentCommand)
	if command == "" {
		return false
	}
	fields := strings.Fields(command)
	if len(fields) == 0 {
		return false
	}
	return strings.EqualFold(filepath.Base(fields[0]), "codex")
}

func (d *Daemon) waitForPromptDeliveryConfirmation(ctx context.Context, paneID string, profile AgentProfile, phase string, baseline promptDeliveryBaseline) (promptDeliveryWaitState, error) {
	if !baseline.hasWorking {
		return d.waitForPromptDeliveryIdleOrWorking(ctx, paneID, profile, phase)
	}
	return d.waitForFreshPromptDeliveryMarker(ctx, paneID, profile, codexWorkingFreshnessProbeTimeout, phase, baseline)
}

func (d *Daemon) waitForPromptDeliveryIdleOrWorking(ctx context.Context, paneID string, profile AgentProfile, phase string) (promptDeliveryWaitState, error) {
	if err := d.amux.WaitIdle(ctx, paneID, codexPromptRetryIdleProbeTime); err != nil {
		return d.waitForPromptDeliveryMarker(ctx, paneID, profile, codexPromptRetryIdleProbeTime, phase)
	}

	snapshot, err := d.capturePromptDeliverySnapshot(ctx, paneID)
	if err != nil {
		if isPaneGoneError(err) {
			return promptDeliveryWaitAgentGone, fmt.Errorf("%w: pane disappeared while waiting for idle %s", ErrPromptDeliveryNotConfirmed, phase)
		}
		return promptDeliveryWaitError, fmt.Errorf("capture prompt delivery state while waiting for idle %s: %w", phase, err)
	}
	if promptDeliveryReturnedToShell(profile, snapshot) {
		return promptDeliveryWaitAgentGone, promptDeliveryFailure(fmt.Sprintf("%s and codex returned to shell", phase), snapshot)
	}
	if snapshot.Exited {
		return promptDeliveryWaitAgentGone, promptDeliveryFailure(fmt.Sprintf("%s and pane exited", phase), snapshot)
	}
	return promptDeliveryWaitTimedOut, promptDeliveryFailure(fmt.Sprintf("pane remained idle %s", phase), snapshot)
}

func (d *Daemon) waitForPromptDeliveryMarker(ctx context.Context, paneID string, profile AgentProfile, timeout time.Duration, phase string) (promptDeliveryWaitState, error) {
	if err := d.amux.WaitContent(ctx, paneID, codexWorkingText, timeout); err != nil {
		if !errors.Is(err, amuxapi.ErrWaitContentTimeout) {
			return promptDeliveryWaitError, fmt.Errorf("wait for %q %s: %w", codexWorkingText, phase, err)
		}

		snapshot, captureErr := d.amux.CapturePane(ctx, paneID)
		if captureErr != nil {
			if isPaneGoneError(captureErr) {
				return promptDeliveryWaitAgentGone, fmt.Errorf("%w: pane disappeared while waiting for %q %s", ErrPromptDeliveryNotConfirmed, codexWorkingText, phase)
			}
			return promptDeliveryWaitError, fmt.Errorf("capture pane while waiting for %q %s: %w", codexWorkingText, phase, captureErr)
		}
		if promptDeliveryReturnedToShell(profile, snapshot) {
			return promptDeliveryWaitAgentGone, promptDeliveryFailure(fmt.Sprintf("%s and codex returned to shell", phase), snapshot)
		}
		if snapshot.Exited {
			return promptDeliveryWaitAgentGone, promptDeliveryFailure(fmt.Sprintf("%s and pane exited", phase), snapshot)
		}
		return promptDeliveryWaitTimedOut, promptDeliveryFailure(fmt.Sprintf("wait for %q %s timed out", codexWorkingText, phase), snapshot)
	}
	return promptDeliveryWaitObserved, nil
}

func (d *Daemon) waitForFreshPromptDeliveryMarker(ctx context.Context, paneID string, profile AgentProfile, timeout time.Duration, phase string, baseline promptDeliveryBaseline) (promptDeliveryWaitState, error) {
	deadline := d.now().Add(timeout)

	for {
		snapshot, err := d.capturePromptDeliverySnapshot(ctx, paneID)
		if err != nil {
			if isPaneGoneError(err) {
				return promptDeliveryWaitAgentGone, fmt.Errorf("%w: pane disappeared while waiting for fresh %q %s", ErrPromptDeliveryNotConfirmed, codexWorkingText, phase)
			}
			return promptDeliveryWaitError, fmt.Errorf("capture prompt delivery state while waiting for fresh %q %s: %w", codexWorkingText, phase, err)
		}
		if promptDeliveryHasFreshWorking(baseline, snapshot) {
			return promptDeliveryWaitObserved, nil
		}
		if promptDeliveryReturnedToShell(profile, snapshot) {
			return promptDeliveryWaitAgentGone, promptDeliveryFailure(fmt.Sprintf("%s and codex returned to shell", phase), snapshot)
		}
		if snapshot.Exited {
			return promptDeliveryWaitAgentGone, promptDeliveryFailure(fmt.Sprintf("%s and pane exited", phase), snapshot)
		}
		if !d.now().Before(deadline) {
			return promptDeliveryWaitTimedOut, promptDeliveryFailure(fmt.Sprintf("wait for fresh %q %s timed out", codexWorkingText, phase), snapshot)
		}

		sleepFor := codexWorkingFreshnessPollInterval
		if remaining := deadline.Sub(d.now()); remaining < sleepFor {
			sleepFor = remaining
		}
		if sleepFor <= 0 {
			return promptDeliveryWaitTimedOut, promptDeliveryFailure(fmt.Sprintf("wait for fresh %q %s timed out", codexWorkingText, phase), snapshot)
		}
		if err := d.sleep(ctx, sleepFor); err != nil {
			return promptDeliveryWaitError, fmt.Errorf("wait for fresh %q %s: %w", codexWorkingText, phase, err)
		}
	}
}

func (d *Daemon) capturePromptDeliveryBaseline(ctx context.Context, paneID string) (promptDeliveryBaseline, error) {
	snapshot, err := d.capturePromptDeliverySnapshot(ctx, paneID)
	if err != nil {
		return promptDeliveryBaseline{}, fmt.Errorf("capture prompt delivery baseline: %w", err)
	}
	output := strings.ToLower(snapshot.Output())
	return promptDeliveryBaseline{
		output:     output,
		hasWorking: strings.Contains(output, strings.ToLower(codexWorkingText)),
	}, nil
}

func (d *Daemon) capturePromptDeliverySnapshot(ctx context.Context, paneID string) (PaneCapture, error) {
	history, err := d.amux.CaptureHistory(ctx, paneID)
	if err == nil && len(history.Content) > 0 {
		return history, nil
	}

	snapshot, captureErr := d.amux.CapturePane(ctx, paneID)
	if captureErr == nil {
		return snapshot, nil
	}
	if err != nil {
		return PaneCapture{}, fmt.Errorf("capture history: %w; capture pane: %v", err, captureErr)
	}
	return PaneCapture{}, captureErr
}

func promptDeliveryHasFreshWorking(baseline promptDeliveryBaseline, snapshot PaneCapture) bool {
	needle := strings.ToLower(codexWorkingText)
	current := strings.ToLower(snapshot.Output())
	if current == "" || !strings.Contains(current, needle) {
		return false
	}
	if !baseline.hasWorking {
		return true
	}
	if strings.HasPrefix(current, baseline.output) {
		return strings.Contains(current[len(baseline.output):], needle)
	}

	baseCount := strings.Count(baseline.output, needle)
	currentCount := strings.Count(current, needle)
	if currentCount > baseCount {
		return true
	}
	return strings.LastIndex(current, needle) > strings.LastIndex(baseline.output, needle)
}

func promptDeliveryReturnedToShell(profile AgentProfile, snapshot PaneCapture) bool {
	if !strings.EqualFold(profile.Name, "codex") {
		return false
	}
	command := strings.TrimSpace(snapshot.CurrentCommand)
	if command == "" {
		return false
	}
	base := filepath.Base(strings.Fields(command)[0])
	switch strings.ToLower(base) {
	case "bash", "zsh", "sh", "fish":
		return true
	default:
		return false
	}
}

func promptDeliveryFailure(message string, snapshot PaneCapture) error {
	return fmt.Errorf("%w: %s: %s", ErrPromptDeliveryNotConfirmed, message, describePaneSnapshot(snapshot))
}
