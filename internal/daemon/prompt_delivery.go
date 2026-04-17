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
	codexWorkingConfirmationAttempts   = 3
	codexPromptDeliveryExtendedTimeout = 2 * defaultAgentHandshakeTimeout
	codexPromptRetryIdleProbeTime      = 5 * time.Second
)

var ErrPromptDeliveryNotConfirmed = errors.New("prompt delivery not confirmed")

type promptDeliveryWaitState int

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

	state, err := d.waitForPromptDeliveryMarker(ctx, paneID, profile, defaultAgentHandshakeTimeout, "after prompt")
	switch state {
	case promptDeliveryWaitObserved:
		return nil
	case promptDeliveryWaitError, promptDeliveryWaitAgentGone:
		return err
	}

	if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
		return fmt.Errorf("retry prompt delivery: %w", err)
	}
	if err := d.amux.WaitIdle(ctx, paneID, codexPromptRetryIdleProbeTime); err != nil {
		probeState, probeErr := d.waitForPromptDeliveryMarker(ctx, paneID, profile, codexPromptDeliveryExtendedTimeout, "after retry idle failure")
		if probeState == promptDeliveryWaitObserved {
			return nil
		}
		return probeErr
	}

	finalState, finalErr := d.waitForPromptDeliveryMarker(ctx, paneID, profile, codexPromptDeliveryExtendedTimeout, "after retry enter")
	if finalState == promptDeliveryWaitObserved {
		return nil
	}
	return finalErr
}

// Post-start lifecycle prompts can race with Codex input buffering. Send the
// prompt once, then retry only Enter until Codex transitions to Working or the
// bounded retry budget is exhausted.
func (d *Daemon) sendAndConfirmWorking(ctx context.Context, paneID, prompt string) error {
	if err := d.amux.SendKeys(ctx, paneID, prompt, "Enter"); err != nil {
		return err
	}

	profile := AgentProfile{Name: "codex"}
	phase := "after prompt"
	for attempt := 1; attempt <= codexWorkingConfirmationAttempts; attempt++ {
		state, err := d.waitForPromptDeliveryMarker(ctx, paneID, profile, defaultAgentHandshakeTimeout, phase)
		switch state {
		case promptDeliveryWaitObserved:
			return nil
		case promptDeliveryWaitError, promptDeliveryWaitAgentGone:
			return err
		}
		if attempt == codexWorkingConfirmationAttempts {
			return err
		}
		if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
			return fmt.Errorf("retry prompt delivery: %w", err)
		}
		phase = "after retry enter"
	}

	return fmt.Errorf("%w: working confirmation exhausted", ErrPromptDeliveryNotConfirmed)
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
