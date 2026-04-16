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

	waits := []time.Duration{defaultAgentHandshakeTimeout, codexPromptDeliveryExtendedTimeout}
	for index, timeout := range waits {
		state, err := d.waitForPromptDeliveryMarker(ctx, paneID, profile, timeout, "after prompt")
		switch state {
		case promptDeliveryWaitObserved:
			return nil
		case promptDeliveryWaitError:
			return err
		case promptDeliveryWaitAgentGone:
			return err
		case promptDeliveryWaitTimedOut:
			if index == len(waits)-1 {
				return err
			}
		default:
			return err
		}

		if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
			return fmt.Errorf("retry prompt delivery: %w", err)
		}
		if err := d.amux.WaitIdle(ctx, paneID, codexPromptRetryIdleProbeTime); err != nil {
			probeState, probeErr := d.waitForPromptDeliveryMarker(ctx, paneID, profile, waits[index+1], "after retry idle failure")
			switch probeState {
			case promptDeliveryWaitObserved:
				return nil
			case promptDeliveryWaitError:
				return probeErr
			case promptDeliveryWaitAgentGone, promptDeliveryWaitTimedOut:
				return probeErr
			default:
				return probeErr
			}
		}
	}

	return fmt.Errorf("%w: wait for %q after prompt", ErrPromptDeliveryNotConfirmed, codexWorkingText)
}

func (d *Daemon) waitForPromptDeliveryMarker(ctx context.Context, paneID string, profile AgentProfile, timeout time.Duration, phase string) (promptDeliveryWaitState, error) {
	if err := d.amux.WaitContent(ctx, paneID, codexWorkingText, timeout); err != nil {
		if !errors.Is(err, amuxapi.ErrWaitContentTimeout) {
			return 0, fmt.Errorf("wait for %q %s: %w", codexWorkingText, phase, err)
		}

		snapshot, captureErr := d.amux.CapturePane(ctx, paneID)
		if captureErr != nil {
			if isPaneGoneError(captureErr) {
				return promptDeliveryWaitAgentGone, fmt.Errorf("%w: pane disappeared while waiting for %q %s", ErrPromptDeliveryNotConfirmed, codexWorkingText, phase)
			}
			return 0, fmt.Errorf("capture pane while waiting for %q %s: %w", codexWorkingText, phase, captureErr)
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
	detail := describePaneSnapshot(snapshot)
	if detail == "" {
		return fmt.Errorf("%w: %s", ErrPromptDeliveryNotConfirmed, message)
	}
	return fmt.Errorf("%w: %s: %s", ErrPromptDeliveryNotConfirmed, message, detail)
}
