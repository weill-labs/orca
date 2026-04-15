package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

const (
	codexWorkingText         = "Working"
	codexPromptDeliveryRetry = 10
)

// Codex can occasionally miss the initial prompt delivery. The full failure path
// intentionally waits through 11 content checks and 10 idle settles so Orca only
// rolls back after exhausting explicit retries.
func (d *Daemon) confirmPromptDelivery(ctx context.Context, paneID string, profile AgentProfile) error {
	if !strings.EqualFold(profile.Name, "codex") {
		return nil
	}

	for attempt := 0; attempt <= codexPromptDeliveryRetry; attempt++ {
		if err := d.amux.WaitContent(ctx, paneID, codexWorkingText, defaultAgentHandshakeTimeout); err != nil {
			if !errors.Is(err, amuxapi.ErrWaitContentTimeout) {
				return fmt.Errorf("wait for %q after prompt: %w", codexWorkingText, err)
			}
			if attempt == codexPromptDeliveryRetry {
				break
			}
			if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
				return fmt.Errorf("retry prompt delivery: %w", err)
			}
			if err := d.amux.WaitIdle(ctx, paneID, 5*time.Second); err != nil {
				if waitErr := d.amux.WaitContent(ctx, paneID, codexWorkingText, defaultAgentHandshakeTimeout); waitErr == nil {
					return nil
				} else if !errors.Is(waitErr, amuxapi.ErrWaitContentTimeout) {
					return fmt.Errorf("wait for %q after retry idle failure: %w", codexWorkingText, waitErr)
				}
				continue
			}
			continue
		}
		return nil
	}

	return fmt.Errorf("wait for %q after %d retries: %w", codexWorkingText, codexPromptDeliveryRetry, amuxapi.ErrWaitContentTimeout)
}
