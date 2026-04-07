package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

const (
	codexWorkingText         = "Working"
	codexPromptDeliveryRetry = 10
)

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
			if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
				return fmt.Errorf("wait for prompt retry idle: %w", err)
			}
			continue
		}
		return nil
	}

	return fmt.Errorf("wait for %q after %d retries: %w", codexWorkingText, codexPromptDeliveryRetry, amuxapi.ErrWaitContentTimeout)
}
