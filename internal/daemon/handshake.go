package daemon

import (
	"context"
	"fmt"
	"strings"
)

func (d *Daemon) agentHandshake(ctx context.Context, paneID string, profile AgentProfile) error {
	if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
		return fmt.Errorf("wait for startup idle: %w", err)
	}

	trustConfirmed := false
	resumed := false

	for {
		output, err := d.amux.Capture(ctx, paneID)
		if err != nil {
			return fmt.Errorf("capture startup output: %w", err)
		}

		switch {
		case !trustConfirmed && hasTrustPrompt(profile, output):
			if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
				return fmt.Errorf("confirm trust prompt: %w", err)
			}
			trustConfirmed = true
		case !resumed && hasResumePrompt(profile, output):
			if err := d.amux.SendKeys(ctx, paneID, profile.ResumeSequence...); err != nil {
				return fmt.Errorf("resume prior session: %w", err)
			}
			resumed = true
		default:
			return nil
		}

		if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
			return fmt.Errorf("wait for post-startup action idle: %w", err)
		}
	}
}

func hasTrustPrompt(profile AgentProfile, output string) bool {
	switch strings.ToLower(profile.Name) {
	case "codex":
		return strings.Contains(strings.ToLower(output), "do you trust")
	default:
		return false
	}
}

func hasResumePrompt(profile AgentProfile, output string) bool {
	if len(profile.ResumeSequence) == 0 {
		return false
	}

	switch strings.ToLower(profile.Name) {
	case "codex":
		return strings.Contains(strings.ToLower(output), "resume")
	default:
		return false
	}
}
