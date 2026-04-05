package daemon

import (
	"context"
	"fmt"
	"strings"
)

const (
	handshakeStepWait          = "wait for startup idle"
	handshakeStepCapture       = "capture startup output"
	handshakeStepTrustDetected = "trust prompt detected"
	handshakeStepTrustEnter    = "sent Enter to confirm trust prompt"
)

func (d *Daemon) agentHandshake(ctx context.Context, paneID string, profile AgentProfile) error {
	d.handshakeMu.Lock()
	defer d.handshakeMu.Unlock()

	d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepWait)
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
		d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepCapture)

		switch {
		case !trustConfirmed && hasTrustPrompt(profile, output):
			d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepTrustDetected)
			if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
				return fmt.Errorf("confirm trust prompt: %w", err)
			}
			d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepTrustEnter)
			trustConfirmed = true
		case !resumed && hasResumePrompt(profile, output):
			if err := d.resumeAgentInPane(ctx, paneID, profile); err != nil {
				return fmt.Errorf("resume prior session: %w", err)
			}
			resumed = true
		default:
			return nil
		}

		d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepWait)
		if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
			return fmt.Errorf("wait for post-startup action idle: %w", err)
		}
	}
}

func (d *Daemon) emitHandshakeEvent(ctx context.Context, paneID string, profile AgentProfile, message string) {
	event := Event{
		Time:         d.now(),
		Type:         EventWorkerHandshake,
		Project:      d.project,
		PaneID:       paneID,
		AgentProfile: profile.Name,
		Message:      message,
	}

	if worker, err := d.state.WorkerByPane(ctx, d.project, paneID); err == nil {
		event.Issue = worker.Issue
		event.PaneName = worker.PaneName
		if event.AgentProfile == "" {
			event.AgentProfile = worker.AgentProfile
		}
	}

	if tasks, err := d.state.TasksByPane(ctx, d.project, paneID); err == nil && len(tasks) > 0 {
		task := tasks[len(tasks)-1]
		if event.Issue == "" {
			event.Issue = task.Issue
		}
		if event.PaneName == "" {
			event.PaneName = task.PaneName
		}
		event.CloneName = task.CloneName
		event.ClonePath = task.ClonePath
		event.Branch = task.Branch
		if event.AgentProfile == "" {
			event.AgentProfile = task.AgentProfile
		}
	}

	d.emit(ctx, event)
}

func hasTrustPrompt(profile AgentProfile, output string) bool {
	switch normalizedProfileName(profile) {
	case "codex":
		return containsFold(output, "do you trust")
	default:
		return false
	}
}

func hasResumePrompt(profile AgentProfile, output string) bool {
	if len(profile.ResumeSequence) == 0 {
		return false
	}

	switch normalizedProfileName(profile) {
	case "codex":
		return containsFold(output, "resume")
	default:
		return false
	}
}

func normalizedProfileName(profile AgentProfile) string {
	return strings.ToLower(profile.Name)
}

func containsFold(input, needle string) bool {
	return strings.Contains(strings.ToLower(input), needle)
}
