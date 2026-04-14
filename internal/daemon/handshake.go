package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

const (
	handshakeStepWait             = "wait for startup idle"
	handshakeStepWaitTrustContent = "wait for trust prompt content"
	handshakeStepWaitReadyContent = "wait for startup ready content"
	handshakeStepCapture          = "capture startup output"
	handshakeStepTrustDetected    = "trust prompt detected"
	handshakeStepTrustEnter       = "sent Enter to confirm trust prompt"
	handshakeStepReadyValidated   = "validated startup ready content"

	codexReadyPattern = "›"
)

var ErrAgentStartupNotReady = errors.New("agent startup not ready")

func (d *Daemon) agentHandshake(ctx context.Context, paneID string, profile AgentProfile) (PaneCapture, error) {
	d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepWait)
	if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
		return PaneCapture{}, fmt.Errorf("wait for startup idle: %w", err)
	}

	resumed := false

	for {
		trustConfirmed, err := d.confirmTrustPromptIfPresent(ctx, paneID, profile)
		if err != nil {
			return PaneCapture{}, err
		}
		if trustConfirmed {
			continue
		}

		output, err := d.amux.Capture(ctx, paneID)
		if err != nil {
			return PaneCapture{}, fmt.Errorf("capture startup output: %w", err)
		}
		d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepCapture)

		switch {
		case !resumed && hasResumePrompt(profile, output):
			if err := d.resumeAgentInPane(ctx, paneID, profile); err != nil {
				return PaneCapture{}, fmt.Errorf("resume prior session: %w", err)
			}
			resumed = true
		default:
			validated, err := d.waitForReadyMarker(ctx, paneID, profile, startupOutputSnapshot(output))
			if err != nil {
				return PaneCapture{}, err
			}
			return validated, nil
		}

		d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepWait)
		if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
			return PaneCapture{}, fmt.Errorf("wait for post-startup action idle: %w", err)
		}
	}
}

func (d *Daemon) waitForReadyMarker(ctx context.Context, paneID string, profile AgentProfile, snapshot PaneCapture) (PaneCapture, error) {
	pattern, ok := readyPatternText(profile)
	if !ok {
		return snapshot, nil
	}

	validated := snapshot
	if !hasReadyPattern(profile, snapshot.Output()) {
		d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepWaitReadyContent)
		if err := d.amux.WaitContent(ctx, paneID, pattern, defaultAgentHandshakeTimeout); err != nil {
			waitErr := fmt.Errorf("wait for ready pattern %q: %w", pattern, err)
			return PaneCapture{}, fmt.Errorf("%w: %w", ErrAgentStartupNotReady, waitErr)
		}

		var err error
		validated, err = d.amux.CapturePane(ctx, paneID)
		if err != nil {
			return PaneCapture{}, fmt.Errorf("capture ready output: %w", err)
		}
		d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepCapture)
	}

	if !hasReadyPattern(profile, validated.Output()) {
		return PaneCapture{}, fmt.Errorf("%w: validate ready pattern %q: %s", ErrAgentStartupNotReady, pattern, describePaneSnapshot(validated))
	}

	d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepReadyValidated)
	return validated, nil
}

func (d *Daemon) confirmTrustPromptIfPresent(ctx context.Context, paneID string, profile AgentProfile) (bool, error) {
	prompt, ok := trustPromptText(profile)
	if !ok {
		return false, nil
	}

	d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepWaitTrustContent)
	if err := d.amux.WaitContent(ctx, paneID, prompt, defaultTrustPromptTimeout); err != nil {
		if errors.Is(err, amuxapi.ErrWaitContentTimeout) {
			return false, nil
		}
		return false, fmt.Errorf("wait for trust prompt: %w", err)
	}

	d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepTrustDetected)
	if err := d.amux.SendKeys(ctx, paneID, "Enter"); err != nil {
		return false, fmt.Errorf("confirm trust prompt: %w", err)
	}
	d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepTrustEnter)
	d.emitHandshakeEvent(ctx, paneID, profile, handshakeStepWait)
	if err := d.amux.WaitIdle(ctx, paneID, defaultAgentHandshakeTimeout); err != nil {
		return false, fmt.Errorf("wait for post-startup action idle: %w", err)
	}
	return true, nil
}

func (d *Daemon) emitHandshakeEvent(ctx context.Context, paneID string, profile AgentProfile, message string) {
	event := Event{
		Time:         d.now(),
		Type:         EventWorkerHandshake,
		PaneID:       paneID,
		AgentProfile: profile.Name,
		Message:      message,
	}

	if worker, err := d.state.WorkerByPane(ctx, d.project, paneID); err == nil {
		event.Project = worker.Project
		event.Issue = worker.Issue
		event.WorkerID = worker.WorkerID
		event.PaneName = worker.PaneName
		if event.AgentProfile == "" {
			event.AgentProfile = worker.AgentProfile
		}
	}

	if tasks, err := d.state.TasksByPane(ctx, d.project, paneID); err == nil && len(tasks) > 0 {
		task := tasks[len(tasks)-1]
		if event.Project == "" {
			event.Project = task.Project
		}
		if event.Issue == "" {
			event.Issue = task.Issue
		}
		if event.WorkerID == "" {
			event.WorkerID = task.WorkerID
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
	if event.Project == "" {
		event.Project = d.project
	}

	d.emit(ctx, event)
}

func hasTrustPrompt(profile AgentProfile, output string) bool {
	prompt, ok := trustPromptText(profile)
	return ok && containsFold(output, prompt)
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

func hasReadyPattern(profile AgentProfile, output string) bool {
	pattern, ok := readyPatternText(profile)
	return ok && containsFold(output, pattern)
}

func normalizedProfileName(profile AgentProfile) string {
	return strings.ToLower(profile.Name)
}

func containsFold(input, needle string) bool {
	return strings.Contains(strings.ToLower(input), needle)
}

func trustPromptText(profile AgentProfile) (string, bool) {
	switch normalizedProfileName(profile) {
	case "codex":
		return "do you trust", true
	default:
		return "", false
	}
}

func readyPatternText(profile AgentProfile) (string, bool) {
	if pattern := strings.TrimSpace(profile.ReadyPattern); pattern != "" {
		return pattern, true
	}

	switch normalizedProfileName(profile) {
	case "codex":
		return codexReadyPattern, true
	default:
		return "", false
	}
}

func describePaneSnapshot(snapshot PaneCapture) string {
	parts := make([]string, 0, 2)
	if command := strings.TrimSpace(snapshot.CurrentCommand); command != "" {
		parts = append(parts, fmt.Sprintf("current command %q", command))
	}
	if output := strings.TrimSpace(snapshot.Output()); output != "" {
		parts = append(parts, fmt.Sprintf("pane output %q", output))
	}
	if len(parts) == 0 {
		return "pane output empty"
	}
	return strings.Join(parts, "; ")
}

func startupOutputSnapshot(output string) PaneCapture {
	output = strings.TrimSpace(output)
	if output == "" {
		return PaneCapture{}
	}
	return PaneCapture{Content: strings.Split(output, "\n")}
}
