package daemon

import (
	"context"
	"fmt"
	"strings"
)

const (
	assignStartupStepHandshakeReady  = "startup handshake ready"
	assignStartupStepWaitGate        = "waiting for codex startup gate"
	assignStartupStepGateAcquired    = "acquired codex startup gate"
	assignStartupStepPromptSubmitted = "startup prompt submitted; awaiting delivery confirmation"
	assignStartupStepPromptConfirmed = "startup prompt delivery confirmed"
	assignStartupStepGateReleased    = "released codex startup gate"
)

func (d *Daemon) withAssignStartupGate(ctx context.Context, result assignStartupResult, profile AgentProfile, attempt, maxAttempts int, fn func() error) error {
	if !strings.EqualFold(profile.Name, "codex") {
		return fn()
	}

	d.emitAssignStartupTransition(ctx, result, profile, attempt, maxAttempts, assignStartupStepWaitGate)
	d.codexStartupMu.Lock()
	d.emitAssignStartupTransition(ctx, result, profile, attempt, maxAttempts, assignStartupStepGateAcquired)
	defer func() {
		d.emitAssignStartupTransition(ctx, result, profile, attempt, maxAttempts, assignStartupStepGateReleased)
		d.codexStartupMu.Unlock()
	}()

	return fn()
}

func (d *Daemon) emitAssignStartupTransition(ctx context.Context, result assignStartupResult, profile AgentProfile, attempt, maxAttempts int, message string) {
	event := d.assignmentEvent(ActiveAssignment{
		Task:   result.task,
		Worker: result.worker,
	}, profile, EventWorkerStartupTransition, fmt.Sprintf("attempt %d/%d: %s", attempt, maxAttempts, message))
	event.Retry = attempt
	d.emit(ctx, event)
}
