package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	assignStartupMaxAttempts = 3
	assignStartupRetryDelay  = 2 * time.Second
	// Kept for backward compatibility with handshake_test.go assertions.
	assignHandshakeMaxAttempts = assignStartupMaxAttempts
	assignHandshakeRetryDelay  = assignStartupRetryDelay
)

type assignStartupPhase string

const (
	assignStartupPhaseHandshake assignStartupPhase = "handshake"
	assignStartupPhasePrompt    assignStartupPhase = "prompt"
)

type assignStartupResult struct {
	pane   Pane
	task   Task
	worker Worker
}

type assignStartupAttemptError struct {
	phase assignStartupPhase
	err   error
}

func (e assignStartupAttemptError) Error() string {
	switch e.phase {
	case assignStartupPhasePrompt:
		return fmt.Sprintf("send prompt: %v", e.err)
	default:
		return e.err.Error()
	}
}

func (e assignStartupAttemptError) Unwrap() error {
	return e.err
}

func (d *Daemon) startAssignmentWorker(ctx context.Context, projectPath string, clone Clone, task Task, worker Worker, profile AgentProfile, prompt, paneTitle string) (assignStartupResult, error) {
	result := assignStartupResult{task: task, worker: worker}
	maxAttempts := 1
	if strings.EqualFold(profile.Name, "codex") {
		maxAttempts = assignStartupMaxAttempts
	}

	cleanupCtx := context.WithoutCancel(ctx)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		pane, err := d.spawnWorkerPane(ctx, task, worker.WorkerID, clone.Path, profile)
		if err != nil {
			return result, fmt.Errorf("spawn pane: %w", err)
		}

		attemptTask, attemptWorker := d.assignmentStartupState(projectPath, clone, task, worker, pane)
		result = assignStartupResult{
			pane:   pane,
			task:   attemptTask,
			worker: attemptWorker,
		}

		metadata, err := d.assignmentPaneMetadata(ctx, projectPath, pane.ID, profile.Name, attemptTask.Branch, attemptTask.Issue, paneTitle, attemptTask.PRNumber)
		if err != nil {
			return result, fmt.Errorf("build pane metadata: %w", err)
		}
		if err := d.setPaneMetadata(ctx, pane.ID, metadata); err != nil {
			return result, fmt.Errorf("set pane metadata: %w", err)
		}
		if err := d.state.PutTask(ctx, attemptTask); err != nil {
			return result, fmt.Errorf("store pending task: %w", err)
		}
		if err := d.state.PutWorker(ctx, attemptWorker); err != nil {
			return result, fmt.Errorf("store pending worker: %w", err)
		}

		startupSnapshot, err := d.agentHandshake(ctx, pane.ID, profile)
		if err == nil {
			attemptWorker.LastCapture = startupSnapshot.Output()
			result.worker = attemptWorker
			d.emitAssignStartupTransition(ctx, result, profile, attempt, maxAttempts, assignStartupStepHandshakeReady)
			err = d.withAssignStartupGate(ctx, result, profile, attempt, maxAttempts, func() error {
				if err := d.sendNormalizedPromptAndEnter(ctx, pane.ID, prompt); err != nil {
					return assignStartupAttemptError{phase: assignStartupPhasePrompt, err: err}
				}
				d.emitAssignStartupTransition(ctx, result, profile, attempt, maxAttempts, assignStartupStepPromptSubmitted)
				if err := d.confirmPromptDelivery(ctx, pane.ID, profile); err != nil {
					return assignStartupAttemptError{phase: assignStartupPhasePrompt, err: err}
				}
				d.emitAssignStartupTransition(ctx, result, profile, attempt, maxAttempts, assignStartupStepPromptConfirmed)
				return nil
			})
		}
		if err != nil {
			var phaseErr assignStartupAttemptError
			if errors.As(err, &phaseErr) && phaseErr.phase == assignStartupPhasePrompt {
				scrollback := d.captureStartupRetryScrollback(cleanupCtx, pane.ID)
				err = wrapCodexUpdateRequiredFromScrollback(profile, err, scrollback)
				if errors.Is(err, ErrCodexUpdateRequired) {
					return result, err
				}
				if !shouldRetryAssignStartupPromptDelivery(profile, err) {
					return result, err
				}
				d.emitAssignPromptDeliveryRetry(cleanupCtx, result, profile, attempt, maxAttempts, err, scrollback)
				if attempt == maxAttempts {
					return result, fmt.Errorf("prompt delivery failed after %d attempts: %w", maxAttempts, err)
				}
				if err := ignorePaneAlreadyGoneError(d.amux.KillPane(cleanupCtx, paneKillRef(pane))); err != nil {
					return result, fmt.Errorf("kill pane after prompt delivery failure: %w", err)
				}
				if err := d.sleep(ctx, assignStartupRetryDelay); err != nil {
					return result, fmt.Errorf("wait before prompt delivery retry: %w", err)
				}
				continue
			}

			scrollback := d.captureStartupRetryScrollback(cleanupCtx, pane.ID)
			err = wrapCodexUpdateRequiredFromScrollback(profile, err, scrollback)
			retryHandshake := shouldRetryAssignStartupHandshake(profile, err)
			if !retryHandshake {
				return result, fmt.Errorf("agent handshake: %w", err)
			}
			d.emitAssignHandshakeRetry(cleanupCtx, result, profile, attempt, maxAttempts, err, scrollback)
			if attempt == maxAttempts {
				return result, fmt.Errorf("agent handshake failed after %d attempts: %w", maxAttempts, err)
			}
			if err := ignorePaneAlreadyGoneError(d.amux.KillPane(cleanupCtx, paneKillRef(pane))); err != nil {
				return result, fmt.Errorf("kill pane after handshake failure: %w", err)
			}
			if err := d.sleep(ctx, assignStartupRetryDelay); err != nil {
				return result, fmt.Errorf("wait before handshake retry: %w", err)
			}
			continue
		}
		return result, nil
	}

	return result, fmt.Errorf("assignment startup exhausted after %d attempts", maxAttempts)
}

func (d *Daemon) assignmentStartupState(projectPath string, clone Clone, task Task, worker Worker, pane Pane) (Task, Worker) {
	now := d.now()
	startingTask := task
	startingTask.Project = projectPath
	startingTask.PaneID = pane.ID
	startingTask.PaneName = workerPaneName(task.Issue, worker.WorkerID)
	if startingTask.PaneName == "" {
		startingTask.PaneName = pane.Name
	}
	startingTask.CloneName = clone.Name
	startingTask.ClonePath = clone.Path
	startingTask.UpdatedAt = now

	startingWorker := worker
	startingWorker.Project = projectPath
	startingWorker.PaneID = pane.ID
	startingWorker.PaneName = workerPaneName(task.Issue, worker.WorkerID)
	if startingWorker.PaneName == "" {
		startingWorker.PaneName = pane.Name
	}
	startingWorker.Issue = task.Issue
	startingWorker.ClonePath = clone.Path
	if startingWorker.AgentProfile == "" {
		startingWorker.AgentProfile = task.AgentProfile
	}
	startingWorker.LastActivityAt = now
	startingWorker.LastSeenAt = now
	startingWorker.UpdatedAt = now

	return startingTask, startingWorker
}

func shouldRetryAssignStartupHandshake(profile AgentProfile, err error) bool {
	return strings.EqualFold(profile.Name, "codex") && !errors.Is(err, ErrCodexUpdateRequired) && errors.Is(err, ErrAgentStartupNotReady)
}

func shouldRetryAssignStartupPromptDelivery(profile AgentProfile, err error) bool {
	return strings.EqualFold(profile.Name, "codex") && !errors.Is(err, ErrCodexUpdateRequired) && errors.Is(err, ErrPromptDeliveryNotConfirmed)
}

func (d *Daemon) captureStartupRetryScrollback(ctx context.Context, paneID string) []string {
	if history, err := d.amux.CaptureHistory(ctx, paneID); err == nil && len(history.Content) > 0 {
		return crashReportScrollback(history.Content)
	}

	if snapshot, err := d.amux.CapturePane(ctx, paneID); err == nil && len(snapshot.Content) > 0 {
		return crashReportScrollback(snapshot.Content)
	}

	return nil
}

func (d *Daemon) emitAssignHandshakeRetry(ctx context.Context, result assignStartupResult, profile AgentProfile, attempt, maxAttempts int, err error, scrollback []string) {
	d.emitAssignStartupRetry(ctx, result, profile, EventWorkerHandshakeRetry, "startup handshake failed", attempt, maxAttempts, err, scrollback)
}

func (d *Daemon) emitAssignPromptDeliveryRetry(ctx context.Context, result assignStartupResult, profile AgentProfile, attempt, maxAttempts int, err error, scrollback []string) {
	d.emitAssignStartupRetry(ctx, result, profile, EventWorkerPromptDeliveryRetry, "prompt delivery failed", attempt, maxAttempts, err, scrollback)
}

func (d *Daemon) emitAssignStartupRetry(ctx context.Context, result assignStartupResult, profile AgentProfile, eventType, message string, attempt, maxAttempts int, err error, scrollback []string) {
	event := d.assignmentEvent(ActiveAssignment{
		Task:   result.task,
		Worker: result.worker,
	}, profile, eventType, fmt.Sprintf("%s on attempt %d/%d; %s: %v", message, attempt, maxAttempts, assignStartupRetryStatus(attempt, maxAttempts), err))
	event.Retry = attempt
	if len(scrollback) > 0 {
		event.Scrollback = scrollback
	}
	d.emit(ctx, event)
}

func assignStartupRetryStatus(attempt, maxAttempts int) string {
	if attempt >= maxAttempts {
		return "no retries remaining"
	}
	return fmt.Sprintf("retrying in %s", assignStartupRetryDelay)
}
