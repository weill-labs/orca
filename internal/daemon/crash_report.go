package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
)

const crashReportScrollbackLimit = 100

func (d *Daemon) captureCrashReportForRestart(ctx context.Context, task Task, worker Worker, profile AgentProfile) (Worker, error) {
	paneID := strings.TrimSpace(task.PaneID)
	if paneID == "" {
		return worker, nil
	}

	snapshot, err := d.amux.CapturePane(ctx, paneID)
	if err != nil || !snapshot.Exited {
		return worker, err
	}

	history, err := d.amux.CaptureHistory(ctx, paneID)
	if err != nil {
		return worker, fmt.Errorf("capture crash report from pane %s: %w", paneID, err)
	}

	now := d.now()
	restartAttempt := worker.RestartCount + 1
	if restartAttempt <= 0 {
		restartAttempt = 1
	}

	updatedWorker := worker
	updatedWorker.RestartCount = restartAttempt
	if strings.TrimSpace(updatedWorker.WorkerID) != "" {
		updatedWorker.UpdatedAt = now
		if updatedWorker.CreatedAt.IsZero() {
			updatedWorker.CreatedAt = now
		}
		if updatedWorker.LastSeenAt.IsZero() {
			updatedWorker.LastSeenAt = now
		}
		if err := d.state.PutWorker(ctx, updatedWorker); err != nil {
			return worker, fmt.Errorf("store crash report restart attempt: %w", err)
		}
	}

	event := Event{
		Time:           now,
		Type:           EventWorkerCrashReport,
		Project:        task.Project,
		Issue:          task.Issue,
		WorkerID:       firstNonEmpty(updatedWorker.WorkerID, task.WorkerID),
		PaneID:         paneID,
		PaneName:       assignmentPaneName(task, updatedWorker),
		CloneName:      task.CloneName,
		ClonePath:      task.ClonePath,
		Branch:         task.Branch,
		AgentProfile:   profile.Name,
		RestartAttempt: restartAttempt,
		Scrollback:     crashReportScrollback(history.Content),
		Message:        fmt.Sprintf("captured crash report before restart attempt %d", restartAttempt),
	}
	if event.CloneName == "" && event.ClonePath != "" {
		event.CloneName = filepath.Base(event.ClonePath)
	}
	if event.AgentProfile == "" {
		event.AgentProfile = task.AgentProfile
	}

	if err := d.state.RecordEvent(ctx, event); err != nil {
		return worker, fmt.Errorf("record crash report: %w", err)
	}
	if d.events != nil {
		_ = d.events.Emit(ctx, event)
	}

	return updatedWorker, nil
}

func crashReportScrollback(lines []string) []string {
	if len(lines) <= crashReportScrollbackLimit {
		return append([]string(nil), lines...)
	}
	return append([]string(nil), lines[len(lines)-crashReportScrollbackLimit:]...)
}
