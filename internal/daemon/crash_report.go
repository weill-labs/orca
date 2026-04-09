package daemon

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

const crashReportScrollbackLimit = 100

func (d *Daemon) captureCrashReportForRestart(ctx context.Context, task Task, worker Worker, profile AgentProfile) (Worker, error) {
	paneID := strings.TrimSpace(task.PaneID)
	if paneID == "" {
		return worker, nil
	}

	snapshot, err := d.amux.CapturePane(ctx, paneID)
	if err != nil {
		return worker, fmt.Errorf("capture pane %s before restart: %w", paneID, err)
	}
	if !snapshot.Exited {
		return worker, nil
	}

	history, err := d.amux.CaptureHistory(ctx, paneID)
	if err != nil {
		return worker, fmt.Errorf("capture crash report from pane %s: %w", paneID, err)
	}

	now := d.now()
	restartAttempt, firstCrashAt := nextRestartAttempt(worker, now)

	updatedWorker := worker
	updatedWorker.RestartCount = restartAttempt
	updatedWorker.FirstCrashAt = firstCrashAt
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

	event := crashReportEvent(task, updatedWorker, profile, history.Content, restartAttempt, now)

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

func nextRestartAttempt(worker Worker, now time.Time) (restartAttempt int, firstCrashAt time.Time) {
	restartAttempt = worker.RestartCount + 1
	if restartAttempt <= 0 {
		restartAttempt = 1
	}

	firstCrashAt = worker.FirstCrashAt
	switch {
	case firstCrashAt.IsZero() || firstCrashAt.After(now):
		firstCrashAt = now
	case now.Sub(firstCrashAt) > exitedPaneRestartWindow:
		restartAttempt = 1
		firstCrashAt = now
	}

	return restartAttempt, firstCrashAt
}

func crashReportEvent(task Task, worker Worker, profile AgentProfile, history []string, restartAttempt int, now time.Time) Event {
	scrollback := crashReportScrollback(history)
	event := Event{
		Time:           now,
		Type:           EventWorkerCrashReport,
		Project:        firstNonEmpty(task.Project, worker.Project),
		Issue:          task.Issue,
		WorkerID:       firstNonEmpty(worker.WorkerID, task.WorkerID),
		PaneID:         strings.TrimSpace(task.PaneID),
		PaneName:       assignmentPaneName(task, worker),
		CloneName:      task.CloneName,
		ClonePath:      task.ClonePath,
		Branch:         task.Branch,
		AgentProfile:   profile.Name,
		RestartAttempt: restartAttempt,
		Scrollback:     scrollback,
		Message:        fmt.Sprintf("captured crash report before restart attempt %d (%d lines)", restartAttempt, len(scrollback)),
	}
	if event.CloneName == "" && event.ClonePath != "" {
		event.CloneName = filepath.Base(event.ClonePath)
	}
	if event.AgentProfile == "" {
		event.AgentProfile = task.AgentProfile
	}
	return event
}
