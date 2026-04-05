package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	mergedWrapUpPrompt    = "PR merged, wrap up."
	postmortemCommand     = "$postmortem"
	postmortemWaitTimeout = 2 * time.Minute
)

func enforceLifecycleProfile(profile AgentProfile) AgentProfile {
	if !strings.EqualFold(profile.Name, "codex") {
		return profile
	}
	profile.StartCommand = ensureFlag(profile.StartCommand, "--yolo")
	return profile
}

func ensureFlag(command, flag string) string {
	command = strings.TrimSpace(command)
	if command == "" || flag == "" {
		return command
	}

	parts := strings.Fields(command)
	if slices.Contains(parts[1:], flag) {
		return command
	}
	return command + " " + flag
}

func (d *Daemon) ensurePostmortem(ctx context.Context, active ActiveAssignment) error {
	status, message, err := d.postmortemStatus(ctx, active)

	profile, profileErr := d.profileForTask(ctx, active.Task)
	if profileErr != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}

	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerPostmortem,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      fmt.Sprintf("postmortem %s: %s", status, message),
	})
	return err
}

func (d *Daemon) postmortemStatus(ctx context.Context, active ActiveAssignment) (string, string, error) {
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return "skipped", fmt.Sprintf("load agent profile: %v", err), err
	}

	if !profile.PostmortemEnabled {
		return "skipped", "postmortem disabled for agent profile", nil
	}

	return "sent", "postmortem command sent", d.sendPostmortem(ctx, active)
}

func (d *Daemon) sendPostmortem(ctx context.Context, active ActiveAssignment) error {
	if strings.TrimSpace(active.Task.PaneID) == "" {
		return errors.New("worker pane missing")
	}
	if err := d.amux.SendKeys(ctx, active.Task.PaneID, postmortemCommand, "Enter"); err != nil {
		return err
	}
	return d.amux.WaitIdle(ctx, active.Task.PaneID, postmortemWaitTimeout)
}

func (d *Daemon) finishAssignment(ctx context.Context, active ActiveAssignment, status, eventType string, merged bool) error {
	return d.finishAssignmentWithMessage(ctx, active, status, eventType, merged, "")
}

func (d *Daemon) finishAssignmentWithMessage(ctx context.Context, active ActiveAssignment, status, eventType string, merged bool, message string) error {
	var result error
	cleanupCtx := context.WithoutCancel(ctx)

	if merged {
		if err := d.amux.SendKeys(cleanupCtx, active.Task.PaneID, mergedWrapUpPrompt); err != nil {
			result = errors.Join(result, err)
		}
		if err := d.amux.WaitIdle(cleanupCtx, active.Task.PaneID, d.mergeGracePeriod); err != nil {
			result = errors.Join(result, err)
		}
	}

	if status != TaskStatusFailed {
		result = errors.Join(result, d.ensurePostmortem(cleanupCtx, active))
		if active.Task.PaneID != "" {
			metadata, err := d.completionPaneMetadata(cleanupCtx, active, merged)
			if err != nil {
				result = errors.Join(result, err)
			} else {
				result = errors.Join(result, d.setPaneMetadata(cleanupCtx, active.Task.PaneID, metadata))
			}
		}
	}

	if status == TaskStatusCancelled {
		result = errors.Join(result, d.amux.KillPane(cleanupCtx, active.Task.PaneID))
	}

	clone := Clone{
		Name: active.Task.CloneName,
		Path: active.Task.ClonePath,
	}
	if clone.Name == "" && clone.Path != "" {
		clone.Name = filepath.Base(clone.Path)
	}
	result = errors.Join(result, d.cleanupCloneAndRelease(cleanupCtx, clone, active.Task.Branch))

	active.Task.Status = status
	active.Task.UpdatedAt = d.now()
	result = errors.Join(result, d.state.PutTask(cleanupCtx, active.Task))
	result = errors.Join(result, d.state.DeleteWorker(cleanupCtx, d.project, active.Task.PaneID))
	if active.Task.PRNumber > 0 {
		if err := d.state.DeleteMergeEntry(cleanupCtx, d.project, active.Task.PRNumber); err != nil && !errors.Is(err, ErrTaskNotFound) {
			result = errors.Join(result, err)
		}
	}

	if message == "" {
		message = "task finished"
		switch status {
		case TaskStatusCancelled:
			message = "task cancelled"
		case TaskStatusFailed:
			message = "task failed"
		}
	}

	profile, err := d.profileForTask(cleanupCtx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}
	d.emit(cleanupCtx, Event{
		Time:         d.now(),
		Type:         eventType,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    clone.Name,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      message,
	})
	return result
}
