package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/weill-labs/orca/internal/amux"
)

const (
	mergedWrapUpPrompt    = "PR merged, wrap up."
	closedWrapUpPrompt    = "PR closed without merging, wrap up."
	postmortemCommand     = "$postmortem"
	postmortemWaitTimeout = 2 * time.Minute
)

func enforceLifecycleProfile(profile AgentProfile) AgentProfile {
	if !strings.EqualFold(profile.Name, "codex") {
		return profile
	}
	profile.StartCommand = ensureFlag(profile.StartCommand, "--yolo")
	if strings.TrimSpace(profile.ReadyPattern) == "" {
		profile.ReadyPattern = codexReadyPattern
	}
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
		Project:      active.Task.Project,
		Issue:        active.Task.Issue,
		WorkerID:     active.Worker.WorkerID,
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

	err = d.sendPostmortem(ctx, active)
	if err == nil {
		return "sent", "postmortem command sent", nil
	}
	if errors.Is(err, ErrPromptDeliveryNotConfirmed) {
		return "failed", err.Error(), err
	}
	return "sent", "postmortem command sent", err
}

func (d *Daemon) sendPostmortem(ctx context.Context, active ActiveAssignment) error {
	if strings.TrimSpace(active.Task.PaneID) == "" {
		return errors.New("worker pane missing")
	}
	if err := d.sendLifecyclePrompt(ctx, active.Task.PaneID, active.Task.AgentProfile, postmortemCommand); err != nil {
		return err
	}
	return d.amux.WaitIdle(ctx, active.Task.PaneID, postmortemWaitTimeout)
}

func (d *Daemon) sendLifecyclePrompt(ctx context.Context, paneID, agentProfile, prompt string) error {
	if strings.EqualFold(agentProfile, "codex") {
		return d.sendAndConfirmWorking(ctx, paneID, prompt)
	}
	return d.amux.SendKeys(ctx, paneID, prompt, "Enter")
}

func (d *Daemon) finishAssignment(ctx context.Context, active ActiveAssignment, status, eventType string, merged bool) error {
	return d.finishAssignmentWithMessageAndPrompt(ctx, active, status, eventType, merged, "", "")
}

func (d *Daemon) finishAssignmentWithMessage(ctx context.Context, active ActiveAssignment, status, eventType string, merged bool, message string) error {
	return d.finishAssignmentWithMessageAndPrompt(ctx, active, status, eventType, merged, "", message)
}

func (d *Daemon) finishAssignmentWithMessageAndPrompt(ctx context.Context, active ActiveAssignment, status, eventType string, merged bool, wrapUpPrompt, message string) error {
	var result error
	cancelled := status == TaskStatusCancelled
	cleanupCtx := context.WithoutCancel(ctx)
	d.stopTaskMonitorForProject(active.Task.Project, active.Task.Issue)

	if prompt := completionWrapUpPrompt(merged, wrapUpPrompt); prompt != "" {
		if err := d.sendLifecyclePrompt(cleanupCtx, active.Task.PaneID, active.Task.AgentProfile, prompt); err != nil {
			if merged {
				d.emitMergeNotifyFailed(cleanupCtx, active, err)
			}
			result = errors.Join(result, err)
		}
		if err := d.amux.WaitIdle(cleanupCtx, active.Task.PaneID, d.mergeGracePeriod); err != nil {
			if merged {
				d.emitMergeNotifyFailed(cleanupCtx, active, err)
			}
			result = errors.Join(result, err)
		}
	}

	if status != TaskStatusFailed {
		postmortemErr := d.ensurePostmortem(cleanupCtx, active)
		if cancelled {
			postmortemErr = ignorePaneAlreadyGoneError(postmortemErr)
		}
		result = errors.Join(result, postmortemErr)
		if active.Task.PaneID != "" {
			metadata, err := d.completionPaneMetadata(cleanupCtx, active, merged)
			if err != nil {
				result = errors.Join(result, err)
			} else {
				metadataErr := d.setPaneMetadata(cleanupCtx, active.Task.PaneID, metadata)
				if cancelled {
					metadataErr = ignorePaneAlreadyGoneError(metadataErr)
				}
				result = errors.Join(result, metadataErr)
			}
		}
	}

	if cancelled {
		if err := ignorePaneAlreadyGoneError(d.amux.KillPane(cleanupCtx, active.Task.PaneID)); err != nil {
			result = errors.Join(result, err)
		}
	}

	clone := Clone{
		Name: active.Task.CloneName,
		Path: active.Task.ClonePath,
	}
	if clone.Name == "" && clone.Path != "" {
		clone.Name = filepath.Base(clone.Path)
	}
	result = errors.Join(result, d.cleanupCloneAndReleaseForProject(cleanupCtx, active.Task.Project, clone, active.Task.Branch))

	active.Task.Status = status
	active.Task.State = TaskStateDone
	active.Task.UpdatedAt = d.now()
	result = errors.Join(result, d.state.PutTask(cleanupCtx, active.Task))
	result = errors.Join(result, d.releaseWorkerClaim(cleanupCtx, active.Worker))
	if active.Task.PRNumber > 0 {
		if err := d.state.DeleteMergeEntry(cleanupCtx, active.Task.Project, active.Task.PRNumber); err != nil && !errors.Is(err, ErrTaskNotFound) {
			result = errors.Join(result, err)
		}
	}
	d.requestRelayReconnect()

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
		Project:      active.Task.Project,
		Issue:        active.Task.Issue,
		WorkerID:     active.Worker.WorkerID,
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

func (d *Daemon) emitMergeNotifyFailed(ctx context.Context, active ActiveAssignment, err error) {
	if err == nil {
		return
	}

	profile, profileErr := d.profileForTask(ctx, active.Task)
	if profileErr != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}

	event := d.assignmentEvent(active, profile, EventWorkerMergeNotifyFailed, err.Error())
	event.WorkerID = active.Worker.WorkerID
	if event.WorkerID == "" {
		event.WorkerID = active.Task.WorkerID
	}
	d.emit(ctx, event)
}

func completionWrapUpPrompt(merged bool, prompt string) string {
	if strings.TrimSpace(prompt) != "" {
		return prompt
	}
	if merged {
		return mergedWrapUpPrompt
	}
	return ""
}

func paneAlreadyGone(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, amux.ErrPaneNotFound) {
		return true
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "pane not found") ||
		strings.Contains(message, "pane missing") ||
		strings.Contains(message, "no such pane")
}

func ignorePaneAlreadyGoneError(err error) error {
	if paneAlreadyGone(err) {
		return nil
	}
	return err
}
