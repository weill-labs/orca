package daemon

import (
	"errors"
	"strings"
	"time"
)

const (
	TaskStateAssigned      = "assigned"
	TaskStatePRDetected    = "pr_detected"
	TaskStateCIPending     = "ci_pending"
	TaskStateReviewPending = "review_pending"
	TaskStateMerged        = "merged"
	TaskStateDone          = "done"
	TaskStateEscalated     = "escalated"
)

func initialTaskState(prNumber int) string {
	if prNumber > 0 {
		return TaskStatePRDetected
	}
	return TaskStateAssigned
}

func normalizeTaskState(task Task) string {
	if state := strings.TrimSpace(task.State); state != "" {
		return state
	}

	switch task.Status {
	case TaskStatusDone, TaskStatusCancelled, TaskStatusFailed:
		return TaskStateDone
	}
	return initialTaskState(task.PRNumber)
}

func taskStateForAssignment(active ActiveAssignment) string {
	if active.Worker.Health == WorkerHealthEscalated {
		return TaskStateEscalated
	}
	if active.Task.Status == TaskStatusDone || active.Task.Status == TaskStatusCancelled || active.Task.Status == TaskStatusFailed {
		return TaskStateDone
	}
	if active.Task.State == TaskStateMerged {
		return TaskStateMerged
	}
	if active.Task.PRNumber == 0 {
		return TaskStateAssigned
	}

	switch active.Worker.LastCIState {
	case ciStatePending, ciStateFail:
		return TaskStateCIPending
	case ciStatePass, ciStateCancel, ciStateSkipping:
		return TaskStateReviewPending
	}

	if active.Task.State == TaskStateReviewPending ||
		active.Worker.LastMergeableState != "" ||
		active.Worker.LastReviewCount > 0 ||
		active.Worker.LastInlineReviewCommentCount > 0 ||
		active.Worker.LastIssueCommentCount > 0 ||
		active.Worker.ReviewNudgeCount > 0 {
		return TaskStateReviewPending
	}

	return TaskStatePRDetected
}

func taskStateForCIState(ciState string) string {
	switch ciState {
	case ciStatePending, ciStateFail:
		return TaskStateCIPending
	case ciStatePass, ciStateCancel, ciStateSkipping:
		return TaskStateReviewPending
	default:
		return ""
	}
}

func setTaskState(task *Task, state string, now time.Time) bool {
	if task == nil {
		return false
	}

	state = strings.TrimSpace(state)
	if state == "" {
		state = normalizeTaskState(*task)
	}
	if task.State == state {
		return false
	}

	task.State = state
	task.UpdatedAt = now
	return true
}

func isPaneGoneError(err error) bool {
	return errors.Is(err, ErrPaneGone) || paneAlreadyGone(err)
}

func (d *Daemon) escalateTaskState(update *TaskStateUpdate, profile AgentProfile, reason string, now time.Time) {
	if update == nil {
		return
	}

	workerChanged := false
	if update.Active.Worker.Health != WorkerHealthEscalated {
		update.Active.Worker.Health = WorkerHealthEscalated
		update.Active.Worker.LastSeenAt = now
		update.WorkerChanged = true
		workerChanged = true
	}
	taskChanged := setTaskState(&update.Active.Task, TaskStateEscalated, now)
	if taskChanged {
		update.TaskChanged = true
	}

	if workerChanged || taskChanged {
		event := d.assignmentEvent(update.Active, profile, EventWorkerEscalated, reason)
		event.Retry = update.Active.Worker.NudgeCount
		update.Events = append(update.Events, event)
	}
}
