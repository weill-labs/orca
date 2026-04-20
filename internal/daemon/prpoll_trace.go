package daemon

import (
	"context"
	"fmt"
	"strings"
)

func (d *Daemon) tracePRPoll(update *TaskStateUpdate, profile AgentProfile, action string, err error) {
	if update == nil {
		return
	}
	message := formatPRPollTraceMessage(update.Active, action, err)
	if d.logf != nil {
		d.logf("%s", message)
	}
	update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventPRPollTrace, message))
}

func (d *Daemon) emitPRPollTaskTrace(ctx context.Context, task Task, worker Worker, action string, err error) {
	active := ActiveAssignment{Task: task, Worker: worker}
	profile := AgentProfile{Name: task.AgentProfile}
	if loaded, loadErr := d.profileForTask(ctx, task); loadErr == nil {
		profile = loaded
	}
	d.emit(ctx, d.assignmentEvent(active, profile, EventPRPollTrace, formatPRPollTraceMessage(active, action, err)))
}

func (d *Daemon) emitProjectPRPollTrace(ctx context.Context, projectPath, action string, err error) {
	message := fmt.Sprintf("pr poll trace: action=%s", strings.TrimSpace(action))
	if err != nil {
		message += fmt.Sprintf(" error=%q", err)
	}
	event := Event{
		Time:    d.now(),
		Type:    EventPRPollTrace,
		Project: strings.TrimSpace(projectPath),
		Message: message,
	}
	if d.logf != nil {
		d.logf("%s", message)
	}
	d.emit(ctx, event)
}

func formatPRPollTraceMessage(active ActiveAssignment, action string, err error) string {
	message := fmt.Sprintf(
		"pr poll trace: issue=%s pr_number=%d action=%s",
		strings.TrimSpace(active.Task.Issue),
		active.Task.PRNumber,
		strings.TrimSpace(action),
	)
	if err != nil {
		message += fmt.Sprintf(" error=%q", err)
	}
	return message
}
