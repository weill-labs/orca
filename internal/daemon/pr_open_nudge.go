package daemon

import (
	"context"
	"regexp"
	"strings"
	"time"
)

const (
	openPRNudgeIdleThreshold = 2 * time.Minute
	openPRNudgePrompt        = "Tests appear to be passing and no PR is open yet. Commit, push, and open a PR with gh pr create."
)

var (
	goTestPassPattern = regexp.MustCompile(`(?m)^PASS$`)
	goTestOKPattern   = regexp.MustCompile(`(?m)^ok\s+\S+(?:\s+\d+(?:\.\d+)?s|\s+\(cached\))$`)
)

func outputIndicatesTestsPassed(output string) bool {
	output = strings.TrimSpace(output)
	if output == "" {
		return false
	}

	lower := strings.ToLower(output)
	return strings.Contains(lower, "all tests passed") ||
		strings.Contains(lower, "tests passed") ||
		goTestPassPattern.MatchString(output) ||
		goTestOKPattern.MatchString(output)
}

func (d *Daemon) shouldNudgeIdleWorkerToOpenPR(active ActiveAssignment, output string, now time.Time) bool {
	if active.Task.PRNumber != 0 {
		return false
	}
	if active.Worker.NudgeCount > 0 {
		return false
	}
	if !outputIndicatesTestsPassed(output) {
		return false
	}
	if active.Worker.LastActivityAt.IsZero() {
		return false
	}
	return now.Sub(active.Worker.LastActivityAt) > openPRNudgeIdleThreshold
}

func (d *Daemon) nudgeIdleWorkerToOpenPR(ctx context.Context, update *TaskStateUpdate, profile AgentProfile, now time.Time) {
	update.queueNudge(func(ctx context.Context, d *Daemon, update *TaskStateUpdate) {
		if err := d.sendPromptAndEnter(ctx, update.Active.Task.PaneID, openPRNudgePrompt); err != nil {
			return
		}

		update.Active.Worker.Health = WorkerHealthStuck
		update.Active.Worker.NudgeCount++
		update.Active.Worker.UpdatedAt = now
		update.WorkerChanged = true

		event := d.assignmentEvent(update.Active, profile, EventWorkerNudged, "tests passed but no pull request is open")
		event.Retry = update.Active.Worker.NudgeCount
		update.Events = append(update.Events, event)
	})
}
