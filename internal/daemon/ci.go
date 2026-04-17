package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ciStateFail                = "fail"
	ciStatePending             = "pending"
	ciStatePass                = "pass"
	ciStateCancel              = "cancel"
	ciStateSkipping            = "skipping"
	maxCINudges                = 3
	ciFailureRenudgePollWindow = 2
)

func (d *Daemon) handlePRChecksPoll(ctx context.Context, update *TaskStateUpdate, profile AgentProfile) bool {
	ciState, err := d.lookupPRChecksState(ctx, update.Active.Task.Project, update.Active.Task.PRNumber)
	if err != nil {
		if d.handleCIPollLookupError(update, profile, "lookup_checks_error", err) {
			return true
		}
		return false
	}
	now := d.now()

	previous := update.Active.Worker.LastCIState
	if ciState != ciStateFail {
		if d.resetCIWorkerState(&update.Active.Worker, ciState, now) {
			update.WorkerChanged = true
		}
		if nextState := taskStateForCIState(ciState); nextState != "" && setTaskState(&update.Active.Task, nextState, now) {
			update.TaskChanged = true
		}
		return false
	}
	if previous != ciStateFail {
		resetCIFailureNudgeState(&update.Active.Worker)
		if d.recordCIFailureNudge(ctx, update, profile, now) {
			update.WorkerChanged = true
		}
		if setTaskState(&update.Active.Task, TaskStateCIPending, now) {
			update.TaskChanged = true
		}
		return false
	}
	if update.Active.Worker.CIEscalated {
		return false
	}
	update.Active.Worker.CIFailurePollCount++
	update.Active.Worker.LastSeenAt = now
	update.WorkerChanged = true
	if setTaskState(&update.Active.Task, TaskStateCIPending, now) {
		update.TaskChanged = true
	}
	if update.Active.Worker.CIFailurePollCount < ciFailureRenudgePollWindow {
		return false
	}
	if update.Active.Worker.CINudgeCount >= maxCINudges {
		update.Active.Worker.CIEscalated = true
		event := d.assignmentEvent(update.Active, profile, EventWorkerCIEscalated, fmt.Sprintf("CI nudges exhausted after %d attempts; lead intervention required", update.Active.Worker.CINudgeCount))
		event.Retry = update.Active.Worker.CINudgeCount
		update.Events = append(update.Events, event)
		return false
	}
	if d.recordCIFailureNudge(ctx, update, profile, now) {
		update.WorkerChanged = true
	}
	return false
}

func (d *Daemon) handleCIPollLookupError(update *TaskStateUpdate, profile AgentProfile, action string, err error) bool {
	if d.appendGitHubRateLimitEvent(update, profile, err) {
		return true
	}
	d.traceCIPoll(update, profile, action, err)
	return false
}

func (d *Daemon) traceCIPoll(update *TaskStateUpdate, profile AgentProfile, action string, err error) {
	if update == nil {
		return
	}

	message := fmt.Sprintf(
		"ci poll trace: issue=%s pr_number=%d action=%s error=%q",
		update.Active.Task.Issue,
		update.Active.Task.PRNumber,
		action,
		err,
	)
	update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventCIPollTrace, message))
	if d.logf != nil {
		d.logf("%s", message)
	}
}

func (d *Daemon) resetCIWorkerState(worker *Worker, ciState string, now time.Time) bool {
	if worker.LastCIState == ciState &&
		worker.CINudgeCount == 0 &&
		worker.CIFailurePollCount == 0 &&
		!worker.CIEscalated {
		return false
	}

	worker.LastCIState = ciState
	resetCIFailureNudgeState(worker)
	worker.LastSeenAt = now
	return true
}

func (d *Daemon) recordCIFailureNudge(ctx context.Context, update *TaskStateUpdate, profile AgentProfile, now time.Time) bool {
	if !d.nudgeForCIFailure(ctx, update, profile) {
		return false
	}

	update.Active.Worker.LastCIState = ciStateFail
	update.Active.Worker.CINudgeCount++
	update.Active.Worker.CIFailurePollCount = 0
	update.Active.Worker.CIEscalated = false
	update.Active.Worker.LastSeenAt = now
	return true
}

func resetCIFailureNudgeState(worker *Worker) {
	worker.CINudgeCount = 0
	worker.CIFailurePollCount = 0
	worker.CIEscalated = false
}

func (d *Daemon) nudgeForCIFailure(ctx context.Context, update *TaskStateUpdate, profile AgentProfile) bool {
	if profile.NudgeCommand == "" {
		return false
	}

	failedChecks, err := d.lookupFailedPRChecks(ctx, update.Active.Task.Project, update.Active.Task.PRNumber)
	if err != nil {
		d.handleCIPollLookupError(update, profile, "lookup_failed_checks_error", err)
		failedChecks = nil
	}
	prompt := ciFailurePrompt(update.Active.Task.PRNumber, failedChecks)

	if err := d.sendPromptAndCommand(ctx, update.Active.Task.PaneID, prompt, profile.NudgeCommand); err != nil {
		if isPaneGoneError(err) {
			d.escalateTaskState(update, profile, "worker pane missing during ci nudge", d.now())
		}
		return false
	}

	update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerNudgedCI, prompt))
	return true
}

func (d *Daemon) lookupPRChecksState(ctx context.Context, projectPath string, prNumber int) (string, error) {
	output, err := runPRChecksCommand(ctx, d.commandRunner(ctx), projectPath, prNumber, "bucket")
	if err != nil {
		return "", wrapGitHubRateLimitError(err, output, d.now())
	}
	return parsePRChecksState(output)
}

func (d *Daemon) lookupFailedPRChecks(ctx context.Context, projectPath string, prNumber int) ([]prCheck, error) {
	output, err := runPRChecksCommand(ctx, d.commandRunner(ctx), projectPath, prNumber, "bucket,name,link")
	if err != nil {
		return nil, wrapGitHubRateLimitError(err, output, d.now())
	}
	return parseFailedPRChecks(output)
}

type prCheck struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
	Link   string `json:"link"`
}

func lookupPRChecksState(ctx context.Context, commands CommandRunner, project string, prNumber int) (string, error) {
	output, err := runPRChecksCommand(ctx, commands, project, prNumber, "bucket")
	if err != nil {
		return "", err
	}
	return parsePRChecksState(output)
}

func parsePRChecksState(output []byte) (string, error) {
	if len(output) == 0 {
		return "", nil
	}

	var checks []struct {
		Bucket string `json:"bucket"`
	}
	if err := json.Unmarshal(output, &checks); err != nil {
		return "", err
	}

	bestState := ""
	bestRank := 0
	for _, check := range checks {
		rank := ciStateRank(check.Bucket)
		if rank > bestRank {
			bestState = check.Bucket
			bestRank = rank
		}
	}
	return bestState, nil
}

func lookupFailedPRChecks(ctx context.Context, commands CommandRunner, project string, prNumber int) ([]prCheck, error) {
	output, err := runPRChecksCommand(ctx, commands, project, prNumber, "bucket,name,link")
	if err != nil {
		return nil, err
	}
	return parseFailedPRChecks(output)
}

func parseFailedPRChecks(output []byte) ([]prCheck, error) {
	if len(output) == 0 {
		return nil, nil
	}

	var checks []prCheck
	if err := json.Unmarshal(output, &checks); err != nil {
		return nil, err
	}

	failed := make([]prCheck, 0, len(checks))
	for _, check := range checks {
		if check.Bucket == ciStateFail {
			failed = append(failed, check)
		}
	}
	return failed, nil
}

func runPRChecksCommand(ctx context.Context, commands CommandRunner, project string, prNumber int, fields string) ([]byte, error) {
	return commands.Run(ctx, project, "gh", "pr", "checks", fmt.Sprintf("%d", prNumber), "--json", fields)
}

func ciFailurePrompt(prNumber int, failedChecks []prCheck) string {
	if len(failedChecks) == 0 {
		return fmt.Sprintf("CI checks failed on PR #%d. Fix the failures and push.", prNumber)
	}
	if len(failedChecks) == 1 {
		check := failedChecks[0]
		prompt := fmt.Sprintf("CI check %s failed on PR #%d.", check.Name, prNumber)
		if check.Link != "" {
			prompt += " Logs: " + check.Link + "."
		}
		return prompt + " Fix the failure and push."
	}

	names := make([]string, 0, len(failedChecks))
	logs := make([]string, 0, len(failedChecks))
	for _, check := range failedChecks {
		names = append(names, check.Name)
		if check.Link != "" {
			logs = append(logs, fmt.Sprintf("%s: %s", check.Name, check.Link))
		}
	}

	prompt := fmt.Sprintf("CI checks %s failed on PR #%d.", strings.Join(names, ", "), prNumber)
	if len(logs) > 0 {
		prompt += " Logs: " + strings.Join(logs, "; ") + "."
	}
	return prompt + " Fix the failures and push."
}

func ciStateRank(state string) int {
	switch state {
	case ciStateFail:
		return 5
	case ciStatePending:
		return 4
	case ciStatePass:
		return 3
	case ciStateCancel:
		return 2
	case ciStateSkipping:
		return 1
	default:
		return 0
	}
}
