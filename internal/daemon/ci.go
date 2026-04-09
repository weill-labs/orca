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

func (d *Daemon) handlePRChecksPoll(ctx context.Context, update *TaskStateUpdate, profile AgentProfile) {
	ciState, err := d.lookupPRChecksState(ctx, update.Active.Task.Project, update.Active.Task.PRNumber)
	if err != nil {
		return
	}
	now := d.now()

	previous := update.Active.Worker.LastCIState
	if ciState != ciStateFail {
		if d.resetCIWorkerState(&update.Active.Worker, ciState, now) {
			update.WorkerChanged = true
		}
		return
	}
	if previous != ciStateFail {
		resetCIFailureNudgeState(&update.Active.Worker)
		if d.recordCIFailureNudge(ctx, update, profile, now) {
			update.WorkerChanged = true
		}
		return
	}
	if update.Active.Worker.CIEscalated {
		return
	}
	update.Active.Worker.CIFailurePollCount++
	update.Active.Worker.LastSeenAt = now
	update.WorkerChanged = true
	if update.Active.Worker.CIFailurePollCount < ciFailureRenudgePollWindow {
		return
	}
	if update.Active.Worker.CINudgeCount >= maxCINudges {
		update.Active.Worker.CIEscalated = true
		event := d.assignmentEvent(update.Active, profile, EventWorkerCIEscalated, fmt.Sprintf("CI nudges exhausted after %d attempts; lead intervention required", update.Active.Worker.CINudgeCount))
		event.Retry = update.Active.Worker.CINudgeCount
		update.Events = append(update.Events, event)
		return
	}
	if d.recordCIFailureNudge(ctx, update, profile, now) {
		update.WorkerChanged = true
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
		failedChecks = nil
	}
	prompt := ciFailurePrompt(update.Active.Task.PRNumber, failedChecks)

	if err := d.sendPromptAndCommand(ctx, update.Active.Task.PaneID, prompt, profile.NudgeCommand); err != nil {
		return false
	}

	update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerNudgedCI, prompt))
	return true
}

func (d *Daemon) lookupPRChecksState(ctx context.Context, projectPath string, prNumber int) (string, error) {
	return lookupPRChecksState(ctx, d.commandRunner(ctx), projectPath, prNumber)
}

func (d *Daemon) lookupFailedPRChecks(ctx context.Context, projectPath string, prNumber int) ([]prCheck, error) {
	return lookupFailedPRChecks(ctx, d.commandRunner(ctx), projectPath, prNumber)
}

type prCheck struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
	Link   string `json:"link"`
}

func lookupPRChecksState(ctx context.Context, commands CommandRunner, project string, prNumber int) (string, error) {
	output, err := commands.Run(ctx, project, "gh", "pr", "checks", fmt.Sprintf("%d", prNumber), "--json", "bucket")
	if err != nil {
		return "", err
	}
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
	output, err := commands.Run(ctx, project, "gh", "pr", "checks", fmt.Sprintf("%d", prNumber), "--json", "bucket,name,link")
	if err != nil {
		return nil, err
	}
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
