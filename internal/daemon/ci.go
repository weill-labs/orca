package daemon

import (
	"context"
	"encoding/json"
	"fmt"
)

const (
	ciStateFail     = "fail"
	ciStatePending  = "pending"
	ciStatePass     = "pass"
	ciStateCancel   = "cancel"
	ciStateSkipping = "skipping"
)

func (d *Daemon) handlePRChecksPoll(active *assignment) {
	ciState, err := d.lookupPRChecksState(active.ctx, active.prNumber)
	if err != nil {
		return
	}

	previous := active.lastCIState
	if ciState != ciStateFail {
		active.lastCIState = ciState
		return
	}
	if previous == ciStateFail {
		return
	}
	if d.nudgeForCIFailure(active) {
		active.lastCIState = ciStateFail
	}
}

func (d *Daemon) nudgeForCIFailure(active *assignment) bool {
	if active.profile.NudgeCommand == "" {
		return false
	}
	if err := d.amux.SendKeys(active.ctx, active.pane.ID, active.profile.NudgeCommand); err != nil {
		return false
	}

	d.emit(active.ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedCI,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		PRNumber:     active.prNumber,
		Message:      "pull request checks failing",
	})
	return true
}

func (d *Daemon) lookupPRChecksState(ctx context.Context, prNumber int) (string, error) {
	output, err := d.commands.Run(ctx, d.project, "gh", "pr", "checks", fmt.Sprintf("%d", prNumber), "--json", "bucket")
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
