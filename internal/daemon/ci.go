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

func (d *Daemon) handlePRChecksPoll(ctx context.Context, active ActiveAssignment, profile AgentProfile) {
	ciState, err := d.lookupPRChecksState(ctx, active.Task.PRNumber)
	if err != nil {
		return
	}

	previous := active.Worker.LastCIState
	if ciState != ciStateFail {
		if previous != ciState {
			active.Worker.LastCIState = ciState
			active.Worker.UpdatedAt = d.now()
			_ = d.state.PutWorker(ctx, active.Worker)
		}
		return
	}
	if previous == ciStateFail {
		return
	}
	if d.nudgeForCIFailure(ctx, active, profile) {
		active.Worker.LastCIState = ciStateFail
		active.Worker.UpdatedAt = d.now()
		_ = d.state.PutWorker(ctx, active.Worker)
	}
}

func (d *Daemon) nudgeForCIFailure(ctx context.Context, active ActiveAssignment, profile AgentProfile) bool {
	if profile.NudgeCommand == "" {
		return false
	}
	if err := d.amux.SendKeys(ctx, active.Task.PaneID, profile.NudgeCommand); err != nil {
		return false
	}

	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedCI,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
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
