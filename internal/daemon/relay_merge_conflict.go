package daemon

import (
	"context"
	"strings"
)

const relayDefaultBaseBranch = "main"

func relayEventTriggersMergeConflictRefresh(msg relayEventMessage) bool {
	if msg.eventType() != "pull_request" || !msg.merged() {
		return false
	}

	action := msg.action()
	return action == "" || strings.EqualFold(action, "closed")
}

func (d *Daemon) handleRelayMergeConflictRefresh(ctx context.Context, msg relayEventMessage) {
	assignments, err := d.activeAssignmentsForRelayMergeConflictRefresh(ctx, msg)
	if err != nil || len(assignments) == 0 {
		return
	}
	d.dispatchSelectedTaskMonitorChecks(ctx, assignments, taskMonitorCheckMergeConflictPoll)
}

func (d *Daemon) activeAssignmentsForRelayMergeConflictRefresh(ctx context.Context, msg relayEventMessage) ([]ActiveAssignment, error) {
	if repo := strings.TrimSpace(msg.Repo); repo != "" && !d.relayProjectMatches(d.project, repo) {
		return nil, nil
	}
	if !d.relayEventMatchesProjectBaseBranch(ctx, msg, d.project) {
		return nil, nil
	}

	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err != nil {
		return nil, err
	}

	filtered := make([]ActiveAssignment, 0, len(assignments))
	for _, active := range assignments {
		if active.Task.PRNumber == 0 || active.Task.PRNumber == msg.PRNumber {
			continue
		}
		if repo := strings.TrimSpace(msg.Repo); repo != "" && !d.relayProjectMatches(active.Task.Project, repo) {
			continue
		}
		filtered = append(filtered, active)
	}
	return filtered, nil
}

func (d *Daemon) relayEventMatchesProjectBaseBranch(ctx context.Context, msg relayEventMessage, projectPath string) bool {
	baseBranch := strings.TrimSpace(msg.baseBranch())
	if baseBranch == "" {
		return true
	}
	return strings.EqualFold(baseBranch, d.projectBaseBranch(ctx, projectPath))
}

func (msg relayEventMessage) baseBranch() string {
	if branch := strings.TrimSpace(msg.BaseBranch); branch != "" {
		return branch
	}
	if branch := strings.TrimSpace(msg.BaseRef); branch != "" {
		return branch
	}
	return firstNonEmpty(
		relayPayloadSummaryString(msg.PayloadSummary, "base_branch"),
		relayPayloadSummaryString(msg.PayloadSummary, "base_ref"),
	)
}

func (d *Daemon) projectBaseBranch(ctx context.Context, projectPath string) string {
	output, err := d.commandRunner(ctx).Run(ctx, projectPath, "git", "symbolic-ref", "--short", "refs/remotes/origin/HEAD")
	if err != nil {
		return relayDefaultBaseBranch
	}

	baseBranch := strings.TrimSpace(string(output))
	if baseBranch == "" {
		return relayDefaultBaseBranch
	}
	if slash := strings.LastIndex(baseBranch, "/"); slash >= 0 {
		baseBranch = baseBranch[slash+1:]
	}
	baseBranch = strings.TrimSpace(baseBranch)
	if baseBranch == "" {
		return relayDefaultBaseBranch
	}
	return baseBranch
}
