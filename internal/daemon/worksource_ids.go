package daemon

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/weill-labs/orca/internal/worksource"
)

var beadsIssueIdentifierPattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*-[a-z0-9]{3}(?:\.\d+)*$`)

type resolvedWorkSourceIDs struct {
	beadsID  string
	linearID string
}

func (d *Daemon) resolveWorkSourceIDs(ctx context.Context, taskID string) (resolvedWorkSourceIDs, bool, error) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return resolvedWorkSourceIDs{}, false, nil
	}

	resolver, ok := d.workSource.(worksource.IDResolver)
	if !ok {
		return resolvedWorkSourceIDs{
			beadsID:  taskID,
			linearID: taskID,
		}, false, nil
	}

	pair, err := resolver.ResolveID(ctx, taskID)
	return resolvedWorkSourceIDs{
		beadsID:  strings.TrimSpace(pair.BeadsID),
		linearID: strings.TrimSpace(pair.LinearID),
	}, true, err
}

func (d *Daemon) linearIssueIDForStatus(ctx context.Context, projectPath, taskID, state string) (string, bool) {
	taskID = strings.TrimSpace(taskID)
	if taskID == "" {
		return "", false
	}
	if isLinearIssueIdentifier(taskID) {
		return taskID, true
	}

	ids, resolved, err := d.resolveWorkSourceIDs(ctx, taskID)
	if !resolved {
		return taskID, true
	}
	if err != nil {
		d.emitIssueStatusSkipped(ctx, projectPath, taskID, state, fmt.Sprintf("resolve Linear issue id: %v", err))
		return "", false
	}
	if ids.linearID == "" {
		d.emitIssueStatusSkipped(ctx, projectPath, taskID, state, "no linked Linear issue id")
		return "", false
	}
	return ids.linearID, true
}

func (d *Daemon) beadsIDForCompletion(ctx context.Context, active ActiveAssignment, status string, outcome worksource.Outcome) (string, bool) {
	taskID := strings.TrimSpace(active.Task.Issue)
	if taskID == "" {
		return "", false
	}

	ids, resolved, err := d.resolveWorkSourceIDs(ctx, taskID)
	if !resolved {
		if !canCompleteRawBeadsID(d.workSource, taskID) {
			d.logWorkSourceCompleteSkip(active, status, outcome, "task id is not a beads issue id")
			return "", false
		}
		return taskID, true
	}
	if err != nil {
		if isBeadsIssueIdentifier(taskID) {
			d.logWorkSourceCompleteRawIDFallback(active, status, outcome, fmt.Sprintf("resolve beads issue id: %v", err))
			return taskID, true
		}
		d.logWorkSourceCompleteSkip(active, status, outcome, fmt.Sprintf("resolve beads issue id: %v", err))
		return "", false
	}
	if ids.beadsID == "" {
		if isBeadsIssueIdentifier(taskID) {
			d.logWorkSourceCompleteRawIDFallback(active, status, outcome, "resolver returned no beads issue id")
			return taskID, true
		}
		d.logWorkSourceCompleteSkip(active, status, outcome, "no linked beads issue id")
		return "", false
	}
	return ids.beadsID, true
}

func canCompleteRawBeadsID(source worksource.Source, taskID string) bool {
	if isManualWorkSource(source) {
		return true
	}
	return isBeadsIssueIdentifier(taskID)
}

func isManualWorkSource(source worksource.Source) bool {
	switch source.(type) {
	case worksource.ManualSource, *worksource.ManualSource:
		return true
	default:
		return false
	}
}

func isBeadsIssueIdentifier(issue string) bool {
	return beadsIssueIdentifierPattern.MatchString(strings.TrimSpace(issue))
}

func (d *Daemon) emitIssueStatusSkipped(ctx context.Context, projectPath, issue, state, reason string) {
	d.emit(ctx, Event{
		Time:    d.now(),
		Type:    EventIssueStatusSkipped,
		Project: projectPath,
		Issue:   issue,
		Message: fmt.Sprintf("skipped Linear issue status update to %q: %s", state, reason),
	})
}
