package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	ReconcileRecoverableGhost = "recoverable_ghost"
	ReconcileAbandoned        = "abandoned"
	ReconcileStuckCleanup     = "stuck_cleanup"
	ReconcileOrphanPane       = "orphan_pane"

	reconcileActionReported  = "reported"
	reconcileActionFixed     = "fixed"
	reconcileActionFixFailed = "fix_failed"

	reconcilePRStateNone   = "none"
	reconcilePRStateOpen   = "open"
	reconcilePRStateMerged = "merged"
	reconcilePRStateClosed = "closed"
)

var reconcileWorkerPaneNamePattern = regexp.MustCompile(`^w-([A-Z][A-Z0-9]*-\d+)$`)

type ReconcileRequest struct {
	Project string `json:"project,omitempty"`
	Fix     bool   `json:"fix,omitempty"`
}

type ReconcileResult struct {
	Project  string             `json:"project"`
	Fix      bool               `json:"fix"`
	Fixed    int                `json:"fixed"`
	Findings []ReconcileFinding `json:"findings"`
}

type ReconcileFinding struct {
	Kind     string `json:"kind"`
	Issue    string `json:"issue,omitempty"`
	Status   string `json:"status,omitempty"`
	State    string `json:"state,omitempty"`
	PaneID   string `json:"pane_id,omitempty"`
	PaneName string `json:"pane_name,omitempty"`
	Branch   string `json:"branch,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
	PRState  string `json:"pr_state,omitempty"`
	Action   string `json:"action"`
	Message  string `json:"message"`
}

type reconcilePRInfo struct {
	number int
	state  string
	branch string
}

func (d *Daemon) Reconcile(ctx context.Context, req ReconcileRequest) (ReconcileResult, error) {
	projectPath := strings.TrimSpace(req.Project)
	if projectPath == "" {
		projectPath = d.project
	}

	result := ReconcileResult{
		Project: projectPath,
		Fix:     req.Fix,
	}

	tasks, err := d.state.NonTerminalTasks(ctx, projectPath)
	if err != nil {
		return result, fmt.Errorf("list non-terminal tasks: %w", err)
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].Issue < tasks[j].Issue })

	var fixErr error
	for _, task := range tasks {
		finding, ok, err := d.reconcileTaskDrift(ctx, task)
		if err != nil {
			return result, err
		}
		if !ok {
			continue
		}

		if req.Fix && reconcileFindingSafelyFixable(finding) {
			if err := d.fixMergedReconcileFinding(ctx, task, finding); err != nil {
				finding.Action = reconcileActionFixFailed
				finding.Message = finding.Message + ": " + err.Error()
				d.emitReconcileFinding(ctx, projectPath, finding)
				result.Findings = append(result.Findings, finding)
				fixErr = errors.Join(fixErr, err)
				continue
			}
			finding.Action = reconcileActionFixed
			result.Fixed++
		}

		d.emitReconcileFinding(ctx, projectPath, finding)
		result.Findings = append(result.Findings, finding)
	}

	orphanFindings, err := d.reconcilePaneDrift(ctx, projectPath)
	if err != nil {
		return result, errors.Join(fixErr, err)
	}
	for _, finding := range orphanFindings {
		d.emitReconcileFinding(ctx, projectPath, finding)
		result.Findings = append(result.Findings, finding)
	}

	sort.Slice(result.Findings, func(i, j int) bool {
		left := result.Findings[i]
		right := result.Findings[j]
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.Issue != right.Issue {
			return left.Issue < right.Issue
		}
		return left.PaneID < right.PaneID
	})

	return result, fixErr
}

func (d *Daemon) reconcileTaskDrift(ctx context.Context, task Task) (ReconcileFinding, bool, error) {
	if task.Status != TaskStatusActive {
		return ReconcileFinding{}, false, nil
	}

	paneID := strings.TrimSpace(task.PaneID)
	paneExists := false
	if paneID != "" {
		exists, _, err := d.paneExists(ctx, paneID)
		if err != nil {
			return ReconcileFinding{}, false, fmt.Errorf("check pane for %s: %w", task.Issue, err)
		}
		paneExists = exists
	}

	prInfo, err := d.reconcileTaskPRInfo(ctx, task)
	if err != nil {
		return ReconcileFinding{}, false, fmt.Errorf("lookup PR state for %s: %w", task.Issue, err)
	}

	finding := taskReconcileFinding(task, prInfo)
	switch {
	case !paneExists && prInfo.state == reconcilePRStateMerged:
		finding.Kind = ReconcileRecoverableGhost
		finding.Message = "task is active but its pane is missing and its PR is merged"
		return finding, true, nil
	case !paneExists:
		finding.Kind = ReconcileAbandoned
		finding.Message = "task is active but its pane is missing and no safe automatic recovery is available"
		return finding, true, nil
	case paneExists && (prInfo.state == reconcilePRStateMerged || prInfo.state == reconcilePRStateClosed):
		finding.Kind = ReconcileStuckCleanup
		finding.Message = "task still tracks a live pane after its PR reached a terminal state"
		return finding, true, nil
	default:
		return ReconcileFinding{}, false, nil
	}
}

func (d *Daemon) reconcilePaneDrift(ctx context.Context, projectPath string) ([]ReconcileFinding, error) {
	panes, err := d.amux.ListPanes(ctx)
	if err != nil {
		return nil, fmt.Errorf("list amux panes: %w", err)
	}
	sort.Slice(panes, func(i, j int) bool {
		left := panes[i].Ref()
		right := panes[j].Ref()
		if left != right {
			return left < right
		}
		return panes[i].ID < panes[j].ID
	})

	findings := make([]ReconcileFinding, 0)
	for _, pane := range panes {
		issue := issueFromReconcileWorkerPane(pane)
		if issue == "" {
			continue
		}

		task, err := d.state.TaskByIssue(ctx, projectPath, issue)
		if err == nil && taskBlocksAssignment(task.Status) {
			continue
		}
		if err != nil && !errors.Is(err, ErrTaskNotFound) {
			return nil, fmt.Errorf("lookup task for pane %s: %w", pane.Ref(), err)
		}

		findings = append(findings, ReconcileFinding{
			Kind:     ReconcileOrphanPane,
			Issue:    issue,
			PaneID:   strings.TrimSpace(pane.ID),
			PaneName: strings.TrimSpace(pane.Name),
			Action:   reconcileActionReported,
			Message:  "worker pane exists but orca has no active task for it",
		})
	}

	return findings, nil
}

func (d *Daemon) reconcileTaskPRInfo(ctx context.Context, task Task) (reconcilePRInfo, error) {
	info := reconcilePRInfo{
		number: task.PRNumber,
		state:  reconcilePRStateNone,
		branch: strings.TrimSpace(task.Branch),
	}

	if info.number > 0 {
		state, err := d.reconcilePRStateForNumber(ctx, task.Project, info.number)
		if err != nil {
			return reconcilePRInfo{}, err
		}
		info.state = state
		return info, nil
	}

	branch := strings.TrimSpace(task.Branch)
	if branch == "" {
		branch = strings.TrimSpace(task.Issue)
	}
	if branch != "" {
		number, merged, err := d.lookupOpenOrMergedPRNumber(ctx, task.Project, branch)
		if err != nil {
			return reconcilePRInfo{}, err
		}
		if number > 0 {
			info.number = number
			info.branch = branch
			if merged {
				info.state = reconcilePRStateMerged
			} else {
				info.state = reconcilePRStateOpen
			}
			return info, nil
		}
	}

	number, discoveredBranch, err := d.findPRByIssueID(ctx, task.Project, task.Issue)
	if err != nil {
		return reconcilePRInfo{}, err
	}
	if number == 0 {
		return info, nil
	}

	state, err := d.reconcilePRStateForNumber(ctx, task.Project, number)
	if err != nil {
		return reconcilePRInfo{}, err
	}
	info.number = number
	info.state = state
	if strings.TrimSpace(discoveredBranch) != "" {
		info.branch = discoveredBranch
	}
	return info, nil
}

func (d *Daemon) reconcilePRStateForNumber(ctx context.Context, projectPath string, prNumber int) (string, error) {
	terminal, err := d.lookupPRTerminalState(ctx, projectPath, prNumber)
	if err != nil {
		return "", err
	}
	switch {
	case terminal.merged:
		return reconcilePRStateMerged, nil
	case terminal.closedWithoutMerge:
		return reconcilePRStateClosed, nil
	default:
		return reconcilePRStateOpen, nil
	}
}

func taskReconcileFinding(task Task, prInfo reconcilePRInfo) ReconcileFinding {
	prState := prInfo.state
	if prState == "" {
		prState = reconcilePRStateNone
	}
	branch := strings.TrimSpace(task.Branch)
	if strings.TrimSpace(prInfo.branch) != "" {
		branch = strings.TrimSpace(prInfo.branch)
	}
	return ReconcileFinding{
		Issue:    strings.TrimSpace(task.Issue),
		Status:   strings.TrimSpace(task.Status),
		State:    strings.TrimSpace(task.State),
		PaneID:   strings.TrimSpace(task.PaneID),
		PaneName: strings.TrimSpace(task.PaneName),
		Branch:   branch,
		PRNumber: prInfo.number,
		PRState:  prState,
		Action:   reconcileActionReported,
	}
}

func reconcileFindingSafelyFixable(finding ReconcileFinding) bool {
	return finding.PRState == reconcilePRStateMerged &&
		(finding.Kind == ReconcileRecoverableGhost || finding.Kind == ReconcileStuckCleanup)
}

func (d *Daemon) fixMergedReconcileFinding(ctx context.Context, task Task, finding ReconcileFinding) error {
	active := d.reconcileActiveAssignment(ctx, task)
	if active.Task.PRNumber == 0 {
		active.Task.PRNumber = finding.PRNumber
	}
	if strings.TrimSpace(active.Task.Branch) == "" {
		active.Task.Branch = finding.Branch
	}
	if strings.TrimSpace(active.Task.State) == "" {
		active.Task.State = normalizeTaskState(active.Task)
	}

	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}

	message := "pull request merged"
	if err := d.setIssueStatus(ctx, active.Task.Project, active.Task.Issue, IssueStateDone); err != nil {
		message = fmt.Sprintf("pull request merged (failed to update Linear issue status: %v)", err)
	}
	d.emit(ctx, d.assignmentEvent(active, profile, EventPRMerged, message))

	if finding.Kind == ReconcileRecoverableGhost {
		return d.finishMergedAssignmentWithoutPane(ctx, active)
	}
	return d.finishAssignmentWithMessage(ctx, active, TaskStatusDone, EventTaskCompleted, true, "task finished")
}

func (d *Daemon) reconcileActiveAssignment(ctx context.Context, task Task) ActiveAssignment {
	worker := Worker{
		Project:      task.Project,
		WorkerID:     task.WorkerID,
		PaneID:       task.PaneID,
		PaneName:     task.PaneName,
		Issue:        task.Issue,
		ClonePath:    task.ClonePath,
		AgentProfile: task.AgentProfile,
	}
	if strings.TrimSpace(task.WorkerID) != "" {
		if found, err := d.state.WorkerByID(ctx, task.Project, task.WorkerID); err == nil {
			worker = found
		}
	} else if strings.TrimSpace(task.PaneID) != "" {
		if found, err := d.state.WorkerByPane(ctx, task.Project, task.PaneID); err == nil {
			worker = found
		}
	}
	return ActiveAssignment{Task: task, Worker: worker}
}

func (d *Daemon) finishMergedAssignmentWithoutPane(ctx context.Context, active ActiveAssignment) error {
	var result error
	cleanupCtx := d.cleanupContext(ctx)
	d.stopTaskMonitorForProject(active.Task.Project, active.Task.Issue)

	profile, err := d.profileForTask(cleanupCtx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}
	d.emit(cleanupCtx, Event{
		Time:         d.now(),
		Type:         EventWorkerPostmortem,
		Project:      active.Task.Project,
		Issue:        active.Task.Issue,
		WorkerID:     active.Worker.WorkerID,
		PaneID:       active.Task.PaneID,
		PaneName:     assignmentPaneName(active.Task, active.Worker),
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      "postmortem skipped: worker pane missing",
	})

	clone := Clone{
		Name: active.Task.CloneName,
		Path: active.Task.ClonePath,
	}
	if clone.Name == "" && clone.Path != "" {
		clone.Name = filepath.Base(clone.Path)
	}
	result = errors.Join(result, d.cleanupCloneAndReleaseForProject(cleanupCtx, active.Task.Project, clone, active.Task.Branch))

	active.Task.Status = TaskStatusDone
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

	d.emit(cleanupCtx, d.assignmentEvent(active, profile, EventTaskCompleted, "task finished"))
	return result
}

func (d *Daemon) emitReconcileFinding(ctx context.Context, projectPath string, finding ReconcileFinding) {
	message := finding.Message
	if message == "" {
		message = "reconcile drift detected"
	}
	message = fmt.Sprintf("reconcile %s: %s", finding.Kind, message)
	d.emit(ctx, Event{
		Time:     d.now(),
		Type:     EventReconcileFinding,
		Project:  projectPath,
		Issue:    finding.Issue,
		PaneID:   finding.PaneID,
		PaneName: finding.PaneName,
		Branch:   finding.Branch,
		PRNumber: finding.PRNumber,
		Message:  message,
	})
}

func issueFromReconcileWorkerPane(pane Pane) string {
	name := strings.TrimSpace(pane.Name)
	if name == "" {
		return ""
	}
	match := reconcileWorkerPaneNamePattern.FindStringSubmatch(name)
	if len(match) != 2 {
		return ""
	}
	return strings.ToUpper(match[1])
}
