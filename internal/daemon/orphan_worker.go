package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/weill-labs/orca/internal/pool"
	projectpath "github.com/weill-labs/orca/internal/project"
)

type orphanWorkerCleanupMode struct {
	killPane       bool
	taskEvent      bool
	reconcileEvent bool
	branch         string
	message        string
}

func (d *Daemon) cleanupOrphanWorkersForIssue(ctx context.Context, projectPath, issue string, mode orphanWorkerCleanupMode) (int, error) {
	workers, err := d.workersForIssue(ctx, projectPath, issue)
	if err != nil {
		return 0, err
	}

	var result error
	cleaned := 0
	for _, worker := range workers {
		if err := d.cleanupOrphanWorker(ctx, projectPath, worker, mode); err != nil {
			result = errors.Join(result, err)
			continue
		}
		cleaned++
	}
	return cleaned, result
}

func (d *Daemon) reconcileOrphanWorkers(ctx context.Context) {
	workers, err := d.orphanWorkers(ctx, d.project)
	if err != nil {
		if d.logf != nil {
			d.logf("orphan worker reconciliation failed: %v", err)
		}
		return
	}

	for _, worker := range workers {
		if ctx.Err() != nil {
			return
		}
		if d.normalizeClonePathProjectWorkerIfLive(ctx, worker, orphanWorkerCleanupMode{
			reconcileEvent: true,
			message:        "orphan worker row project normalized on daemon startup",
		}) {
			continue
		}
		if err := d.cleanupOrphanWorker(ctx, d.project, worker, orphanWorkerCleanupMode{
			reconcileEvent: true,
			message:        "orphan worker row removed on daemon startup",
		}); err != nil && d.logf != nil {
			d.logf("orphan worker cleanup failed for %s/%s: %v", worker.WorkerID, worker.Issue, err)
		}
	}
}

func (d *Daemon) orphanWorkers(ctx context.Context, projectPath string) ([]Worker, error) {
	workers, err := d.listWorkersForOrphanSweep(ctx, projectPath)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}

	orphaned := make([]Worker, 0)
	for _, worker := range workers {
		issue := normalizeIssueIdentifier(worker.Issue)
		if issue == "" {
			if projectPathFromPoolClone(worker.Project) != "" {
				orphaned = append(orphaned, worker)
			}
			continue
		}
		lookupProjectPath := orphanWorkerCleanupProjectPath(projectPath, worker)
		_, err := d.state.TaskByIssue(ctx, lookupProjectPath, issue)
		if err == nil {
			if projectPathFromPoolClone(worker.Project) != "" {
				orphaned = append(orphaned, worker)
			}
			continue
		} else if !errors.Is(err, ErrTaskNotFound) {
			return nil, fmt.Errorf("lookup task for worker %s: %w", workerRef(worker), err)
		}
		orphaned = append(orphaned, worker)
	}
	sortWorkers(orphaned)
	return orphaned, nil
}

func (d *Daemon) listWorkersForOrphanSweep(ctx context.Context, projectPath string) ([]Worker, error) {
	workers, err := d.state.ListWorkers(ctx, projectPath)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(projectPath) == "" {
		return workers, nil
	}

	allWorkers, err := d.state.ListWorkers(ctx, "")
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(workers))
	for _, worker := range workers {
		seen[workerProjectRef(worker)] = true
	}
	for _, worker := range allWorkers {
		if projectPathFromPoolClone(worker.Project) != strings.TrimSpace(projectPath) {
			continue
		}
		key := workerProjectRef(worker)
		if seen[key] {
			continue
		}
		seen[key] = true
		workers = append(workers, worker)
	}
	return workers, nil
}

func (d *Daemon) workersForIssue(ctx context.Context, projectPath, issue string) ([]Worker, error) {
	issue = normalizeIssueIdentifier(issue)
	if issue == "" {
		return nil, nil
	}

	workers, err := d.state.ListWorkers(ctx, projectPath)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}

	matches := make([]Worker, 0)
	for _, worker := range workers {
		if normalizeIssueIdentifier(worker.Issue) == issue {
			matches = append(matches, worker)
		}
	}
	sortWorkers(matches)
	return matches, nil
}

func (d *Daemon) cleanupOrphanWorker(ctx context.Context, projectPath string, worker Worker, mode orphanWorkerCleanupMode) error {
	cleanupCtx := d.cleanupContext(ctx)
	var result error

	issue := normalizeIssueIdentifier(worker.Issue)
	branch := firstNonEmpty(mode.branch, issue)
	paneID := strings.TrimSpace(worker.PaneID)
	clonePath := strings.TrimSpace(worker.ClonePath)
	cleanupProjectPath := orphanWorkerCleanupProjectPath(projectPath, worker)
	deleteProjectPath := orphanWorkerDeleteProjectPath(projectPath, worker)
	if mode.killPane && paneID != "" && !d.paneHasOtherBlockingTask(cleanupCtx, cleanupProjectPath, paneID, issue) {
		if err := ignorePaneAlreadyGoneError(d.amux.KillPane(cleanupCtx, paneID)); err != nil {
			result = errors.Join(result, fmt.Errorf("kill orphan worker pane %s: %w", paneID, err))
		}
	}

	if clonePath != "" {
		hasMarker, err := pool.HasCloneMarker(clonePath)
		if err != nil {
			result = errors.Join(result, err)
		}
		if hasMarker {
			clone := Clone{
				Name: filepath.Base(clonePath),
				Path: clonePath,
			}
			result = errors.Join(result, d.cleanupCloneAndReleaseForProject(cleanupCtx, cleanupProjectPath, clone, branch))
		}
	}

	if result != nil {
		d.emitOrphanWorkerCleanupFailure(cleanupCtx, cleanupProjectPath, worker, issue, paneID, clonePath, branch, result, mode)
		return result
	}

	ref := workerRef(worker)
	if ref == "" {
		result = errors.Join(result, errors.New("orphan worker has no worker or pane reference"))
	} else if err := d.state.DeleteWorker(cleanupCtx, deleteProjectPath, ref); err != nil && !errors.Is(err, ErrWorkerNotFound) {
		result = errors.Join(result, err)
	}

	if result != nil {
		d.emitOrphanWorkerCleanupFailure(cleanupCtx, cleanupProjectPath, worker, issue, paneID, clonePath, branch, result, mode)
		return result
	}

	d.requestRelayReconnect()
	if mode.taskEvent {
		d.emit(cleanupCtx, Event{
			Time:         d.now(),
			Type:         EventTaskCancelled,
			Project:      projectPath,
			Issue:        issue,
			WorkerID:     strings.TrimSpace(worker.WorkerID),
			PaneID:       paneID,
			PaneName:     strings.TrimSpace(worker.PaneName),
			CloneName:    cloneNameForPath(worker.ClonePath),
			ClonePath:    clonePath,
			Branch:       branch,
			AgentProfile: strings.TrimSpace(worker.AgentProfile),
			Message:      firstNonEmpty(mode.message, "orphan worker cancelled and cleared"),
		})
	}
	if mode.reconcileEvent {
		d.emitOrphanWorkerReconcileFinding(cleanupCtx, cleanupProjectPath, worker, issue, paneID, clonePath, branch, firstNonEmpty(mode.message, "orphan worker row removed"))
	}
	return nil
}

func (d *Daemon) normalizeClonePathProjectWorkerIfLive(ctx context.Context, worker Worker, mode orphanWorkerCleanupMode) bool {
	normalizedProject := projectPathFromPoolClone(worker.Project)
	if normalizedProject == "" {
		return false
	}
	issue := normalizeIssueIdentifier(worker.Issue)
	if issue == "" {
		return false
	}
	task, err := d.state.TaskByIssue(ctx, normalizedProject, issue)
	if err != nil || !workerMatchesTask(worker, task) {
		return false
	}

	cleanupCtx := d.cleanupContext(ctx)
	originalProject := strings.TrimSpace(worker.Project)
	worker.Project = normalizedProject
	var result error
	if err := d.state.PutWorker(cleanupCtx, worker); err != nil {
		result = errors.Join(result, fmt.Errorf("normalize worker project: %w", err))
	} else if ref := workerRef(worker); ref == "" {
		result = errors.Join(result, errors.New("clone-path-project worker has no worker or pane reference"))
	} else if err := d.state.DeleteWorker(cleanupCtx, originalProject, ref); err != nil && !errors.Is(err, ErrWorkerNotFound) {
		result = errors.Join(result, fmt.Errorf("delete malformed worker row: %w", err))
	}

	paneID := strings.TrimSpace(worker.PaneID)
	clonePath := strings.TrimSpace(worker.ClonePath)
	branch := firstNonEmpty(mode.branch, issue)
	if result != nil {
		d.emitOrphanWorkerCleanupFailure(cleanupCtx, normalizedProject, worker, issue, paneID, clonePath, branch, result, mode)
		return true
	}
	d.requestRelayReconnect()
	if mode.reconcileEvent {
		d.emitOrphanWorkerReconcileFinding(cleanupCtx, normalizedProject, worker, issue, paneID, clonePath, branch, firstNonEmpty(mode.message, "orphan worker row project normalized"))
	}
	return true
}

func orphanWorkerCleanupProjectPath(projectPath string, worker Worker) string {
	if project := projectPathFromPoolClone(worker.Project); project != "" {
		return project
	}
	if project := strings.TrimSpace(worker.Project); project != "" {
		return project
	}
	if project := projectPathFromPoolClone(worker.ClonePath); project != "" {
		return project
	}
	return strings.TrimSpace(projectPath)
}

func orphanWorkerDeleteProjectPath(projectPath string, worker Worker) string {
	if project := strings.TrimSpace(worker.Project); project != "" {
		return project
	}
	return strings.TrimSpace(projectPath)
}

func projectPathFromPoolClone(clonePath string) string {
	return projectpath.ProjectRootFromPoolClonePath(clonePath)
}

func (d *Daemon) emitOrphanWorkerCleanupFailure(ctx context.Context, projectPath string, worker Worker, issue, paneID, clonePath, branch string, err error, mode orphanWorkerCleanupMode) {
	if !mode.reconcileEvent || err == nil {
		return
	}
	d.emitOrphanWorkerReconcileFinding(ctx, projectPath, worker, issue, paneID, clonePath, branch, fmt.Sprintf("orphan worker cleanup failed: %v", err))
}

func (d *Daemon) emitOrphanWorkerReconcileFinding(ctx context.Context, projectPath string, worker Worker, issue, paneID, clonePath, branch, message string) {
	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventReconcileFinding,
		Project:      projectPath,
		Issue:        issue,
		WorkerID:     strings.TrimSpace(worker.WorkerID),
		PaneID:       paneID,
		PaneName:     strings.TrimSpace(worker.PaneName),
		CloneName:    cloneNameForPath(clonePath),
		ClonePath:    clonePath,
		Branch:       branch,
		AgentProfile: strings.TrimSpace(worker.AgentProfile),
		Message:      fmt.Sprintf("reconcile %s: %s", ReconcileOrphanWorker, message),
	})
}

func (d *Daemon) paneHasOtherBlockingTask(ctx context.Context, projectPath, paneID, issue string) bool {
	tasks, err := d.state.TasksByPane(ctx, projectPath, paneID)
	if err != nil {
		if d.logf != nil {
			d.logf("orphan worker pane task lookup failed for %s: %v", paneID, err)
		}
		return true
	}
	for _, task := range tasks {
		if normalizeIssueIdentifier(task.Issue) == issue {
			continue
		}
		if taskBlocksAssignment(task.Status) {
			return true
		}
	}
	return false
}

func sortWorkers(workers []Worker) {
	sort.Slice(workers, func(i, j int) bool {
		left := workerRef(workers[i])
		right := workerRef(workers[j])
		if left != right {
			return left < right
		}
		return normalizeIssueIdentifier(workers[i].Issue) < normalizeIssueIdentifier(workers[j].Issue)
	})
}

func workerMatchesTask(worker Worker, task Task) bool {
	workerID := strings.TrimSpace(worker.WorkerID)
	taskWorkerID := strings.TrimSpace(task.WorkerID)
	return workerID == "" || taskWorkerID == "" || workerID == taskWorkerID
}

func workerProjectRef(worker Worker) string {
	return strings.TrimSpace(worker.Project) + "\x00" + workerRef(worker)
}

func workerRef(worker Worker) string {
	return firstNonEmpty(worker.WorkerID, worker.PaneID, worker.PaneName)
}

func cloneNameForPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	return filepath.Base(path)
}
