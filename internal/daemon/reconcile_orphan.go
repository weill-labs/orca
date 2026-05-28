package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/weill-labs/orca/internal/pool"
)

const orphanPaneAdoptionPrompt = "Recovered orphan pane with orca reconcile --adopt-orphans."

type orphanCloneAdopter interface {
	Adopt(ctx context.Context, projectPath string, clone Clone) error
}

func (d *Daemon) adoptOrphanPane(ctx context.Context, projectPath string, finding ReconcileFinding) (string, error) {
	issue := normalizeIssueIdentifier(finding.Issue)
	if issue == "" {
		return "", errors.New("orphan pane issue is required")
	}
	if task, err := d.state.TaskByIssue(ctx, projectPath, issue); err == nil && taskBlocksAssignment(task.Status) {
		return "", fmt.Errorf("task %s is already active", issue)
	} else if err != nil && !errors.Is(err, ErrTaskNotFound) {
		return "", fmt.Errorf("lookup task %s before adoption: %w", issue, err)
	}

	pane, err := d.liveOrphanPane(ctx, finding)
	if err != nil {
		return "", err
	}
	paneRef := firstNonEmpty(pane.ID, pane.Name)
	metadata, err := d.amux.Metadata(ctx, paneRef)
	if err != nil {
		return "", fmt.Errorf("load orphan pane metadata: %w", err)
	}

	snapshot, snapshotErr := d.captureOrphanPane(ctx, paneRef)
	poolRoot := filepath.Join(projectPath, OrcaPoolSubdir)
	rawClonePath := firstNonEmpty(pane.CWD, snapshot.CWD)
	if rawClonePath == "" && snapshotErr != nil {
		return "", fmt.Errorf("capture orphan pane cwd: %w", snapshotErr)
	}
	clonePath := clampToCloneRoot(poolRoot, rawClonePath)
	clonePath, err = pool.ValidateClonePath(poolRoot, clonePath)
	if err != nil {
		return "", fmt.Errorf("validate orphan pane clone path: %w", err)
	}

	branch := firstNonEmpty(metadata["branch"], finding.Branch, issue)
	agentProfile := orphanPaneAgentProfile(metadata, snapshot)
	taskTitle := firstNonEmpty(metadata["task"], issue)
	workerID := d.orphanPaneWorkerID(ctx, projectPath, pane, metadata)
	paneName := firstNonEmpty(pane.Name, finding.PaneName, workerPaneName(issue, workerID))
	now := d.now()

	clone := Clone{
		Name:          filepath.Base(clonePath),
		Path:          clonePath,
		CurrentBranch: branch,
		AssignedTask:  issue,
	}
	if err := d.adoptOrphanClone(ctx, projectPath, clone); err != nil {
		return "", err
	}

	task := Task{
		Project:      projectPath,
		Issue:        issue,
		Status:       TaskStatusActive,
		State:        TaskStateAssigned,
		Prompt:       orphanPaneAdoptionPrompt,
		WorkerID:     workerID,
		PaneID:       strings.TrimSpace(pane.ID),
		PaneName:     paneName,
		CloneName:    clone.Name,
		ClonePath:    clone.Path,
		Branch:       branch,
		AgentProfile: agentProfile,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	worker := Worker{
		Project:        projectPath,
		WorkerID:       workerID,
		PaneID:         task.PaneID,
		PaneName:       paneName,
		Issue:          issue,
		ClonePath:      clone.Path,
		AgentProfile:   agentProfile,
		Health:         WorkerHealthHealthy,
		LastCapture:    snapshot.Output(),
		LastActivityAt: now,
		CreatedAt:      now,
		LastSeenAt:     now,
		UpdatedAt:      now,
	}
	// Store the worker before the task. If the daemon stops or task storage fails
	// after this point, reconcile still sees the pane as orphaned because no
	// active task blocks the finding; the next adoption attempt can complete it.
	if err := d.state.PutWorker(ctx, worker); err != nil {
		return "", fmt.Errorf("store adopted worker: %w", err)
	}
	if err := d.state.PutTask(ctx, task); err != nil {
		return "", fmt.Errorf("store adopted task: %w", err)
	}

	message := "orphan pane adopted into active task"
	if snapshotErr != nil {
		message = appendAdoptionWarning(message, "pane capture failed", snapshotErr)
	}
	if metadata, err := d.assignmentPaneMetadata(ctx, projectPath, paneRef, agentProfile, branch, issue, taskTitle, 0, false, ""); err != nil {
		message = appendAdoptionWarning(message, "metadata refresh failed", err)
	} else if err := d.setPaneMetadata(ctx, paneRef, metadata); err != nil {
		message = appendAdoptionWarning(message, "metadata refresh failed", err)
	}

	active := ActiveAssignment{Task: task, Worker: worker}
	d.emit(ctx, d.assignmentEvent(active, AgentProfile{Name: agentProfile}, EventWorkerRecovered, "orphan pane adopted by reconcile --adopt-orphans"))
	d.requestRelayReconnect()
	return message, nil
}

func (d *Daemon) liveOrphanPane(ctx context.Context, finding ReconcileFinding) (Pane, error) {
	pane := Pane{
		ID:   strings.TrimSpace(finding.PaneID),
		Name: strings.TrimSpace(finding.PaneName),
		CWD:  strings.TrimSpace(finding.ClonePath),
	}
	if pane.ID != "" {
		if err := d.ensureLivePane(ctx, pane.ID); err != nil {
			return Pane{}, fmt.Errorf("orphan pane %s is no longer live: %w", pane.ID, err)
		}
	}
	if pane.CWD != "" {
		return pane, nil
	}

	panes, err := d.amux.ListPanes(ctx)
	if err != nil {
		return Pane{}, fmt.Errorf("list panes for orphan adoption: %w", err)
	}
	for _, candidate := range panes {
		if paneMatchesReference(candidate, pane.ID) || paneMatchesReference(candidate, pane.Name) {
			return candidate, nil
		}
	}
	return Pane{}, fmt.Errorf("orphan pane %s is no longer listed", firstNonEmpty(pane.ID, pane.Name))
}

func (d *Daemon) captureOrphanPane(ctx context.Context, paneRef string) (PaneCapture, error) {
	history, historyErr := d.amux.CaptureHistory(ctx, paneRef)
	if historyErr == nil && paneCaptureHasData(history) {
		return history, nil
	}

	snapshot, snapshotErr := d.amux.CapturePane(ctx, paneRef)
	if snapshotErr == nil {
		return snapshot, nil
	}
	return PaneCapture{}, errors.Join(historyErr, snapshotErr)
}

func paneCaptureHasData(snapshot PaneCapture) bool {
	return len(snapshot.Content) > 0 ||
		strings.TrimSpace(snapshot.CWD) != "" ||
		strings.TrimSpace(snapshot.CurrentCommand) != "" ||
		snapshot.Exited
}

func orphanPaneAgentProfile(metadata map[string]string, snapshot PaneCapture) string {
	if profile := strings.TrimSpace(metadata["agent_profile"]); profile != "" {
		return profile
	}
	if codexPromptTargetRunning(snapshot) || containsFold(snapshot.Output(), "OpenAI Codex") {
		return "codex"
	}
	if command := strings.TrimSpace(snapshot.CurrentCommand); command != "" {
		fields := strings.Fields(command)
		if len(fields) > 0 && strings.EqualFold(filepath.Base(fields[0]), "claude") {
			return "claude"
		}
	}
	if containsFold(snapshot.Output(), "Claude Code") {
		return "claude"
	}
	return "codex"
}

func (d *Daemon) orphanPaneWorkerID(ctx context.Context, projectPath string, pane Pane, metadata map[string]string) string {
	if workerID := strings.TrimSpace(metadata["worker_id"]); workerID != "" {
		return workerID
	}
	if pane.ID != "" {
		if worker, err := d.state.WorkerByPane(ctx, projectPath, pane.ID); err == nil && strings.TrimSpace(worker.WorkerID) != "" {
			return strings.TrimSpace(worker.WorkerID)
		}
	}
	if pane.Name != "" {
		return pane.Name
	}
	if pane.ID != "" {
		return "pane-" + pane.ID
	}
	return "adopted-worker"
}

func (d *Daemon) adoptOrphanClone(ctx context.Context, projectPath string, clone Clone) error {
	adopter, ok := d.pool.(orphanCloneAdopter)
	if !ok {
		return errors.New("orphan clone adoption requires pool implementation with Adopt")
	}
	return adopter.Adopt(ctx, projectPath, clone)
}

func appendAdoptionWarning(message, label string, err error) string {
	if err == nil {
		return message
	}
	return fmt.Sprintf("%s (%s: %v)", message, label, err)
}

func clampToCloneRoot(poolRoot, clonePath string) string {
	root := filepath.Clean(strings.TrimSpace(poolRoot))
	path := filepath.Clean(strings.TrimSpace(clonePath))
	if root == "" || path == "" {
		return clonePath
	}

	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return clonePath
	}
	parts := strings.SplitN(filepath.ToSlash(rel), "/", 2)
	if len(parts) == 0 || parts[0] == "" || parts[0] == "." || parts[0] == ".." {
		return clonePath
	}
	return filepath.Join(root, filepath.FromSlash(parts[0]))
}
