package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/weill-labs/orca/internal/linear"
)

var linearIssueIdentifierPattern = regexp.MustCompile(`^[A-Z][A-Z0-9]*-\d+$`)

const (
	ansiDimOn              = "\x1b[2m"
	ansiDimOff             = "\x1b[22m"
	ansiStrikethroughOn    = "\x1b[9m"
	ansiStrikethroughOff   = "\x1b[29m"
	legacyWorkerPanePrefix = "worker-"
	workerPanePrefix       = "w-"
)

type trackedStatus string

const (
	trackedStatusActive    trackedStatus = "active"
	trackedStatusCompleted trackedStatus = "completed"
)

type trackedIssueRef struct {
	ID     string        `json:"id"`
	Status trackedStatus `json:"status,omitempty"`
}

type trackedPRRef struct {
	Number int           `json:"number"`
	Status trackedStatus `json:"status,omitempty"`
}

func (d *Daemon) rollbackAssignment(ctx context.Context, clone Clone, pane Pane, branch string) error {
	return d.rollbackAssignmentForProject(ctx, d.project, clone, pane, branch)
}

func (d *Daemon) rollbackAssignmentForProject(ctx context.Context, projectPath string, clone Clone, pane Pane, branch string) error {
	result := d.amux.KillPane(ctx, pane.ID)
	result = errors.Join(result, d.cleanupCloneAndReleaseForProject(ctx, projectPath, clone, branch))
	return result
}

func (d *Daemon) cleanupCloneAndRelease(ctx context.Context, clone Clone, branch string) error {
	return d.cleanupCloneAndReleaseForProject(ctx, d.project, clone, branch)
}

func (d *Daemon) cleanupCloneAndReleaseForProject(ctx context.Context, projectPath string, clone Clone, branch string) error {
	if clone.CurrentBranch == "" {
		clone.CurrentBranch = branch
	}
	if clone.AssignedTask == "" {
		clone.AssignedTask = branch
	}
	return d.pool.Release(ctx, projectPath, clone)
}

func workerGitIdentity(workerID string) (name string, email string) {
	trimmed := strings.TrimSpace(workerID)
	if trimmed == "" {
		return "", ""
	}
	return "Orca " + trimmed, trimmed + "@orca.local"
}

func (d *Daemon) prepareClone(ctx context.Context, clonePath, branch, workerID string) error {
	commands := [][]string{
		{"checkout", "main"},
		{"pull"},
	}
	if name, email := workerGitIdentity(workerID); name != "" && email != "" {
		commands = append(commands,
			[]string{"config", "user.name", name},
			[]string{"config", "user.email", email},
		)
	}
	commands = append(commands, []string{"checkout", "-B", branch})
	for _, args := range commands {
		if _, err := d.commands.Run(ctx, clonePath, "git", args...); err != nil {
			return err
		}
	}
	return nil
}

func (d *Daemon) prepareAdoptedClone(ctx context.Context, clonePath, branch, workerID string) error {
	commands := [][]string{
		{"fetch", "origin"},
	}
	if name, email := workerGitIdentity(workerID); name != "" && email != "" {
		commands = append(commands,
			[]string{"config", "user.name", name},
			[]string{"config", "user.email", email},
		)
	}
	commands = append(commands, []string{"checkout", "-B", branch, "origin/" + branch})
	for _, args := range commands {
		if _, err := d.commands.Run(ctx, clonePath, "git", args...); err != nil {
			return err
		}
	}
	return nil
}

func (d *Daemon) releaseWorkerClaim(ctx context.Context, worker Worker) error {
	if strings.TrimSpace(worker.WorkerID) == "" {
		return nil
	}

	now := d.now()
	worker.PaneID = ""
	worker.PaneName = worker.WorkerID
	worker.Issue = ""
	worker.ClonePath = ""
	worker.Health = WorkerHealthHealthy
	worker.LastReviewCount = 0
	worker.LastInlineReviewCommentCount = 0
	worker.LastIssueCommentCount = 0
	worker.ReviewNudgeCount = 0
	worker.LastCIState = ""
	worker.CINudgeCount = 0
	worker.CIFailurePollCount = 0
	worker.CIEscalated = false
	worker.LastMergeableState = ""
	worker.NudgeCount = 0
	worker.RestartCount = 0
	worker.LastCapture = ""
	worker.LastActivityAt = time.Time{}
	if worker.CreatedAt.IsZero() {
		worker.CreatedAt = now
	}
	worker.LastSeenAt = now
	worker.UpdatedAt = now
	return d.state.PutWorker(ctx, worker)
}

func taskBlocksAssignment(status string) bool {
	switch status {
	case "", TaskStatusDone, TaskStatusCancelled, TaskStatusFailed:
		return false
	default:
		return true
	}
}

func workerPaneName(issue, stableRef string) string {
	issue = strings.TrimSpace(issue)
	if issue != "" {
		return workerPanePrefix + issue
	}
	return strings.TrimSpace(stableRef)
}

func legacyWorkerPaneName(issue string) string {
	issue = strings.TrimSpace(issue)
	if issue == "" {
		return ""
	}
	return legacyWorkerPanePrefix + issue
}

func stableWorkerRef(task Task, worker Worker) string {
	for _, candidate := range []string{
		strings.TrimSpace(task.WorkerID),
		strings.TrimSpace(worker.WorkerID),
	} {
		if candidate == "" || isNumericPaneRef(candidate) {
			continue
		}
		return candidate
	}

	issue := firstNonEmpty(task.Issue, worker.Issue)
	for _, candidate := range []string{
		strings.TrimSpace(task.PaneName),
		strings.TrimSpace(worker.PaneName),
	} {
		if candidate == "" || isNumericPaneRef(candidate) {
			continue
		}
		if candidate == workerPaneName(issue, "") || candidate == legacyWorkerPaneName(issue) {
			continue
		}
		return candidate
	}

	return ""
}

func assignmentMetadata(agentProfile, branch, task string) map[string]string {
	metadata := map[string]string{
		"agent_profile": agentProfile,
		"branch":        branch,
		"task":          task,
	}
	return metadata
}

func resolveTaskTitle(issue, title string) string {
	title = strings.TrimSpace(title)
	if title != "" {
		return title
	}
	return strings.TrimSpace(issue)
}

func completedTaskTitle(title string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		return ""
	}
	return ansiDimOn + ansiStrikethroughOn + title + ansiStrikethroughOff + ansiDimOff
}

func (d *Daemon) paneTaskTitle(ctx context.Context, paneID, issue string) (string, error) {
	metadata, err := d.amuxClient(ctx).Metadata(ctx, paneID)
	if err != nil {
		return "", fmt.Errorf("load pane metadata: %w", err)
	}
	return resolveTaskTitle(issue, metadata["task"]), nil
}

func firstTitle(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func isLinearIssueIdentifier(issue string) bool {
	return linearIssueIdentifierPattern.MatchString(strings.TrimSpace(issue))
}

func trackedIssueMetadata(issue string, status trackedStatus) map[string]string {
	issue = strings.TrimSpace(issue)
	if issue == "" {
		return nil
	}

	data, err := json.Marshal([]trackedIssueRef{{
		ID:     issue,
		Status: status,
	}})
	if err != nil {
		return nil
	}
	return map[string]string{"tracked_issues": string(data)}
}

func trackedPRMetadata(prNumber int, status trackedStatus) map[string]string {
	if prNumber <= 0 {
		return nil
	}

	data, err := json.Marshal([]trackedPRRef{{
		Number: prNumber,
		Status: status,
	}})
	if err != nil {
		return nil
	}
	return map[string]string{"tracked_prs": string(data)}
}

func taskCompletionMetadata(issue string, prNumber int, merged bool) map[string]string {
	metadata := mergeMetadata(
		map[string]string{"status": "done"},
		trackedIssueMetadata(issue, trackedStatusCompleted),
	)
	if merged {
		metadata = mergeMetadata(metadata, trackedPRMetadata(prNumber, trackedStatusCompleted))
	}
	return metadata
}

func mergeMetadata(parts ...map[string]string) map[string]string {
	var merged map[string]string
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		if merged == nil {
			merged = make(map[string]string)
		}
		for key, value := range part {
			merged[key] = value
		}
	}
	return merged
}

func encodeTrackedIssues(refs []trackedIssueRef) (string, error) {
	data, err := json.Marshal(refs)
	if err != nil {
		return "", fmt.Errorf("marshal tracked issues: %w", err)
	}
	return string(data), nil
}

func encodeTrackedPRs(refs []trackedPRRef) (string, error) {
	data, err := json.Marshal(refs)
	if err != nil {
		return "", fmt.Errorf("marshal tracked prs: %w", err)
	}
	return string(data), nil
}

func trackedIssueStatusForTask(task Task) trackedStatus {
	switch task.Status {
	case "", TaskStatusDone, TaskStatusCancelled, TaskStatusFailed:
		return trackedStatusCompleted
	default:
		return trackedStatusActive
	}
}

func trackedPRStatusForTask(task Task) (trackedStatus, bool) {
	if task.PRNumber <= 0 {
		return "", false
	}

	switch task.Status {
	case TaskStatusDone:
		return trackedStatusCompleted, true
	case "", TaskStatusCancelled, TaskStatusFailed:
		return "", false
	default:
		return trackedStatusActive, true
	}
}

func trackedIssuesForPane(tasks []Task, currentIssue string, currentStatus trackedStatus) []trackedIssueRef {
	refs := make([]trackedIssueRef, 0, len(tasks)+1)
	currentIssue = strings.TrimSpace(currentIssue)
	foundCurrent := false

	for _, task := range tasks {
		issue := strings.TrimSpace(task.Issue)
		if issue == "" {
			continue
		}

		status := trackedIssueStatusForTask(task)
		if issue == currentIssue {
			status = currentStatus
			foundCurrent = true
		}

		refs = append(refs, trackedIssueRef{ID: issue, Status: status})
	}

	if !foundCurrent && currentIssue != "" {
		refs = append(refs, trackedIssueRef{ID: currentIssue, Status: currentStatus})
	}

	return refs
}

func trackedPRsForPane(tasks []Task, currentIssue string, currentPR int, currentStatus *trackedStatus) ([]trackedPRRef, bool) {
	refs := make([]trackedPRRef, 0, len(tasks)+1)
	currentIssue = strings.TrimSpace(currentIssue)
	includeKey := false
	foundCurrent := false

	for _, task := range tasks {
		if task.PRNumber <= 0 {
			continue
		}

		if strings.TrimSpace(task.Issue) == currentIssue {
			includeKey = true
			if currentPR > 0 && currentStatus != nil {
				refs = append(refs, trackedPRRef{Number: currentPR, Status: *currentStatus})
				foundCurrent = true
			}
			continue
		}

		status, ok := trackedPRStatusForTask(task)
		if !ok {
			continue
		}

		refs = append(refs, trackedPRRef{Number: task.PRNumber, Status: status})
		includeKey = true
	}

	if !foundCurrent && currentPR > 0 && currentStatus != nil {
		refs = append(refs, trackedPRRef{Number: currentPR, Status: *currentStatus})
		includeKey = true
	}

	return refs, includeKey
}

func (d *Daemon) trackedPaneMetadata(ctx context.Context, projectPath, paneID, currentIssue string, currentIssueStatus trackedStatus, currentPR int, currentPRStatus *trackedStatus) (map[string]string, error) {
	tasks, err := d.state.TasksByPane(ctx, projectPath, paneID)
	if err != nil {
		return nil, fmt.Errorf("load pane task history: %w", err)
	}

	metadata := make(map[string]string)

	issueRefs := trackedIssuesForPane(tasks, currentIssue, currentIssueStatus)
	if len(issueRefs) > 0 {
		encoded, err := encodeTrackedIssues(issueRefs)
		if err != nil {
			return nil, err
		}
		metadata["tracked_issues"] = encoded
	}

	prRefs, includePRs := trackedPRsForPane(tasks, currentIssue, currentPR, currentPRStatus)
	if includePRs {
		encoded, err := encodeTrackedPRs(prRefs)
		if err != nil {
			return nil, err
		}
		metadata["tracked_prs"] = encoded
	}

	return metadata, nil
}

func (d *Daemon) assignmentPaneMetadata(ctx context.Context, projectPath, paneID, agentProfile, branch, issue, task string, prNumber int) (map[string]string, error) {
	var prStatus *trackedStatus
	if prNumber > 0 {
		status := trackedStatusActive
		prStatus = &status
	}

	tracked, err := d.trackedPaneMetadata(ctx, projectPath, paneID, issue, trackedStatusActive, prNumber, prStatus)
	if err != nil {
		return nil, err
	}
	return mergeMetadata(assignmentMetadata(agentProfile, branch, task), tracked), nil
}

func (d *Daemon) prPaneMetadata(ctx context.Context, active ActiveAssignment, prNumber int) (map[string]string, error) {
	status := trackedStatusActive
	return d.trackedPaneMetadata(ctx, active.Task.Project, active.Task.PaneID, active.Task.Issue, trackedStatusActive, prNumber, &status)
}

func (d *Daemon) completionPaneMetadata(ctx context.Context, active ActiveAssignment, merged bool) (map[string]string, error) {
	var prStatus *trackedStatus
	prNumber := 0
	if merged && active.Task.PRNumber > 0 {
		status := trackedStatusCompleted
		prStatus = &status
		prNumber = active.Task.PRNumber
	}

	tracked, err := d.trackedPaneMetadata(ctx, active.Task.Project, active.Task.PaneID, active.Task.Issue, trackedStatusCompleted, prNumber, prStatus)
	if err != nil {
		return nil, err
	}

	taskTitle, err := d.paneTaskTitle(ctx, active.Task.PaneID, active.Task.Issue)
	if err != nil {
		return nil, err
	}

	return mergeMetadata(map[string]string{
		"status": "done",
		"task":   completedTaskTitle(taskTitle),
	}, tracked), nil
}

func (d *Daemon) setPaneMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	return d.amuxClient(ctx).SetMetadata(ctx, paneID, metadata)
}

func (d *Daemon) resolveAssignmentTitle(ctx context.Context, issue string, title string) string {
	resolved := resolveTaskTitle(issue, title)
	if strings.TrimSpace(title) != "" || d.issueTracker == nil || !isLinearIssueIdentifier(issue) {
		return resolved
	}

	issueTitle, err := d.issueTracker.IssueTitle(ctx, issue)
	if err != nil {
		return resolved
	}
	return resolveTaskTitle(issue, issueTitle)
}

func (d *Daemon) setIssueStatus(ctx context.Context, projectPath, issue, state string) error {
	if d.issueTracker == nil {
		return nil
	}
	err := d.issueTracker.SetIssueStatus(ctx, issue, state)
	if err == nil {
		return nil
	}
	if errors.Is(err, linear.ErrEntityNotFound) {
		d.emit(ctx, Event{
			Time:    d.now(),
			Type:    EventIssueStatusSkipped,
			Project: projectPath,
			Issue:   issue,
			Message: fmt.Sprintf("skipped Linear issue status update to %q: %v", state, err),
		})
		return nil
	}
	return err
}

func (d *Daemon) profileForTask(ctx context.Context, task Task) (AgentProfile, error) {
	profile, err := d.config.AgentProfile(ctx, task.AgentProfile)
	if err != nil {
		return AgentProfile{}, err
	}
	if profile.Name == "" {
		profile.Name = task.AgentProfile
	}
	return profile, nil
}

func (d *Daemon) mergeQueueEvent(active *ActiveAssignment, eventType string, prNumber int, message string, at time.Time) Event {
	event := Event{
		Time:     at,
		Type:     eventType,
		PRNumber: prNumber,
		Message:  message,
	}
	if active == nil {
		return event
	}

	event.Project = active.Task.Project
	event.Issue = active.Task.Issue
	event.WorkerID = active.Task.WorkerID
	event.PaneID = active.Task.PaneID
	event.PaneName = assignmentPaneName(active.Task, active.Worker)
	event.CloneName = active.Task.CloneName
	if event.CloneName == "" && active.Task.ClonePath != "" {
		event.CloneName = filepath.Base(active.Task.ClonePath)
	}
	event.ClonePath = active.Task.ClonePath
	event.Branch = active.Task.Branch
	event.AgentProfile = active.Task.AgentProfile
	return event
}

func (d *Daemon) requireStarted() error {
	if !d.started.Load() {
		return ErrNotStarted
	}
	return nil
}

func (d *Daemon) emit(ctx context.Context, event Event) {
	if err := d.state.RecordEvent(ctx, event); err != nil {
		_ = err
	}
	if d.events != nil {
		if err := d.events.Emit(ctx, event); err != nil {
			_ = err
		}
	}
}

func assignmentPaneName(task Task, worker Worker) string {
	paneName := strings.TrimSpace(task.PaneName)
	if paneName == "" {
		paneName = strings.TrimSpace(worker.PaneName)
	}
	if paneName == "" {
		paneName = strings.TrimSpace(task.WorkerID)
	}
	if paneName == "" {
		paneName = strings.TrimSpace(worker.WorkerID)
	}
	if paneName == "" {
		paneName = strings.TrimSpace(task.PaneID)
	}
	return paneName
}

func isNumericPaneRef(ref string) bool {
	if strings.TrimSpace(ref) == "" {
		return false
	}
	_, err := strconv.Atoi(ref)
	return err == nil
}

func (d *Daemon) normalizeStoredPaneRef(ctx context.Context, task *Task, worker *Worker) error {
	if task == nil {
		return nil
	}

	currentWorker := derefWorker(worker)
	stableRef := stableWorkerRef(*task, currentWorker)
	if stableRef == "" {
		return nil
	}
	paneName := workerPaneName(firstNonEmpty(task.Issue, currentWorker.Issue), stableRef)

	now := d.now()
	if task.WorkerID != stableRef || strings.TrimSpace(task.PaneName) != paneName {
		task.WorkerID = stableRef
		task.PaneName = paneName
		task.UpdatedAt = now
		if err := d.state.PutTask(ctx, *task); err != nil {
			return fmt.Errorf("normalize task pane ref: %w", err)
		}
	}

	if worker == nil {
		return nil
	}

	if worker.WorkerID == stableRef && strings.TrimSpace(worker.PaneName) == paneName {
		return nil
	}

	worker.WorkerID = stableRef
	worker.PaneName = paneName
	worker.LastSeenAt = now
	if err := d.state.PutWorker(ctx, *worker); err != nil {
		return fmt.Errorf("normalize worker pane ref: %w", err)
	}
	return nil
}

func derefWorker(worker *Worker) Worker {
	if worker == nil {
		return Worker{}
	}
	return *worker
}

func (d *Daemon) sendPromptAndEnter(ctx context.Context, paneID, prompt string) error {
	trimmed := strings.TrimRight(prompt, "\r\n")
	if trimmed == "" {
		return errors.New("prompt is empty")
	}
	client := d.amuxClient(ctx)
	if err := client.SendKeys(ctx, paneID, trimmed); err != nil {
		return err
	}
	if err := client.WaitIdleSettle(ctx, paneID, defaultAgentHandshakeTimeout, defaultPromptSettleDuration); err != nil {
		return err
	}
	return client.SendKeys(ctx, paneID, "Enter")
}

func (d *Daemon) startAgentInPane(ctx context.Context, paneID string, profile AgentProfile) error {
	if err := d.amux.SendKeys(ctx, paneID, profile.StartCommand, "Enter"); err != nil {
		return fmt.Errorf("restart agent in pane %s: %w", paneID, err)
	}
	if err := d.agentHandshake(ctx, paneID, profile); err != nil {
		return fmt.Errorf("agent handshake: %w", err)
	}
	return nil
}

func (d *Daemon) resumeAgentInPane(ctx context.Context, paneID string, profile AgentProfile) error {
	if len(profile.ResumeSequence) == 0 {
		return nil
	}

	if !strings.EqualFold(profile.Name, "codex") || len(profile.ResumeSequence) < 3 {
		return d.amux.SendKeys(ctx, paneID, profile.ResumeSequence...)
	}

	if err := d.amux.SendKeys(ctx, paneID, profile.ResumeSequence[:2]...); err != nil {
		return err
	}
	if err := d.amux.WaitContent(ctx, paneID, "›", defaultAgentHandshakeTimeout); err != nil {
		return err
	}
	return d.amux.SendKeys(ctx, paneID, profile.ResumeSequence[2:]...)
}

func ensureTrailingNewline(input string) string {
	if strings.HasSuffix(input, "\n") {
		return input
	}
	return input + "\n"
}
