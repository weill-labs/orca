package daemon

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"
)

var markdownHeadingPattern = regexp.MustCompile(`^(#{1,6})\s+(.+?)\s*$`)

const (
	maxReviewNudges                 = 3
	reviewNudgeIdleWindowMultiplier = 2
)

type reviewWorkerState struct {
	reviewCount        int
	inlineCommentCount int
	commentCount       int
	commentWatermark   string
	reviewNudgeCount   int
}

type prReviewPayload struct {
	ReviewDecision string     `json:"reviewDecision"`
	Reviews        []prReview `json:"reviews"`
	ReviewComments []prReviewComment
	Comments       []prComment `json:"comments"`
}

type prReview struct {
	State       string    `json:"state"`
	Body        string    `json:"body"`
	SubmittedAt time.Time `json:"submittedAt"`
	Author      struct {
		Login string `json:"login"`
	} `json:"author"`
}

type prComment struct {
	ID     string `json:"id"`
	Body   string `json:"body"`
	Author struct {
		Login string `json:"login"`
	} `json:"author"`
}

type prFeedback struct {
	Author string
	Path   string
	Line   int
	Body   string
}

func (d *Daemon) checkTaskCapture(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}

	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return update
	}

	snapshot, err := d.amuxClient(ctx).CapturePane(ctx, active.Task.PaneID)
	if err != nil {
		if isPaneGoneError(err) {
			d.escalateTaskState(&update, profile, "worker pane missing during capture check", d.now())
		}
		return update
	}

	now := d.now()
	if snapshot.Exited {
		return d.checkExitedPaneCapture(active, profile, snapshot, now)
	}

	output := snapshot.Output()
	d.recordWorkerOutput(&update, profile, output, now)
	d.maybeResetExitedPaneRestartWindow(&update, now)

	if d.shouldNudgeIdleWorkerToOpenPR(update.Active, output, now) {
		d.nudgeIdleWorkerToOpenPR(ctx, &update, profile, now)
		return update
	}
	if d.matchesStuckPattern(profile, output) {
		d.nudgeOrEscalate(ctx, &update, profile, "matched stuck text pattern", now)
		return update
	}
	if profile.StuckTimeout > 0 && now.Sub(update.Active.Worker.LastActivityAt) >= profile.StuckTimeout {
		d.nudgeOrEscalate(ctx, &update, profile, "idle timeout exceeded", now)
	}

	return update
}

func (d *Daemon) checkExitedPaneCapture(active ActiveAssignment, profile AgentProfile, snapshot PaneCapture, now time.Time) TaskStateUpdate {
	return d.exitedPaneStateUpdate(active, profile, snapshot, now, false)
}

func (d *Daemon) checkTaskReviewPoll(ctx context.Context, active ActiveAssignment, profile AgentProfile) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}
	previousReviewState := reviewWorkerState{
		reviewCount:        active.Worker.LastReviewCount,
		inlineCommentCount: active.Worker.LastInlineReviewCommentCount,
		commentCount:       active.Worker.LastIssueCommentCount,
		reviewNudgeCount:   active.Worker.ReviewNudgeCount,
	}
	traceAndReturn := func(current *reviewWorkerState, blockingCount int, idleResult, action string, persisted bool) TaskStateUpdate {
		d.traceReviewPollDecision(&update, profile, previousReviewState, current, blockingCount, idleResult, action, persisted)
		return update
	}

	payload, ok, err := d.lookupPRReviews(ctx, active.Task.Project, active.Task.PRNumber)
	if err != nil {
		d.appendGitHubRateLimitEvent(&update, profile, err)
		return traceAndReturn(nil, 0, reviewPollIdleNotChecked, "lookup_reviews_error", false)
	}
	if !ok {
		return traceAndReturn(nil, 0, reviewPollIdleNotChecked, "reviews_unavailable", false)
	}
	now := d.now()
	items := reviewItems(payload.Reviews, payload.ReviewComments)

	reviewCount := len(payload.Reviews)
	inlineCommentCount := len(payload.ReviewComments)
	commentCount := len(payload.Comments)
	previousReviewCount := update.Active.Worker.LastReviewCount
	previousInlineCommentCount := update.Active.Worker.LastInlineReviewCommentCount
	previousCommentCount := update.Active.Worker.LastIssueCommentCount
	previousCommentWatermark := update.Active.Worker.LastIssueCommentWatermark
	nextReviewState := reviewWorkerState{
		reviewCount:        reviewCount,
		inlineCommentCount: inlineCommentCount,
		commentCount:       commentCount,
		commentWatermark:   issueCommentWatermark(payload.Comments),
		reviewNudgeCount:   update.Active.Worker.ReviewNudgeCount,
	}
	if payload.ReviewDecision != "CHANGES_REQUESTED" && nextReviewState.reviewNudgeCount != 0 {
		nextReviewState.reviewNudgeCount = 0
	}
	if previousReviewCount > reviewCount || previousInlineCommentCount > inlineCommentCount || previousCommentCount > commentCount {
		d.persistReviewWorkerState(&update.Active.Worker, nextReviewState, now)
		update.WorkerChanged = true
		return traceAndReturn(&nextReviewState, 0, reviewPollIdleNotChecked, "persist_review_watermark_reset", true)
	}
	if previousReviewCount == reviewCount &&
		previousInlineCommentCount == inlineCommentCount &&
		previousCommentCount == commentCount &&
		previousCommentWatermark == nextReviewState.commentWatermark {
		action := "no_new_review_feedback"
		persisted := false
		if nextReviewState.reviewNudgeCount != update.Active.Worker.ReviewNudgeCount {
			d.persistReviewWorkerState(&update.Active.Worker, nextReviewState, now)
			update.WorkerChanged = true
			action = "persist_review_nudge_reset"
			persisted = true
		}
		return traceAndReturn(&nextReviewState, 0, reviewPollIdleNotChecked, action, persisted)
	}
	if payload.ReviewDecision == "APPROVED" || latestReviewContainsLGTM(payload.Reviews) {
		d.persistReviewWorkerState(&update.Active.Worker, nextReviewState, now)
		update.WorkerChanged = true
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventReviewApproved, "pull request approved"))
		return traceAndReturn(&nextReviewState, 0, reviewPollIdleNotChecked, "persist_approved_review_feedback", true)
	}

	newReviews := reviewItems(payload.Reviews[previousReviewCount:], payload.ReviewComments[previousInlineCommentCount:])
	newComments := changedIssueComments(payload.Comments, previousCommentWatermark)
	blocking := blockingReviewFeedback(payload.ReviewDecision, newReviews, newComments)
	if len(blocking) == 0 {
		d.persistReviewWorkerState(&update.Active.Worker, nextReviewState, now)
		update.WorkerChanged = true
		return traceAndReturn(&nextReviewState, 0, reviewPollIdleNotChecked, "persist_non_blocking_feedback", true)
	}
	if nextReviewState.reviewNudgeCount >= maxReviewNudges {
		d.persistReviewWorkerState(&update.Active.Worker, nextReviewState, now)
		update.WorkerChanged = true
		d.notifyCallerPaneReviewEscalation(ctx, update.Active, blockingReviewFeedback(payload.ReviewDecision, items, payload.Comments))

		event := d.assignmentEvent(update.Active, profile, EventWorkerReviewEscalated, fmt.Sprintf("review nudges exhausted after %d attempts; lead intervention required", nextReviewState.reviewNudgeCount))
		event.Retry = nextReviewState.reviewNudgeCount
		update.Events = append(update.Events, event)
		return traceAndReturn(&nextReviewState, len(blocking), reviewPollIdleNotChecked, "escalate_review_feedback", true)
	}

	feedback := formatBlockingReviewFeedback(update.Active.Task.PRNumber, blocking)
	// A fresh capture can update LastCapture/LastActivityAt before we decide whether to defer.
	idle, idleResult := d.workerAppearsIdleForReviewNudge(ctx, &update, profile, now)
	if !idle {
		return traceAndReturn(&nextReviewState, len(blocking), idleResult, "defer_review_nudge", false)
	}
	nudgedReviewState := nextReviewState
	nudgedReviewState.reviewNudgeCount++
	update.queueNudge(func(ctx context.Context, d *Daemon, update *TaskStateUpdate) {
		if err := d.sendPromptAndEnter(ctx, update.Active.Task.PaneID, feedback); err != nil {
			if isPaneGoneError(err) {
				d.escalateTaskState(update, profile, "worker pane missing during review nudge", now)
			}
			return
		}

		d.persistReviewWorkerState(&update.Active.Worker, nudgedReviewState, now)
		update.WorkerChanged = true
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerNudgedReview, fmt.Sprintf("sent %d new blocking review(s) to worker", len(blocking))))
	})
	return traceAndReturn(&nextReviewState, len(blocking), idleResult, "queue_review_nudge", false)
}

func (d *Daemon) persistReviewWorkerState(worker *Worker, state reviewWorkerState, now time.Time) {
	worker.LastReviewCount = state.reviewCount
	worker.LastInlineReviewCommentCount = state.inlineCommentCount
	worker.LastIssueCommentCount = state.commentCount
	worker.LastIssueCommentWatermark = state.commentWatermark
	worker.ReviewNudgeCount = state.reviewNudgeCount
	worker.LastSeenAt = now
}

const (
	reviewPollIdleNotChecked   = "not_checked"
	reviewPollIdle             = "idle"
	reviewPollIdleRecentOutput = "recent_output"
	reviewPollIdleFreshOutput  = "fresh_output"
	reviewPollIdlePaneMissing  = "pane_missing"
	reviewPollIdleCaptureError = "capture_error"
	reviewPollIdlePaneExited   = "pane_exited"
)

func (d *Daemon) workerAppearsIdleForReviewNudge(ctx context.Context, update *TaskStateUpdate, profile AgentProfile, now time.Time) (bool, string) {
	if d.workerHadRecentOutput(update.Active.Worker, now) {
		return false, reviewPollIdleRecentOutput
	}

	snapshot, err := d.amuxClient(ctx).CapturePane(ctx, update.Active.Task.PaneID)
	if err != nil {
		if isPaneGoneError(err) {
			d.escalateTaskState(update, profile, "worker pane missing during review poll", now)
			return false, reviewPollIdlePaneMissing
		}
		return true, reviewPollIdleCaptureError
	}
	if snapshot.Exited {
		return false, reviewPollIdlePaneExited
	}
	if d.recordWorkerOutput(update, profile, snapshot.Output(), now) {
		return false, reviewPollIdleFreshOutput
	}
	return true, reviewPollIdle
}

func (d *Daemon) traceReviewPollDecision(update *TaskStateUpdate, profile AgentProfile, previous reviewWorkerState, current *reviewWorkerState, blockingCount int, idleResult, action string, persisted bool) {
	if update == nil {
		return
	}

	message := fmt.Sprintf(
		"review poll trace: issue=%s pr_number=%d prev=%s curr=%s blocking_count=%d idle_result=%s action=%s persisted=%t",
		strings.TrimSpace(update.Active.Task.Issue),
		update.Active.Task.PRNumber,
		formatReviewPollWatermarks(previous),
		formatOptionalReviewPollWatermarks(current),
		blockingCount,
		formatReviewPollIdleResult(idleResult),
		strings.TrimSpace(action),
		persisted,
	)
	if d.logf != nil {
		d.logf("%s", message)
	}
	update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventReviewPollTrace, message))
}

func formatReviewPollWatermarks(state reviewWorkerState) string {
	return fmt.Sprintf(
		"reviews:%d/inline:%d/comments:%d",
		state.reviewCount,
		state.inlineCommentCount,
		state.commentCount,
	)
}

func formatOptionalReviewPollWatermarks(state *reviewWorkerState) string {
	if state == nil {
		return "unavailable"
	}
	return formatReviewPollWatermarks(*state)
}

func formatReviewPollIdleResult(result string) string {
	result = strings.TrimSpace(result)
	if result == "" {
		return reviewPollIdleNotChecked
	}
	return result
}

func (d *Daemon) workerHadRecentOutput(worker Worker, now time.Time) bool {
	threshold := d.reviewNudgeIdleThreshold()
	if threshold <= 0 || worker.LastActivityAt.IsZero() {
		return false
	}
	return now.Sub(worker.LastActivityAt) <= threshold
}

func (d *Daemon) reviewNudgeIdleThreshold() time.Duration {
	if d.captureInterval <= 0 {
		return 0
	}
	return reviewNudgeIdleWindowMultiplier * d.captureInterval
}

func (d *Daemon) recordWorkerOutput(update *TaskStateUpdate, profile AgentProfile, output string, now time.Time) bool {
	if output == update.Active.Worker.LastCapture {
		return false
	}

	wasEscalated := update.Active.Worker.Health == WorkerHealthEscalated
	wasStuck := update.Active.Worker.Health == WorkerHealthStuck ||
		update.Active.Worker.Health == WorkerHealthEscalated ||
		update.Active.Worker.NudgeCount > 0
	update.Active.Worker.LastCapture = output
	update.Active.Worker.LastActivityAt = now
	update.Active.Worker.Health = WorkerHealthHealthy
	update.Active.Worker.NudgeCount = 0
	update.Active.Worker.LastSeenAt = now
	update.Active.Task.UpdatedAt = now
	update.WorkerChanged = true
	update.TaskChanged = true
	update.Active.Task.State = taskStateForAssignment(update.Active)

	if wasStuck {
		if wasEscalated {
			update.PaneMetadataRemovals = append(update.PaneMetadataRemovals, "status")
		}
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerRecovered, "worker output changed"))
	}
	return true
}

func (d *Daemon) lookupPRReviews(ctx context.Context, projectPath string, prNumber int) (prReviewPayload, bool, error) {
	return d.gitHubClientForContext(ctx, projectPath).lookupPRReviews(ctx, prNumber)
}

func latestReviewContainsLGTM(reviews []prReview) bool {
	if len(reviews) == 0 {
		return false
	}
	return bodyContainsStandaloneLGTM(reviews[len(reviews)-1].Body)
}

func bodyContainsStandaloneLGTM(body string) bool {
	lines := strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n")
	for _, line := range lines {
		if strings.EqualFold(strings.TrimSpace(line), "LGTM") {
			return true
		}
	}
	return false
}

func formatBlockingReviewFeedback(prNumber int, feedback []prFeedback) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "New blocking PR review feedback on #%d:\n", prNumber)
	for _, item := range feedback {
		builder.WriteString(formatFeedbackLine(item))
		builder.WriteString("\n")
	}
	builder.WriteString("\nAddress the feedback in the PR review and push an update.")
	return builder.String()
}

func (d *Daemon) notifyCallerPaneReviewEscalation(ctx context.Context, active ActiveAssignment, feedback []prFeedback) {
	targetPane := d.taskPaneTarget(active.Task)
	if targetPane == "" || len(feedback) == 0 {
		return
	}

	_ = d.amuxClient(ctx).SendKeys(ctx, targetPane, formatLeadReviewEscalation(active, feedback), "Enter")
}

func formatLeadReviewEscalation(active ActiveAssignment, feedback []prFeedback) string {
	var builder strings.Builder

	issue := strings.TrimSpace(active.Task.Issue)
	paneName := assignmentPaneName(active.Task, active.Worker)

	if issue != "" && paneName != "" && active.Task.PRNumber > 0 {
		fmt.Fprintf(&builder, "Review nudges exhausted for %s in %s on PR #%d.\n", issue, paneName, active.Task.PRNumber)
	} else if paneName != "" && active.Task.PRNumber > 0 {
		fmt.Fprintf(&builder, "Review nudges exhausted for %s on PR #%d.\n", paneName, active.Task.PRNumber)
	} else {
		builder.WriteString("Review nudges exhausted.\n")
	}

	builder.WriteString("Unresolved review feedback:\n")
	for _, item := range feedback {
		builder.WriteString(formatFeedbackLine(item))
		builder.WriteString("\n")
	}

	if paneName != "" {
		fmt.Fprintf(&builder, "\nIntervene in %s to address the feedback or reassign the task.", paneName)
	} else {
		builder.WriteString("\nIntervene to address the feedback or reassign the task.")
	}

	return builder.String()
}

func isBlockingIssueComment(comment prComment) bool {
	login := strings.ToLower(strings.TrimSpace(comment.Author.Login))
	switch login {
	case "github-actions", "github-actions[bot]":
	default:
		return false
	}

	return !bodyContainsStandaloneLGTM(comment.Body) && !isReviewProgressIssueComment(comment.Body)
}

func isReviewProgressIssueComment(body string) bool {
	if blockingIssueSection(body) != "" {
		return false
	}

	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(body, "\r\n", "\n"), "…", "..."))
	if strings.Contains(normalized, "reviewing...") {
		return true
	}
	if strings.Contains(normalized, "working...") && strings.Contains(normalized, "get back to you") {
		return true
	}
	return strings.Contains(normalized, "view job run") && strings.Contains(normalized, "- [ ]")
}

func summarizeBlockingIssueComment(body string) string {
	section := blockingIssueSection(body)
	if section == "" {
		return normalizeReviewBody(body)
	}

	titles := extractBlockingIssueTitles(section)
	if len(titles) > 0 {
		return strings.Join(titles, "; ")
	}
	return normalizeReviewBody(section)
}

func blockingIssueSection(body string) string {
	lines := strings.Split(strings.ReplaceAll(body, "\r\n", "\n"), "\n")
	start := -1
	level := 0
	for i, line := range lines {
		headingLevel, title, ok := parseMarkdownHeading(line)
		if !ok {
			continue
		}
		title = strings.TrimSpace(strings.TrimSuffix(title, ":"))
		if !strings.EqualFold(title, "Blocking Issue") && !strings.EqualFold(title, "Blocking Issues") {
			continue
		}
		start = i + 1
		level = headingLevel
		break
	}
	if start == -1 {
		return ""
	}

	end := len(lines)
	for i := start; i < len(lines); i++ {
		headingLevel, _, ok := parseMarkdownHeading(lines[i])
		if ok && headingLevel <= level {
			end = i
			break
		}
	}

	return strings.TrimSpace(strings.Join(lines[start:end], "\n"))
}

func extractBlockingIssueTitles(section string) []string {
	lines := strings.Split(section, "\n")
	titles := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}

		if strings.HasPrefix(trimmed, "**") && strings.HasSuffix(trimmed, "**") {
			title := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "**"), "**"))
			title = trimLeadingIssueNumber(title)
			if title != "" {
				titles = append(titles, title)
			}
			continue
		}

		headingLevel, title, ok := parseMarkdownHeading(trimmed)
		if !ok || headingLevel < 4 {
			continue
		}
		title = trimLeadingIssueNumber(strings.TrimSpace(title))
		if title != "" {
			titles = append(titles, title)
		}
	}
	return titles
}

func parseMarkdownHeading(line string) (int, string, bool) {
	matches := markdownHeadingPattern.FindStringSubmatch(strings.TrimSpace(line))
	if len(matches) != 3 {
		return 0, "", false
	}
	return len(matches[1]), matches[2], true
}

func trimLeadingIssueNumber(title string) string {
	for index, r := range title {
		if r < '0' || r > '9' {
			if index > 0 && r == '.' {
				return strings.TrimSpace(title[index+1:])
			}
			return title
		}
	}
	return title
}

func normalizeReviewBody(body string) string {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return "requested changes without a review body."
	}
	return strings.Join(strings.Fields(trimmed), " ")
}
