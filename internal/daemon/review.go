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
		return update
	}

	now := d.now()
	if snapshot.Exited {
		return d.checkExitedPaneCapture(active, profile, snapshot, now)
	}

	output := snapshot.Output()
	d.recordWorkerOutput(&update, profile, output, now)

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

	payload, ok, err := d.lookupPRReviews(ctx, active.Task.Project, active.Task.PRNumber)
	if err != nil || !ok {
		return update
	}
	now := d.now()
	items := reviewItems(payload.Reviews, payload.ReviewComments)

	reviewCount := len(items)
	commentCount := len(payload.Comments)
	previousReviewCount := update.Active.Worker.LastReviewCount
	previousCommentCount := update.Active.Worker.LastIssueCommentCount
	resetReviewNudgeCount := payload.ReviewDecision != "CHANGES_REQUESTED" && update.Active.Worker.ReviewNudgeCount != 0
	if resetReviewNudgeCount {
		update.Active.Worker.ReviewNudgeCount = 0
		update.Active.Worker.LastSeenAt = now
		update.WorkerChanged = true
	}
	if previousReviewCount > reviewCount || previousCommentCount > commentCount {
		d.persistReviewWorkerState(&update.Active.Worker, reviewCount, commentCount, now)
		update.WorkerChanged = true
		return update
	}
	if previousReviewCount == reviewCount && previousCommentCount == commentCount {
		return update
	}
	if payload.ReviewDecision == "APPROVED" || latestReviewContainsLGTM(payload.Reviews) {
		d.persistReviewWorkerState(&update.Active.Worker, reviewCount, commentCount, now)
		update.WorkerChanged = true
		return update
	}

	newReviews := items[previousReviewCount:]
	newComments := payload.Comments[previousCommentCount:]
	blocking := blockingReviewFeedback(payload.ReviewDecision, newReviews, newComments)
	if len(blocking) == 0 {
		d.persistReviewWorkerState(&update.Active.Worker, reviewCount, commentCount, now)
		update.WorkerChanged = true
		return update
	}
	if update.Active.Worker.ReviewNudgeCount >= maxReviewNudges {
		d.persistReviewWorkerState(&update.Active.Worker, reviewCount, commentCount, now)
		update.WorkerChanged = true
		d.notifyCallerPaneReviewEscalation(ctx, update.Active, blockingReviewFeedback(payload.ReviewDecision, items, payload.Comments))

		event := d.assignmentEvent(update.Active, profile, EventWorkerReviewEscalated, fmt.Sprintf("review nudges exhausted after %d attempts; lead intervention required", update.Active.Worker.ReviewNudgeCount))
		event.Retry = update.Active.Worker.ReviewNudgeCount
		update.Events = append(update.Events, event)
		return update
	}

	feedback := formatBlockingReviewFeedback(update.Active.Task.PRNumber, blocking)
	// A fresh capture can update LastCapture/LastActivityAt before we decide whether to defer.
	if !d.workerAppearsIdleForReviewNudge(ctx, &update, profile, now) {
		return update
	}
	update.queueNudge(func(ctx context.Context, d *Daemon, update *TaskStateUpdate) {
		if err := d.sendPromptAndEnter(ctx, update.Active.Task.PaneID, feedback); err != nil {
			return
		}

		update.Active.Worker.ReviewNudgeCount++
		d.persistReviewWorkerState(&update.Active.Worker, reviewCount, commentCount, now)
		update.WorkerChanged = true
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventWorkerNudgedReview, fmt.Sprintf("sent %d new blocking review(s) to worker", len(blocking))))
	})
	return update
}

func (d *Daemon) persistReviewWorkerState(worker *Worker, reviewCount, commentCount int, now time.Time) {
	worker.LastReviewCount = reviewCount
	worker.LastIssueCommentCount = commentCount
	worker.LastSeenAt = now
}

func (d *Daemon) workerAppearsIdleForReviewNudge(ctx context.Context, update *TaskStateUpdate, profile AgentProfile, now time.Time) bool {
	if d.workerHadRecentOutput(update.Active.Worker, now) {
		return false
	}

	snapshot, err := d.amuxClient(ctx).CapturePane(ctx, update.Active.Task.PaneID)
	if err != nil {
		return true
	}
	if snapshot.Exited {
		return false
	}
	if d.recordWorkerOutput(update, profile, snapshot.Output(), now) {
		return false
	}
	return true
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

	return !bodyContainsStandaloneLGTM(comment.Body)
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
