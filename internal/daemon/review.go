package daemon

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

var markdownHeadingPattern = regexp.MustCompile(`^(#{1,6})\s+(.+?)\s*$`)

const maxReviewNudges = 3

type prReviewPayload struct {
	ReviewDecision string      `json:"reviewDecision"`
	Reviews        []prReview  `json:"reviews"`
	Comments       []prComment `json:"comments"`
}

type prReview struct {
	State  string `json:"state"`
	Body   string `json:"body"`
	Author struct {
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
	Body   string
}

func (d *Daemon) handlePRReviewPoll(ctx context.Context, active ActiveAssignment, profile AgentProfile) {
	payload, ok, err := d.lookupPRReviews(ctx, active.Task.PRNumber)
	if err != nil || !ok {
		return
	}

	reviewCount := len(payload.Reviews)
	commentCount := len(payload.Comments)
	previousReviewCount := active.Worker.LastReviewCount
	previousCommentCount := active.Worker.LastIssueCommentCount
	resetReviewNudgeCount := payload.ReviewDecision != "CHANGES_REQUESTED" && active.Worker.ReviewNudgeCount != 0
	if resetReviewNudgeCount {
		active.Worker.ReviewNudgeCount = 0
	}
	if previousReviewCount > reviewCount || previousCommentCount > commentCount {
		d.persistReviewWorkerState(ctx, &active.Worker, reviewCount, commentCount)
		return
	}
	if previousReviewCount == reviewCount && previousCommentCount == commentCount {
		if resetReviewNudgeCount {
			active.Worker.UpdatedAt = d.now()
			_ = d.state.PutWorker(ctx, active.Worker)
		}
		return
	}
	if payload.ReviewDecision == "APPROVED" || latestReviewContainsLGTM(payload.Reviews) {
		d.persistReviewWorkerState(ctx, &active.Worker, reviewCount, commentCount)
		return
	}

	newReviews := payload.Reviews[previousReviewCount:]
	newComments := payload.Comments[previousCommentCount:]
	blocking := blockingReviewFeedback(payload.ReviewDecision, newReviews, newComments)
	if len(blocking) == 0 {
		d.persistReviewWorkerState(ctx, &active.Worker, reviewCount, commentCount)
		return
	}
	if active.Worker.ReviewNudgeCount >= maxReviewNudges {
		d.persistReviewWorkerState(ctx, &active.Worker, reviewCount, commentCount)
		d.emit(ctx, Event{
			Time:         d.now(),
			Type:         EventWorkerReviewEscalated,
			Project:      d.project,
			Issue:        active.Task.Issue,
			PaneID:       active.Task.PaneID,
			PaneName:     active.Task.PaneName,
			CloneName:    active.Task.CloneName,
			ClonePath:    active.Task.ClonePath,
			Branch:       active.Task.Branch,
			AgentProfile: profile.Name,
			PRNumber:     active.Task.PRNumber,
			Retry:        active.Worker.ReviewNudgeCount,
			Message:      fmt.Sprintf("review nudges exhausted after %d attempts; lead intervention required", active.Worker.ReviewNudgeCount),
		})
		return
	}

	feedback := formatBlockingReviewFeedback(active.Task.PRNumber, blocking)
	if err := d.sendPromptAndEnter(ctx, active.Task.PaneID, feedback); err != nil {
		return
	}

	active.Worker.ReviewNudgeCount++
	d.persistReviewWorkerState(ctx, &active.Worker, reviewCount, commentCount)

	d.emit(ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedReview,
		Project:      d.project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     active.Task.PaneName,
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      fmt.Sprintf("sent %d new blocking review(s) to worker", len(blocking)),
	})
}

func (d *Daemon) persistReviewWorkerState(ctx context.Context, worker *Worker, reviewCount, commentCount int) {
	worker.LastReviewCount = reviewCount
	worker.LastIssueCommentCount = commentCount
	worker.UpdatedAt = d.now()
	_ = d.state.PutWorker(ctx, *worker)
}

func (d *Daemon) lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error) {
	return d.github.lookupPRReviews(ctx, prNumber)
}

func blockingReviewFeedback(reviewDecision string, reviews []prReview, comments []prComment) []prFeedback {
	blocking := make([]prFeedback, 0, len(reviews)+len(comments))
	if reviewDecision == "CHANGES_REQUESTED" {
		for _, review := range reviews {
			if review.State != "CHANGES_REQUESTED" {
				continue
			}
			blocking = append(blocking, prFeedback{
				Author: review.Author.Login,
				Body:   normalizeReviewBody(review.Body),
			})
		}
	}
	for _, comment := range comments {
		if !isBlockingIssueComment(comment) {
			continue
		}
		blocking = append(blocking, prFeedback{
			Author: comment.Author.Login,
			Body:   summarizeBlockingIssueComment(comment.Body),
		})
	}
	return blocking
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
		author := strings.TrimSpace(item.Author)
		if author == "" {
			author = "reviewer"
		}
		body := normalizeReviewBody(item.Body)
		fmt.Fprintf(&builder, "- %s: %s\n", author, body)
	}
	builder.WriteString("\nAddress the feedback in the PR review and push an update.")
	return builder.String()
}

func isBlockingIssueComment(comment prComment) bool {
	login := strings.ToLower(strings.TrimSpace(comment.Author.Login))
	switch login {
	case "github-actions", "github-actions[bot]":
	default:
		return false
	}

	return blockingIssueSection(comment.Body) != ""
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
