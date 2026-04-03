package daemon

import (
	"context"
	"fmt"
	"strings"
)

type prReviewPayload struct {
	ReviewDecision string     `json:"reviewDecision"`
	Reviews        []prReview `json:"reviews"`
}

type prReview struct {
	State  string `json:"state"`
	Body   string `json:"body"`
	Author struct {
		Login string `json:"login"`
	} `json:"author"`
}

func (d *Daemon) handlePRReviewPoll(ctx context.Context, active ActiveAssignment, profile AgentProfile) {
	payload, ok, err := d.lookupPRReviews(ctx, active.Task.PRNumber)
	if err != nil || !ok {
		return
	}

	previousCount := active.Worker.LastReviewCount
	if previousCount > len(payload.Reviews) {
		active.Worker.LastReviewCount = len(payload.Reviews)
		active.Worker.UpdatedAt = d.now()
		_ = d.state.PutWorker(ctx, active.Worker)
		return
	}
	if previousCount == len(payload.Reviews) {
		return
	}

	newReviews := payload.Reviews[previousCount:]
	blocking := blockingReviews(payload.ReviewDecision, newReviews)
	if len(blocking) == 0 {
		active.Worker.LastReviewCount = len(payload.Reviews)
		active.Worker.UpdatedAt = d.now()
		_ = d.state.PutWorker(ctx, active.Worker)
		return
	}

	feedback := formatBlockingReviewFeedback(active.Task.PRNumber, blocking)
	if err := d.amux.SendKeys(ctx, active.Task.PaneID, ensureTrailingNewline(feedback)); err != nil {
		return
	}

	active.Worker.LastReviewCount = len(payload.Reviews)
	active.Worker.UpdatedAt = d.now()
	_ = d.state.PutWorker(ctx, active.Worker)

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

func (d *Daemon) lookupPRReviews(ctx context.Context, prNumber int) (prReviewPayload, bool, error) {
	return d.github.lookupPRReviews(ctx, prNumber)
}

func blockingReviews(reviewDecision string, reviews []prReview) []prReview {
	if reviewDecision != "CHANGES_REQUESTED" {
		return nil
	}

	blocking := make([]prReview, 0, len(reviews))
	for _, review := range reviews {
		if review.State == "CHANGES_REQUESTED" {
			blocking = append(blocking, review)
		}
	}
	return blocking
}

func formatBlockingReviewFeedback(prNumber int, reviews []prReview) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "New blocking PR review feedback on #%d:\n", prNumber)
	for _, review := range reviews {
		author := strings.TrimSpace(review.Author.Login)
		if author == "" {
			author = "reviewer"
		}
		body := normalizeReviewBody(review.Body)
		fmt.Fprintf(&builder, "- %s: %s\n", author, body)
	}
	builder.WriteString("\nAddress the feedback in the PR review and push an update.\n")
	return builder.String()
}

func normalizeReviewBody(body string) string {
	trimmed := strings.TrimSpace(body)
	if trimmed == "" {
		return "requested changes without a review body."
	}
	return strings.Join(strings.Fields(trimmed), " ")
}
