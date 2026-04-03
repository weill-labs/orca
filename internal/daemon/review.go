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

func (d *Daemon) handlePRReviewPoll(active *assignment) {
	payload, ok, err := d.lookupPRReviews(active.ctx, active.prNumber)
	if err != nil || !ok {
		return
	}

	previousCount := int(active.lastReviewCount.Load())
	if previousCount > len(payload.Reviews) {
		active.lastReviewCount.Store(int64(len(payload.Reviews)))
		return
	}
	if previousCount == len(payload.Reviews) {
		return
	}

	newReviews := payload.Reviews[previousCount:]
	blocking := blockingReviews(payload.ReviewDecision, newReviews)
	if len(blocking) == 0 {
		active.lastReviewCount.Store(int64(len(payload.Reviews)))
		return
	}

	feedback := formatBlockingReviewFeedback(active.prNumber, blocking)
	if err := d.amux.SendKeys(active.ctx, active.pane.ID, ensureTrailingNewline(feedback)); err != nil {
		return
	}

	active.lastReviewCount.Store(int64(len(payload.Reviews)))
	d.emit(active.ctx, Event{
		Time:         d.now(),
		Type:         EventWorkerNudgedReview,
		Project:      d.project,
		Issue:        active.task.Issue,
		PaneID:       active.pane.ID,
		PaneName:     active.pane.Name,
		CloneName:    active.clone.Name,
		ClonePath:    active.clone.Path,
		Branch:       active.task.Branch,
		AgentProfile: active.profile.Name,
		PRNumber:     active.prNumber,
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
