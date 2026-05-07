package daemon

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

type prReviewComment struct {
	Body         string    `json:"body"`
	Path         string    `json:"path"`
	Line         int       `json:"line"`
	OriginalLine int       `json:"original_line"`
	CreatedAt    time.Time `json:"created_at"`
	User         struct {
		Login string `json:"login"`
	} `json:"user"`
}

type prReviewThread struct {
	ID         string
	IsResolved bool
	Path       string
	Line       int
	Comments   []prReviewThreadComment
}

type prReviewThreadComment struct {
	ID        string
	Body      string
	Author    string
	CreatedAt time.Time
}

type prReviewThreadsGraphQLPayload struct {
	Data struct {
		Repository struct {
			PullRequest *struct {
				ReviewThreads struct {
					Nodes []struct {
						ID         string `json:"id"`
						IsResolved bool   `json:"isResolved"`
						Path       string `json:"path"`
						Line       int    `json:"line"`
						Comments   struct {
							Nodes []struct {
								ID        string    `json:"id"`
								Body      string    `json:"body"`
								CreatedAt time.Time `json:"createdAt"`
								Author    struct {
									Login string `json:"login"`
								} `json:"author"`
							} `json:"nodes"`
						} `json:"comments"`
					} `json:"nodes"`
				} `json:"reviewThreads"`
			} `json:"pullRequest"`
		} `json:"repository"`
	} `json:"data"`
}

type prReviewItem struct {
	Author          string
	Body            string
	State           string
	Path            string
	Line            int
	OccurredAt      time.Time
	IsInlineComment bool
	order           int
}

func reviewItems(reviews []prReview, comments []prReviewComment) []prReviewItem {
	items := make([]prReviewItem, 0, len(reviews)+len(comments))
	for index, review := range reviews {
		items = append(items, prReviewItem{
			Author:     review.Author.Login,
			Body:       review.Body,
			State:      review.State,
			OccurredAt: review.SubmittedAt,
			order:      index,
		})
	}
	baseOrder := len(reviews)
	for index, comment := range comments {
		items = append(items, prReviewItem{
			Author:          comment.User.Login,
			Body:            comment.Body,
			Path:            strings.TrimSpace(comment.Path),
			Line:            reviewCommentLine(comment),
			OccurredAt:      comment.CreatedAt,
			IsInlineComment: true,
			order:           baseOrder + index,
		})
	}

	sort.SliceStable(items, func(i, j int) bool {
		left := items[i]
		right := items[j]
		if left.OccurredAt.IsZero() || right.OccurredAt.IsZero() {
			return left.order < right.order
		}
		return left.OccurredAt.Before(right.OccurredAt)
	})

	return items
}

func blockingReviewFeedback(reviewDecision string, items []prReviewItem, comments []prComment) []prFeedback {
	blocking := make([]prFeedback, 0, len(items)+len(comments))
	if reviewDecision == "CHANGES_REQUESTED" {
		for _, item := range items {
			if !item.IsInlineComment && item.State != "CHANGES_REQUESTED" {
				continue
			}
			blocking = append(blocking, prFeedback{
				Author: item.Author,
				Path:   item.Path,
				Line:   item.Line,
				Body:   item.Body,
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

func reviewThreadFeedback(thread prReviewThread) prFeedback {
	comment, ok := latestReviewThreadComment(thread)
	body := "unresolved review thread."
	author := ""
	if ok {
		body = comment.Body
		author = comment.Author
	}
	return prFeedback{
		Author: author,
		Path:   thread.Path,
		Line:   thread.Line,
		Body:   body,
	}
}

func latestReviewThreadComment(thread prReviewThread) (prReviewThreadComment, bool) {
	if len(thread.Comments) == 0 {
		return prReviewThreadComment{}, false
	}
	return thread.Comments[len(thread.Comments)-1], true
}

func reviewCommentLine(comment prReviewComment) int {
	if comment.Line > 0 {
		return comment.Line
	}
	return comment.OriginalLine
}

func formatFeedbackLocation(feedback prFeedback) string {
	path := strings.TrimSpace(feedback.Path)
	switch {
	case path == "":
		return ""
	case feedback.Line <= 0:
		return path
	default:
		return path + ":" + strconv.Itoa(feedback.Line)
	}
}

func formatFeedbackLine(feedback prFeedback) string {
	author := strings.TrimSpace(feedback.Author)
	if author == "" {
		author = "reviewer"
	}
	body := normalizeReviewBody(feedback.Body)
	if location := formatFeedbackLocation(feedback); location != "" {
		return fmt.Sprintf("- %s on %s: %s", author, location, body)
	}
	return fmt.Sprintf("- %s: %s", author, body)
}
