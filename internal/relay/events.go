package relay

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Envelope struct {
	ID             uint64            `json:"id"`
	Repo           string            `json:"repo"`
	PRNumber       int               `json:"pr_number"`
	EventType      string            `json:"event_type"`
	Timestamp      time.Time         `json:"timestamp"`
	PayloadSummary map[string]string `json:"payload_summary"`
}

func VerifyGitHubSignature(secret string, body []byte, signature string) bool {
	if secret == "" || signature == "" || !strings.HasPrefix(signature, "sha256=") {
		return false
	}

	received, err := hex.DecodeString(strings.TrimPrefix(signature, "sha256="))
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(secret))
	if _, err := mac.Write(body); err != nil {
		return false
	}

	return hmac.Equal(received, mac.Sum(nil))
}

func NormalizeWebhookEvent(eventType string, body []byte, receivedAt time.Time) (Envelope, bool, error) {
	var payload githubWebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		return Envelope{}, false, fmt.Errorf("decode webhook payload: %w", err)
	}
	if payload.Repository.FullName == "" {
		return Envelope{}, false, fmt.Errorf("decode webhook payload: missing repository.full_name")
	}

	base := Envelope{
		Repo:      payload.Repository.FullName,
		EventType: eventType,
	}

	switch eventType {
	case "pull_request_review":
		if payload.PullRequest.Number == 0 {
			return Envelope{}, false, fmt.Errorf("pull_request_review payload missing pull_request.number")
		}
		base.PRNumber = payload.PullRequest.Number
		base.Timestamp = firstTime(payload.Review.SubmittedAt, receivedAt)
		base.PayloadSummary = compactSummary(map[string]string{
			"action":       payload.Action,
			"author":       payload.Review.User.Login,
			"review_state": payload.Review.State,
		})
		return base, true, nil
	case "issue_comment":
		if payload.Issue.PullRequest == nil {
			return Envelope{}, false, nil
		}
		if payload.Issue.Number == 0 {
			return Envelope{}, false, fmt.Errorf("issue_comment payload missing issue.number")
		}
		base.PRNumber = payload.Issue.Number
		base.Timestamp = firstTime(payload.Comment.UpdatedAt, payload.Comment.CreatedAt, receivedAt)
		base.PayloadSummary = compactSummary(map[string]string{
			"action":      payload.Action,
			"author":      payload.Comment.User.Login,
			"comment_url": payload.Comment.HTMLURL,
		})
		return base, true, nil
	case "check_suite":
		prNumber, ok := firstAssociatedPR(payload.CheckSuite.PullRequests)
		if !ok {
			return Envelope{}, false, nil
		}
		base.PRNumber = prNumber
		base.Timestamp = firstTime(payload.CheckSuite.UpdatedAt, payload.CheckSuite.CreatedAt, receivedAt)
		base.PayloadSummary = compactSummary(map[string]string{
			"action":      payload.Action,
			"status":      payload.CheckSuite.Status,
			"conclusion":  payload.CheckSuite.Conclusion,
			"head_branch": payload.CheckSuite.HeadBranch,
		})
		return base, true, nil
	case "check_run":
		prNumber, ok := firstAssociatedPR(payload.CheckRun.PullRequests)
		if !ok {
			return Envelope{}, false, nil
		}
		base.PRNumber = prNumber
		base.Timestamp = firstPointerTime(payload.CheckRun.CompletedAt, payload.CheckRun.StartedAt)
		base.Timestamp = firstTime(base.Timestamp, payload.CheckRun.UpdatedAt, payload.CheckRun.CreatedAt, receivedAt)
		base.PayloadSummary = compactSummary(map[string]string{
			"action":     payload.Action,
			"check_name": payload.CheckRun.Name,
			"status":     payload.CheckRun.Status,
			"conclusion": payload.CheckRun.Conclusion,
		})
		return base, true, nil
	case "pull_request":
		if payload.PullRequest.Number == 0 {
			return Envelope{}, false, fmt.Errorf("pull_request payload missing pull_request.number")
		}
		base.PRNumber = payload.PullRequest.Number
		base.Timestamp = firstTime(payload.PullRequest.UpdatedAt, payload.PullRequest.CreatedAt, receivedAt)
		base.PayloadSummary = compactSummary(map[string]string{
			"action": payload.Action,
			"state":  payload.PullRequest.State,
			"merged": strconv.FormatBool(payload.PullRequest.Merged),
		})
		return base, true, nil
	default:
		return Envelope{}, false, nil
	}
}

type githubWebhookPayload struct {
	Action      string             `json:"action"`
	Repository  repositoryPayload  `json:"repository"`
	PullRequest pullRequestPayload `json:"pull_request"`
	Review      reviewPayload      `json:"review"`
	Issue       issuePayload       `json:"issue"`
	Comment     commentPayload     `json:"comment"`
	CheckSuite  checkSuitePayload  `json:"check_suite"`
	CheckRun    checkRunPayload    `json:"check_run"`
}

type repositoryPayload struct {
	FullName string `json:"full_name"`
}

type pullRequestPayload struct {
	Number    int       `json:"number"`
	State     string    `json:"state"`
	Merged    bool      `json:"merged"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type reviewPayload struct {
	State       string      `json:"state"`
	SubmittedAt time.Time   `json:"submitted_at"`
	User        userPayload `json:"user"`
}

type userPayload struct {
	Login string `json:"login"`
}

type issuePayload struct {
	Number      int               `json:"number"`
	PullRequest *pullRequestLinks `json:"pull_request"`
}

type pullRequestLinks struct {
	URL string `json:"url"`
}

type commentPayload struct {
	HTMLURL   string      `json:"html_url"`
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
	User      userPayload `json:"user"`
}

type checkSuitePayload struct {
	Status       string                 `json:"status"`
	Conclusion   string                 `json:"conclusion"`
	HeadBranch   string                 `json:"head_branch"`
	PullRequests []pullRequestReference `json:"pull_requests"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
}

type checkRunPayload struct {
	Name         string                 `json:"name"`
	Status       string                 `json:"status"`
	Conclusion   string                 `json:"conclusion"`
	PullRequests []pullRequestReference `json:"pull_requests"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	StartedAt    *time.Time             `json:"started_at"`
	CompletedAt  *time.Time             `json:"completed_at"`
}

type pullRequestReference struct {
	Number int `json:"number"`
}

func firstAssociatedPR(pullRequests []pullRequestReference) (int, bool) {
	if len(pullRequests) == 0 || pullRequests[0].Number == 0 {
		return 0, false
	}
	return pullRequests[0].Number, true
}

func compactSummary(values map[string]string) map[string]string {
	summary := make(map[string]string, len(values))
	for key, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		summary[key] = value
	}
	return summary
}

func firstPointerTime(values ...*time.Time) time.Time {
	for _, value := range values {
		if value != nil && !value.IsZero() {
			return value.UTC()
		}
	}
	return time.Time{}
}

func firstTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value.UTC()
		}
	}
	return time.Time{}
}
