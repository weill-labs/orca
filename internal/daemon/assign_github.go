package daemon

import (
	"context"
	"fmt"
	"strings"
)

type gitHubIssueDetails struct {
	Title string
	Body  string
}

func (d *Daemon) lookupGitHubIssue(ctx context.Context, projectPath, issue string) (gitHubIssueDetails, bool, error) {
	number, ok := parseGitHubIssueNumber(issue)
	if !ok {
		return gitHubIssueDetails{}, false, nil
	}

	issueDetails, err := d.githubForProject(projectPath).lookupIssue(ctx, number)
	if err != nil {
		return gitHubIssueDetails{}, true, fmt.Errorf("lookup GitHub issue %s: %w", issue, err)
	}

	return gitHubIssueDetails{
		Title: strings.TrimSpace(issueDetails.Title),
		Body:  strings.TrimSpace(issueDetails.Body),
	}, true, nil
}

func withGitHubIssueContext(issue string, details gitHubIssueDetails, prompt string) string {
	lines := []string{"GitHub issue " + strings.TrimSpace(issue)}
	if title := strings.TrimSpace(details.Title); title != "" {
		lines = append(lines, "Title: "+title)
	}
	if body := strings.TrimSpace(details.Body); body != "" {
		lines = append(lines, "", "Body:", body)
	}
	if taskPrompt := strings.TrimSpace(prompt); taskPrompt != "" {
		lines = append(lines, "", "Task:", taskPrompt)
	}
	return strings.Join(lines, "\n")
}
