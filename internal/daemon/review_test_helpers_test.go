package daemon

import "fmt"

func queuePRReviewPayload(deps *testDeps, prNumber int, payload string) {
	deps.commands.queue("gh", []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prReviewJSONFields}, payload, nil)
	if payload == "" {
		return
	}
	deps.commands.queue("gh", []string{"api", fmt.Sprintf("repos/{owner}/{repo}/pulls/%d/comments?per_page=100", prNumber)}, `[]`, nil)
}
