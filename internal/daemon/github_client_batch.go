package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

const prTerminalStateGraphQLPageSize = 100

type prTerminalStateKey struct {
	Project  string
	PRNumber int
}

type githubPRTerminalStateRef struct {
	Key      prTerminalStateKey
	Owner    string
	Repo     string
	PRNumber int
}

type prTerminalStateGraphQLPayload struct {
	Data   map[string]*prTerminalStateGraphQLRepository `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type prTerminalStateGraphQLRepository struct {
	PullRequest *prTerminalStateGraphQLPullRequest `json:"pullRequest"`
}

type prTerminalStateGraphQLPullRequest struct {
	Number   int     `json:"number"`
	Merged   bool    `json:"merged"`
	MergedAt *string `json:"mergedAt"`
	State    string  `json:"state"`
}

func (c *gitHubCLIClient) lookupPRTerminalStates(ctx context.Context, refs []githubPRTerminalStateRef) (map[prTerminalStateKey]prTerminalState, error) {
	states := make(map[prTerminalStateKey]prTerminalState, len(refs))
	for start := 0; start < len(refs); start += prTerminalStateGraphQLPageSize {
		end := start + prTerminalStateGraphQLPageSize
		if end > len(refs) {
			end = len(refs)
		}

		page, err := c.lookupPRTerminalStatePage(ctx, refs[start:end])
		if err != nil {
			return nil, err
		}
		for key, state := range page {
			states[key] = state
		}
	}
	return states, nil
}

func (c *gitHubCLIClient) lookupPRTerminalStatePage(ctx context.Context, refs []githubPRTerminalStateRef) (map[prTerminalStateKey]prTerminalState, error) {
	if len(refs) == 0 {
		return nil, nil
	}

	output, err := c.run(ctx, prTerminalStateGraphQLArgs(refs)...)
	if err != nil {
		return nil, err
	}
	if len(output) == 0 {
		return nil, fmt.Errorf("github graphql PR terminal state lookup returned empty response")
	}

	var payload prTerminalStateGraphQLPayload
	if err := json.Unmarshal(output, &payload); err != nil {
		return nil, err
	}
	if len(payload.Errors) > 0 {
		return nil, fmt.Errorf("github graphql PR terminal state lookup: %s", payload.Errors[0].Message)
	}

	states := make(map[prTerminalStateKey]prTerminalState, len(refs))
	for i, ref := range refs {
		repo := payload.Data[fmt.Sprintf("pr%d", i)]
		if repo == nil || repo.PullRequest == nil {
			continue
		}
		states[ref.Key] = terminalStateFromGraphQLPullRequest(*repo.PullRequest)
	}
	return states, nil
}

func prTerminalStateGraphQLArgs(refs []githubPRTerminalStateRef) []string {
	return []string{
		"api",
		"graphql",
		"-f", "query=" + prTerminalStateGraphQLQuery(refs),
	}
}

func prTerminalStateGraphQLQuery(refs []githubPRTerminalStateRef) string {
	var query strings.Builder
	query.WriteString("query{")
	for i, ref := range refs {
		fmt.Fprintf(
			&query,
			"pr%d:repository(owner:%s,name:%s){pullRequest(number:%d){number mergedAt merged state}}",
			i,
			graphQLStringLiteral(ref.Owner),
			graphQLStringLiteral(ref.Repo),
			ref.PRNumber,
		)
	}
	query.WriteString("}")
	return query.String()
}

func graphQLStringLiteral(value string) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return `""`
	}
	return string(encoded)
}

func terminalStateFromGraphQLPullRequest(pr prTerminalStateGraphQLPullRequest) prTerminalState {
	state := strings.TrimSpace(pr.State)
	mergedAt := ""
	if pr.MergedAt != nil {
		mergedAt = strings.TrimSpace(*pr.MergedAt)
	}

	merged := pr.Merged || strings.EqualFold(state, "merged") || mergedAt != ""
	closed := strings.EqualFold(state, "closed")
	return prTerminalState{
		merged:             merged,
		closedWithoutMerge: closed && !merged,
	}
}
