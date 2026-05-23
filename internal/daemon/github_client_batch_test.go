package daemon

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestGitHubCLIClientLookupPRTerminalStatesPaginatesRefs(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	client := newBatchGitHubTestClient(commands)

	refs := make([]githubPRTerminalStateRef, 0, prTerminalStateGraphQLPageSize+5)
	for i := 0; i < prTerminalStateGraphQLPageSize+5; i++ {
		prNumber := 1000 + i
		refs = append(refs, githubPRTerminalStateRef{
			Key:      prTerminalStateKey{Project: "/repo", PRNumber: prNumber},
			Owner:    "weill-labs",
			Repo:     "orca",
			PRNumber: prNumber,
		})
	}

	firstPage := refs[:prTerminalStateGraphQLPageSize]
	secondPage := refs[prTerminalStateGraphQLPageSize:]
	commands.queue("gh", prTerminalStateGraphQLArgs(firstPage), prTerminalStateGraphQLResponse(firstPage), nil)
	commands.queue("gh", prTerminalStateGraphQLArgs(secondPage), prTerminalStateGraphQLResponse(secondPage), nil)

	got, err := client.lookupPRTerminalStates(context.Background(), refs)
	if err != nil {
		t.Fatalf("lookupPRTerminalStates() error = %v", err)
	}
	if gotLen, want := len(got), len(refs); gotLen != want {
		t.Fatalf("len(terminal states) = %d, want %d", gotLen, want)
	}
	for _, ref := range refs {
		state, ok := got[ref.Key]
		if !ok {
			t.Fatalf("terminal state for PR #%d missing", ref.PRNumber)
		}
		if !state.merged {
			t.Fatalf("terminal state for PR #%d merged = false, want true", ref.PRNumber)
		}
	}
	if gotCount, want := commands.countCalls("gh", prTerminalStateGraphQLArgs(firstPage)), 1; gotCount != want {
		t.Fatalf("first GraphQL page call count = %d, want %d", gotCount, want)
	}
	if gotCount, want := commands.countCalls("gh", prTerminalStateGraphQLArgs(secondPage)), 1; gotCount != want {
		t.Fatalf("second GraphQL page call count = %d, want %d", gotCount, want)
	}
}

func TestGitHubCLIClientLookupPRTerminalStatePageHandlesEmptyInputs(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	client := newBatchGitHubTestClient(commands)

	got, err := client.lookupPRTerminalStatePage(context.Background(), nil)
	if err != nil {
		t.Fatalf("lookupPRTerminalStatePage(nil) error = %v", err)
	}
	if got != nil {
		t.Fatalf("lookupPRTerminalStatePage(nil) = %#v, want nil", got)
	}

	refs := []githubPRTerminalStateRef{
		{Key: prTerminalStateKey{Project: "/repo", PRNumber: 42}, Owner: "weill-labs", Repo: "orca", PRNumber: 42},
	}
	commands.queue("gh", prTerminalStateGraphQLArgs(refs), "", nil)
	_, err = client.lookupPRTerminalStatePage(context.Background(), refs)
	if err == nil || !strings.Contains(err.Error(), "empty response") {
		t.Fatalf("lookupPRTerminalStatePage(empty response) error = %v, want empty response error", err)
	}
}

func TestGitHubCLIClientLookupPRTerminalStatePageSkipsMissingPullRequests(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	client := newBatchGitHubTestClient(commands)

	refs := []githubPRTerminalStateRef{
		{Key: prTerminalStateKey{Project: "/repo", PRNumber: 41}, Owner: "weill-labs", Repo: "orca", PRNumber: 41},
		{Key: prTerminalStateKey{Project: "/repo", PRNumber: 42}, Owner: "weill-labs", Repo: "orca", PRNumber: 42},
		{Key: prTerminalStateKey{Project: "/repo", PRNumber: 43}, Owner: "weill-labs", Repo: "orca", PRNumber: 43},
	}
	commands.queue("gh", prTerminalStateGraphQLArgs(refs), `{
		"data": {
			"pr0": null,
			"pr1": {"pullRequest": null},
			"pr2": {"pullRequest": {"number": 43, "merged": true, "mergedAt": "2026-04-02T12:00:00Z", "state": "MERGED"}}
		}
	}`, nil)

	got, err := client.lookupPRTerminalStatePage(context.Background(), refs)
	if err != nil {
		t.Fatalf("lookupPRTerminalStatePage() error = %v", err)
	}
	if gotLen, want := len(got), 1; gotLen != want {
		t.Fatalf("len(terminal states) = %d, want %d: %#v", gotLen, want, got)
	}
	if !got[refs[2].Key].merged {
		t.Fatalf("terminal state for PR #%d merged = false, want true", refs[2].PRNumber)
	}
}

func newBatchGitHubTestClient(commands *fakeCommands) *gitHubCLIClient {
	return newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "/repo",
		commands:    commands,
		now:         func() time.Time { return time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC) },
		sleep:       noSleep,
		maxAttempts: 1,
	})
}

func prTerminalStateGraphQLResponse(refs []githubPRTerminalStateRef) string {
	out := `{"data":{`
	for i, ref := range refs {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf(`"pr%d":{"pullRequest":{"number":%d,"merged":true,"mergedAt":"2026-04-02T12:00:00Z","state":"MERGED"}}`, i, ref.PRNumber)
	}
	out += `}}`
	return out
}
