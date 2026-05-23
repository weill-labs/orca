package daemon

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestGitHubCLIClientLookupPRTerminalStatesPaginatesRefs(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	client := newGitHubCLIClient(gitHubCLIClientConfig{
		project:     "/repo",
		commands:    commands,
		now:         func() time.Time { return time.Date(2026, 4, 2, 9, 0, 0, 0, time.UTC) },
		sleep:       noSleep,
		maxAttempts: 1,
	})

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
	commands.queue("gh", prTerminalStateGraphQLArgs(firstPage), prTerminalStateGraphQLPayload(firstPage), nil)
	commands.queue("gh", prTerminalStateGraphQLArgs(secondPage), prTerminalStateGraphQLPayload(secondPage), nil)

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

func prTerminalStateGraphQLPayload(refs []githubPRTerminalStateRef) string {
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

