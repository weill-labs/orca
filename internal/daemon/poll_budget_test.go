package daemon

import (
	"context"
	"fmt"
	"testing"
)

func TestRunPollTickKeepsGitHubCallsWithinPerTaskBudget(t *testing.T) {
	t.Parallel()

	const taskCount = 5

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	assignments := make([]ActiveAssignment, 0, taskCount)

	for i := 0; i < taskCount; i++ {
		issue := fmt.Sprintf("LAB-%04d", 1400+i)
		paneID := fmt.Sprintf("pane-%d", i+1)
		prNumber := 500 + i

		seedTaskMonitorAssignment(t, deps, issue, paneID, prNumber)
		assignments = append(assignments, activeTaskMonitorAssignment(t, deps, issue))

		deps.commands.queue("gh", []string{"pr", "checks", fmt.Sprintf("%d", prNumber), "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
		deps.commands.queue("gh", []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prSnapshotJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null,"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","updatedAt":"2026-04-02T09:00:00Z","reviewDecision":"REVIEW_REQUIRED","reviews":[],"comments":[]}`, nil)
	}

	d.runPollTick(context.Background())

	got := len(deps.commands.callsByName("gh"))
	wantMax := taskCount * 2
	if got > wantMax {
		t.Fatalf("gh calls in one poll tick = %d, want <= %d for %d active tasks", got, wantMax, taskCount)
	}
}
