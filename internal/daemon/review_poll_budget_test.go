package daemon

import (
	"context"
	"testing"
)

func TestPRReviewPollingSkipsInlineCommentLookupWhenReviewSnapshotIsUnchanged(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[]}`)
	queuePRReviewPayload(deps, 42, `{"reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[]}`)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN"}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN"}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "initial review poll cycle completion")
	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "unchanged review poll cycle completion")

	apiArgs := []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}
	if got, want := deps.commands.countCalls("gh", apiArgs), 1; got != want {
		t.Fatalf("inline review comments lookup count = %d, want %d when review snapshot is unchanged", got, want)
	}
}
