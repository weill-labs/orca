package daemon

import (
	"context"
	"testing"
)

func TestPRReviewPollingSkipsInlineCommentLookupWhenReviewSnapshotIsUnchanged(t *testing.T) {
	tests := []struct {
		name                   string
		inlineCommentsJSON     string
		wantInlineCommentCount int
	}{
		{
			name:                   "no inline comments",
			inlineCommentsJSON:     `[]`,
			wantInlineCommentCount: 0,
		},
		{
			name: "existing inline comments keep stored count",
			inlineCommentsJSON: `[{
				"body":"Please rename this helper.",
				"path":"internal/daemon/review.go",
				"line":151,
				"original_line":151,
				"created_at":"2026-04-02T09:01:00Z",
				"user":{"login":"alice"}
			}]`,
			wantInlineCommentCount: 1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			captureTicker := newFakeTicker()
			prTicker := newFakeTicker()
			deps.tickers.enqueue(captureTicker, prTicker)
			deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
			deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
			deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null,"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","updatedAt":"2026-04-02T09:00:00Z","reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[]}`, nil)
			deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
			deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null,"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","updatedAt":"2026-04-02T09:00:00Z","reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[]}`, nil)
			deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
			deps.commands.queue("gh", []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}, tt.inlineCommentsJSON, nil)

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

			waitFor(t, "worker inline comment count persisted across skipped fetch", func() bool {
				worker, ok := deps.state.worker("pane-1")
				return ok && worker.LastInlineReviewCommentCount == tt.wantInlineCommentCount
			})
		})
	}
}

func TestPRReviewPollingEmitsApprovalAfterSkippingInlineCommentLookup(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null,"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","updatedAt":"2026-04-02T09:00:00Z","reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null,"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","updatedAt":"2026-04-02T09:00:00Z","reviewDecision":"CHANGES_REQUESTED","reviews":[],"comments":[]}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null,"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","updatedAt":"2026-04-02T09:02:00Z","reviewDecision":"APPROVED","reviews":[],"comments":[]}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}, `[{
		"body":"Please rename this helper.",
		"path":"internal/daemon/review.go",
		"line":151,
		"original_line":151,
		"created_at":"2026-04-02T09:01:00Z",
		"user":{"login":"alice"}
	}]`, nil)

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
	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "approval review poll cycle completion")

	apiArgs := []string{"api", "repos/{owner}/{repo}/pulls/42/comments?per_page=100"}
	if got, want := deps.commands.countCalls("gh", apiArgs), 1; got != want {
		t.Fatalf("inline review comments lookup count = %d, want %d after approval transition", got, want)
	}

	waitFor(t, "approval event emitted with carried inline comment count", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastInlineReviewCommentCount == 1 &&
			worker.ReviewApproved &&
			deps.events.countType(EventReviewApproved) == 1 &&
			stateEventCountByType(deps.state, EventReviewApproved) == 1
	})
}
