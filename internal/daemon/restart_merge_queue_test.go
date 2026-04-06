package daemon

import (
	"context"
	"testing"
	"time"
)

func TestEnqueueResumesPersistedMergeQueueStateAfterRestart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		tickBeforeRestart bool
		wantRebaseCalls   int
	}{
		{
			name:            "queued entry restarts from rebase",
			wantRebaseCalls: 1,
		},
		{
			name:              "awaiting checks entry skips repeated rebase",
			tickBeforeRestart: true,
			wantRebaseCalls:   1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			firstPollTicker := newFakeTicker()
			secondPollTicker := newFakeTicker()
			deps.tickers.enqueue(newFakeTicker(), firstPollTicker, newFakeTicker(), secondPollTicker)
			deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--state", "open", "--json", "number"}, `[]`, nil)
			deps.commands.queue("gh", []string{"pr", "update-branch", "42", "--rebase"}, ``, nil)
			deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
			deps.commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, nil)

			first := deps.newDaemon(t)
			ctx := context.Background()
			if err := first.Start(ctx); err != nil {
				t.Fatalf("first Start() error = %v", err)
			}

			if err := first.Assign(ctx, "LAB-689", "Implement merge queue", "codex"); err != nil {
				t.Fatalf("Assign() error = %v", err)
			}

			task, ok := deps.state.task("LAB-689")
			if !ok {
				t.Fatal("task missing from state")
			}
			task.PRNumber = 42
			deps.state.putTaskForTest(task)

			if _, err := first.Enqueue(ctx, 42); err != nil {
				t.Fatalf("Enqueue(42) error = %v", err)
			}

			if tt.tickBeforeRestart {
				firstPollTicker.tick(deps.clock.Now())
				waitFor(t, "queued rebase before restart", func() bool {
					entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
					return err == nil && entry != nil && entry.Status == MergeQueueStatusAwaitingChecks
				})
			}

			if err := first.Stop(context.Background()); err != nil {
				t.Fatalf("first Stop() error = %v", err)
			}

			second := deps.newDaemon(t)
			if err := second.Start(ctx); err != nil {
				t.Fatalf("second Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = second.Stop(context.Background())
			})

			if !tt.tickBeforeRestart {
				secondPollTicker.tick(deps.clock.Now())
				waitFor(t, "post-restart merge queue rebase", func() bool {
					return deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}) == 1
				})
				waitFor(t, "post-restart awaiting checks", func() bool {
					entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
					return err == nil && entry != nil && entry.Status == MergeQueueStatusAwaitingChecks
				})
			}
			secondPollTicker.tick(deps.clock.Now())
			waitFor(t, "post-restart merge queue merge", func() bool {
				return deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}) == 1
			})
			if got, want := deps.commands.countCalls("gh", []string{"pr", "checks", "42", "--json", "bucket"}), 1; got != want {
				t.Fatalf("checks call count = %d, want %d", got, want)
			}

			if got, want := deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}), tt.wantRebaseCalls; got != want {
				t.Fatalf("rebase call count = %d, want %d", got, want)
			}
		})
	}
}

func TestStartResetsTransientMergeQueueStatuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     string
		wantStatus string
	}{
		{
			name:       "rebasing resumes from queued",
			status:     MergeQueueStatusRebasing,
			wantStatus: MergeQueueStatusQueued,
		},
		{
			name:       "merging resumes from awaiting checks",
			status:     MergeQueueStatusMerging,
			wantStatus: MergeQueueStatusAwaitingChecks,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
			now := deps.clock.Now()
			deps.state.mergeQueue = []MergeQueueEntry{
				{
					Project:   "/tmp/project",
					Issue:     "LAB-689",
					PRNumber:  42,
					Status:    tt.status,
					CreatedAt: now.Add(-time.Minute),
					UpdatedAt: now.Add(-time.Minute),
				},
			}

			d := deps.newDaemon(t)
			ctx := context.Background()
			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			entry, err := deps.state.MergeEntry(ctx, "/tmp/project", 42)
			if err != nil {
				t.Fatalf("MergeEntry() error = %v", err)
			}
			if entry == nil {
				t.Fatal("MergeEntry() = nil, want entry")
			}
			if got, want := entry.Status, tt.wantStatus; got != want {
				t.Fatalf("entry.Status = %q, want %q", got, want)
			}
		})
	}
}

func TestRestartDropsMergedQueueEntryWithoutFailureNotice(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(newFakeTicker(), pollTicker)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-06T03:00:00Z"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-06T03:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()

	task := Task{
		Project:      "/tmp/project",
		Issue:        "LAB-689",
		Status:       TaskStatusActive,
		Prompt:       "Implement merge queue",
		PaneID:       "pane-1",
		PaneName:     "worker-1",
		CloneName:    "clone-01",
		ClonePath:    deps.pool.clone.Path,
		Branch:       "LAB-689",
		AgentProfile: "codex",
		PRNumber:     42,
		CreatedAt:    deps.clock.Now(),
		UpdatedAt:    deps.clock.Now(),
	}
	deps.state.putTaskForTest(task)
	if err := deps.state.PutWorker(ctx, Worker{
		Project:        "/tmp/project",
		PaneID:         "pane-1",
		PaneName:       "worker-1",
		Issue:          "LAB-689",
		ClonePath:      deps.pool.clone.Path,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastActivityAt: deps.clock.Now(),
		UpdatedAt:      deps.clock.Now(),
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	deps.state.mergeQueue = []MergeQueueEntry{
		{
			Project:   "/tmp/project",
			Issue:     "LAB-689",
			PRNumber:  42,
			Status:    MergeQueueStatusMerging,
			CreatedAt: deps.clock.Now().Add(-time.Minute),
			UpdatedAt: deps.clock.Now().Add(-time.Minute),
		},
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	entry, err := deps.state.MergeEntry(ctx, "/tmp/project", 42)
	if err != nil {
		t.Fatalf("MergeEntry() error = %v", err)
	}
	if entry == nil || entry.Status != MergeQueueStatusAwaitingChecks {
		t.Fatalf("entry after startup = %#v, want awaiting checks", entry)
	}

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "merged queue entry deleted", func() bool {
		entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
		return err == nil && entry == nil
	})

	if got := deps.commands.countCalls("gh", []string{"pr", "merge", "42", "--squash"}); got != 0 {
		t.Fatalf("merge command count = %d, want 0", got)
	}
	if got := deps.amux.countKey("pane-1", mergeQueueMergeFailedPrompt(42)+"\n"); got != 0 {
		t.Fatalf("failure prompt count = %d, want 0", got)
	}
	if got := deps.events.countType(EventPRLandingFailed); got != 0 {
		t.Fatalf("landing failed event count = %d, want 0", got)
	}

	pollTicker.tick(deps.clock.Now())
	waitFor(t, "assignment completed after merged queue cleanup", func() bool {
		task, ok := deps.state.task("LAB-689")
		return ok && task.Status == TaskStatusDone
	})
}
