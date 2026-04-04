package daemon

import (
	"context"
	"testing"
)

func TestEnqueueResumesPersistedMergeQueueStateAfterRestart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		tickBeforeRestart bool
		wantRebaseCalls   int
		restartTicks      int
	}{
		{
			name:            "queued entry restarts from rebase",
			wantRebaseCalls: 1,
			restartTicks:    2,
		},
		{
			name:              "awaiting checks entry skips repeated rebase",
			tickBeforeRestart: true,
			wantRebaseCalls:   1,
			restartTicks:      1,
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

			for i := 0; i < tt.restartTicks; i++ {
				secondPollTicker.tick(deps.clock.Now())
			}
			if !tt.tickBeforeRestart {
				waitFor(t, "post-restart merge queue rebase", func() bool {
					return deps.commands.countCalls("gh", []string{"pr", "update-branch", "42", "--rebase"}) == 1
				})
			}
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
