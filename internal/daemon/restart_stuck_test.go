package daemon

import (
	"context"
	"testing"
	"time"
)

func TestStuckDetectionResumesFromPersistedNudgeCountAfterRestart(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	firstCaptureTicker := newFakeTicker()
	secondCaptureTicker := newFakeTicker()
	deps.tickers.enqueue(firstCaptureTicker, newFakeTicker(), secondCaptureTicker, newFakeTicker())
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		StuckTextPatterns: []string{"permission prompt"},
		StuckTimeout:      time.Hour,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   2,
	}
	deps.amux.captureSequence("pane-1", []string{"permission prompt", "permission prompt", "permission prompt"})

	first := deps.newDaemon(t)
	ctx := context.Background()
	if err := first.Start(ctx); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}

	if err := first.Assign(ctx, "LAB-710", "Keep nudging after restart", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	firstCaptureTicker.tick(deps.clock.Now())
	waitFor(t, "first persisted nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.NudgeCount == 1 && deps.amux.countKey("pane-1", "\n") == 1
	})

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

	secondCaptureTicker.tick(deps.clock.Now())
	waitFor(t, "second persisted nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.NudgeCount == 2 && deps.amux.countKey("pane-1", "\n") == 2
	})

	secondCaptureTicker.tick(deps.clock.Now())
	waitFor(t, "escalation after persisted retries", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.Health == WorkerHealthEscalated
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 2; got != want {
		t.Fatalf("nudge count after restart = %d, want %d", got, want)
	}
}
