package daemon

import (
	"context"
	"testing"
	"time"
)

func TestOutputIndicatesTestsPassed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		output string
		want   bool
	}{
		{
			name:   "matches explicit tests passed text",
			output: "All tests passed.",
			want:   true,
		},
		{
			name:   "matches go pass line",
			output: "=== RUN   TestAssign\n--- PASS: TestAssign (0.00s)\nPASS\nok  \tgithub.com/weill-labs/orca/internal/daemon\t0.123s",
			want:   true,
		},
		{
			name:   "ignores conversational ok line",
			output: "ok let me push the changes",
			want:   false,
		},
		{
			name:   "ignores ordinary worker output",
			output: "working on the next helper now",
			want:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := outputIndicatesTestsPassed(tt.output); got != tt.want {
				t.Fatalf("outputIndicatesTestsPassed(%q) = %v, want %v", tt.output, got, tt.want)
			}
		})
	}
}

func TestStuckDetectionNudgesIdleWorkerToOpenPROnceTestsPass(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.amux.captureSequence("pane-1", []string{
		"=== RUN   TestAssign\n--- PASS: TestAssign (0.00s)\nPASS\nok  \tgithub.com/weill-labs/orca/internal/daemon\t0.123s",
		"=== RUN   TestAssign\n--- PASS: TestAssign (0.00s)\nPASS\nok  \tgithub.com/weill-labs/orca/internal/daemon\t0.123s",
		"=== RUN   TestAssign\n--- PASS: TestAssign (0.00s)\nPASS\nok  \tgithub.com/weill-labs/orca/internal/daemon\t0.123s",
	})

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-892", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	captureTicker.tick(deps.clock.Now())
	waitFor(t, "initial passing capture", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCapture != ""
	})

	deps.clock.Advance(119 * time.Second)
	captureTicker.tick(deps.clock.Now())
	if got := deps.amux.countKey("pane-1", openPRNudgePrompt+"\n"); got != 0 {
		t.Fatalf("pr-open nudge count before 2m idle = %d, want 0", got)
	}

	deps.clock.Advance(2 * time.Second)
	captureTicker.tick(deps.clock.Now())
	waitFor(t, "pr-open nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.Health == WorkerHealthStuck &&
			worker.NudgeCount == 1 &&
			deps.amux.countKey("pane-1", openPRNudgePrompt+"\n") == 1
	})

	if got, want := deps.events.countType(EventWorkerNudged), 1; got != want {
		t.Fatalf("worker nudged event count = %d, want %d", got, want)
	}
}
