package daemon

import (
	"context"
	"testing"
)

func TestPRPollResumesFromPersistedCIStateAfterRestart(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	firstPollTicker := newFakeTicker()
	secondPollTicker := newFakeTicker()
	deps.tickers.enqueue(newFakeTicker(), firstPollTicker, newFakeTicker(), secondPollTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	for range 3 {
		deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket,name,link"}, ciFailedChecksOutput, nil)
	}

	first := deps.newDaemon(t)
	ctx := context.Background()
	if err := first.Start(ctx); err != nil {
		t.Fatalf("first Start() error = %v", err)
	}

	if err := first.Assign(ctx, "LAB-689", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	tickAndWaitForHeartbeat(t, first, deps, firstPollTicker, adaptivePRFastPollInterval, "initial CI poll cycle completion")
	waitFor(t, "initial CI nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			worker.LastCIState == ciStateFail &&
			deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 1
	})

	tickAndWaitForHeartbeat(t, first, deps, firstPollTicker, adaptivePRFastPollInterval, "persisted failing CI schedule cycle completion")
	waitFor(t, "persisted failing CI schedule before restart", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 2
	})
	if got, want := deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n"), 1; got != want {
		t.Fatalf("CI nudge count before restart = %d, want %d", got, want)
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

	tickAndWaitForHeartbeat(t, second, deps, secondPollTicker, adaptivePRFastPollInterval, "scheduled CI nudge after restart cycle completion")
	waitFor(t, "scheduled CI nudge after restart", func() bool {
		return deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 2 &&
			deps.events.countType(EventWorkerNudgedCI) == 2
	})

	tickAndWaitForHeartbeat(t, second, deps, secondPollTicker, adaptivePRFastPollInterval, "CI recovery poll cycle completion")
	waitFor(t, "CI recovery poll", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCIState == ciStatePass
	})

	tickAndWaitForHeartbeat(t, second, deps, secondPollTicker, adaptivePRFastPollInterval, "CI nudge after recovery cycle completion")
	waitFor(t, "CI nudge after recovery", func() bool {
		return deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 3 &&
			deps.events.countType(EventWorkerNudgedCI) == 3
	})
}
