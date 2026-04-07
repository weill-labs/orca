package daemon

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestDaemonSkipsBufferedPollTickWhilePollCycleIsRunning(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedTaskMonitorAssignment(t, deps, "LAB-890", "pane-1", 42)

	checkArgs := []string{"pr", "checks", "42", "--json", "bucket"}
	firstChecks := deps.commands.block("gh", checkArgs)
	deps.commands.queue("gh", checkArgs, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergeable"}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, ``, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}

	var releaseFirst sync.Once
	var releaseSecond sync.Once
	var secondChecks commandBlock
	t.Cleanup(func() {
		releaseFirst.Do(func() {
			close(firstChecks.release)
		})
		releaseSecond.Do(func() {
			close(secondChecks.release)
		})
		_ = d.Stop(context.Background())
	})

	pollTicker.tick(deps.clock.Now())
	waitForTaskMonitorBlocks(t, firstChecks.started)

	secondChecks = deps.commands.block("gh", checkArgs)
	pollTicker.tick(deps.clock.Now())

	releaseFirst.Do(func() {
		close(firstChecks.release)
	})
	waitFor(t, "first poll cycle completion", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCIState == ciStatePending
	})

	select {
	case <-secondChecks.started:
		releaseSecond.Do(func() {
			close(secondChecks.release)
		})
		t.Fatal("second poll cycle started after the first cycle finished, want buffered tick skipped")
	case <-time.After(200 * time.Millisecond):
	}

	if got, want := deps.commands.countCalls("gh", checkArgs), 1; got != want {
		t.Fatalf("gh pr checks call count = %d, want %d", got, want)
	}
}
