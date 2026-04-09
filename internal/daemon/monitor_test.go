package daemon

import (
	"context"
	"errors"
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

func TestDaemonCaptureMonitorOpensAmuxCircuitAfterThreeFailures(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-924", "pane-1", 0)
	deps.amux.capturePaneErr = errors.New("amux unavailable")

	d := deps.newDaemon(t)
	ctx := context.Background()
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	for i := 0; i < 3; i++ {
		d.runCaptureTick(ctx)
		if got, want := deps.amux.captureCount("pane-1"), i+1; got != want {
			t.Fatalf("capture count after failure %d = %d, want %d", i+1, got, want)
		}
	}
	if err := d.monitorAmuxCircuit.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("amux circuit Allow() error = %v, want %v", err, ErrCircuitBreakerOpen)
	}
	if got, want := deps.events.countType(EventDaemonCircuitOpened), 1; got != want {
		t.Fatalf("circuit opened event count = %d, want %d", got, want)
	}
	if got, want := deps.events.lastMessage(EventDaemonCircuitOpened), "monitor amux circuit opened after 3 consecutive failures"; got != want {
		t.Fatalf("opened event message = %q, want %q", got, want)
	}

	d.runCaptureTick(ctx)
	if got, want := deps.amux.captureCount("pane-1"), 3; got != want {
		t.Fatalf("capture count with open circuit = %d, want %d", got, want)
	}

	deps.clock.Advance(60 * time.Second)
	deps.amux.capturePaneErr = nil
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{paneCaptureFromOutput("worker output")})

	d.runCaptureTick(ctx)
	if got, want := deps.amux.captureCount("pane-1"), 4; got != want {
		t.Fatalf("capture count after cooldown = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventDaemonCircuitClosed), 1; got != want {
		t.Fatalf("circuit closed event count = %d, want %d", got, want)
	}
	if got, want := deps.events.lastMessage(EventDaemonCircuitClosed), "monitor amux circuit closed after cooldown"; got != want {
		t.Fatalf("closed event message = %q, want %q", got, want)
	}
}

func TestDaemonPollMonitorOpensGitHubCircuitAfterThreeFailures(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-924", "pane-1", 0)

	lookupArgs := []string{"pr", "list", "--head", "LAB-924", "--json", "number"}
	for i := 0; i < 3; i++ {
		deps.commands.queue("gh", lookupArgs, ``, errors.New("github unavailable"))
	}
	deps.commands.queue("gh", lookupArgs, `[]`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	for i := 0; i < 3; i++ {
		d.runPollTick(ctx)
		if got, want := deps.commands.countCalls("gh", lookupArgs), i+1; got != want {
			t.Fatalf("github call count after failure %d = %d, want %d", i+1, got, want)
		}
	}
	if err := d.monitorGitHubCircuit.Allow(); !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Fatalf("github circuit Allow() error = %v, want %v", err, ErrCircuitBreakerOpen)
	}
	if got, want := deps.events.countType(EventDaemonCircuitOpened), 1; got != want {
		t.Fatalf("circuit opened event count = %d, want %d", got, want)
	}
	if got, want := deps.events.lastMessage(EventDaemonCircuitOpened), "monitor github circuit opened after 3 consecutive failures"; got != want {
		t.Fatalf("opened event message = %q, want %q", got, want)
	}

	d.runPollTick(ctx)
	if got, want := deps.commands.countCalls("gh", lookupArgs), 3; got != want {
		t.Fatalf("github call count with open circuit = %d, want %d", got, want)
	}

	deps.clock.Advance(60 * time.Second)
	d.runPollTick(ctx)
	if got, want := deps.commands.countCalls("gh", lookupArgs), 4; got != want {
		t.Fatalf("github call count after cooldown = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventDaemonCircuitClosed), 1; got != want {
		t.Fatalf("circuit closed event count = %d, want %d", got, want)
	}
}
