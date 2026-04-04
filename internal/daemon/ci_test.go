package daemon

import (
	"context"
	"errors"
	"testing"
)

func TestPRPollNudgesWorkerOncePerFailingCITransition(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	for range 4 {
		deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	}

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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "initial CI nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 1 && deps.events.countType(EventWorkerNudgedCI) == 1
	})

	if got, want := deps.events.countType(EventWorkerNudgedCI), 1; got != want {
		t.Fatalf("ci nudge event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket"), 1; got != want {
		t.Fatalf("gh pr checks calls = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second CI poll", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 2
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 1; got != want {
		t.Fatalf("nudge count after repeated failure = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedCI), 1; got != want {
		t.Fatalf("ci nudge event count after repeated failure = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "passing CI poll", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 3
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 1; got != want {
		t.Fatalf("nudge count after CI recovery = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "second CI failure nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 2 && deps.events.countType(EventWorkerNudgedCI) == 2
	})

	if got, want := deps.events.countType(EventWorkerNudgedCI), 2; got != want {
		t.Fatalf("ci nudge event count after second failure = %d, want %d", got, want)
	}
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventWorkerNudgedCI)
}

func TestPRPollRetriesCINudgeAfterSendKeysFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	prTicker := newFakeTicker()
	deps.tickers.enqueue(newFakeTicker(), prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	for range 2 {
		deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	}
	deps.amux.sendKeysResults = []error{nil, nil, errors.New("ci nudge failed"), nil}

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

	prTicker.tick(deps.clock.Now())
	waitFor(t, "failed CI nudge attempt", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 1
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 0; got != want {
		t.Fatalf("successful nudge count after failed send = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedCI), 0; got != want {
		t.Fatalf("ci nudge event count after failed send = %d, want %d", got, want)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "retried CI nudge", func() bool {
		return deps.amux.countKey("pane-1", "\n") == 1 && deps.events.countType(EventWorkerNudgedCI) == 1
	})
}

func TestEventWorkerNudgedCIUsesDotDelimitedName(t *testing.T) {
	t.Parallel()

	if got, want := EventWorkerNudgedCI, "worker.nudged_ci"; got != want {
		t.Fatalf("EventWorkerNudgedCI = %q, want %q", got, want)
	}
}
