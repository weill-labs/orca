package daemon

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

const ciFailedChecksOutput = `[{"bucket":"fail","name":"test","link":"https://ci.example.com/test"},{"bucket":"fail","name":"lint","link":"https://ci.example.com/lint"}]`

func expectedCINudgePrompt(prNumber int) string {
	return fmt.Sprintf("CI checks test, lint failed on PR #%d. Logs: test: https://ci.example.com/test; lint: https://ci.example.com/lint. Fix the failures and push.", prNumber)
}

func expectedGenericCINudgePrompt(prNumber int) string {
	return fmt.Sprintf("CI checks failed on PR #%d. Fix the failures and push.", prNumber)
}

func TestCIFailurePrompt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		prNumber     int
		failedChecks []prCheck
		want         string
	}{
		{
			name:     "no checks",
			prNumber: 42,
			want:     "CI checks failed on PR #42. Fix the failures and push.",
		},
		{
			name:     "single check with link",
			prNumber: 42,
			failedChecks: []prCheck{
				{Name: "test", Link: "https://ci.example.com/test"},
			},
			want: "CI check test failed on PR #42. Logs: https://ci.example.com/test. Fix the failure and push.",
		},
		{
			name:     "single check without link",
			prNumber: 42,
			failedChecks: []prCheck{
				{Name: "test"},
			},
			want: "CI check test failed on PR #42. Fix the failure and push.",
		},
		{
			name:     "multiple checks with links",
			prNumber: 42,
			failedChecks: []prCheck{
				{Name: "test", Link: "https://ci.example.com/test"},
				{Name: "lint", Link: "https://ci.example.com/lint"},
			},
			want: "CI checks test, lint failed on PR #42. Logs: test: https://ci.example.com/test; lint: https://ci.example.com/lint. Fix the failures and push.",
		},
		{
			name:     "multiple checks with partial links",
			prNumber: 42,
			failedChecks: []prCheck{
				{Name: "test", Link: "https://ci.example.com/test"},
				{Name: "lint"},
			},
			want: "CI checks test, lint failed on PR #42. Logs: test: https://ci.example.com/test. Fix the failures and push.",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := ciFailurePrompt(tt.prNumber, tt.failedChecks); got != tt.want {
				t.Fatalf("ciFailurePrompt() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPRPollRenudgesFailingCIOnScheduleAndEscalatesAfterMax(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	for _, bucket := range []string{"fail", "fail", "fail", "fail", "fail", "fail", "fail", "pass", "fail"} {
		deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"`+bucket+`"}]`, nil)
	}
	for range 4 {
		deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket,name,link"}, ciFailedChecksOutput, nil)
	}
	for range 9 {
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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "initial CI poll cycle completion")
	waitFor(t, "initial CI nudge", func() bool {
		return deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 1 &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket,name,link") == 1 &&
			deps.events.countType(EventWorkerNudgedCI) == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "first deferred CI poll cycle completion")
	waitFor(t, "first repeated failing CI poll", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 2
	})
	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "scheduled second CI nudge cycle completion")
	waitFor(t, "scheduled second CI nudge", func() bool {
		return deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 2 &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket,name,link") == 2 &&
			deps.events.countType(EventWorkerNudgedCI) == 2
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "second deferred CI poll cycle completion")
	waitFor(t, "second repeated failing CI poll", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 4
	})
	if got, want := deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n"), 2; got != want {
		t.Fatalf("nudge count after deferred failing poll = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "scheduled third CI nudge cycle completion")
	waitFor(t, "scheduled third CI nudge", func() bool {
		return deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 3 &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket,name,link") == 3 &&
			deps.events.countType(EventWorkerNudgedCI) == 3
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "third deferred CI poll cycle completion")
	waitFor(t, "third repeated failing CI poll", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 6
	})
	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "CI escalation cycle completion")
	waitFor(t, "ci escalation after nudge exhaustion", func() bool {
		return deps.events.countType(EventWorkerCIEscalated) == 1
	})
	if got, want := deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n"), 3; got != want {
		t.Fatalf("nudge count after CI escalation = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "passing CI recovery poll cycle completion")
	waitFor(t, "passing CI poll resets schedule", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 8 &&
			worker.LastCIState == ciStatePass
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "failing CI nudge after reset cycle completion")
	waitFor(t, "failing CI nudge after reset", func() bool {
		return deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 4 &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket,name,link") == 4 &&
			deps.events.countType(EventWorkerNudgedCI) == 4 &&
			deps.events.countType(EventWorkerCIEscalated) == 1
	})

	deps.events.requireTypes(t,
		EventDaemonStarted,
		EventTaskAssigned,
		EventPRDetected,
		EventWorkerNudgedCI,
		EventWorkerCIEscalated,
	)
}

func TestPRPollRetriesCINudgeAfterSendKeysFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	prTicker := newFakeTicker()
	deps.tickers.enqueue(newFakeTicker(), prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket,name,link"}, ciFailedChecksOutput, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket,name,link"}, ciFailedChecksOutput, nil)
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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "failed CI nudge attempt cycle completion")
	waitFor(t, "failed CI nudge attempt", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 1
	})

	if got, want := deps.amux.countKey("pane-1", "\n"), 0; got != want {
		t.Fatalf("successful nudge count after failed send = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedCI), 0; got != want {
		t.Fatalf("ci nudge event count after failed send = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "retried CI nudge cycle completion")
	waitFor(t, "retried CI nudge", func() bool {
		return deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 1 &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket,name,link") == 2 &&
			deps.events.countType(EventWorkerNudgedCI) == 1
	})
}

func TestPRPollFallsBackToGenericCINudgeWhenDetailedCheckLookupFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	prTicker := newFakeTicker()
	deps.tickers.enqueue(newFakeTicker(), prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket,name,link"}, ``, errors.New("details lookup failed"))
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)

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
	waitFor(t, "generic CI nudge", func() bool {
		return deps.amux.countKey("pane-1", expectedGenericCINudgePrompt(42)+"\n") == 1 &&
			deps.events.countType(EventWorkerNudgedCI) == 1
	})
}

func TestPRPollRetriesScheduledCIRenudgeAfterSendKeysFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	prTicker := newFakeTicker()
	deps.tickers.enqueue(newFakeTicker(), prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-689", "--json", "number"}, `[{"number":42}]`, nil)
	for range 4 {
		deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
		deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":null}`, nil)
	}
	for range 3 {
		deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket,name,link"}, ciFailedChecksOutput, nil)
	}
	deps.amux.sendKeysResults = []error{nil, nil, nil, nil, errors.New("ci re-nudge failed"), nil}

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

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "initial scheduled CI nudge cycle completion")
	waitFor(t, "initial CI nudge", func() bool {
		return deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 1 &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket,name,link") == 1 &&
			deps.events.countType(EventWorkerNudgedCI) == 1
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "deferred scheduled re-nudge cycle completion")
	waitFor(t, "deferred scheduled re-nudge", func() bool {
		return deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 2
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "failed scheduled re-nudge cycle completion")
	waitFor(t, "failed scheduled re-nudge attempt", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket") == 3 &&
			worker.CIFailurePollCount == ciFailureRenudgePollWindow
	})
	if got, want := deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n"), 1; got != want {
		t.Fatalf("successful nudge count after failed scheduled re-nudge = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedCI), 1; got != want {
		t.Fatalf("ci nudge event count after failed scheduled re-nudge = %d, want %d", got, want)
	}

	tickAndWaitForHeartbeat(t, d, deps, prTicker, time.Second, "retried scheduled re-nudge cycle completion")
	waitFor(t, "retried scheduled re-nudge", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok &&
			deps.amux.countKey("pane-1", expectedCINudgePrompt(42)+"\n") == 2 &&
			deps.commands.countCall("gh", "pr", "checks", "42", "--json", "bucket,name,link") == 3 &&
			deps.events.countType(EventWorkerNudgedCI) == 2 &&
			worker.CIFailurePollCount == 0
	})
}

func TestEventWorkerCIEscalatedUsesDotDelimitedName(t *testing.T) {
	t.Parallel()

	if got, want := EventWorkerCIEscalated, "worker.ci_escalated"; got != want {
		t.Fatalf("EventWorkerCIEscalated = %q, want %q", got, want)
	}
}

func TestEventWorkerNudgedCIUsesDotDelimitedName(t *testing.T) {
	t.Parallel()

	if got, want := EventWorkerNudgedCI, "worker.nudged_ci"; got != want {
		t.Fatalf("EventWorkerNudgedCI = %q, want %q", got, want)
	}
}
