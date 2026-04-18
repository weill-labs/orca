package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPRPollEmitsGitHubRateLimitEventWhenCICheckStateLookupIsRateLimited(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1367", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, "HTTP 429: API rate limit exceeded\nRetry-After: 120\n", errors.New("gh: HTTP 429"))

	d := deps.newDaemon(t)

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-1367"))
	d.applyTaskStateUpdate(context.Background(), update)

	if got, want := deps.events.countType(EventPRRateLimited), 1; got != want {
		t.Fatalf("GitHub rate limit event count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventCIPollTrace), 0; got != want {
		t.Fatalf("CI poll trace event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 0; got != want {
		t.Fatalf("mergeable lookup count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}), 0; got != want {
		t.Fatalf("review lookup count = %d, want %d", got, want)
	}
}

func TestPRPollLogsAndEmitsTraceWhenCICheckStateLookupFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	logs := &fakeLogSink{}
	seedTaskMonitorAssignment(t, deps, "LAB-1367", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, ``, errors.New("ci lookup failed"))
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}, ``, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = logs.Printf
	})

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-1367"))
	d.applyTaskStateUpdate(context.Background(), update)

	want := `ci poll trace: issue=LAB-1367 pr_number=42 action=lookup_checks_error error="ci lookup failed"`
	assertLatestCIPollTrace(t, deps, logs, 1, want)
	if got, wantCount := deps.events.countType(EventPRRateLimited), 0; got != wantCount {
		t.Fatalf("GitHub rate limit event count = %d, want %d", got, wantCount)
	}
}

func TestPRPollLogsAndEmitsTraceWhenDetailedCICheckLookupFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	logs := &fakeLogSink{}
	seedTaskMonitorAssignment(t, deps, "LAB-1367", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prTerminalStateJSONFields}, `{"mergedAt":null}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"fail"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket,name,link"}, ``, errors.New("detailed ci lookup failed"))

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = logs.Printf
	})

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-1367"))
	d.applyTaskStateUpdate(context.Background(), update)

	want := `ci poll trace: issue=LAB-1367 pr_number=42 action=lookup_failed_checks_error error="detailed ci lookup failed"`
	assertLatestCIPollTrace(t, deps, logs, 1, want)
	if got, want := deps.amux.countKey("pane-1", expectedGenericCINudgePrompt(42)+"\n"), 1; got != want {
		t.Fatalf("generic CI nudge count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedCI), 1; got != want {
		t.Fatalf("CI nudge event count = %d, want %d", got, want)
	}
}

func assertLatestCIPollTrace(t *testing.T, deps *testDeps, logs *fakeLogSink, wantCount int, want string) {
	t.Helper()

	if got, wantCount := deps.events.countType(EventCIPollTrace), wantCount; got != wantCount {
		t.Fatalf("trace event sink count = %d, want %d", got, wantCount)
	}
	event, ok := deps.events.lastEventOfType(EventCIPollTrace)
	if !ok {
		t.Fatal("missing CI poll trace event in event sink")
	}
	if got, want := event.Message, want; got != want {
		t.Fatalf("event sink trace message = %q, want %q", got, want)
	}

	if got, wantCount := stateEventCountByType(deps.state, EventCIPollTrace), wantCount; got != wantCount {
		t.Fatalf("trace db event count = %d, want %d", got, wantCount)
	}
	event, ok = lastStateEventOfType(deps.state, EventCIPollTrace)
	if !ok {
		t.Fatal("missing CI poll trace event in state store")
	}
	if got, want := event.Message, want; got != want {
		t.Fatalf("state trace message = %q, want %q", got, want)
	}

	var traceLogs []string
	for _, message := range logs.messages() {
		if strings.HasPrefix(message, "ci poll trace:") {
			traceLogs = append(traceLogs, message)
		}
	}
	if got, wantCount := len(traceLogs), wantCount; got != wantCount {
		t.Fatalf("trace log count = %d, want %d", got, wantCount)
	}
	if got, want := traceLogs[len(traceLogs)-1], want; got != want {
		t.Fatalf("last trace log = %q, want %q", got, want)
	}
}
