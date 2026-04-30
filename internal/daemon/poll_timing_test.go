package daemon

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDaemonPollLoopLogsPollTickTiming(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)

	logs := &fakeLogSink{}
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = logs.Printf
	})

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.clock.Advance(5 * time.Second)
	pollTicker.tick(deps.clock.Now())

	message := waitForPollTickTimingLog(t, logs)
	for _, fragment := range []string{
		"daemon poll tick timing:",
		"total=",
		"merge_updates_initial=",
		"state_read=",
		"resume_workers=",
		"pane_reconcile=",
		"schedule_filter=",
		"monitor_checks=",
		"dispatch_merge_queue=",
		"merge_updates_final=",
		"missing_pr_reconcile=",
		"github_calls=0",
		"github_total=0s",
	} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("poll timing log = %q, want fragment %q", message, fragment)
		}
	}
}

func TestDaemonPollLoopLogsPollBetweenTickGap(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)

	logs := &fakeLogSink{}
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = logs.Printf
		opts.PollInterval = 5 * time.Second
	})

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.clock.Advance(5 * time.Second)
	pollTicker.tick(deps.clock.Now())
	waitForPollTickTimingLog(t, logs)

	deps.clock.Advance(7 * time.Second)
	pollTicker.tick(deps.clock.Now())

	message := waitForPollBetweenTickLog(t, logs)
	for _, fragment := range []string{
		"daemon poll between ticks:",
		"gap=7s",
		"interval=5s",
	} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("poll between-ticks log = %q, want fragment %q", message, fragment)
		}
	}
}

func TestDaemonPollLoopTimingTracksStateAndGitHubDurations(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)
	seedTaskMonitorAssignment(t, deps, "LAB-1486", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":null,"state":"OPEN","closedAt":null,"mergeable":"MERGEABLE","mergeStateStatus":"CLEAN","updatedAt":"2026-04-02T09:00:00Z","reviewDecision":"","reviews":[],"comments":[]}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)

	state := &advancingStateStore{fakeState: deps.state, clock: deps.clock}
	commands := &advancingCommandRunner{base: deps.commands, clock: deps.clock, ghDelay: 3 * time.Second}
	logs := &fakeLogSink{}
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.State = state
		opts.Commands = commands
		opts.Logf = logs.Printf
	})

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	state.setNonTerminalTasksDelay(2 * time.Second)
	deps.clock.Advance(5 * time.Second)
	pollTicker.tick(deps.clock.Now())

	message := waitForPollTickTimingLog(t, logs)
	for _, fragment := range []string{
		"state_read=2s",
		"schedule_filter=2s",
		"github_calls=4",
		"github_total=3s",
	} {
		if !strings.Contains(message, fragment) {
			t.Fatalf("poll timing log = %q, want fragment %q", message, fragment)
		}
	}
}

func waitForPollTickTimingLog(t *testing.T, logs *fakeLogSink) string {
	t.Helper()

	var message string
	waitFor(t, "poll tick timing log", func() bool {
		for _, candidate := range logs.messages() {
			if strings.Contains(candidate, "daemon poll tick timing:") {
				message = candidate
				return true
			}
		}
		return false
	})
	return message
}

func waitForPollBetweenTickLog(t *testing.T, logs *fakeLogSink) string {
	t.Helper()

	var message string
	waitFor(t, "poll between-ticks log", func() bool {
		for _, candidate := range logs.messages() {
			if strings.Contains(candidate, "daemon poll between ticks:") {
				message = candidate
				return true
			}
		}
		return false
	})
	return message
}

type advancingStateStore struct {
	*fakeState
	clock *fakeClock

	mu                    sync.Mutex
	nonTerminalTasksDelay time.Duration
}

func (s *advancingStateStore) setNonTerminalTasksDelay(delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nonTerminalTasksDelay = delay
}

func (s *advancingStateStore) NonTerminalTasks(ctx context.Context, project string) ([]Task, error) {
	s.mu.Lock()
	delay := s.nonTerminalTasksDelay
	s.mu.Unlock()
	if delay > 0 {
		s.clock.Advance(delay)
	}
	return s.fakeState.NonTerminalTasks(ctx, project)
}

type advancingCommandRunner struct {
	base    *fakeCommands
	clock   *fakeClock
	ghDelay time.Duration
}

func (r *advancingCommandRunner) Run(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	if name == "gh" && r.ghDelay > 0 {
		r.clock.Advance(r.ghDelay)
	}
	return r.base.Run(ctx, dir, name, args...)
}
