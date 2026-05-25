package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type pollTickTimingContextKey struct{}

type pollTickTiming struct {
	kind      string
	now       func() time.Time
	startedAt time.Time

	mu             sync.Mutex
	stages         []pollTickStageTiming
	githubCalls    int
	githubDuration time.Duration
	githubAPI      githubAPICounters
}

type pollTickStageTiming struct {
	name     string
	duration time.Duration
}

func newPollTickTiming(kind string, now func() time.Time) *pollTickTiming {
	if now == nil {
		now = time.Now
	}
	return &pollTickTiming{
		kind:      kind,
		now:       now,
		startedAt: now(),
	}
}

func withPollTickTiming(ctx context.Context, timing *pollTickTiming) context.Context {
	if timing == nil {
		return ctx
	}
	return context.WithValue(ctx, pollTickTimingContextKey{}, timing)
}

func pollTickTimingFromContext(ctx context.Context) *pollTickTiming {
	if ctx == nil {
		return nil
	}
	if timing, ok := ctx.Value(pollTickTimingContextKey{}).(*pollTickTiming); ok {
		return timing
	}
	return nil
}

func pollTickGitHubCall(ctx context.Context, args ...string) func() {
	if timing := pollTickTimingFromContext(ctx); timing != nil {
		return timing.githubCall(classifyGitHubAPICall(ctx, args))
	}
	return func() {}
}

func (t *pollTickTiming) stage(name string) func() {
	if t == nil {
		return func() {}
	}
	startedAt := t.now()
	return func() {
		t.recordStage(name, t.now().Sub(startedAt))
	}
}

func (t *pollTickTiming) githubCall(call githubAPICallMetrics) func() {
	if t == nil {
		return func() {}
	}
	startedAt := t.now()
	return func() {
		t.recordGitHubCall(t.now().Sub(startedAt), call)
	}
}

func (t *pollTickTiming) recordStage(name string, duration time.Duration) {
	if t == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.stages = append(t.stages, pollTickStageTiming{name: name, duration: duration})
}

func (t *pollTickTiming) recordGitHubCall(duration time.Duration, call githubAPICallMetrics) {
	if t == nil {
		return
	}
	if duration < 0 {
		duration = 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.githubCalls++
	t.githubDuration += duration
	t.githubAPI.add(call)
}

func (t *pollTickTiming) githubAPICounters() githubAPICounters {
	if t == nil {
		return githubAPICounters{}
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.githubAPI
}

func (t *pollTickTiming) log(logf func(string, ...any)) {
	if t == nil || logf == nil {
		return
	}

	total := t.now().Sub(t.startedAt)
	if total < 0 {
		total = 0
	}

	t.mu.Lock()
	stages := append([]pollTickStageTiming(nil), t.stages...)
	githubCalls := t.githubCalls
	githubDuration := t.githubDuration
	githubAPI := t.githubAPI
	t.mu.Unlock()

	parts := make([]string, 0, len(stages)+9)
	parts = append(parts, fmt.Sprintf("daemon %s tick timing:", t.kind))
	parts = append(parts, "total="+formatPollTickDuration(total))
	for _, stage := range stages {
		parts = append(parts, stage.name+"="+formatPollTickDuration(stage.duration))
	}
	parts = append(parts, fmt.Sprintf("github_calls=%d", githubCalls))
	parts = append(parts, "github_total="+formatPollTickDuration(githubDuration))
	parts = append(parts, fmt.Sprintf("github_graphql_calls=%d", githubAPI.GraphQLCalls))
	parts = append(parts, fmt.Sprintf("github_graphql_estimated_points=%d", githubAPI.GraphQLEstimatedPoints))
	parts = append(parts, fmt.Sprintf("github_graphql_terminal_state_calls=%d", githubAPI.GraphQLTerminalStateCalls))
	parts = append(parts, fmt.Sprintf("github_graphql_review_calls=%d", githubAPI.GraphQLReviewCalls))
	parts = append(parts, fmt.Sprintf("github_rest_calls=%d", githubAPI.RESTCalls))

	logf("%s", strings.Join(parts, " "))
}

func (d *Daemon) logPollBetweenTicks(previousFinishedAt, startedAt time.Time, interval time.Duration) {
	if d.logf == nil || previousFinishedAt.IsZero() {
		return
	}
	gap := startedAt.Sub(previousFinishedAt)
	if gap < 0 {
		gap = 0
	}
	d.logf(
		"daemon poll between ticks: gap=%s interval=%s",
		formatPollTickDuration(gap),
		formatPollTickDuration(interval),
	)
}

func formatPollTickDuration(duration time.Duration) string {
	if duration <= 0 {
		return "0s"
	}
	return duration.Round(time.Millisecond).String()
}

type timingCommandRunner struct {
	base   CommandRunner
	timing *pollTickTiming
}

func newTimingCommandRunner(base CommandRunner, timing *pollTickTiming) CommandRunner {
	if base == nil || timing == nil {
		return base
	}
	return &timingCommandRunner{base: base, timing: timing}
}

func (r *timingCommandRunner) Run(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	if name != "gh" {
		return r.base.Run(ctx, dir, name, args...)
	}
	done := r.timing.githubCall(classifyGitHubAPICall(ctx, args))
	defer done()
	return r.base.Run(ctx, dir, name, args...)
}
