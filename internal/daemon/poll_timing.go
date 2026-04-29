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

func pollTickGitHubCall(ctx context.Context) func() {
	if timing := pollTickTimingFromContext(ctx); timing != nil {
		return timing.githubCall()
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

func (t *pollTickTiming) githubCall() func() {
	if t == nil {
		return func() {}
	}
	startedAt := t.now()
	return func() {
		t.recordGitHubCall(t.now().Sub(startedAt))
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

func (t *pollTickTiming) recordGitHubCall(duration time.Duration) {
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
	t.mu.Unlock()

	parts := make([]string, 0, len(stages)+4)
	parts = append(parts, fmt.Sprintf("daemon %s tick timing:", t.kind))
	parts = append(parts, "total="+formatPollTickDuration(total))
	for _, stage := range stages {
		parts = append(parts, stage.name+"="+formatPollTickDuration(stage.duration))
	}
	parts = append(parts, fmt.Sprintf("github_calls=%d", githubCalls))
	parts = append(parts, "github_total="+formatPollTickDuration(githubDuration))

	logf("%s", strings.Join(parts, " "))
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
	done := r.timing.githubCall()
	defer done()
	return r.base.Run(ctx, dir, name, args...)
}
