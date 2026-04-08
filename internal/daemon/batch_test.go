package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestBatchAssignsEachEntryAndSleepsBetweenAssignments(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())

	cloneTwoPath := filepath.Join(filepath.Dir(deps.pool.clone.Path), "clone-02")
	if err := os.MkdirAll(cloneTwoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cloneTwoPath, err)
	}
	deps.pool.clones = []Clone{
		deps.pool.clone,
		{Name: "clone-02", Path: cloneTwoPath},
	}
	deps.amux.spawnResults = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}

	var sleeps []time.Duration
	deps.sleep = func(_ context.Context, delay time.Duration) error {
		sleeps = append(sleeps, delay)
		deps.clock.Advance(delay)
		return nil
	}

	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	result, err := d.Batch(ctx, BatchRequest{
		Entries: []BatchEntry{
			{Issue: "LAB-689", Agent: "codex", Prompt: "Implement daemon core"},
			{Issue: "LAB-690", Agent: "codex", Prompt: "Implement merge queue", Title: "Merge queue title"},
		},
		Delay: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Batch() error = %v", err)
	}

	if got, want := sleeps, []time.Duration{5 * time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep durations = %#v, want %#v", got, want)
	}
	if got, want := len(result.Results), 2; got != want {
		t.Fatalf("result count = %d, want %d", got, want)
	}
	if got, want := []string{result.Results[0].Issue, result.Results[1].Issue}, []string{"LAB-689", "LAB-690"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result issues = %#v, want %#v", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 2; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 2; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}

	taskOne, ok := deps.state.task("LAB-689")
	if !ok {
		t.Fatal("missing first task")
	}
	if got, want := taskOne.Status, TaskStatusActive; got != want {
		t.Fatalf("first task status = %q, want %q", got, want)
	}

	taskTwo, ok := deps.state.task("LAB-690")
	if !ok {
		t.Fatal("missing second task")
	}
	if got, want := taskTwo.Status, TaskStatusActive; got != want {
		t.Fatalf("second task status = %q, want %q", got, want)
	}

	deps.amux.requireMetadata(t, "pane-2", map[string]string{
		"agent_profile":  "codex",
		"branch":         "LAB-690",
		"task":           "Merge queue title",
		"tracked_issues": `[{"id":"LAB-690","status":"active"}]`,
	})
}

func TestBatchValidatesManifestBeforeAssigning(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	_, err := d.Batch(ctx, BatchRequest{
		Entries: []BatchEntry{{Agent: "codex", Prompt: "Implement daemon core"}},
		Delay:   5 * time.Second,
	})
	if err == nil {
		t.Fatal("Batch() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "batch manifest entry 1 requires issue") {
		t.Fatalf("Batch() error = %v, want manifest validation", err)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
}

func TestValidateBatchEntriesRejectsAdditionalInvalidInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		entries []BatchEntry
		wantErr string
	}{
		{name: "empty", entries: nil, wantErr: "batch manifest requires at least one entry"},
		{name: "missing agent", entries: []BatchEntry{{Issue: "LAB-689", Prompt: "Implement daemon core"}}, wantErr: "batch manifest entry 1 requires agent"},
		{name: "missing prompt", entries: []BatchEntry{{Issue: "LAB-689", Agent: "codex"}}, wantErr: "batch manifest entry 1 requires prompt"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateBatchEntries(tt.entries)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateBatchEntries() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestBatchRejectsNegativeDelay(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	_, err := d.Batch(ctx, BatchRequest{
		Entries: []BatchEntry{{Issue: "LAB-689", Agent: "codex", Prompt: "Implement daemon core"}},
		Delay:   -time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "batch delay must be non-negative") {
		t.Fatalf("Batch() error = %v, want negative delay validation", err)
	}
}

func TestBatchContinuesAfterAssignErrorAndLogsFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())

	cloneTwoPath := filepath.Join(filepath.Dir(deps.pool.clone.Path), "clone-02")
	if err := os.MkdirAll(cloneTwoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cloneTwoPath, err)
	}
	deps.pool.clones = []Clone{
		deps.pool.clone,
		{Name: "clone-02", Path: cloneTwoPath},
	}
	deps.amux.spawnResults = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}

	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	result, err := d.Batch(ctx, BatchRequest{
		Entries: []BatchEntry{
			{Issue: "LAB-689", Agent: "codex", Prompt: "Implement daemon core"},
			{Issue: "LAB-690", Agent: "missing", Prompt: "Implement missing profile handling"},
			{Issue: "LAB-691", Agent: "codex", Prompt: "Implement merge queue"},
		},
	})
	if err != nil {
		t.Fatalf("Batch() error = %v", err)
	}
	if got, want := len(result.Results), 2; got != want {
		t.Fatalf("result count = %d, want %d", got, want)
	}
	if got, want := []string{result.Results[0].Issue, result.Results[1].Issue}, []string{"LAB-689", "LAB-691"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result issues = %#v, want %#v", got, want)
	}
	if got, want := len(result.Failures), 1; got != want {
		t.Fatalf("failure count = %d, want %d", got, want)
	}
	if got, want := result.Failures[0].Issue, "LAB-690"; got != want {
		t.Fatalf("failure issue = %q, want %q", got, want)
	}
	if got := result.Failures[0].Error; !strings.Contains(got, `load agent profile "missing"`) {
		t.Fatalf("failure error = %q, want missing profile context", got)
	}
	if got, want := deps.pool.acquireCallCount(), 2; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 2; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
	if _, ok := deps.state.task("LAB-690"); ok {
		t.Fatal("failed task stored")
	}
	if got, want := deps.events.countType(EventTaskAssignFailed), 1; got != want {
		t.Fatalf("assign failure events = %d, want %d", got, want)
	}
	if message := deps.events.lastMessage(EventTaskAssignFailed); !strings.Contains(message, `load agent profile "missing"`) {
		t.Fatalf("assign failure message = %q, want missing profile context", message)
	}
}

func TestBatchSleepsBetweenEntriesEvenWhenPreviousEntryFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())

	cloneTwoPath := filepath.Join(filepath.Dir(deps.pool.clone.Path), "clone-02")
	cloneThreePath := filepath.Join(filepath.Dir(deps.pool.clone.Path), "clone-03")
	for _, path := range []string{cloneTwoPath, cloneThreePath} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", path, err)
		}
	}
	deps.pool.clones = []Clone{
		deps.pool.clone,
		{Name: "clone-02", Path: cloneTwoPath},
		{Name: "clone-03", Path: cloneThreePath},
	}
	deps.amux.spawnResults = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}

	var sleeps []time.Duration
	deps.sleep = func(_ context.Context, delay time.Duration) error {
		sleeps = append(sleeps, delay)
		deps.clock.Advance(delay)
		return nil
	}

	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	result, err := d.Batch(ctx, BatchRequest{
		Entries: []BatchEntry{
			{Issue: "LAB-689", Agent: "codex", Prompt: "Implement daemon core"},
			{Issue: "LAB-690", Agent: "missing", Prompt: "Implement missing profile handling"},
			{Issue: "LAB-691", Agent: "codex", Prompt: "Implement merge queue"},
		},
		Delay: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Batch() error = %v", err)
	}

	if got, want := sleeps, []time.Duration{5 * time.Second, 5 * time.Second}; !reflect.DeepEqual(got, want) {
		t.Fatalf("sleep durations = %#v, want %#v", got, want)
	}
	if got, want := len(result.Results), 2; got != want {
		t.Fatalf("result count = %d, want %d", got, want)
	}
	if got, want := []string{result.Results[0].Issue, result.Results[1].Issue}, []string{"LAB-689", "LAB-691"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("result issues = %#v, want %#v", got, want)
	}
	if got, want := len(result.Failures), 1; got != want {
		t.Fatalf("failure count = %d, want %d", got, want)
	}
	if got, want := result.Failures[0].Issue, "LAB-690"; got != want {
		t.Fatalf("failure issue = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventTaskAssignFailed), 1; got != want {
		t.Fatalf("assign failure events = %d, want %d", got, want)
	}
}

func TestBatchPropagatesTaskLookupError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	d.state = &taskLookupErrorState{
		StateStore: d.state,
		issue:      "LAB-689",
		err:        errors.New("task lookup failed"),
		failAfter:  1,
	}
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	_, err := d.Batch(ctx, BatchRequest{
		Entries: []BatchEntry{{Issue: "LAB-689", Agent: "codex", Prompt: "Implement daemon core"}},
	})
	if err == nil || !strings.Contains(err.Error(), "load task LAB-689: task lookup failed") {
		t.Fatalf("Batch() error = %v, want task lookup error", err)
	}
}

func TestBatchPropagatesSleepError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())

	cloneTwoPath := filepath.Join(filepath.Dir(deps.pool.clone.Path), "clone-02")
	if err := os.MkdirAll(cloneTwoPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cloneTwoPath, err)
	}
	deps.pool.clones = []Clone{
		deps.pool.clone,
		{Name: "clone-02", Path: cloneTwoPath},
	}
	deps.amux.spawnResults = []Pane{
		{ID: "pane-1", Name: "worker-1"},
		{ID: "pane-2", Name: "worker-2"},
	}
	deps.sleep = func(context.Context, time.Duration) error {
		return errors.New("sleep failed")
	}

	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	_, err := d.Batch(ctx, BatchRequest{
		Entries: []BatchEntry{
			{Issue: "LAB-689", Agent: "codex", Prompt: "Implement daemon core"},
			{Issue: "LAB-690", Agent: "codex", Prompt: "Implement merge queue"},
		},
		Delay: time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "wait between batch assigns: sleep failed") {
		t.Fatalf("Batch() error = %v, want sleep error", err)
	}
}

type taskLookupErrorState struct {
	StateStore
	issue     string
	err       error
	failAfter int
	calls     int
}

func (s *taskLookupErrorState) TaskByIssue(ctx context.Context, project, issue string) (Task, error) {
	s.calls++
	if issue == s.issue && s.calls > s.failAfter {
		return Task{}, s.err
	}
	return s.StateStore.TaskByIssue(ctx, project, issue)
}
