package daemon

import (
	"context"
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
