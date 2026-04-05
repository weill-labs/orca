package daemon

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type daemonResumer interface {
	Resume(context.Context, string) error
}

func TestResumeRestartsExistingWorkerInPlace(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-757", "pane-1")
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("seeded worker missing")
	}
	worker.Health = WorkerHealthEscalated
	worker.NudgeCount = 2
	worker.LastCapture = "shell prompt"
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	deps.amux.mu.Lock()
	deps.amux.paneExistsCalls = nil
	deps.amux.captureCalls = nil
	deps.amux.mu.Unlock()

	resumer, ok := any(d).(daemonResumer)
	if !ok {
		t.Fatal("Daemon does not implement Resume")
	}

	if err := resumer.Resume(ctx, "LAB-757"); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	if got, want := deps.amux.paneExistsCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pane exists calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{"codex --yolo resume\n", "\n", "."})
	if got, want := len(deps.amux.waitIdleCalls), 2; got != want {
		t.Fatalf("waitIdle call count = %d, want %d", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 0; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
	if got, want := deps.pool.acquireCallCount(), 0; got != want {
		t.Fatalf("pool acquire calls = %d, want %d", got, want)
	}

	task, ok := deps.state.task("LAB-757")
	if !ok {
		t.Fatal("task missing after resume")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
}

func TestResumeRejectsMissingPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-757", "pane-1")

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	deps.amux.mu.Lock()
	deps.amux.paneExistsCalls = nil
	deps.amux.paneExists = map[string]bool{"pane-1": false}
	deps.amux.mu.Unlock()

	resumer, ok := any(d).(daemonResumer)
	if !ok {
		t.Fatal("Daemon does not implement Resume")
	}

	err := resumer.Resume(ctx, "LAB-757")
	if err == nil {
		t.Fatal("Resume() succeeded, want error")
	}
	if !strings.Contains(err.Error(), "pane") {
		t.Fatalf("Resume() error = %v, want pane context", err)
	}

	if got, want := deps.amux.paneExistsCalls, []string{"pane-1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pane exists calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
	if got := len(deps.amux.waitIdleCalls); got != 0 {
		t.Fatalf("waitIdle calls = %d, want 0", got)
	}
}
