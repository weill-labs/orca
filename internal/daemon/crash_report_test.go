package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestResumeRecordsCrashReportBeforeRestartingExitedWorker(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-925", "pane-1")

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("seeded worker missing")
	}
	worker.Health = WorkerHealthEscalated
	worker.RestartCount = 2
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

	scrollback := make([]string, 120)
	for i := range scrollback {
		scrollback[i] = fmt.Sprintf("line %03d", i+1)
	}

	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Exited:  true,
		Content: []string{"worker crashed"},
	}})
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Exited:  true,
		Content: scrollback,
	}})
	deps.amux.mu.Lock()
	deps.amux.paneExistsCalls = nil
	deps.amux.captureCalls = nil
	deps.amux.historyCaptureCalls = nil
	deps.amux.mu.Unlock()

	if err := d.Resume(ctx, "LAB-925", ""); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	if got, want := deps.amux.captureHistoryCount("pane-1"), 1; got != want {
		t.Fatalf("capture history count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerCrashReport), 1; got != want {
		t.Fatalf("worker crash report events = %d, want %d", got, want)
	}

	event, ok := deps.events.lastEventOfType(EventWorkerCrashReport)
	if !ok {
		t.Fatal("crash report event missing")
	}
	if got, want := event.Issue, "LAB-925"; got != want {
		t.Fatalf("event.Issue = %q, want %q", got, want)
	}
	if got, want := event.WorkerID, "worker-01"; got != want {
		t.Fatalf("event.WorkerID = %q, want %q", got, want)
	}
	if got, want := event.PaneID, "pane-1"; got != want {
		t.Fatalf("event.PaneID = %q, want %q", got, want)
	}
	if got, want := event.RestartAttempt, 3; got != want {
		t.Fatalf("event.RestartAttempt = %d, want %d", got, want)
	}
	if got, want := len(event.Scrollback), 100; got != want {
		t.Fatalf("len(event.Scrollback) = %d, want %d", got, want)
	}
	if got, want := event.Scrollback[0], "line 021"; got != want {
		t.Fatalf("event.Scrollback[0] = %q, want %q", got, want)
	}
	if got, want := event.Scrollback[len(event.Scrollback)-1], "line 120"; got != want {
		t.Fatalf("event.Scrollback[last] = %q, want %q", got, want)
	}
	if got, want := event.Time, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("event.Time = %v, want %v", got, want)
	}
	if !strings.Contains(event.Message, "restart attempt 3") {
		t.Fatalf("event.Message = %q, want restart attempt context", event.Message)
	}

	updatedWorker, ok := deps.state.worker("worker-01")
	if !ok {
		t.Fatal("worker missing after crash report resume")
	}
	if got, want := updatedWorker.RestartCount, 3; got != want {
		t.Fatalf("worker.RestartCount = %d, want %d", got, want)
	}
	if got, want := updatedWorker.FirstCrashAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("worker.FirstCrashAt = %v, want %v", got, want)
	}
}

func TestResumeRecordsCrashReportBeforeRestartingExitedPaneWithoutStoredWorker(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-925", "pane-1")

	task, ok := deps.state.task("LAB-925")
	if !ok {
		t.Fatal("seeded task missing")
	}
	task.Status = TaskStatusCancelled
	task.UpdatedAt = deps.clock.Now()
	if err := deps.state.PutTask(context.Background(), task); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}
	if err := deps.state.DeleteWorker(context.Background(), task.Project, task.WorkerID); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Exited:  true,
		Content: []string{"worker crashed"},
	}})
	deps.amux.captureHistorySequence("pane-1", []PaneCapture{{
		Exited:  true,
		Content: []string{"line 001", "line 002"},
	}})
	deps.amux.mu.Lock()
	deps.amux.paneExistsCalls = nil
	deps.amux.captureCalls = nil
	deps.amux.historyCaptureCalls = nil
	deps.amux.mu.Unlock()

	if err := d.Resume(ctx, "LAB-925", ""); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	event, ok := deps.events.lastEventOfType(EventWorkerCrashReport)
	if !ok {
		t.Fatal("crash report event missing")
	}
	if got, want := event.RestartAttempt, 1; got != want {
		t.Fatalf("event.RestartAttempt = %d, want %d", got, want)
	}
	if got, want := event.WorkerID, task.WorkerID; got != want {
		t.Fatalf("event.WorkerID = %q, want %q", got, want)
	}

	resumedTask, ok := deps.state.task("LAB-925")
	if !ok {
		t.Fatal("task missing after resume")
	}
	resumedWorker, ok := deps.state.worker(resumedTask.WorkerID)
	if !ok {
		t.Fatal("worker missing after resume")
	}
	if got, want := resumedWorker.RestartCount, 1; got != want {
		t.Fatalf("worker.RestartCount = %d, want %d", got, want)
	}
}

func TestResumeSkipsCrashReportForLivePane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-925", "pane-1")

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Content: []string{"still running"},
	}})
	deps.amux.mu.Lock()
	deps.amux.paneExistsCalls = nil
	deps.amux.captureCalls = nil
	deps.amux.historyCaptureCalls = nil
	deps.amux.mu.Unlock()

	if err := d.Resume(ctx, "LAB-925", ""); err != nil {
		t.Fatalf("Resume() error = %v", err)
	}

	if got, want := deps.amux.captureHistoryCount("pane-1"), 0; got != want {
		t.Fatalf("capture history count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerCrashReport), 0; got != want {
		t.Fatalf("worker crash report events = %d, want %d", got, want)
	}
}

func TestResumeReturnsErrorWhenCrashReportCaptureFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	seedActiveAssignment(t, deps, "LAB-925", "pane-1")

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{
		Exited: true,
	}})
	deps.amux.captureHistoryErrors("pane-1", []error{errors.New("history failed")})
	deps.amux.mu.Lock()
	deps.amux.paneExistsCalls = nil
	deps.amux.captureCalls = nil
	deps.amux.historyCaptureCalls = nil
	deps.amux.sentKeys = nil
	deps.amux.mu.Unlock()

	err := d.Resume(ctx, "LAB-925", "")
	if err == nil || !strings.Contains(err.Error(), "capture crash report") {
		t.Fatalf("Resume() error = %v, want crash report context", err)
	}

	if got, want := deps.events.countType(EventWorkerCrashReport), 0; got != want {
		t.Fatalf("worker crash report events = %d, want %d", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
}

func TestNextRestartAttemptResetsWindow(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)
	restartAttempt, firstCrashAt := nextRestartAttempt(Worker{
		RestartCount: 2,
		FirstCrashAt: now.Add(-6 * time.Minute),
	}, now)

	if got, want := restartAttempt, 1; got != want {
		t.Fatalf("restartAttempt = %d, want %d", got, want)
	}
	if got, want := firstCrashAt, now; !got.Equal(want) {
		t.Fatalf("firstCrashAt = %v, want %v", got, want)
	}
}

func TestSQLiteStateAdapterRecordsCrashReportPayload(t *testing.T) {
	t.Parallel()

	store := openDaemonStateStore(t)
	adapter := newSQLiteStateAdapter(store)
	now := time.Date(2026, 4, 9, 12, 0, 0, 0, time.UTC)

	if err := adapter.PutTask(context.Background(), Task{
		Project:      "/repo",
		Issue:        "LAB-925",
		Status:       TaskStatusActive,
		PaneID:       "pane-1",
		AgentProfile: "codex",
		CreatedAt:    now,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}

	if err := adapter.RecordEvent(context.Background(), Event{
		Time:           now,
		Type:           EventWorkerCrashReport,
		Project:        "/repo",
		Issue:          "LAB-925",
		WorkerID:       "worker-01",
		PaneID:         "pane-1",
		RestartAttempt: 2,
		Scrollback:     []string{"line 001", "line 002"},
		Message:        "captured crash report before restart attempt 2",
	}); err != nil {
		t.Fatalf("RecordEvent() error = %v", err)
	}

	taskStatus, err := store.TaskStatus(context.Background(), "/repo", "LAB-925")
	if err != nil {
		t.Fatalf("TaskStatus() error = %v", err)
	}
	if got, want := len(taskStatus.Events), 1; got != want {
		t.Fatalf("len(taskStatus.Events) = %d, want %d", got, want)
	}

	var payload Event
	if err := json.Unmarshal(taskStatus.Events[0].Payload, &payload); err != nil {
		t.Fatalf("json.Unmarshal(payload) error = %v", err)
	}
	if got, want := payload.Type, EventWorkerCrashReport; got != want {
		t.Fatalf("payload.Type = %q, want %q", got, want)
	}
	if got, want := payload.WorkerID, "worker-01"; got != want {
		t.Fatalf("payload.WorkerID = %q, want %q", got, want)
	}
	if got, want := payload.PaneID, "pane-1"; got != want {
		t.Fatalf("payload.PaneID = %q, want %q", got, want)
	}
	if got, want := payload.RestartAttempt, 2; got != want {
		t.Fatalf("payload.RestartAttempt = %d, want %d", got, want)
	}
	if got, want := payload.Scrollback, []string{"line 001", "line 002"}; fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
		t.Fatalf("payload.Scrollback = %#v, want %#v", got, want)
	}
}
