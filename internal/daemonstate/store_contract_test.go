package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	legacy "github.com/weill-labs/orca/internal/state"
)

type storeContract interface {
	ProjectStatus(context.Context, string) (ProjectStatus, error)
	ProjectStatusAllHosts(context.Context, string) (ProjectStatus, error)
	UpsertDaemon(context.Context, string, DaemonStatus) error
	EnsureClone(context.Context, string, string) (legacy.CloneRecord, error)
	TryOccupyClone(context.Context, string, string, string, string) (bool, error)
	UpsertTask(context.Context, string, Task) error
	UpsertWorker(context.Context, string, Worker) error
	ClaimWorker(context.Context, string, Worker) (Worker, error)
	AppendEvent(context.Context, Event) (Event, error)
	TaskStatus(context.Context, string, string) (TaskStatus, error)
	TaskStatusAllHosts(context.Context, string, string) (TaskStatus, error)
	ListWorkers(context.Context, string) ([]Worker, error)
	ListClones(context.Context, string) ([]Clone, error)
	Events(context.Context, string, int64) (<-chan Event, <-chan error)
	UpdateTaskStatus(context.Context, string, string, string, time.Time) (Task, error)
	DeleteWorker(context.Context, string, string) error
	DeleteTask(context.Context, string, string) error
	MarkCloneFree(context.Context, string, string) error
	MarkDaemonStopped(context.Context, string, time.Time) error
	lookupCloneRecord(context.Context, string, string) (legacy.CloneRecord, error)
	AllNonTerminalTasks(context.Context) ([]Task, error)
	ActiveAssignments(context.Context, string) ([]Assignment, error)
	AllActiveAssignments(context.Context) ([]Assignment, error)
	ActiveAssignmentByIssue(context.Context, string, string) (Assignment, error)
	ActiveAssignmentByBranch(context.Context, string, string) (Assignment, error)
	ActiveAssignmentByPRNumber(context.Context, string, int) (Assignment, error)
	AllMergeEntries(context.Context) ([]MergeQueueEntry, error)
	EnqueueMergeEntry(context.Context, MergeQueueEntry) (int, error)
	ClaimTask(context.Context, string, Task) (*Task, error)
	listTasks(context.Context, string) ([]Task, error)
	NonTerminalTasks(context.Context, string) ([]Task, error)
	TasksByPane(context.Context, string, string) ([]Task, error)
	WorkerByID(context.Context, string, string) (Worker, error)
	WorkerByPane(context.Context, string, string) (Worker, error)
	StaleCloneOccupancies(context.Context, string) ([]CloneOccupancy, error)
	MergeEntry(context.Context, string, int) (*MergeQueueEntry, error)
	MergeEntries(context.Context, string) ([]MergeQueueEntry, error)
	UpdateMergeEntry(context.Context, MergeQueueEntry) error
	DeleteMergeEntry(context.Context, string, int) error
}

type storeContractHarness struct {
	store             storeContract
	setNow            func(time.Time)
	setHost           func(string)
	assertHostColumns func(*testing.T)
}

func testStoreLifecycleAndQueries(t *testing.T, h storeContractHarness) {
	t.Helper()

	project := "/repo"
	now := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	h.setNow(now)

	status, err := h.store.ProjectStatus(context.Background(), project)
	if err != nil {
		t.Fatalf("ProjectStatus() empty error = %v", err)
	}
	if status.Daemon != nil {
		t.Fatalf("empty status daemon = %#v, want nil", status.Daemon)
	}

	if err := h.store.UpsertDaemon(context.Background(), project, DaemonStatus{
		Session: "orca",
		PID:     42,
		Status:  "running",
	}); err != nil {
		t.Fatalf("UpsertDaemon() error = %v", err)
	}

	clonePath := filepath.Join(t.TempDir(), "orca01")
	record, err := h.store.EnsureClone(context.Background(), project, clonePath)
	if err != nil {
		t.Fatalf("EnsureClone() error = %v", err)
	}
	if got, want := record.Status, legacy.CloneStatusFree; got != want {
		t.Fatalf("record.Status = %q, want %q", got, want)
	}

	ok, err := h.store.TryOccupyClone(context.Background(), project, clonePath, "LAB-718", "LAB-718")
	if err != nil {
		t.Fatalf("TryOccupyClone() error = %v", err)
	}
	if !ok {
		t.Fatal("TryOccupyClone() = false, want true")
	}

	ok, err = h.store.TryOccupyClone(context.Background(), project, clonePath, "LAB-718", "LAB-718")
	if err != nil {
		t.Fatalf("TryOccupyClone() second call error = %v", err)
	}
	if ok {
		t.Fatal("TryOccupyClone() second call = true, want false")
	}

	prNumber := 17
	if err := h.store.UpsertTask(context.Background(), project, Task{
		Issue:     "LAB-718",
		Status:    "active",
		State:     "assigned",
		Agent:     "codex",
		Prompt:    "Implement socket IPC",
		WorkerID:  "worker-01",
		ClonePath: clonePath,
		PRNumber:  &prNumber,
	}); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	if err := h.store.UpsertWorker(context.Background(), project, Worker{
		WorkerID:      "worker-01",
		CurrentPaneID: "pane-1",
		Agent:         "codex",
		State:         "healthy",
		Issue:         "LAB-718",
		ClonePath:     clonePath,
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	firstEvent, err := h.store.AppendEvent(context.Background(), Event{
		Project:  project,
		Kind:     "task.assigned",
		Issue:    "LAB-718",
		WorkerID: "worker-01",
		Message:  "LAB-718 assigned",
		Payload:  []byte(`{"pane":"pane-1"}`),
	})
	if err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	secondEvent, err := h.store.AppendEvent(context.Background(), Event{
		Project: project,
		Kind:    "task.updated",
		Issue:   "LAB-718",
		Message: "LAB-718 updated",
	})
	if err != nil {
		t.Fatalf("AppendEvent() second error = %v", err)
	}

	status, err = h.store.ProjectStatus(context.Background(), project)
	if err != nil {
		t.Fatalf("ProjectStatus() error = %v", err)
	}
	if status.Daemon == nil || status.Daemon.Status != "running" {
		t.Fatalf("status.Daemon = %#v, want running daemon", status.Daemon)
	}
	if got, want := status.Summary.Active, 1; got != want {
		t.Fatalf("status.Summary.Active = %d, want %d", got, want)
	}
	if got, want := status.Summary.Workers, 1; got != want {
		t.Fatalf("status.Summary.Workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.FreeClones, 0; got != want {
		t.Fatalf("status.Summary.FreeClones = %d, want %d", got, want)
	}

	taskStatus, err := h.store.TaskStatus(context.Background(), project, "LAB-718")
	if err != nil {
		t.Fatalf("TaskStatus() error = %v", err)
	}
	if got, want := taskStatus.Task.Issue, "LAB-718"; got != want {
		t.Fatalf("taskStatus.Task.Issue = %q, want %q", got, want)
	}
	if got, want := taskStatus.Task.State, "assigned"; got != want {
		t.Fatalf("taskStatus.Task.State = %q, want %q", got, want)
	}
	if taskStatus.Task.PRNumber == nil || *taskStatus.Task.PRNumber != 17 {
		t.Fatalf("taskStatus.Task.PRNumber = %#v, want 17", taskStatus.Task.PRNumber)
	}
	if got, want := len(taskStatus.Events), 2; got != want {
		t.Fatalf("len(taskStatus.Events) = %d, want %d", got, want)
	}
	if string(taskStatus.Events[0].Payload) != `{"pane":"pane-1"}` {
		t.Fatalf("taskStatus.Events[0].Payload = %s", taskStatus.Events[0].Payload)
	}

	workers, err := h.store.ListWorkers(context.Background(), project)
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	if got, want := len(workers), 1; got != want {
		t.Fatalf("len(workers) = %d, want %d", got, want)
	}

	clones, err := h.store.ListClones(context.Background(), project)
	if err != nil {
		t.Fatalf("ListClones() error = %v", err)
	}
	if got, want := len(clones), 1; got != want {
		t.Fatalf("len(clones) = %d, want %d", got, want)
	}
	if got, want := clones[0].Status, "occupied"; got != want {
		t.Fatalf("clones[0].Status = %q, want %q", got, want)
	}

	eventsCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsCh, errCh := h.store.Events(eventsCtx, project, firstEvent.ID)
	select {
	case event := <-eventsCh:
		if event.ID != secondEvent.ID {
			t.Fatalf("Events() event.ID = %d, want %d", event.ID, secondEvent.ID)
		}
	case err := <-errCh:
		t.Fatalf("Events() err = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Events()")
	}
	cancel()
	select {
	case err, ok := <-errCh:
		if ok && err != nil {
			t.Fatalf("Events() unexpected err after cancel = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Events() cancel shutdown")
	}

	updatedTask, err := h.store.UpdateTaskStatus(context.Background(), project, "LAB-718", "cancelled", time.Time{})
	if err != nil {
		t.Fatalf("UpdateTaskStatus() error = %v", err)
	}
	if got, want := updatedTask.Status, "cancelled"; got != want {
		t.Fatalf("updatedTask.Status = %q, want %q", got, want)
	}

	if err := h.store.DeleteWorker(context.Background(), project, "worker-01"); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}
	if err := h.store.DeleteWorker(context.Background(), project, "worker-01"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteWorker() second error = %v, want ErrNotFound", err)
	}

	if err := h.store.MarkCloneFree(context.Background(), project, clonePath); err != nil {
		t.Fatalf("MarkCloneFree() error = %v", err)
	}
	if err := h.store.MarkCloneFree(context.Background(), project, "missing"); !errors.Is(err, legacy.ErrCloneNotFound) {
		t.Fatalf("MarkCloneFree() missing error = %v, want ErrCloneNotFound", err)
	}

	if err := h.store.MarkDaemonStopped(context.Background(), project, time.Time{}); err != nil {
		t.Fatalf("MarkDaemonStopped() error = %v", err)
	}
	status, err = h.store.ProjectStatus(context.Background(), project)
	if err != nil {
		t.Fatalf("ProjectStatus() after stop error = %v", err)
	}
	if status.Daemon == nil || status.Daemon.Status != "stopped" || status.Daemon.PID != 0 {
		t.Fatalf("status.Daemon after stop = %#v, want stopped daemon", status.Daemon)
	}
}

func testStoreNotFoundBehavior(t *testing.T, h storeContractHarness) {
	t.Helper()

	project := "/repo"

	if _, err := h.store.TaskStatus(context.Background(), project, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TaskStatus() missing error = %v, want ErrNotFound", err)
	}
	if err := h.store.DeleteWorker(context.Background(), project, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteWorker() missing error = %v, want ErrNotFound", err)
	}
	if _, err := h.store.lookupCloneRecord(context.Background(), project, "missing"); !errors.Is(err, legacy.ErrCloneNotFound) {
		t.Fatalf("lookupCloneRecord() missing error = %v, want ErrCloneNotFound", err)
	}
}

func testStoreAllActiveQueriesAcrossProjects(t *testing.T, h storeContractHarness) {
	t.Helper()

	now := time.Date(2026, 4, 7, 10, 0, 0, 0, time.UTC)

	for _, task := range []Task{
		{Issue: "LAB-901", Status: "active", Agent: "codex", WorkerID: "worker-a", ClonePath: "/clones/a", CreatedAt: now, UpdatedAt: now},
		{Issue: "LAB-902", Status: "starting", Agent: "codex", WorkerID: "worker-b", ClonePath: "/clones/b", CreatedAt: now, UpdatedAt: now.Add(time.Minute)},
		{Issue: "LAB-903", Status: "done", Agent: "codex", WorkerID: "worker-c", ClonePath: "/clones/c", CreatedAt: now, UpdatedAt: now.Add(2 * time.Minute)},
	} {
		project := "/repo-a"
		if task.Issue == "LAB-902" {
			project = "/repo-b"
		}
		if err := h.store.UpsertTask(context.Background(), project, task); err != nil {
			t.Fatalf("UpsertTask(%s) error = %v", task.Issue, err)
		}
	}

	if err := h.store.UpsertWorker(context.Background(), "/repo-a", Worker{
		WorkerID:      "worker-a",
		CurrentPaneID: "pane-a",
		Agent:         "codex",
		State:         "healthy",
		Issue:         "LAB-901",
		ClonePath:     "/clones/a",
		CreatedAt:     now,
		LastSeenAt:    now,
	}); err != nil {
		t.Fatalf("UpsertWorker(/repo-a) error = %v", err)
	}
	if err := h.store.UpsertWorker(context.Background(), "/repo-b", Worker{
		WorkerID:      "worker-b",
		CurrentPaneID: "pane-b",
		Agent:         "codex",
		State:         "healthy",
		Issue:         "LAB-902",
		ClonePath:     "/clones/b",
		CreatedAt:     now.Add(time.Minute),
		LastSeenAt:    now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(/repo-b) error = %v", err)
	}

	if _, err := h.store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:   "/repo-a",
		Issue:     "LAB-901",
		PRNumber:  41,
		Status:    "queued",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("EnqueueMergeEntry(/repo-a) error = %v", err)
	}
	if _, err := h.store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:   "/repo-b",
		Issue:     "LAB-902",
		PRNumber:  42,
		Status:    "awaiting_checks",
		CreatedAt: now.Add(time.Minute),
		UpdatedAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("EnqueueMergeEntry(/repo-b) error = %v", err)
	}

	tasks, err := h.store.AllNonTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("AllNonTerminalTasks() error = %v", err)
	}
	if got, want := len(tasks), 2; got != want {
		t.Fatalf("len(AllNonTerminalTasks()) = %d, want %d", got, want)
	}

	assignments, err := h.store.AllActiveAssignments(context.Background())
	if err != nil {
		t.Fatalf("AllActiveAssignments() error = %v", err)
	}
	if got, want := len(assignments), 1; got != want {
		t.Fatalf("len(AllActiveAssignments()) = %d, want %d", got, want)
	}
	if got, want := assignments[0].Task.Issue, "LAB-901"; got != want {
		t.Fatalf("AllActiveAssignments()[0].Task.Issue = %q, want %q", got, want)
	}

	entries, err := h.store.AllMergeEntries(context.Background())
	if err != nil {
		t.Fatalf("AllMergeEntries() error = %v", err)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("len(AllMergeEntries()) = %d, want %d", got, want)
	}
}

func testStoreGlobalStatusFansOutAcrossProjects(t *testing.T, h storeContractHarness) {
	t.Helper()

	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)

	if err := h.store.UpsertTask(context.Background(), "/repo-a", Task{
		Issue: "LAB-901", Status: "active", Agent: "codex", Prompt: "Fix global status queries", WorkerID: "worker-a", ClonePath: "/clones/a", CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertTask(/repo-a) error = %v", err)
	}
	if err := h.store.UpsertTask(context.Background(), "/repo-b", Task{
		Issue: "LAB-902", Status: "done", Agent: "claude", Prompt: "Verify global status output", WorkerID: "worker-b", ClonePath: "/clones/b", CreatedAt: now.Add(time.Minute), UpdatedAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(/repo-b) error = %v", err)
	}

	if err := h.store.UpsertWorker(context.Background(), "/repo-a", Worker{
		WorkerID: "worker-a", CurrentPaneID: "pane-a", Agent: "codex", State: "healthy", Issue: "LAB-901", ClonePath: "/clones/a", CreatedAt: now, LastSeenAt: now,
	}); err != nil {
		t.Fatalf("UpsertWorker(/repo-a) error = %v", err)
	}
	if err := h.store.UpsertWorker(context.Background(), "/repo-b", Worker{
		WorkerID: "worker-b", CurrentPaneID: "pane-b", Agent: "claude", State: "stuck", Issue: "LAB-902", ClonePath: "/clones/b", CreatedAt: now.Add(time.Minute), LastSeenAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(/repo-b) error = %v", err)
	}

	if _, err := h.store.EnsureClone(context.Background(), "/repo-a", "/clones/a"); err != nil {
		t.Fatalf("EnsureClone(/repo-a) error = %v", err)
	}
	if _, err := h.store.EnsureClone(context.Background(), "/repo-b", "/clones/b"); err != nil {
		t.Fatalf("EnsureClone(/repo-b) error = %v", err)
	}
	if ok, err := h.store.TryOccupyClone(context.Background(), "/repo-a", "/clones/a", "LAB-901", "LAB-901"); err != nil {
		t.Fatalf("TryOccupyClone(/repo-a) error = %v", err)
	} else if !ok {
		t.Fatal("TryOccupyClone(/repo-a) = false, want true")
	}

	tasks, err := h.store.listTasks(context.Background(), "")
	if err != nil {
		t.Fatalf("listTasks(global) error = %v", err)
	}
	if got, want := len(tasks), 2; got != want {
		t.Fatalf("len(listTasks(global)) = %d, want %d", got, want)
	}
	if got, want := tasks[0].Project, "/repo-b"; got != want {
		t.Fatalf("listTasks(global)[0].Project = %q, want %q", got, want)
	}
	if got, want := tasks[1].Project, "/repo-a"; got != want {
		t.Fatalf("listTasks(global)[1].Project = %q, want %q", got, want)
	}

	workers, err := h.store.ListWorkers(context.Background(), "")
	if err != nil {
		t.Fatalf("ListWorkers(global) error = %v", err)
	}
	if got, want := len(workers), 2; got != want {
		t.Fatalf("len(ListWorkers(global)) = %d, want %d", got, want)
	}
	if got, want := workers[0].Project, "/repo-b"; got != want {
		t.Fatalf("ListWorkers(global)[0].Project = %q, want %q", got, want)
	}
	if got, want := workers[1].Project, "/repo-a"; got != want {
		t.Fatalf("ListWorkers(global)[1].Project = %q, want %q", got, want)
	}

	clones, err := h.store.ListClones(context.Background(), "")
	if err != nil {
		t.Fatalf("ListClones(global) error = %v", err)
	}
	if got, want := len(clones), 2; got != want {
		t.Fatalf("len(ListClones(global)) = %d, want %d", got, want)
	}

	status, err := h.store.ProjectStatus(context.Background(), "")
	if err != nil {
		t.Fatalf("ProjectStatus(global) error = %v", err)
	}
	if got, want := status.Summary.Tasks, 2; got != want {
		t.Fatalf("status.Summary.Tasks = %d, want %d", got, want)
	}
	if got, want := status.Summary.Active, 1; got != want {
		t.Fatalf("status.Summary.Active = %d, want %d", got, want)
	}
	if got, want := status.Summary.Done, 1; got != want {
		t.Fatalf("status.Summary.Done = %d, want %d", got, want)
	}
	if got, want := status.Summary.Workers, 2; got != want {
		t.Fatalf("status.Summary.Workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.HealthyWorkers, 1; got != want {
		t.Fatalf("status.Summary.HealthyWorkers = %d, want %d", got, want)
	}
	if got, want := status.Summary.StuckWorkers, 1; got != want {
		t.Fatalf("status.Summary.StuckWorkers = %d, want %d", got, want)
	}
	if got, want := status.Summary.Clones, 2; got != want {
		t.Fatalf("status.Summary.Clones = %d, want %d", got, want)
	}
	if got, want := status.Summary.FreeClones, 1; got != want {
		t.Fatalf("status.Summary.FreeClones = %d, want %d", got, want)
	}
}

func testStorePersistsWorkerMonitorStateAndMergeQueue(t *testing.T, h storeContractHarness) {
	t.Helper()

	project := "/repo"
	now := time.Date(2026, 4, 3, 9, 30, 0, 0, time.UTC)

	if err := h.store.UpsertWorker(context.Background(), project, Worker{
		WorkerID: "worker-01", CurrentPaneID: "pane-1", Agent: "codex", State: "escalated", Issue: "LAB-735", ClonePath: "/clones/orca01",
		LastReviewCount: 2, LastInlineReviewCommentCount: 1, LastIssueCommentCount: 4, ReviewNudgeCount: 3, LastCIState: "fail",
		CINudgeCount: 2, CIFailurePollCount: 1, CIEscalated: true, LastMergeableState: "blocked", NudgeCount: 3, LastCapture: "permission prompt",
		LastActivityAt: now, LastPRNumber: 42, LastPushAt: now.Add(-2 * time.Minute), LastPRPollAt: now.Add(-time.Minute), LastReviewUpdatedAt: now.Add(-90 * time.Second),
		RestartCount: 2, FirstCrashAt: now.Add(-3 * time.Minute), CreatedAt: now, LastSeenAt: now,
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	workers, err := h.store.ListWorkers(context.Background(), project)
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	if got, want := len(workers), 1; got != want {
		t.Fatalf("len(workers) = %d, want %d", got, want)
	}
	worker := workers[0]
	if got, want := worker.LastReviewCount, 2; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
	}
	if got, want := worker.LastInlineReviewCommentCount, 1; got != want {
		t.Fatalf("worker.LastInlineReviewCommentCount = %d, want %d", got, want)
	}
	if got, want := worker.LastIssueCommentCount, 4; got != want {
		t.Fatalf("worker.LastIssueCommentCount = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 3; got != want {
		t.Fatalf("worker.ReviewNudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.LastCIState, "fail"; got != want {
		t.Fatalf("worker.LastCIState = %q, want %q", got, want)
	}
	if got, want := worker.CINudgeCount, 2; got != want {
		t.Fatalf("worker.CINudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.CIFailurePollCount, 1; got != want {
		t.Fatalf("worker.CIFailurePollCount = %d, want %d", got, want)
	}
	if got, want := worker.CIEscalated, true; got != want {
		t.Fatalf("worker.CIEscalated = %t, want %t", got, want)
	}
	if got, want := worker.LastMergeableState, "blocked"; got != want {
		t.Fatalf("worker.LastMergeableState = %q, want %q", got, want)
	}
	if got, want := worker.NudgeCount, 3; got != want {
		t.Fatalf("worker.NudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.LastCapture, "permission prompt"; got != want {
		t.Fatalf("worker.LastCapture = %q, want %q", got, want)
	}
	if got, want := worker.LastActivityAt, now; !got.Equal(want) {
		t.Fatalf("worker.LastActivityAt = %v, want %v", got, want)
	}
	if got, want := worker.LastPRNumber, 42; got != want {
		t.Fatalf("worker.LastPRNumber = %d, want %d", got, want)
	}
	if got, want := worker.LastPushAt, now.Add(-2*time.Minute); !got.Equal(want) {
		t.Fatalf("worker.LastPushAt = %v, want %v", got, want)
	}
	if got, want := worker.LastPRPollAt, now.Add(-time.Minute); !got.Equal(want) {
		t.Fatalf("worker.LastPRPollAt = %v, want %v", got, want)
	}
	if got, want := worker.LastReviewUpdatedAt, now.Add(-90*time.Second); !got.Equal(want) {
		t.Fatalf("worker.LastReviewUpdatedAt = %v, want %v", got, want)
	}
	if got, want := worker.RestartCount, 2; got != want {
		t.Fatalf("worker.RestartCount = %d, want %d", got, want)
	}
	if got, want := worker.FirstCrashAt, now.Add(-3*time.Minute); !got.Equal(want) {
		t.Fatalf("worker.FirstCrashAt = %v, want %v", got, want)
	}

	position, err := h.store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project: project, Issue: "LAB-735", PRNumber: 42, Status: "queued", CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("EnqueueMergeEntry() error = %v", err)
	}
	if got, want := position, 1; got != want {
		t.Fatalf("position = %d, want %d", got, want)
	}

	entry, err := h.store.MergeEntry(context.Background(), project, 42)
	if err != nil {
		t.Fatalf("MergeEntry() error = %v", err)
	}
	if entry == nil {
		t.Fatal("MergeEntry() = nil, want entry")
	}
	if got, want := entry.PRNumber, 42; got != want {
		t.Fatalf("entry.PRNumber = %d, want %d", got, want)
	}
	if got, want := entry.Status, "queued"; got != want {
		t.Fatalf("entry.Status = %q, want %q", got, want)
	}

	entries, err := h.store.MergeEntries(context.Background(), project)
	if err != nil {
		t.Fatalf("MergeEntries() error = %v", err)
	}
	if got, want := len(entries), 1; got != want {
		t.Fatalf("len(entries) = %d, want %d", got, want)
	}
	if got, want := entries[0].PRNumber, 42; got != want {
		t.Fatalf("entries[0].PRNumber = %d, want %d", got, want)
	}

	entry.Status = "awaiting_checks"
	entry.UpdatedAt = now.Add(time.Minute)
	if err := h.store.UpdateMergeEntry(context.Background(), *entry); err != nil {
		t.Fatalf("UpdateMergeEntry() error = %v", err)
	}

	updatedEntry, err := h.store.MergeEntry(context.Background(), project, 42)
	if err != nil {
		t.Fatalf("MergeEntry() after update error = %v", err)
	}
	if updatedEntry == nil {
		t.Fatal("MergeEntry() after update = nil, want entry")
	}
	if got, want := updatedEntry.Status, "awaiting_checks"; got != want {
		t.Fatalf("updatedEntry.Status = %q, want %q", got, want)
	}

	if err := h.store.DeleteMergeEntry(context.Background(), project, 42); err != nil {
		t.Fatalf("DeleteMergeEntry() error = %v", err)
	}
	emptyEntry, err := h.store.MergeEntry(context.Background(), project, 42)
	if err != nil {
		t.Fatalf("MergeEntry() after delete error = %v", err)
	}
	if emptyEntry != nil {
		t.Fatalf("MergeEntry() after delete = %#v, want nil", emptyEntry)
	}
}

func testStoreWorkerByPaneAndNonTerminalTasks(t *testing.T, h storeContractHarness) {
	t.Helper()

	project := "/repo"
	now := time.Date(2026, 4, 4, 11, 0, 0, 0, time.UTC)
	prNumber := 42

	for _, task := range []Task{
		{Issue: "LAB-740", Status: "starting", State: "assigned", Agent: "codex", Prompt: "Recover startup", WorkerID: "worker-01", ClonePath: "/clones/clone-01", CreatedAt: now, UpdatedAt: now},
		{Issue: "LAB-741", Status: "active", State: "review_pending", Agent: "codex", Prompt: "Keep running", WorkerID: "worker-02", ClonePath: "/clones/clone-02", PRNumber: &prNumber, CreatedAt: now, UpdatedAt: now.Add(time.Minute)},
		{Issue: "LAB-742", Status: "done", State: "done", Agent: "codex", Prompt: "Finished", WorkerID: "worker-03", ClonePath: "/clones/clone-03", CreatedAt: now, UpdatedAt: now.Add(2 * time.Minute)},
	} {
		if err := h.store.UpsertTask(context.Background(), project, task); err != nil {
			t.Fatalf("UpsertTask(%s) error = %v", task.Issue, err)
		}
	}

	if err := h.store.UpsertWorker(context.Background(), project, Worker{
		WorkerID: "worker-02", CurrentPaneID: "pane-2", Agent: "codex", State: "escalated", Issue: "LAB-741", ClonePath: "/clones/clone-02",
		LastReviewCount: 2, LastInlineReviewCommentCount: 1, LastIssueCommentCount: 4, ReviewNudgeCount: 3, LastCIState: "fail",
		CINudgeCount: 2, CIFailurePollCount: 1, CIEscalated: true, LastMergeableState: "CONFLICTING", NudgeCount: 3, LastCapture: "permission prompt",
		LastActivityAt: now, LastPRNumber: 42, LastPushAt: now.Add(-4 * time.Minute), LastPRPollAt: now.Add(-2 * time.Minute), LastReviewUpdatedAt: now.Add(-3 * time.Minute),
		RestartCount: 2, FirstCrashAt: now.Add(-2 * time.Minute), CreatedAt: now, LastSeenAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	tasks, err := h.store.NonTerminalTasks(context.Background(), project)
	if err != nil {
		t.Fatalf("NonTerminalTasks() error = %v", err)
	}
	if got, want := len(tasks), 2; got != want {
		t.Fatalf("len(tasks) = %d, want %d", got, want)
	}
	if got, want := tasks[0].Issue, "LAB-741"; got != want {
		t.Fatalf("tasks[0].Issue = %q, want %q", got, want)
	}
	if got, want := tasks[0].State, "review_pending"; got != want {
		t.Fatalf("tasks[0].State = %q, want %q", got, want)
	}
	if tasks[0].PRNumber == nil || *tasks[0].PRNumber != 42 {
		t.Fatalf("tasks[0].PRNumber = %#v, want 42", tasks[0].PRNumber)
	}
	if got, want := tasks[1].Issue, "LAB-740"; got != want {
		t.Fatalf("tasks[1].Issue = %q, want %q", got, want)
	}
	if got, want := tasks[1].State, "assigned"; got != want {
		t.Fatalf("tasks[1].State = %q, want %q", got, want)
	}

	paneTasks, err := h.store.TasksByPane(context.Background(), project, "pane-2")
	if err != nil {
		t.Fatalf("TasksByPane() error = %v", err)
	}
	if got, want := len(paneTasks), 1; got != want {
		t.Fatalf("len(paneTasks) = %d, want %d", got, want)
	}
	if got, want := paneTasks[0].Issue, "LAB-741"; got != want {
		t.Fatalf("paneTasks[0].Issue = %q, want %q", got, want)
	}
	if got, want := paneTasks[0].State, "review_pending"; got != want {
		t.Fatalf("paneTasks[0].State = %q, want %q", got, want)
	}
	if paneTasks[0].PRNumber == nil || *paneTasks[0].PRNumber != 42 {
		t.Fatalf("paneTasks[0].PRNumber = %#v, want 42", paneTasks[0].PRNumber)
	}

	worker, err := h.store.WorkerByPane(context.Background(), project, "pane-2")
	if err != nil {
		t.Fatalf("WorkerByPane() error = %v", err)
	}
	if got, want := worker.LastReviewCount, 2; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
	}
	if got, want := worker.LastInlineReviewCommentCount, 1; got != want {
		t.Fatalf("worker.LastInlineReviewCommentCount = %d, want %d", got, want)
	}
	if got, want := worker.LastIssueCommentCount, 4; got != want {
		t.Fatalf("worker.LastIssueCommentCount = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 3; got != want {
		t.Fatalf("worker.ReviewNudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.LastCIState, "fail"; got != want {
		t.Fatalf("worker.LastCIState = %q, want %q", got, want)
	}
	if got, want := worker.CINudgeCount, 2; got != want {
		t.Fatalf("worker.CINudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.CIFailurePollCount, 1; got != want {
		t.Fatalf("worker.CIFailurePollCount = %d, want %d", got, want)
	}
	if got, want := worker.CIEscalated, true; got != want {
		t.Fatalf("worker.CIEscalated = %t, want %t", got, want)
	}
	if got, want := worker.LastMergeableState, "CONFLICTING"; got != want {
		t.Fatalf("worker.LastMergeableState = %q, want %q", got, want)
	}
	if got, want := worker.NudgeCount, 3; got != want {
		t.Fatalf("worker.NudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.LastCapture, "permission prompt"; got != want {
		t.Fatalf("worker.LastCapture = %q, want %q", got, want)
	}
	if got, want := worker.LastPRNumber, 42; got != want {
		t.Fatalf("worker.LastPRNumber = %d, want %d", got, want)
	}
	if got, want := worker.LastPushAt, now.Add(-4*time.Minute); !got.Equal(want) {
		t.Fatalf("worker.LastPushAt = %v, want %v", got, want)
	}
	if got, want := worker.LastPRPollAt, now.Add(-2*time.Minute); !got.Equal(want) {
		t.Fatalf("worker.LastPRPollAt = %v, want %v", got, want)
	}
	if got, want := worker.LastReviewUpdatedAt, now.Add(-3*time.Minute); !got.Equal(want) {
		t.Fatalf("worker.LastReviewUpdatedAt = %v, want %v", got, want)
	}
	if got, want := worker.RestartCount, 2; got != want {
		t.Fatalf("worker.RestartCount = %d, want %d", got, want)
	}
	if got, want := worker.FirstCrashAt, now.Add(-2*time.Minute); !got.Equal(want) {
		t.Fatalf("worker.FirstCrashAt = %v, want %v", got, want)
	}
	if _, err := h.store.WorkerByPane(context.Background(), project, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("WorkerByPane() missing error = %v, want ErrNotFound", err)
	}
}

func testStoreStaleCloneOccupancies(t *testing.T, newHarness func(*testing.T) storeContractHarness) {
	t.Helper()

	tests := []struct {
		name         string
		queryProject string
		want         []CloneOccupancy
	}{
		{
			name:         "project query returns only stale occupancies for that project",
			queryProject: "/repo-a",
			want: []CloneOccupancy{
				{Project: "/repo-a", Path: "/clones/a-missing", CurrentBranch: "LAB-743", AssignedTask: "LAB-743"},
				{Project: "/repo-a", Path: "/clones/a-cancelled", CurrentBranch: "LAB-742", AssignedTask: "LAB-742"},
				{Project: "/repo-a", Path: "/clones/a-done", CurrentBranch: "LAB-741", AssignedTask: "LAB-741"},
			},
		},
		{
			name:         "global query returns stale occupancies across projects",
			queryProject: "",
			want: []CloneOccupancy{
				{Project: "/repo-b", Path: "/clones/b-failed", CurrentBranch: "LAB-745", AssignedTask: "LAB-745"},
				{Project: "/repo-a", Path: "/clones/a-missing", CurrentBranch: "LAB-743", AssignedTask: "LAB-743"},
				{Project: "/repo-a", Path: "/clones/a-cancelled", CurrentBranch: "LAB-742", AssignedTask: "LAB-742"},
				{Project: "/repo-a", Path: "/clones/a-done", CurrentBranch: "LAB-741", AssignedTask: "LAB-741"},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t)
			now := time.Date(2026, 4, 5, 9, 0, 0, 0, time.UTC)
			h.setNow(now)

			registerClone := func(project, path, issue, status string) {
				t.Helper()

				if _, err := h.store.EnsureClone(context.Background(), project, path); err != nil {
					t.Fatalf("EnsureClone(%q, %q) error = %v", project, path, err)
				}
				ok, err := h.store.TryOccupyClone(context.Background(), project, path, issue, issue)
				if err != nil {
					t.Fatalf("TryOccupyClone(%q, %q) error = %v", project, path, err)
				}
				if !ok {
					t.Fatalf("TryOccupyClone(%q, %q) = false, want true", project, path)
				}
				if status == "" {
					now = now.Add(time.Minute)
					h.setNow(now)
					return
				}
				if err := h.store.UpsertTask(context.Background(), project, Task{
					Issue: issue, Status: status, Agent: "codex", Prompt: "Track clone occupancy", WorkerID: "worker-" + issue, ClonePath: path, CreatedAt: now, UpdatedAt: now,
				}); err != nil {
					t.Fatalf("UpsertTask(%q, %q) error = %v", project, issue, err)
				}
				now = now.Add(time.Minute)
				h.setNow(now)
			}

			registerClone("/repo-a", "/clones/a-active", "LAB-740", "active")
			registerClone("/repo-a", "/clones/a-done", "LAB-741", "done")
			registerClone("/repo-a", "/clones/a-cancelled", "LAB-742", "cancelled")
			registerClone("/repo-a", "/clones/a-missing", "LAB-743", "")
			registerClone("/repo-a", "/clones/a-starting", "LAB-744", "starting")
			registerClone("/repo-b", "/clones/b-failed", "LAB-745", "failed")

			got, err := h.store.StaleCloneOccupancies(context.Background(), tt.queryProject)
			if err != nil {
				t.Fatalf("StaleCloneOccupancies() error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("len(StaleCloneOccupancies()) = %d, want %d", len(got), len(tt.want))
			}
			for i := range tt.want {
				if got[i].Project != tt.want[i].Project {
					t.Fatalf("got[%d].Project = %q, want %q", i, got[i].Project, tt.want[i].Project)
				}
				if got[i].Path != tt.want[i].Path {
					t.Fatalf("got[%d].Path = %q, want %q", i, got[i].Path, tt.want[i].Path)
				}
				if got[i].CurrentBranch != tt.want[i].CurrentBranch {
					t.Fatalf("got[%d].CurrentBranch = %q, want %q", i, got[i].CurrentBranch, tt.want[i].CurrentBranch)
				}
				if got[i].AssignedTask != tt.want[i].AssignedTask {
					t.Fatalf("got[%d].AssignedTask = %q, want %q", i, got[i].AssignedTask, tt.want[i].AssignedTask)
				}
			}
		})
	}
}

func testStoreMergeQueueOrderingAndNotFound(t *testing.T, h storeContractHarness) {
	t.Helper()

	project := "/repo"
	now := time.Date(2026, 4, 6, 10, 0, 0, 0, time.UTC)
	h.setNow(now)

	position, err := h.store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project: project, Issue: "LAB-751", PRNumber: 43, Status: "awaiting_checks",
	})
	if err != nil {
		t.Fatalf("EnqueueMergeEntry(43) error = %v", err)
	}
	if got, want := position, 1; got != want {
		t.Fatalf("position for PR 43 = %d, want %d", got, want)
	}

	position, err = h.store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project: project, Issue: "LAB-750", PRNumber: 42, Status: "queued", CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("EnqueueMergeEntry(42) error = %v", err)
	}
	if got, want := position, 2; got != want {
		t.Fatalf("position for PR 42 = %d, want %d", got, want)
	}

	entry, err := h.store.MergeEntry(context.Background(), project, 43)
	if err != nil {
		t.Fatalf("MergeEntry(43) error = %v", err)
	}
	if entry == nil {
		t.Fatal("MergeEntry(43) = nil, want entry")
	}
	if !entry.CreatedAt.Equal(now) {
		t.Fatalf("entry.CreatedAt = %v, want %v", entry.CreatedAt, now)
	}
	if !entry.UpdatedAt.Equal(now) {
		t.Fatalf("entry.UpdatedAt = %v, want %v", entry.UpdatedAt, now)
	}

	entries, err := h.store.MergeEntries(context.Background(), project)
	if err != nil {
		t.Fatalf("MergeEntries() error = %v", err)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("len(entries) = %d, want %d", got, want)
	}
	if got, want := entries[0].PRNumber, 42; got != want {
		t.Fatalf("entries[0].PRNumber = %d, want %d", got, want)
	}
	if got, want := entries[1].PRNumber, 43; got != want {
		t.Fatalf("entries[1].PRNumber = %d, want %d", got, want)
	}

	if err := h.store.UpdateMergeEntry(context.Background(), MergeQueueEntry{
		Project: project, Issue: "LAB-799", PRNumber: 99, Status: "queued",
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateMergeEntry(missing) error = %v, want ErrNotFound", err)
	}
	if err := h.store.DeleteMergeEntry(context.Background(), project, 99); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteMergeEntry(missing) error = %v, want ErrNotFound", err)
	}
}

func testStoreSchemaIncludesHostColumns(t *testing.T, h storeContractHarness) {
	t.Helper()
	if h.assertHostColumns == nil {
		t.Fatal("assertHostColumns is nil")
	}
	h.assertHostColumns(t)
}

func testStoreProjectStatusAllHostsAcrossHosts(t *testing.T, h storeContractHarness) {
	t.Helper()
	if h.setHost == nil {
		t.Fatal("setHost is nil")
	}

	ctx := context.Background()
	project := "/repo"
	hostA := "host-a"
	hostB := "host-b"
	hostC := "host-c"
	base := time.Date(2026, 4, 9, 9, 0, 0, 0, time.UTC)
	activePR := 101

	h.setHost(hostA)
	h.setNow(base)
	if err := h.store.UpsertDaemon(ctx, "", DaemonStatus{
		Session:   "global-a",
		PID:       11,
		Status:    "running",
		StartedAt: base,
		UpdatedAt: base,
	}); err != nil {
		t.Fatalf("UpsertDaemon(global host-a) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, project, Task{
		Issue:     "LAB-2401",
		Status:    "active",
		State:     "assigned",
		Agent:     "codex",
		WorkerID:  "worker-a",
		ClonePath: "/clones/a",
		Branch:    "branch-a",
		PRNumber:  &activePR,
		CreatedAt: base,
		UpdatedAt: base,
	}); err != nil {
		t.Fatalf("UpsertTask(active host-a) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, project, Task{
		Issue:     "LAB-2402",
		Status:    "queued",
		State:     "queued",
		Agent:     "codex",
		CreatedAt: base.Add(time.Minute),
		UpdatedAt: base.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(queued host-a) error = %v", err)
	}
	if err := h.store.UpsertWorker(ctx, project, Worker{
		WorkerID:      "worker-a",
		CurrentPaneID: "pane-a",
		Agent:         "codex",
		State:         "healthy",
		Issue:         "LAB-2401",
		ClonePath:     "/clones/a",
		CreatedAt:     base,
		LastSeenAt:    base,
	}); err != nil {
		t.Fatalf("UpsertWorker(host-a) error = %v", err)
	}
	if _, err := h.store.EnsureClone(ctx, project, "/clones/a"); err != nil {
		t.Fatalf("EnsureClone(/clones/a) error = %v", err)
	}
	if ok, err := h.store.TryOccupyClone(ctx, project, "/clones/a", "branch-a", "LAB-2401"); err != nil {
		t.Fatalf("TryOccupyClone(/clones/a) error = %v", err)
	} else if !ok {
		t.Fatal("TryOccupyClone(/clones/a) = false, want true")
	}
	if _, err := h.store.EnsureClone(ctx, project, "/clones/free-a"); err != nil {
		t.Fatalf("EnsureClone(/clones/free-a) error = %v", err)
	}

	h.setHost(hostB)
	h.setNow(base.Add(2 * time.Minute))
	if err := h.store.UpsertDaemon(ctx, project, DaemonStatus{
		Session:   "repo-b",
		PID:       22,
		Status:    "running",
		StartedAt: base.Add(2 * time.Minute),
		UpdatedAt: base.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertDaemon(project host-b) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, project, Task{
		Issue:     "LAB-2403",
		Status:    "done",
		State:     "done",
		Agent:     "codex",
		WorkerID:  "worker-b",
		ClonePath: "/clones/b",
		CreatedAt: base.Add(2 * time.Minute),
		UpdatedAt: base.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(done host-b) error = %v", err)
	}
	if err := h.store.UpsertWorker(ctx, project, Worker{
		WorkerID:      "worker-b",
		CurrentPaneID: "pane-b",
		Agent:         "codex",
		State:         "stuck",
		Issue:         "LAB-2403",
		ClonePath:     "/clones/b",
		CreatedAt:     base.Add(2 * time.Minute),
		LastSeenAt:    base.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(host-b) error = %v", err)
	}
	if _, err := h.store.EnsureClone(ctx, project, "/clones/b"); err != nil {
		t.Fatalf("EnsureClone(/clones/b) error = %v", err)
	}

	h.setHost(hostC)
	h.setNow(base.Add(3 * time.Minute))
	if err := h.store.UpsertDaemon(ctx, project, DaemonStatus{
		Session:   "repo-c",
		PID:       33,
		Status:    "running",
		StartedAt: base.Add(3 * time.Minute),
		UpdatedAt: base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertDaemon(project host-c) error = %v", err)
	}
	if err := h.store.UpsertDaemon(ctx, "", DaemonStatus{
		Session:   "global-c",
		PID:       44,
		Status:    "running",
		StartedAt: base.Add(3 * time.Minute),
		UpdatedAt: base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertDaemon(global host-c) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, project, Task{
		Issue:     "LAB-2404",
		Status:    "cancelled",
		State:     "done",
		Agent:     "claude",
		WorkerID:  "worker-c",
		ClonePath: "/clones/c",
		CreatedAt: base.Add(3 * time.Minute),
		UpdatedAt: base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(cancelled host-c) error = %v", err)
	}
	if err := h.store.UpsertWorker(ctx, project, Worker{
		WorkerID:      "worker-c",
		CurrentPaneID: "pane-c",
		Agent:         "claude",
		State:         "healthy",
		Issue:         "LAB-2404",
		ClonePath:     "/clones/c",
		CreatedAt:     base.Add(3 * time.Minute),
		LastSeenAt:    base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(host-c) error = %v", err)
	}
	if _, err := h.store.EnsureClone(ctx, project, "/clones/c"); err != nil {
		t.Fatalf("EnsureClone(/clones/c) error = %v", err)
	}

	h.setHost(hostA)
	status, err := h.store.ProjectStatus(ctx, project)
	if err != nil {
		t.Fatalf("ProjectStatus() error = %v", err)
	}
	if status.Daemon == nil || status.Daemon.Session != "global-a" {
		t.Fatalf("ProjectStatus().Daemon = %#v, want global-a fallback", status.Daemon)
	}
	if got, want := status.Summary.Tasks, 2; got != want {
		t.Fatalf("host-scoped tasks = %d, want %d", got, want)
	}
	if got, want := status.Summary.Active, 1; got != want {
		t.Fatalf("host-scoped active = %d, want %d", got, want)
	}
	if got, want := status.Summary.Queued, 1; got != want {
		t.Fatalf("host-scoped queued = %d, want %d", got, want)
	}
	if got, want := status.Summary.Workers, 1; got != want {
		t.Fatalf("host-scoped workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.HealthyWorkers, 1; got != want {
		t.Fatalf("host-scoped healthy workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.Clones, 2; got != want {
		t.Fatalf("host-scoped clones = %d, want %d", got, want)
	}
	if got, want := status.Summary.FreeClones, 1; got != want {
		t.Fatalf("host-scoped free clones = %d, want %d", got, want)
	}

	worker, err := h.store.WorkerByID(ctx, project, "worker-a")
	if err != nil {
		t.Fatalf("WorkerByID(worker-a) error = %v", err)
	}
	if got, want := worker.Issue, "LAB-2401"; got != want {
		t.Fatalf("WorkerByID(worker-a).Issue = %q, want %q", got, want)
	}

	assignments, err := h.store.ActiveAssignments(ctx, project)
	if err != nil {
		t.Fatalf("ActiveAssignments(project) error = %v", err)
	}
	if got, want := len(assignments), 1; got != want {
		t.Fatalf("len(ActiveAssignments(project)) = %d, want %d", got, want)
	}
	assignment, err := h.store.ActiveAssignmentByIssue(ctx, project, "LAB-2401")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if got, want := assignment.Task.Issue, "LAB-2401"; got != want {
		t.Fatalf("ActiveAssignmentByIssue().Task.Issue = %q, want %q", got, want)
	}
	assignment, err = h.store.ActiveAssignmentByBranch(ctx, project, "branch-a")
	if err != nil {
		t.Fatalf("ActiveAssignmentByBranch() error = %v", err)
	}
	if got, want := assignment.Worker.WorkerID, "worker-a"; got != want {
		t.Fatalf("ActiveAssignmentByBranch().Worker.WorkerID = %q, want %q", got, want)
	}
	assignment, err = h.store.ActiveAssignmentByPRNumber(ctx, project, activePR)
	if err != nil {
		t.Fatalf("ActiveAssignmentByPRNumber() error = %v", err)
	}
	if got, want := assignment.Task.Issue, "LAB-2401"; got != want {
		t.Fatalf("ActiveAssignmentByPRNumber().Task.Issue = %q, want %q", got, want)
	}

	taskStatus, err := h.store.TaskStatusAllHosts(ctx, project, "LAB-2403")
	if err != nil {
		t.Fatalf("TaskStatusAllHosts(project) error = %v", err)
	}
	if got, want := taskStatus.Task.Issue, "LAB-2403"; got != want {
		t.Fatalf("TaskStatusAllHosts(project).Task.Issue = %q, want %q", got, want)
	}

	allHosts, err := h.store.ProjectStatusAllHosts(ctx, project)
	if err != nil {
		t.Fatalf("ProjectStatusAllHosts(project) error = %v", err)
	}
	if got, want := allHosts.Summary.Tasks, 4; got != want {
		t.Fatalf("all-host tasks = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Active, 1; got != want {
		t.Fatalf("all-host active = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Queued, 1; got != want {
		t.Fatalf("all-host queued = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Done, 1; got != want {
		t.Fatalf("all-host done = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Cancelled, 1; got != want {
		t.Fatalf("all-host cancelled = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Workers, 3; got != want {
		t.Fatalf("all-host workers = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.HealthyWorkers, 2; got != want {
		t.Fatalf("all-host healthy workers = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.StuckWorkers, 1; got != want {
		t.Fatalf("all-host stuck workers = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Clones, 4; got != want {
		t.Fatalf("all-host clones = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.FreeClones, 3; got != want {
		t.Fatalf("all-host free clones = %d, want %d", got, want)
	}
	if got, want := len(allHosts.Daemons), 3; got != want {
		t.Fatalf("len(allHosts.Daemons) = %d, want %d", got, want)
	}
	gotSessions := map[string]string{}
	for _, daemon := range allHosts.Daemons {
		gotSessions[daemon.Host] = daemon.Session
	}
	if got, want := gotSessions[hostA], "global-a"; got != want {
		t.Fatalf("daemon session for %s = %q, want %q", hostA, got, want)
	}
	if got, want := gotSessions[hostB], "repo-b"; got != want {
		t.Fatalf("daemon session for %s = %q, want %q", hostB, got, want)
	}
	if got, want := gotSessions[hostC], "repo-c"; got != want {
		t.Fatalf("daemon session for %s = %q, want %q", hostC, got, want)
	}
}

func testStoreGlobalHostScopedQueriesAcrossHosts(t *testing.T, h storeContractHarness) {
	t.Helper()
	if h.setHost == nil {
		t.Fatal("setHost is nil")
	}

	ctx := context.Background()
	hostA := "host-a"
	hostB := "host-b"
	base := time.Date(2026, 4, 10, 9, 0, 0, 0, time.UTC)
	activePR := 201

	h.setHost(hostA)
	h.setNow(base)
	if err := h.store.UpsertDaemon(ctx, "", DaemonStatus{
		Session:   "global-a",
		PID:       51,
		Status:    "running",
		StartedAt: base,
		UpdatedAt: base,
	}); err != nil {
		t.Fatalf("UpsertDaemon(global host-a) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, "/repo-a", Task{
		Issue:     "LAB-2501",
		Status:    "active",
		State:     "assigned",
		Agent:     "codex",
		WorkerID:  "worker-a",
		ClonePath: "/clones/a",
		Branch:    "branch-a",
		PRNumber:  &activePR,
		CreatedAt: base,
		UpdatedAt: base,
	}); err != nil {
		t.Fatalf("UpsertTask(active host-a) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, "/repo-b", Task{
		Issue:     "LAB-2502",
		Status:    "starting",
		State:     "assigned",
		Agent:     "codex",
		WorkerID:  "worker-b",
		ClonePath: "/clones/b",
		CreatedAt: base.Add(time.Minute),
		UpdatedAt: base.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(starting host-a) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, "/repo-b", Task{
		Issue:     "LAB-2503",
		Status:    "queued",
		State:     "queued",
		Agent:     "codex",
		CreatedAt: base.Add(2 * time.Minute),
		UpdatedAt: base.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(queued host-a) error = %v", err)
	}
	if err := h.store.UpsertWorker(ctx, "/repo-a", Worker{
		WorkerID:      "worker-a",
		CurrentPaneID: "pane-a",
		Agent:         "codex",
		State:         "healthy",
		Issue:         "LAB-2501",
		ClonePath:     "/clones/a",
		CreatedAt:     base,
		LastSeenAt:    base,
	}); err != nil {
		t.Fatalf("UpsertWorker(worker-a) error = %v", err)
	}
	if err := h.store.UpsertWorker(ctx, "/repo-b", Worker{
		WorkerID:      "worker-b",
		CurrentPaneID: "pane-b",
		Agent:         "codex",
		State:         "stuck",
		Issue:         "LAB-2502",
		ClonePath:     "/clones/b",
		CreatedAt:     base.Add(time.Minute),
		LastSeenAt:    base.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(worker-b) error = %v", err)
	}
	if _, err := h.store.EnsureClone(ctx, "/repo-a", "/clones/a"); err != nil {
		t.Fatalf("EnsureClone(/repo-a,/clones/a) error = %v", err)
	}
	if ok, err := h.store.TryOccupyClone(ctx, "/repo-a", "/clones/a", "branch-a", "LAB-2501"); err != nil {
		t.Fatalf("TryOccupyClone(/repo-a,/clones/a) error = %v", err)
	} else if !ok {
		t.Fatal("TryOccupyClone(/repo-a,/clones/a) = false, want true")
	}
	if _, err := h.store.EnsureClone(ctx, "/repo-b", "/clones/free-b"); err != nil {
		t.Fatalf("EnsureClone(/repo-b,/clones/free-b) error = %v", err)
	}

	h.setHost(hostB)
	h.setNow(base.Add(3 * time.Minute))
	if err := h.store.UpsertDaemon(ctx, "", DaemonStatus{
		Session:   "global-b",
		PID:       52,
		Status:    "running",
		StartedAt: base.Add(3 * time.Minute),
		UpdatedAt: base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertDaemon(global host-b) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, "/repo-a", Task{
		Issue:     "LAB-2504",
		Status:    "done",
		State:     "done",
		Agent:     "claude",
		WorkerID:  "worker-c",
		ClonePath: "/clones/c",
		CreatedAt: base.Add(3 * time.Minute),
		UpdatedAt: base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(done host-b) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, "/repo-b", Task{
		Issue:     "LAB-2505",
		Status:    "cancelled",
		State:     "done",
		Agent:     "claude",
		WorkerID:  "worker-d",
		ClonePath: "/clones/d",
		CreatedAt: base.Add(4 * time.Minute),
		UpdatedAt: base.Add(4 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(cancelled host-b) error = %v", err)
	}
	if err := h.store.UpsertWorker(ctx, "/repo-a", Worker{
		WorkerID:      "worker-c",
		CurrentPaneID: "pane-c",
		Agent:         "claude",
		State:         "healthy",
		Issue:         "LAB-2504",
		ClonePath:     "/clones/c",
		CreatedAt:     base.Add(3 * time.Minute),
		LastSeenAt:    base.Add(3 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(worker-c) error = %v", err)
	}
	if err := h.store.UpsertWorker(ctx, "/repo-b", Worker{
		WorkerID:      "worker-d",
		CurrentPaneID: "pane-d",
		Agent:         "claude",
		State:         "stuck",
		Issue:         "LAB-2505",
		ClonePath:     "/clones/d",
		CreatedAt:     base.Add(4 * time.Minute),
		LastSeenAt:    base.Add(4 * time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(worker-d) error = %v", err)
	}
	if _, err := h.store.EnsureClone(ctx, "/repo-a", "/clones/c"); err != nil {
		t.Fatalf("EnsureClone(/repo-a,/clones/c) error = %v", err)
	}
	if _, err := h.store.EnsureClone(ctx, "/repo-b", "/clones/d"); err != nil {
		t.Fatalf("EnsureClone(/repo-b,/clones/d) error = %v", err)
	}

	h.setHost(hostA)
	status, err := h.store.ProjectStatus(ctx, "")
	if err != nil {
		t.Fatalf("ProjectStatus(global) error = %v", err)
	}
	if got, want := status.Summary.Tasks, 3; got != want {
		t.Fatalf("host-scoped global tasks = %d, want %d", got, want)
	}
	if got, want := status.Summary.Active, 1; got != want {
		t.Fatalf("host-scoped global active = %d, want %d", got, want)
	}
	if got, want := status.Summary.Queued, 1; got != want {
		t.Fatalf("host-scoped global queued = %d, want %d", got, want)
	}
	if got, want := status.Summary.Workers, 2; got != want {
		t.Fatalf("host-scoped global workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.HealthyWorkers, 1; got != want {
		t.Fatalf("host-scoped global healthy workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.StuckWorkers, 1; got != want {
		t.Fatalf("host-scoped global stuck workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.Clones, 2; got != want {
		t.Fatalf("host-scoped global clones = %d, want %d", got, want)
	}
	if got, want := status.Summary.FreeClones, 1; got != want {
		t.Fatalf("host-scoped global free clones = %d, want %d", got, want)
	}

	tasks, err := h.store.listTasks(ctx, "")
	if err != nil {
		t.Fatalf("listTasks(global host-scoped) error = %v", err)
	}
	if got, want := len(tasks), 3; got != want {
		t.Fatalf("len(listTasks(global host-scoped)) = %d, want %d", got, want)
	}
	workers, err := h.store.ListWorkers(ctx, "")
	if err != nil {
		t.Fatalf("ListWorkers(global host-scoped) error = %v", err)
	}
	if got, want := len(workers), 2; got != want {
		t.Fatalf("len(ListWorkers(global host-scoped)) = %d, want %d", got, want)
	}
	clones, err := h.store.ListClones(ctx, "")
	if err != nil {
		t.Fatalf("ListClones(global host-scoped) error = %v", err)
	}
	if got, want := len(clones), 2; got != want {
		t.Fatalf("len(ListClones(global host-scoped)) = %d, want %d", got, want)
	}
	nonTerminal, err := h.store.NonTerminalTasks(ctx, "")
	if err != nil {
		t.Fatalf("NonTerminalTasks(global host-scoped) error = %v", err)
	}
	if got, want := len(nonTerminal), 2; got != want {
		t.Fatalf("len(NonTerminalTasks(global host-scoped)) = %d, want %d", got, want)
	}
	if got, want := nonTerminal[0].Project, "/repo-b"; got != want {
		t.Fatalf("NonTerminalTasks(global host-scoped)[0].Project = %q, want %q", got, want)
	}

	paneTasks, err := h.store.TasksByPane(ctx, "", "pane-a")
	if err != nil {
		t.Fatalf("TasksByPane(global) error = %v", err)
	}
	if got, want := len(paneTasks), 1; got != want {
		t.Fatalf("len(TasksByPane(global)) = %d, want %d", got, want)
	}
	if got, want := paneTasks[0].Project, "/repo-a"; got != want {
		t.Fatalf("TasksByPane(global)[0].Project = %q, want %q", got, want)
	}
	worker, err := h.store.WorkerByPane(ctx, "", "pane-a")
	if err != nil {
		t.Fatalf("WorkerByPane(global) error = %v", err)
	}
	if got, want := worker.Project, "/repo-a"; got != want {
		t.Fatalf("WorkerByPane(global).Project = %q, want %q", got, want)
	}

	taskStatus, err := h.store.TaskStatusAllHosts(ctx, "", "LAB-2504")
	if err != nil {
		t.Fatalf("TaskStatusAllHosts(global) error = %v", err)
	}
	if got, want := taskStatus.Task.Project, "/repo-a"; got != want {
		t.Fatalf("TaskStatusAllHosts(global).Task.Project = %q, want %q", got, want)
	}

	allHosts, err := h.store.ProjectStatusAllHosts(ctx, "")
	if err != nil {
		t.Fatalf("ProjectStatusAllHosts(global) error = %v", err)
	}
	if got, want := allHosts.Summary.Tasks, 5; got != want {
		t.Fatalf("all-host global tasks = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Active, 1; got != want {
		t.Fatalf("all-host global active = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Queued, 1; got != want {
		t.Fatalf("all-host global queued = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Done, 1; got != want {
		t.Fatalf("all-host global done = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Cancelled, 1; got != want {
		t.Fatalf("all-host global cancelled = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Workers, 4; got != want {
		t.Fatalf("all-host global workers = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.HealthyWorkers, 2; got != want {
		t.Fatalf("all-host global healthy workers = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.StuckWorkers, 2; got != want {
		t.Fatalf("all-host global stuck workers = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Clones, 4; got != want {
		t.Fatalf("all-host global clones = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.FreeClones, 3; got != want {
		t.Fatalf("all-host global free clones = %d, want %d", got, want)
	}
	if got, want := len(allHosts.Daemons), 2; got != want {
		t.Fatalf("len(allHosts.Daemons) = %d, want %d", got, want)
	}
}

func testStoreHostScopedMutationPaths(t *testing.T, h storeContractHarness) {
	t.Helper()
	if h.setHost == nil {
		t.Fatal("setHost is nil")
	}

	ctx := context.Background()
	project := "/repo"
	hostA := "host-a"
	hostB := "host-b"
	base := time.Date(2026, 4, 11, 9, 0, 0, 0, time.UTC)

	h.setHost(hostA)
	h.setNow(base)
	if err := h.store.UpsertWorker(ctx, project, Worker{
		WorkerID:   "worker-free-a",
		Agent:      "codex",
		State:      "healthy",
		CreatedAt:  base,
		LastSeenAt: base,
	}); err != nil {
		t.Fatalf("UpsertWorker(worker-free-a) error = %v", err)
	}

	h.setHost(hostB)
	h.setNow(base.Add(time.Minute))
	if err := h.store.UpsertWorker(ctx, project, Worker{
		WorkerID:   "worker-free-b",
		Agent:      "codex",
		State:      "healthy",
		CreatedAt:  base.Add(time.Minute),
		LastSeenAt: base.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(worker-free-b) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, project, Task{
		Issue:     "LAB-2601",
		Status:    "done",
		State:     "done",
		Agent:     "codex",
		CreatedAt: base.Add(time.Minute),
		UpdatedAt: base.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(host-b claimable) error = %v", err)
	}
	if err := h.store.UpsertTask(ctx, project, Task{
		Issue:     "LAB-2602",
		Status:    "active",
		State:     "assigned",
		Agent:     "codex",
		CreatedAt: base.Add(time.Minute),
		UpdatedAt: base.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(host-b protected) error = %v", err)
	}
	if _, err := h.store.EnsureClone(ctx, project, "/clones/shared"); err != nil {
		t.Fatalf("EnsureClone(host-b /clones/shared) error = %v", err)
	}

	h.setHost(hostA)
	h.setNow(base.Add(2 * time.Minute))
	claimedExisting, err := h.store.ClaimWorker(ctx, project, Worker{
		Agent:      "codex",
		State:      "healthy",
		Issue:      "LAB-2603",
		LastSeenAt: base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimWorker(existing) error = %v", err)
	}
	if got, want := claimedExisting.WorkerID, "worker-free-a"; got != want {
		t.Fatalf("ClaimWorker(existing).WorkerID = %q, want %q", got, want)
	}
	if got, want := claimedExisting.Issue, "LAB-2603"; got != want {
		t.Fatalf("ClaimWorker(existing).Issue = %q, want %q", got, want)
	}

	if _, err := h.store.ClaimTask(ctx, project, Task{
		Issue:     "LAB-2604",
		Status:    "queued",
		State:     "queued",
		Agent:     "codex",
		Prompt:    "create fresh task",
		CreatedAt: base.Add(2 * time.Minute),
		UpdatedAt: base.Add(2 * time.Minute),
	}); err != nil {
		t.Fatalf("ClaimTask(insert) error = %v", err)
	}
	reclaimedTask, err := h.store.ClaimTask(ctx, project, Task{
		Issue:      "LAB-2601",
		Status:     "queued",
		State:      "queued",
		Agent:      "codex",
		Prompt:     "reclaim finished task",
		CallerPane: "pane-claim",
		Branch:     "branch-claim",
		CreatedAt:  base.Add(2 * time.Minute),
		UpdatedAt:  base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimTask(update) error = %v", err)
	}
	if reclaimedTask == nil || reclaimedTask.Issue != "LAB-2601" {
		t.Fatalf("ClaimTask(update) = %#v, want existing task", reclaimedTask)
	}

	if err := h.store.DeleteTask(ctx, project, "LAB-2602"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteTask(other host) error = %v, want ErrNotFound", err)
	}
	if err := h.store.DeleteWorker(ctx, project, "worker-free-b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteWorker(other host) error = %v, want ErrNotFound", err)
	}
	if ok, err := h.store.TryOccupyClone(ctx, project, "/clones/shared", "branch-a", "LAB-2605"); err != nil {
		t.Fatalf("TryOccupyClone(other host) error = %v", err)
	} else if ok {
		t.Fatal("TryOccupyClone(other host) = true, want false")
	}
	if err := h.store.MarkCloneFree(ctx, project, "/clones/shared"); !errors.Is(err, legacy.ErrCloneNotFound) {
		t.Fatalf("MarkCloneFree(other host) error = %v, want ErrCloneNotFound", err)
	}

	insertedWorker, err := h.store.ClaimWorker(ctx, project, Worker{
		Agent:      "claude",
		State:      "healthy",
		Issue:      "LAB-2606",
		LastSeenAt: base.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("ClaimWorker(insert) error = %v", err)
	}
	if insertedWorker.WorkerID == "" {
		t.Fatal("ClaimWorker(insert).WorkerID = empty, want generated worker id")
	}

	if _, err := h.store.TaskStatus(ctx, project, "LAB-2604"); err != nil {
		t.Fatalf("TaskStatus(LAB-2604) error = %v", err)
	}
	if _, err := h.store.TaskStatus(ctx, project, "LAB-2601"); err != nil {
		t.Fatalf("TaskStatus(LAB-2601) error = %v", err)
	}
}

func currentStoreTestHostname(t *testing.T) string {
	t.Helper()

	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("Hostname() error = %v", err)
	}
	if strings.TrimSpace(hostname) == "" {
		t.Fatal("Hostname() returned empty host")
	}
	return strings.TrimSpace(hostname)
}

func TestStoreHostHelpers(t *testing.T) {
	t.Parallel()

	if got := defaultStoreHost(); got == "" {
		t.Fatal("defaultStoreHost() returned empty host")
	}
	if got, want := normalizeStoreHost("  host-a  "), "host-a"; got != want {
		t.Fatalf("normalizeStoreHost() = %q, want %q", got, want)
	}
	if got, want := postgresHostMatch("tasks.host", 3), "(tasks.host = $3 OR tasks.host = '')"; got != want {
		t.Fatalf("postgresHostMatch() = %q, want %q", got, want)
	}
	if got, want := sqliteHostMatch("tasks.host"), "(tasks.host = ? OR tasks.host = '')"; got != want {
		t.Fatalf("sqliteHostMatch() = %q, want %q", got, want)
	}
}
