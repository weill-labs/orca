package state

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	legacy "github.com/weill-labs/orca/internal/state"
)

var execRetryStubDriverSeq atomic.Uint64

func TestSQLiteStoreLifecycleAndQueries(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	project := "/repo"
	now := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	status, err := store.ProjectStatus(context.Background(), project)
	if err != nil {
		t.Fatalf("ProjectStatus() empty error = %v", err)
	}
	if status.Daemon != nil {
		t.Fatalf("empty status daemon = %#v, want nil", status.Daemon)
	}

	if err := store.UpsertDaemon(context.Background(), project, DaemonStatus{
		Session: "orca",
		PID:     42,
		Status:  "running",
	}); err != nil {
		t.Fatalf("UpsertDaemon() error = %v", err)
	}

	clonePath := filepath.Join(t.TempDir(), "orca01")
	record, err := store.EnsureClone(context.Background(), project, clonePath)
	if err != nil {
		t.Fatalf("EnsureClone() error = %v", err)
	}
	if got, want := record.Status, legacy.CloneStatusFree; got != want {
		t.Fatalf("record.Status = %q, want %q", got, want)
	}

	ok, err := store.TryOccupyClone(context.Background(), project, clonePath, "LAB-718", "LAB-718")
	if err != nil {
		t.Fatalf("TryOccupyClone() error = %v", err)
	}
	if !ok {
		t.Fatal("TryOccupyClone() = false, want true")
	}

	ok, err = store.TryOccupyClone(context.Background(), project, clonePath, "LAB-718", "LAB-718")
	if err != nil {
		t.Fatalf("TryOccupyClone() second call error = %v", err)
	}
	if ok {
		t.Fatal("TryOccupyClone() second call = true, want false")
	}

	prNumber := 17
	if err := store.UpsertTask(context.Background(), project, Task{
		Issue:     "LAB-718",
		Status:    "active",
		Agent:     "codex",
		Prompt:    "Implement socket IPC",
		WorkerID:  "pane-1",
		ClonePath: clonePath,
		PRNumber:  &prNumber,
	}); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	if err := store.UpsertWorker(context.Background(), project, Worker{
		PaneID:    "pane-1",
		Agent:     "codex",
		State:     "healthy",
		Issue:     "LAB-718",
		ClonePath: clonePath,
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	firstEvent, err := store.AppendEvent(context.Background(), Event{
		Project: project,
		Kind:    "task.assigned",
		Issue:   "LAB-718",
		Message: "LAB-718 assigned",
		Payload: []byte(`{"pane":"pane-1"}`),
	})
	if err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	secondEvent, err := store.AppendEvent(context.Background(), Event{
		Project: project,
		Kind:    "task.updated",
		Issue:   "LAB-718",
		Message: "LAB-718 updated",
	})
	if err != nil {
		t.Fatalf("AppendEvent() second error = %v", err)
	}

	status, err = store.ProjectStatus(context.Background(), project)
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

	taskStatus, err := store.TaskStatus(context.Background(), project, "LAB-718")
	if err != nil {
		t.Fatalf("TaskStatus() error = %v", err)
	}
	if got, want := taskStatus.Task.Issue, "LAB-718"; got != want {
		t.Fatalf("taskStatus.Task.Issue = %q, want %q", got, want)
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

	workers, err := store.ListWorkers(context.Background(), project)
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	if got, want := len(workers), 1; got != want {
		t.Fatalf("len(workers) = %d, want %d", got, want)
	}

	clones, err := store.ListClones(context.Background(), project)
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
	eventsCh, errCh := store.Events(eventsCtx, project, firstEvent.ID)
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

	updatedTask, err := store.UpdateTaskStatus(context.Background(), project, "LAB-718", "cancelled", time.Time{})
	if err != nil {
		t.Fatalf("UpdateTaskStatus() error = %v", err)
	}
	if got, want := updatedTask.Status, "cancelled"; got != want {
		t.Fatalf("updatedTask.Status = %q, want %q", got, want)
	}

	if err := store.DeleteWorker(context.Background(), project, "pane-1"); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}
	if err := store.DeleteWorker(context.Background(), project, "pane-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteWorker() second error = %v, want ErrNotFound", err)
	}

	if err := store.MarkCloneFree(context.Background(), project, clonePath); err != nil {
		t.Fatalf("MarkCloneFree() error = %v", err)
	}
	if err := store.MarkCloneFree(context.Background(), project, "missing"); !errors.Is(err, legacy.ErrCloneNotFound) {
		t.Fatalf("MarkCloneFree() missing error = %v, want ErrCloneNotFound", err)
	}

	if err := store.MarkDaemonStopped(context.Background(), project, time.Time{}); err != nil {
		t.Fatalf("MarkDaemonStopped() error = %v", err)
	}
	status, err = store.ProjectStatus(context.Background(), project)
	if err != nil {
		t.Fatalf("ProjectStatus() after stop error = %v", err)
	}
	if status.Daemon == nil || status.Daemon.Status != "stopped" || status.Daemon.PID != 0 {
		t.Fatalf("status.Daemon after stop = %#v, want stopped daemon", status.Daemon)
	}
}

func TestSQLiteStoreNotFoundAndHelpers(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	project := "/repo"

	if _, err := store.TaskStatus(context.Background(), project, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TaskStatus() missing error = %v, want ErrNotFound", err)
	}

	if err := store.DeleteWorker(context.Background(), project, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteWorker() missing error = %v, want ErrNotFound", err)
	}

	if _, err := store.lookupCloneRecord(context.Background(), project, "missing"); !errors.Is(err, legacy.ErrCloneNotFound) {
		t.Fatalf("lookupCloneRecord() missing error = %v, want ErrCloneNotFound", err)
	}

	formatted := formatTime(time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC))
	if parsed := parseTime(formatted); parsed.IsZero() {
		t.Fatal("parseTime(formatTime(...)) returned zero time")
	}
	if parsed := parseTime("not-a-time"); !parsed.IsZero() {
		t.Fatalf("parseTime(invalid) = %v, want zero time", parsed)
	}

	if !isBusyError(errors.New("SQLITE_BUSY")) {
		t.Fatal("isBusyError(SQLITE_BUSY) = false, want true")
	}
	if !isBusyError(errors.New("database is locked")) {
		t.Fatal("isBusyError(database is locked) = false, want true")
	}
	if isBusyError(errors.New("different error")) {
		t.Fatal("isBusyError(different error) = true, want false")
	}
	if isBusyError(nil) {
		t.Fatal("isBusyError(nil) = true, want false")
	}
}

func TestSQLiteExecWithRetryRetriesBusyError(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	store := &SQLiteStore{
		db: openExecRetryStubDB(t, func(context.Context, string, []driver.NamedValue) error {
			if calls.Add(1) == 1 {
				return errors.New("SQLITE_BUSY")
			}
			return nil
		}),
	}

	if err := store.execWithRetry(context.Background(), "SELECT 1"); err != nil {
		t.Fatalf("execWithRetry() error = %v", err)
	}
	if got, want := calls.Load(), int32(2); got != want {
		t.Fatalf("execWithRetry() call count = %d, want %d", got, want)
	}
}

func TestSQLiteExecWithRetryReturnsContextErrorWhenWaitIsCancelled(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	var calls atomic.Int32
	store := &SQLiteStore{
		db: openExecRetryStubDB(t, func(context.Context, string, []driver.NamedValue) error {
			calls.Add(1)
			cancel()
			return errors.New("database is locked")
		}),
	}

	err := store.execWithRetry(ctx, "SELECT 1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("execWithRetry() error = %v, want %v", err, context.Canceled)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("execWithRetry() call count = %d, want %d", got, want)
	}
}

func TestSQLiteExecWithRetryReturnsNonBusyErrorImmediately(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("boom")
	var calls atomic.Int32
	store := &SQLiteStore{
		db: openExecRetryStubDB(t, func(context.Context, string, []driver.NamedValue) error {
			calls.Add(1)
			return wantErr
		}),
	}

	err := store.execWithRetry(context.Background(), "SELECT 1")
	if !errors.Is(err, wantErr) {
		t.Fatalf("execWithRetry() error = %v, want %v", err, wantErr)
	}
	if got, want := calls.Load(), int32(1); got != want {
		t.Fatalf("execWithRetry() call count = %d, want %d", got, want)
	}
}

func TestSQLiteExecWithRetryReturnsLastBusyErrorAfterMaxAttempts(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("SQLITE_BUSY")
	var calls atomic.Int32
	store := &SQLiteStore{
		db: openExecRetryStubDB(t, func(context.Context, string, []driver.NamedValue) error {
			calls.Add(1)
			return wantErr
		}),
	}

	err := store.execWithRetry(context.Background(), "SELECT 1")
	if !errors.Is(err, wantErr) {
		t.Fatalf("execWithRetry() error = %v, want %v", err, wantErr)
	}
	if got, want := calls.Load(), int32(20); got != want {
		t.Fatalf("execWithRetry() call count = %d, want %d", got, want)
	}
}

func TestSQLiteStorePersistsWorkerMonitorStateAndMergeQueue(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	project := "/repo"
	now := time.Date(2026, 4, 3, 9, 30, 0, 0, time.UTC)

	if err := store.UpsertWorker(context.Background(), project, Worker{
		PaneID:                "pane-1",
		Agent:                 "codex",
		State:                 "escalated",
		Issue:                 "LAB-735",
		ClonePath:             "/clones/orca01",
		LastReviewCount:       2,
		LastIssueCommentCount: 4,
		ReviewNudgeCount:      3,
		LastCIState:           "fail",
		LastMergeableState:    "blocked",
		NudgeCount:            3,
		LastCapture:           "permission prompt",
		LastActivityAt:        now,
		UpdatedAt:             now,
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	workers, err := store.ListWorkers(context.Background(), project)
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
	if got, want := worker.LastIssueCommentCount, 4; got != want {
		t.Fatalf("worker.LastIssueCommentCount = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 3; got != want {
		t.Fatalf("worker.ReviewNudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.LastCIState, "fail"; got != want {
		t.Fatalf("worker.LastCIState = %q, want %q", got, want)
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

	position, err := store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:   project,
		Issue:     "LAB-735",
		PRNumber:  42,
		Status:    "queued",
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("EnqueueMergeEntry() error = %v", err)
	}
	if got, want := position, 1; got != want {
		t.Fatalf("position = %d, want %d", got, want)
	}

	entry, err := store.NextMergeEntry(context.Background(), project)
	if err != nil {
		t.Fatalf("NextMergeEntry() error = %v", err)
	}
	if entry == nil {
		t.Fatal("NextMergeEntry() = nil, want entry")
	}
	if got, want := entry.PRNumber, 42; got != want {
		t.Fatalf("entry.PRNumber = %d, want %d", got, want)
	}
	if got, want := entry.Status, "queued"; got != want {
		t.Fatalf("entry.Status = %q, want %q", got, want)
	}

	entries, err := store.MergeEntries(context.Background(), project)
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
	if err := store.UpdateMergeEntry(context.Background(), *entry); err != nil {
		t.Fatalf("UpdateMergeEntry() error = %v", err)
	}

	updatedEntry, err := store.NextMergeEntry(context.Background(), project)
	if err != nil {
		t.Fatalf("NextMergeEntry() after update error = %v", err)
	}
	if updatedEntry == nil {
		t.Fatal("NextMergeEntry() after update = nil, want entry")
	}
	if got, want := updatedEntry.Status, "awaiting_checks"; got != want {
		t.Fatalf("updatedEntry.Status = %q, want %q", got, want)
	}

	if err := store.DeleteMergeEntry(context.Background(), project, 42); err != nil {
		t.Fatalf("DeleteMergeEntry() error = %v", err)
	}
	emptyEntry, err := store.NextMergeEntry(context.Background(), project)
	if err != nil {
		t.Fatalf("NextMergeEntry() after delete error = %v", err)
	}
	if emptyEntry != nil {
		t.Fatalf("NextMergeEntry() after delete = %#v, want nil", emptyEntry)
	}
}

func TestSQLiteStoreWorkerByPaneAndNonTerminalTasks(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	project := "/repo"
	now := time.Date(2026, 4, 4, 11, 0, 0, 0, time.UTC)

	prNumber := 42
	for _, task := range []Task{
		{
			Issue:     "LAB-740",
			Status:    "starting",
			Agent:     "codex",
			Prompt:    "Recover startup",
			WorkerID:  "pane-1",
			ClonePath: "/clones/clone-01",
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			Issue:     "LAB-741",
			Status:    "active",
			Agent:     "codex",
			Prompt:    "Keep running",
			WorkerID:  "pane-2",
			ClonePath: "/clones/clone-02",
			PRNumber:  &prNumber,
			CreatedAt: now,
			UpdatedAt: now.Add(time.Minute),
		},
		{
			Issue:     "LAB-742",
			Status:    "done",
			Agent:     "codex",
			Prompt:    "Finished",
			WorkerID:  "pane-3",
			ClonePath: "/clones/clone-03",
			CreatedAt: now,
			UpdatedAt: now.Add(2 * time.Minute),
		},
	} {
		if err := store.UpsertTask(context.Background(), project, task); err != nil {
			t.Fatalf("UpsertTask(%s) error = %v", task.Issue, err)
		}
	}

	if err := store.UpsertWorker(context.Background(), project, Worker{
		PaneID:                "pane-2",
		Agent:                 "codex",
		State:                 "escalated",
		Issue:                 "LAB-741",
		ClonePath:             "/clones/clone-02",
		LastReviewCount:       2,
		LastIssueCommentCount: 4,
		ReviewNudgeCount:      3,
		LastCIState:           "fail",
		LastMergeableState:    "CONFLICTING",
		NudgeCount:            3,
		LastCapture:           "permission prompt",
		LastActivityAt:        now,
		UpdatedAt:             now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	tasks, err := store.NonTerminalTasks(context.Background(), project)
	if err != nil {
		t.Fatalf("NonTerminalTasks() error = %v", err)
	}
	if got, want := len(tasks), 2; got != want {
		t.Fatalf("len(tasks) = %d, want %d", got, want)
	}
	if got, want := tasks[0].Issue, "LAB-741"; got != want {
		t.Fatalf("tasks[0].Issue = %q, want %q", got, want)
	}
	if tasks[0].PRNumber == nil || *tasks[0].PRNumber != 42 {
		t.Fatalf("tasks[0].PRNumber = %#v, want 42", tasks[0].PRNumber)
	}
	if got, want := tasks[1].Issue, "LAB-740"; got != want {
		t.Fatalf("tasks[1].Issue = %q, want %q", got, want)
	}

	paneTasks, err := store.TasksByPane(context.Background(), project, "pane-2")
	if err != nil {
		t.Fatalf("TasksByPane() error = %v", err)
	}
	if got, want := len(paneTasks), 1; got != want {
		t.Fatalf("len(paneTasks) = %d, want %d", got, want)
	}
	if got, want := paneTasks[0].Issue, "LAB-741"; got != want {
		t.Fatalf("paneTasks[0].Issue = %q, want %q", got, want)
	}
	if paneTasks[0].PRNumber == nil || *paneTasks[0].PRNumber != 42 {
		t.Fatalf("paneTasks[0].PRNumber = %#v, want 42", paneTasks[0].PRNumber)
	}

	worker, err := store.WorkerByPane(context.Background(), project, "pane-2")
	if err != nil {
		t.Fatalf("WorkerByPane() error = %v", err)
	}
	if got, want := worker.LastReviewCount, 2; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
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
	if got, want := worker.LastMergeableState, "CONFLICTING"; got != want {
		t.Fatalf("worker.LastMergeableState = %q, want %q", got, want)
	}
	if got, want := worker.NudgeCount, 3; got != want {
		t.Fatalf("worker.NudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.LastCapture, "permission prompt"; got != want {
		t.Fatalf("worker.LastCapture = %q, want %q", got, want)
	}
	if _, err := store.WorkerByPane(context.Background(), project, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("WorkerByPane() missing error = %v, want ErrNotFound", err)
	}
}

func newTestStore(t *testing.T) *SQLiteStore {
	t.Helper()

	store, err := OpenSQLite(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return store
}

func openExecRetryStubDB(t *testing.T, execFn func(context.Context, string, []driver.NamedValue) error) *sql.DB {
	t.Helper()

	driverName := strings.NewReplacer("/", "_", " ", "_").Replace(
		fmt.Sprintf("exec-retry-%s-%d", t.Name(), execRetryStubDriverSeq.Add(1)),
	)
	sql.Register(driverName, execRetryStubDriver{execFn: execFn})

	db, err := sql.Open(driverName, "")
	if err != nil {
		t.Fatalf("sql.Open(%q) error = %v", driverName, err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return db
}

type execRetryStubDriver struct {
	execFn func(context.Context, string, []driver.NamedValue) error
}

func (d execRetryStubDriver) Open(string) (driver.Conn, error) {
	return execRetryStubConn{execFn: d.execFn}, nil
}

type execRetryStubConn struct {
	execFn func(context.Context, string, []driver.NamedValue) error
}

func (c execRetryStubConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("Prepare not implemented")
}

func (c execRetryStubConn) Close() error {
	return nil
}

func (c execRetryStubConn) Begin() (driver.Tx, error) {
	return nil, errors.New("Begin not implemented")
}

func (c execRetryStubConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if err := c.execFn(ctx, query, args); err != nil {
		return nil, err
	}
	return driver.RowsAffected(1), nil
}
