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
		WorkerID:  "worker-01",
		ClonePath: clonePath,
		PRNumber:  &prNumber,
	}); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	if err := store.UpsertWorker(context.Background(), project, Worker{
		WorkerID:      "worker-01",
		CurrentPaneID: "pane-1",
		Agent:         "codex",
		State:         "healthy",
		Issue:         "LAB-718",
		ClonePath:     clonePath,
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	firstEvent, err := store.AppendEvent(context.Background(), Event{
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

	if err := store.DeleteWorker(context.Background(), project, "worker-01"); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}
	if err := store.DeleteWorker(context.Background(), project, "worker-01"); !errors.Is(err, ErrNotFound) {
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

func TestSQLiteStoreMigratesPaneKeyedWorkersToStableWorkerIDs(t *testing.T) {
	t.Parallel()

	project := "/repo"
	dbPath := filepath.Join(t.TempDir(), "state.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}

	legacyUpdatedAt := formatTime(time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC))
	legacyCreatedAt := formatTime(time.Date(2026, 4, 2, 10, 5, 0, 0, time.UTC))
	statements := []string{
		`CREATE TABLE tasks (
			project TEXT NOT NULL,
			issue TEXT NOT NULL,
			status TEXT NOT NULL,
			agent TEXT NOT NULL,
			prompt TEXT NOT NULL DEFAULT '',
			worker_id TEXT NOT NULL DEFAULT '',
			clone_path TEXT NOT NULL DEFAULT '',
			pr_number INTEGER,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (project, issue)
		)`,
		`CREATE TABLE workers (
			project TEXT NOT NULL,
			pane_id TEXT NOT NULL,
			agent TEXT NOT NULL,
			state TEXT NOT NULL,
			issue TEXT NOT NULL DEFAULT '',
			clone_path TEXT NOT NULL DEFAULT '',
			updated_at TEXT NOT NULL,
			PRIMARY KEY (project, pane_id)
		)`,
		`CREATE TABLE events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			project TEXT NOT NULL,
			kind TEXT NOT NULL,
			issue TEXT NOT NULL DEFAULT '',
			message TEXT NOT NULL,
			payload TEXT,
			created_at TEXT NOT NULL
		)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("db.Exec(%q) error = %v", statement, err)
		}
	}

	if _, err := db.Exec(`
		INSERT INTO tasks(project, issue, status, agent, prompt, worker_id, clone_path, created_at, updated_at)
		VALUES(?, ?, 'active', 'codex', 'Implement worker identity', 'pane-9', '/tmp/clone-01', ?, ?)
	`, project, "LAB-894", legacyCreatedAt, legacyUpdatedAt); err != nil {
		t.Fatalf("insert task error = %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO workers(project, pane_id, agent, state, issue, clone_path, updated_at)
		VALUES(?, 'pane-9', 'codex', 'healthy', 'LAB-894', '/tmp/clone-01', ?)
	`, project, legacyUpdatedAt); err != nil {
		t.Fatalf("insert worker error = %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO events(project, kind, issue, message, payload, created_at)
		VALUES(?, 'task.assigned', 'LAB-894', 'assigned', '{"type":"task.assigned","issue":"LAB-894","pane_id":"pane-9"}', ?)
	`, project, legacyCreatedAt); err != nil {
		t.Fatalf("insert event error = %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("db.Close() error = %v", err)
	}

	store, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	workers, err := store.ListWorkers(context.Background(), project)
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	if got, want := len(workers), 1; got != want {
		t.Fatalf("len(workers) = %d, want %d", got, want)
	}
	if got, want := workers[0].WorkerID, "worker-01"; got != want {
		t.Fatalf("workers[0].WorkerID = %q, want %q", got, want)
	}
	if got, want := workers[0].CurrentPaneID, "pane-9"; got != want {
		t.Fatalf("workers[0].CurrentPaneID = %q, want %q", got, want)
	}
	if got, want := workers[0].CreatedAt, parseTime(legacyUpdatedAt); !got.Equal(want) {
		t.Fatalf("workers[0].CreatedAt = %v, want %v", got, want)
	}
	if got, want := workers[0].LastSeenAt, parseTime(legacyUpdatedAt); !got.Equal(want) {
		t.Fatalf("workers[0].LastSeenAt = %v, want %v", got, want)
	}

	taskStatus, err := store.TaskStatus(context.Background(), project, "LAB-894")
	if err != nil {
		t.Fatalf("TaskStatus() error = %v", err)
	}
	if got, want := taskStatus.Task.WorkerID, "worker-01"; got != want {
		t.Fatalf("taskStatus.Task.WorkerID = %q, want %q", got, want)
	}
	if got, want := taskStatus.Task.CurrentPaneID, "pane-9"; got != want {
		t.Fatalf("taskStatus.Task.CurrentPaneID = %q, want %q", got, want)
	}
	if got, want := len(taskStatus.Events), 1; got != want {
		t.Fatalf("len(taskStatus.Events) = %d, want %d", got, want)
	}
	if got, want := taskStatus.Events[0].WorkerID, "worker-01"; got != want {
		t.Fatalf("taskStatus.Events[0].WorkerID = %q, want %q", got, want)
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

func TestSQLiteStoreAllActiveQueriesAcrossProjects(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Date(2026, 4, 7, 10, 0, 0, 0, time.UTC)

	for _, task := range []Task{
		{
			Issue:     "LAB-901",
			Status:    "active",
			Agent:     "codex",
			WorkerID:  "worker-a",
			ClonePath: "/clones/a",
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			Issue:     "LAB-902",
			Status:    "starting",
			Agent:     "codex",
			WorkerID:  "worker-b",
			ClonePath: "/clones/b",
			CreatedAt: now,
			UpdatedAt: now.Add(time.Minute),
		},
		{
			Issue:     "LAB-903",
			Status:    "done",
			Agent:     "codex",
			WorkerID:  "worker-c",
			ClonePath: "/clones/c",
			CreatedAt: now,
			UpdatedAt: now.Add(2 * time.Minute),
		},
	} {
		project := "/repo-a"
		if task.Issue == "LAB-902" {
			project = "/repo-b"
		}
		if err := store.UpsertTask(context.Background(), project, task); err != nil {
			t.Fatalf("UpsertTask(%s) error = %v", task.Issue, err)
		}
	}

	if err := store.UpsertWorker(context.Background(), "/repo-a", Worker{
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

	if err := store.UpsertWorker(context.Background(), "/repo-b", Worker{
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

	if _, err := store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:   "/repo-a",
		Issue:     "LAB-901",
		PRNumber:  41,
		Status:    "queued",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("EnqueueMergeEntry(/repo-a) error = %v", err)
	}
	if _, err := store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:   "/repo-b",
		Issue:     "LAB-902",
		PRNumber:  42,
		Status:    "awaiting_checks",
		CreatedAt: now.Add(time.Minute),
		UpdatedAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("EnqueueMergeEntry(/repo-b) error = %v", err)
	}

	tasks, err := store.AllNonTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("AllNonTerminalTasks() error = %v", err)
	}
	if got, want := len(tasks), 2; got != want {
		t.Fatalf("len(AllNonTerminalTasks()) = %d, want %d", got, want)
	}

	assignments, err := store.AllActiveAssignments(context.Background())
	if err != nil {
		t.Fatalf("AllActiveAssignments() error = %v", err)
	}
	if got, want := len(assignments), 1; got != want {
		t.Fatalf("len(AllActiveAssignments()) = %d, want %d", got, want)
	}
	if got, want := assignments[0].Task.Issue, "LAB-901"; got != want {
		t.Fatalf("AllActiveAssignments()[0].Task.Issue = %q, want %q", got, want)
	}

	entries, err := store.AllMergeEntries(context.Background())
	if err != nil {
		t.Fatalf("AllMergeEntries() error = %v", err)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("len(AllMergeEntries()) = %d, want %d", got, want)
	}
}

func TestSQLiteStoreGlobalStatusFansOutAcrossProjects(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Date(2026, 4, 7, 12, 0, 0, 0, time.UTC)

	if err := store.UpsertTask(context.Background(), "/repo-a", Task{
		Issue:     "LAB-901",
		Status:    "active",
		Agent:     "codex",
		Prompt:    "Fix global status queries",
		WorkerID:  "worker-a",
		ClonePath: "/clones/a",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertTask(/repo-a) error = %v", err)
	}
	if err := store.UpsertTask(context.Background(), "/repo-b", Task{
		Issue:     "LAB-902",
		Status:    "done",
		Agent:     "claude",
		Prompt:    "Verify global status output",
		WorkerID:  "worker-b",
		ClonePath: "/clones/b",
		CreatedAt: now.Add(time.Minute),
		UpdatedAt: now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertTask(/repo-b) error = %v", err)
	}

	if err := store.UpsertWorker(context.Background(), "/repo-a", Worker{
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
	if err := store.UpsertWorker(context.Background(), "/repo-b", Worker{
		WorkerID:      "worker-b",
		CurrentPaneID: "pane-b",
		Agent:         "claude",
		State:         "stuck",
		Issue:         "LAB-902",
		ClonePath:     "/clones/b",
		CreatedAt:     now.Add(time.Minute),
		LastSeenAt:    now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("UpsertWorker(/repo-b) error = %v", err)
	}

	if _, err := store.EnsureClone(context.Background(), "/repo-a", "/clones/a"); err != nil {
		t.Fatalf("EnsureClone(/repo-a) error = %v", err)
	}
	if _, err := store.EnsureClone(context.Background(), "/repo-b", "/clones/b"); err != nil {
		t.Fatalf("EnsureClone(/repo-b) error = %v", err)
	}
	if ok, err := store.TryOccupyClone(context.Background(), "/repo-a", "/clones/a", "LAB-901", "LAB-901"); err != nil {
		t.Fatalf("TryOccupyClone(/repo-a) error = %v", err)
	} else if !ok {
		t.Fatal("TryOccupyClone(/repo-a) = false, want true")
	}

	tasks, err := store.listTasks(context.Background(), "")
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

	workers, err := store.ListWorkers(context.Background(), "")
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

	clones, err := store.ListClones(context.Background(), "")
	if err != nil {
		t.Fatalf("ListClones(global) error = %v", err)
	}
	if got, want := len(clones), 2; got != want {
		t.Fatalf("len(ListClones(global)) = %d, want %d", got, want)
	}

	status, err := store.ProjectStatus(context.Background(), "")
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
		WorkerID:                     "worker-01",
		CurrentPaneID:                "pane-1",
		Agent:                        "codex",
		State:                        "escalated",
		Issue:                        "LAB-735",
		ClonePath:                    "/clones/orca01",
		LastReviewCount:              2,
		LastInlineReviewCommentCount: 1,
		LastIssueCommentCount:        4,
		ReviewNudgeCount:             3,
		LastCIState:                  "fail",
		CINudgeCount:                 2,
		CIFailurePollCount:           1,
		CIEscalated:                  true,
		LastMergeableState:           "blocked",
		NudgeCount:                   3,
		LastCapture:                  "permission prompt",
		LastActivityAt:               now,
		RestartCount:                 2,
		FirstCrashAt:                 now.Add(-3 * time.Minute),
		CreatedAt:                    now,
		LastSeenAt:                   now,
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
	if got, want := worker.RestartCount, 2; got != want {
		t.Fatalf("worker.RestartCount = %d, want %d", got, want)
	}
	if got, want := worker.FirstCrashAt, now.Add(-3*time.Minute); !got.Equal(want) {
		t.Fatalf("worker.FirstCrashAt = %v, want %v", got, want)
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

	entry, err := store.MergeEntry(context.Background(), project, 42)
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

	updatedEntry, err := store.MergeEntry(context.Background(), project, 42)
	if err != nil {
		t.Fatalf("MergeEntry() after update error = %v", err)
	}
	if updatedEntry == nil {
		t.Fatal("MergeEntry() after update = nil, want entry")
	}
	if got, want := updatedEntry.Status, "awaiting_checks"; got != want {
		t.Fatalf("updatedEntry.Status = %q, want %q", got, want)
	}

	if err := store.DeleteMergeEntry(context.Background(), project, 42); err != nil {
		t.Fatalf("DeleteMergeEntry() error = %v", err)
	}
	emptyEntry, err := store.MergeEntry(context.Background(), project, 42)
	if err != nil {
		t.Fatalf("MergeEntry() after delete error = %v", err)
	}
	if emptyEntry != nil {
		t.Fatalf("MergeEntry() after delete = %#v, want nil", emptyEntry)
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
			WorkerID:  "worker-01",
			ClonePath: "/clones/clone-01",
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			Issue:     "LAB-741",
			Status:    "active",
			Agent:     "codex",
			Prompt:    "Keep running",
			WorkerID:  "worker-02",
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
			WorkerID:  "worker-03",
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
		WorkerID:                     "worker-02",
		CurrentPaneID:                "pane-2",
		Agent:                        "codex",
		State:                        "escalated",
		Issue:                        "LAB-741",
		ClonePath:                    "/clones/clone-02",
		LastReviewCount:              2,
		LastInlineReviewCommentCount: 1,
		LastIssueCommentCount:        4,
		ReviewNudgeCount:             3,
		LastCIState:                  "fail",
		CINudgeCount:                 2,
		CIFailurePollCount:           1,
		CIEscalated:                  true,
		LastMergeableState:           "CONFLICTING",
		NudgeCount:                   3,
		LastCapture:                  "permission prompt",
		LastActivityAt:               now,
		RestartCount:                 2,
		FirstCrashAt:                 now.Add(-2 * time.Minute),
		CreatedAt:                    now,
		LastSeenAt:                   now.Add(time.Minute),
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
	if got, want := worker.RestartCount, 2; got != want {
		t.Fatalf("worker.RestartCount = %d, want %d", got, want)
	}
	if got, want := worker.FirstCrashAt, now.Add(-2*time.Minute); !got.Equal(want) {
		t.Fatalf("worker.FirstCrashAt = %v, want %v", got, want)
	}
	if _, err := store.WorkerByPane(context.Background(), project, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("WorkerByPane() missing error = %v, want ErrNotFound", err)
	}
}

func TestSQLiteStoreMergeQueueOrderingAndNotFound(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	project := "/repo"
	now := time.Date(2026, 4, 6, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	position, err := store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:  project,
		Issue:    "LAB-751",
		PRNumber: 43,
		Status:   "awaiting_checks",
	})
	if err != nil {
		t.Fatalf("EnqueueMergeEntry(43) error = %v", err)
	}
	if got, want := position, 1; got != want {
		t.Fatalf("position for PR 43 = %d, want %d", got, want)
	}

	position, err = store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:   project,
		Issue:     "LAB-750",
		PRNumber:  42,
		Status:    "queued",
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("EnqueueMergeEntry(42) error = %v", err)
	}
	if got, want := position, 2; got != want {
		t.Fatalf("position for PR 42 = %d, want %d", got, want)
	}

	entry, err := store.MergeEntry(context.Background(), project, 43)
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

	entries, err := store.MergeEntries(context.Background(), project)
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

	if err := store.UpdateMergeEntry(context.Background(), MergeQueueEntry{
		Project:  project,
		Issue:    "LAB-799",
		PRNumber: 99,
		Status:   "queued",
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateMergeEntry(missing) error = %v, want ErrNotFound", err)
	}

	if err := store.DeleteMergeEntry(context.Background(), project, 99); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteMergeEntry(missing) error = %v, want ErrNotFound", err)
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
