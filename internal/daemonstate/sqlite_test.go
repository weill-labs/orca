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
)

var execRetryStubDriverSeq atomic.Uint64

func TestSQLiteStoreLifecycleAndQueries(t *testing.T) {
	t.Parallel()
	testStoreLifecycleAndQueries(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreCloneFailureQuarantine(t *testing.T) {
	t.Parallel()
	testStoreCloneFailureQuarantine(t, newSQLiteContractHarness(t))
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

func TestSQLiteStoreBackfillsEscalatedTaskStateFromWorkerHealth(t *testing.T) {
	t.Parallel()

	project := "/repo"
	dbPath := filepath.Join(t.TempDir(), "state.db")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}

	now := formatTime(time.Date(2026, 4, 12, 9, 0, 0, 0, time.UTC))
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
			worker_id TEXT NOT NULL,
			agent_profile TEXT NOT NULL,
			current_pane_id TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL,
			issue TEXT NOT NULL DEFAULT '',
			clone_path TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			PRIMARY KEY (project, worker_id)
		)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("db.Exec(%q) error = %v", statement, err)
		}
	}

	if _, err := db.Exec(`
		INSERT INTO tasks(project, issue, status, agent, prompt, worker_id, clone_path, pr_number, created_at, updated_at)
		VALUES(?, 'LAB-895', 'active', 'codex', 'Recover missing pane', 'worker-01', '/tmp/clone-01', 42, ?, ?)
	`, project, now, now); err != nil {
		t.Fatalf("insert task error = %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO workers(project, worker_id, agent_profile, current_pane_id, state, issue, clone_path, created_at, last_seen_at)
		VALUES(?, 'worker-01', 'codex', 'pane-1', 'escalated', 'LAB-895', '/tmp/clone-01', ?, ?)
	`, project, now, now); err != nil {
		t.Fatalf("insert worker error = %v", err)
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

	taskStatus, err := store.TaskStatus(context.Background(), project, "LAB-895")
	if err != nil {
		t.Fatalf("TaskStatus() error = %v", err)
	}
	if got, want := taskStatus.Task.State, "escalated"; got != want {
		t.Fatalf("taskStatus.Task.State = %q, want %q", got, want)
	}
}

func TestSQLiteStoreQueryContext(t *testing.T) {
	t.Parallel()

	store, err := OpenSQLite(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	event, err := store.AppendEvent(context.Background(), Event{
		Project:   "/repo",
		Kind:      "task.assigned",
		Issue:     "LAB-1282",
		Message:   "assigned",
		CreatedAt: time.Date(2026, 4, 15, 12, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}

	rows, err := store.QueryContext(context.Background(), `SELECT id, kind FROM events WHERE project = ?`, "/repo")
	if err != nil {
		t.Fatalf("QueryContext() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}

	var (
		id   int64
		kind string
	)
	if err := rows.Scan(&id, &kind); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if got, want := id, event.ID; got != want {
		t.Fatalf("queried id = %d, want %d", got, want)
	}
	if got, want := kind, event.Kind; got != want {
		t.Fatalf("queried kind = %q, want %q", got, want)
	}
	if rows.Next() {
		t.Fatal("rows.Next() = true, want false")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() error = %v", err)
	}
}

func TestSQLiteStoreNotFoundAndHelpers(t *testing.T) {
	t.Parallel()

	testStoreNotFoundBehavior(t, newSQLiteContractHarness(t))

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
	testStoreAllActiveQueriesAcrossProjects(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreGlobalStatusFansOutAcrossProjects(t *testing.T) {
	t.Parallel()
	testStoreGlobalStatusFansOutAcrossProjects(t, newSQLiteContractHarness(t))
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
	testStorePersistsWorkerMonitorStateAndMergeQueue(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreWorkerByPaneAndNonTerminalTasks(t *testing.T) {
	t.Parallel()
	testStoreWorkerByPaneAndNonTerminalTasks(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreStaleCloneOccupancies(t *testing.T) {
	t.Parallel()
	testStoreStaleCloneOccupancies(t, newSQLiteContractHarness)
}

func TestSQLiteStoreMergeQueueOrderingAndNotFound(t *testing.T) {
	t.Parallel()
	testStoreMergeQueueOrderingAndNotFound(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreSchemaIncludesHostColumns(t *testing.T) {
	t.Parallel()
	testStoreSchemaIncludesHostColumns(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreKnownProjectsHostScoped(t *testing.T) {
	t.Parallel()
	testStoreKnownProjectsHostScoped(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreProjectStatusAllHostsAcrossHosts(t *testing.T) {
	t.Parallel()
	testStoreProjectStatusAllHostsAcrossHosts(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreGlobalHostScopedQueriesAcrossHosts(t *testing.T) {
	t.Parallel()
	testStoreGlobalHostScopedQueriesAcrossHosts(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreHostScopedMutationPaths(t *testing.T) {
	t.Parallel()
	testStoreHostScopedMutationPaths(t, newSQLiteContractHarness(t))
}

func TestSQLiteStoreUpsertTaskWritesCurrentHost(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Date(2026, 4, 21, 12, 0, 0, 0, time.UTC)

	if err := store.UpsertTask(context.Background(), "/repo", Task{
		Issue:     "LAB-1422",
		Status:    "active",
		State:     "assigned",
		Agent:     "codex",
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	var host string
	if err := store.db.QueryRowContext(context.Background(), `
		SELECT host
		FROM tasks
		WHERE project = ? AND issue = ?
	`, "/repo", "LAB-1422").Scan(&host); err != nil {
		t.Fatalf("query task host error = %v", err)
	}

	if got, want := host, currentStoreTestHostname(t); got != want {
		t.Fatalf("tasks.host = %q, want %q", got, want)
	}
}

func TestSQLiteStoreNonTerminalTasksFiltersOtherHosts(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	currentHost := currentStoreTestHostname(t)
	otherHost := currentHost + "-other"

	if _, err := store.db.ExecContext(context.Background(), `
		INSERT INTO tasks(project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES
			('/repo', 'LAB-2001', ?, 'active', 'assigned', 'codex', '', '', 'worker-01', '/clones/a', 'lab-2001', NULL, '2026-04-21T12:00:00Z', '2026-04-21T12:00:00Z'),
			('/repo', 'LAB-2002', ?, 'active', 'assigned', 'codex', '', '', 'worker-02', '/clones/b', 'lab-2002', NULL, '2026-04-21T12:01:00Z', '2026-04-21T12:01:00Z')
	`, currentHost, otherHost); err != nil {
		t.Fatalf("insert tasks error = %v", err)
	}
	if _, err := store.db.ExecContext(context.Background(), `
		INSERT INTO workers(project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, created_at, last_seen_at)
		VALUES
			('/repo', 'worker-01', ?, 'codex', 'pane-1', 'healthy', 'LAB-2001', '/clones/a', '2026-04-21T12:00:00Z', '2026-04-21T12:00:00Z'),
			('/repo', 'worker-02', ?, 'codex', 'pane-2', 'healthy', 'LAB-2002', '/clones/b', '2026-04-21T12:01:00Z', '2026-04-21T12:01:00Z')
	`, currentHost, otherHost); err != nil {
		t.Fatalf("insert workers error = %v", err)
	}

	tasks, err := store.NonTerminalTasks(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NonTerminalTasks() error = %v", err)
	}
	if got, want := len(tasks), 1; got != want {
		t.Fatalf("len(tasks) = %d, want %d", got, want)
	}
	if got, want := tasks[0].Issue, "LAB-2001"; got != want {
		t.Fatalf("tasks[0].Issue = %q, want %q", got, want)
	}
}

func TestSQLiteStoreProjectStatusUsesHostScopedDaemonStatuses(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	currentHost := currentStoreTestHostname(t)
	otherHost := currentHost + "-other"

	if _, err := store.db.ExecContext(context.Background(), `
		INSERT INTO daemon_statuses(host, project, session, pid, status, started_at, updated_at)
		VALUES
			(?, '/repo', 'current-session', 111, 'running', '2026-04-21T12:00:00Z', '2026-04-21T12:00:05Z'),
			(?, '/repo', 'other-session', 222, 'running', '2026-04-21T12:00:00Z', '2026-04-21T12:00:06Z')
	`, currentHost, otherHost); err != nil {
		t.Fatalf("insert daemon statuses error = %v", err)
	}

	status, err := store.ProjectStatus(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("ProjectStatus() error = %v", err)
	}
	if status.Daemon == nil {
		t.Fatal("status.Daemon = nil, want current-host daemon")
	}
	if got, want := status.Daemon.Session, "current-session"; got != want {
		t.Fatalf("status.Daemon.Session = %q, want %q", got, want)
	}
}

func TestSQLiteStoreProjectStatusAllHostsAggregatesAcrossHosts(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	store.SetHost("host-b")
	ctx := context.Background()

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO daemon_statuses(host, project, session, pid, status, started_at, updated_at)
		VALUES
			('host-a', '', 'global-a', 11, 'running', '2026-04-21T11:00:00Z', '2026-04-21T11:00:05Z'),
			('host-b', '/repo', 'repo-b', 22, 'running', '2026-04-21T11:00:00Z', '2026-04-21T11:00:06Z')
	`); err != nil {
		t.Fatalf("insert daemon statuses error = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO tasks(project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES
			('/repo', 'LAB-2101', 'host-a', 'active', 'assigned', 'codex', '', '', 'worker-a', '/clones/a', 'lab-2101', NULL, '2026-04-21T11:10:00Z', '2026-04-21T11:10:00Z'),
			('/repo', 'LAB-2102', 'host-b', 'active', 'assigned', 'codex', '', '', 'worker-b', '/clones/b', 'lab-2102', NULL, '2026-04-21T11:11:00Z', '2026-04-21T11:11:00Z')
	`); err != nil {
		t.Fatalf("insert tasks error = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO workers(project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, created_at, last_seen_at)
		VALUES
			('/repo', 'worker-a', 'host-a', 'codex', 'pane-a', 'healthy', 'LAB-2101', '/clones/a', '2026-04-21T11:10:00Z', '2026-04-21T11:10:00Z'),
			('/repo', 'worker-b', 'host-b', 'codex', 'pane-b', 'healthy', 'LAB-2102', '/clones/b', '2026-04-21T11:11:00Z', '2026-04-21T11:11:00Z')
	`); err != nil {
		t.Fatalf("insert workers error = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO clones(project, path, host, status, issue, branch, updated_at)
		VALUES
			('/repo', '/clones/a', 'host-a', 'occupied', 'LAB-2101', 'lab-2101', '2026-04-21T11:10:00Z'),
			('/repo', '/clones/b', 'host-b', 'free', '', '', '2026-04-21T11:11:00Z')
	`); err != nil {
		t.Fatalf("insert clones error = %v", err)
	}

	status, err := store.ProjectStatus(ctx, "/repo")
	if err != nil {
		t.Fatalf("ProjectStatus() error = %v", err)
	}
	if got, want := status.Summary.Tasks, 1; got != want {
		t.Fatalf("host-scoped tasks = %d, want %d", got, want)
	}
	if got, want := status.Summary.Workers, 1; got != want {
		t.Fatalf("host-scoped workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.Clones, 1; got != want {
		t.Fatalf("host-scoped clones = %d, want %d", got, want)
	}
	if status.Daemon == nil || status.Daemon.Session != "repo-b" {
		t.Fatalf("host-scoped daemon = %#v, want repo-b", status.Daemon)
	}

	allHosts, err := store.ProjectStatusAllHosts(ctx, "/repo")
	if err != nil {
		t.Fatalf("ProjectStatusAllHosts() error = %v", err)
	}
	if got, want := allHosts.Summary.Tasks, 2; got != want {
		t.Fatalf("all-host tasks = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Workers, 2; got != want {
		t.Fatalf("all-host workers = %d, want %d", got, want)
	}
	if got, want := allHosts.Summary.Clones, 2; got != want {
		t.Fatalf("all-host clones = %d, want %d", got, want)
	}
	if got, want := len(allHosts.Daemons), 2; got != want {
		t.Fatalf("len(allHosts.Daemons) = %d, want %d", got, want)
	}
}

func TestSQLiteStoreTaskStatusAllHostsReadsOtherHost(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	store.SetHost("host-a")
	ctx := context.Background()

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO tasks(project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES('/repo', 'LAB-2201', 'host-b', 'active', 'assigned', 'codex', '', '', 'worker-b', '/clones/b', 'lab-2201', NULL, '2026-04-21T12:00:00Z', '2026-04-21T12:00:00Z')
	`); err != nil {
		t.Fatalf("insert task error = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO workers(project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, created_at, last_seen_at)
		VALUES('/repo', 'worker-b', 'host-b', 'codex', 'pane-b', 'healthy', 'LAB-2201', '/clones/b', '2026-04-21T12:00:00Z', '2026-04-21T12:00:00Z')
	`); err != nil {
		t.Fatalf("insert worker error = %v", err)
	}

	if _, err := store.TaskStatus(ctx, "/repo", "LAB-2201"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TaskStatus() error = %v, want ErrNotFound", err)
	}
	status, err := store.TaskStatusAllHosts(ctx, "/repo", "LAB-2201")
	if err != nil {
		t.Fatalf("TaskStatusAllHosts() error = %v", err)
	}
	if got, want := status.Task.Issue, "LAB-2201"; got != want {
		t.Fatalf("status.Task.Issue = %q, want %q", got, want)
	}
}

func TestSQLiteStoreGlobalActiveAssignmentsFilterOtherHosts(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	store.SetHost("host-a")
	ctx := context.Background()

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO tasks(project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES
			('/repo-a', 'LAB-2301', 'host-a', 'active', 'assigned', 'codex', '', '', 'worker-a', '/clones/a', 'branch-a', 301, '2026-04-21T13:00:00Z', '2026-04-21T13:00:00Z'),
			('/repo-b', 'LAB-2302', 'host-b', 'active', 'assigned', 'codex', '', '', 'worker-b', '/clones/b', 'branch-b', 302, '2026-04-21T13:01:00Z', '2026-04-21T13:01:00Z')
	`); err != nil {
		t.Fatalf("insert tasks error = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO workers(project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, created_at, last_seen_at)
		VALUES
			('/repo-a', 'worker-a', 'host-a', 'codex', 'pane-a', 'healthy', 'LAB-2301', '/clones/a', '2026-04-21T13:00:00Z', '2026-04-21T13:00:00Z'),
			('/repo-b', 'worker-b', 'host-b', 'codex', 'pane-b', 'healthy', 'LAB-2302', '/clones/b', '2026-04-21T13:01:00Z', '2026-04-21T13:01:00Z')
	`); err != nil {
		t.Fatalf("insert workers error = %v", err)
	}

	assignments, err := store.ActiveAssignments(ctx, "")
	if err != nil {
		t.Fatalf("ActiveAssignments() error = %v", err)
	}
	if got, want := len(assignments), 1; got != want {
		t.Fatalf("len(assignments) = %d, want %d", got, want)
	}
	if got, want := assignments[0].Task.Issue, "LAB-2301"; got != want {
		t.Fatalf("assignments[0].Task.Issue = %q, want %q", got, want)
	}
	if _, err := store.ActiveAssignmentByIssue(ctx, "", "LAB-2302"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ActiveAssignmentByIssue(other host) error = %v, want ErrNotFound", err)
	}
	if _, err := store.ActiveAssignmentByBranch(ctx, "", "branch-a"); err != nil {
		t.Fatalf("ActiveAssignmentByBranch(current host) error = %v", err)
	}
	if _, err := store.ActiveAssignmentByPRNumber(ctx, "", 302); !errors.Is(err, ErrNotFound) {
		t.Fatalf("ActiveAssignmentByPRNumber(other host) error = %v, want ErrNotFound", err)
	}
}

func TestSQLiteStoreEnsureSchemaMigratesLegacyDaemonStatus(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	store.SetHost("legacy-host")
	ctx := context.Background()

	if _, err := store.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS daemon_status (
			project TEXT PRIMARY KEY,
			session TEXT NOT NULL,
			pid INTEGER NOT NULL DEFAULT 0,
			status TEXT NOT NULL,
			started_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
	`); err != nil {
		t.Fatalf("create legacy daemon_status table error = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO daemon_status(project, session, pid, status, started_at, updated_at)
		VALUES('/repo', 'legacy-session', 123, 'running', '2026-04-21T14:00:00Z', '2026-04-21T14:00:01Z')
	`); err != nil {
		t.Fatalf("insert legacy daemon_status row error = %v", err)
	}
	if _, err := store.db.ExecContext(ctx, `DELETE FROM daemon_statuses WHERE host = ? AND project = ?`, "legacy-host", "/repo"); err != nil {
		t.Fatalf("delete migrated daemon status error = %v", err)
	}

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() error = %v", err)
	}

	var session string
	if err := store.db.QueryRowContext(ctx, `
		SELECT session
		FROM daemon_statuses
		WHERE host = ? AND project = ?
	`, "legacy-host", "/repo").Scan(&session); err != nil {
		t.Fatalf("query migrated daemon status error = %v", err)
	}
	if got, want := session, "legacy-session"; got != want {
		t.Fatalf("migrated session = %q, want %q", got, want)
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

func newSQLiteContractHarness(t *testing.T) storeContractHarness {
	t.Helper()

	store := newTestStore(t)
	return storeContractHarness{
		store: store,
		setNow: func(now time.Time) {
			store.now = func() time.Time { return now }
		},
		setHost: func(host string) {
			store.SetHost(host)
		},
		assertHostColumns: func(t *testing.T) {
			t.Helper()

			for _, spec := range []struct {
				table  string
				column string
			}{
				{table: "tasks", column: "host"},
				{table: "workers", column: "host"},
				{table: "clones", column: "host"},
			} {
				hasColumn, err := store.tableHasColumn(context.Background(), spec.table, spec.column)
				if err != nil {
					t.Fatalf("tableHasColumn(%q, %q) error = %v", spec.table, spec.column, err)
				}
				if !hasColumn {
					t.Fatalf("%s.%s column missing", spec.table, spec.column)
				}
			}
		},
	}
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
