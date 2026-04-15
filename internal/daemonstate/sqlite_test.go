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
		assertHostColumns: func(t *testing.T) {
			t.Helper()

			for _, spec := range []struct {
				table  string
				column string
			}{
				{table: "tasks", column: "host"},
				{table: "workers", column: "host"},
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
