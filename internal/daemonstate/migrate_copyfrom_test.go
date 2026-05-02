package state

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestCopyMigrationTablesUseCopyFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		table     string
		columns   []string
		wantRows  int
		copyFn    func(context.Context, *SQLiteStore, pgx.Tx) error
		assertRow func(*testing.T, [][]any)
	}{
		{
			name:     "tasks",
			table:    "tasks",
			columns:  []string{"project", "issue", "host", "status", "state", "agent", "prompt", "caller_pane", "worker_id", "clone_path", "branch", "pr_number", "pr_repo", "created_at", "updated_at"},
			wantRows: 2,
			copyFn:   copyTasks,
			assertRow: func(t *testing.T, rows [][]any) {
				t.Helper()

				assertCopyString(t, rows[0], 0, "/repo-alpha")
				assertCopyString(t, rows[0], 1, "LAB-1304")
				assertCopyInt64(t, rows[0], 11, 104)
				assertCopyTime(t, rows[0], 13, time.Date(2026, 4, 15, 9, 10, 0, 0, time.UTC))
				assertCopyNil(t, rows[1], 11)
			},
		},
		{
			name:     "workers",
			table:    "workers",
			columns:  []string{"project", "worker_id", "host", "agent_profile", "current_pane_id", "state", "issue", "clone_path", "last_review_count", "last_inline_review_comment_count", "last_issue_comment_count", "last_issue_comment_watermark", "review_nudge_count", "review_approved", "last_ci_state", "ci_nudge_count", "ci_failure_poll_count", "ci_escalated", "last_mergeable_state", "nudge_count", "last_capture", "last_activity_at", "last_pr_number", "last_push_at", "last_pr_poll_at", "restart_count", "first_crash_at", "created_at", "last_seen_at"},
			wantRows: 2,
			copyFn:   copyWorkers,
			assertRow: func(t *testing.T, rows [][]any) {
				t.Helper()

				assertCopyString(t, rows[0], 0, "/repo-alpha")
				assertCopyString(t, rows[0], 1, "worker-01")
				assertCopyBool(t, rows[0], 13, true)
				assertCopyBool(t, rows[0], 17, true)
				assertCopyTime(t, rows[0], 21, time.Date(2026, 4, 15, 9, 25, 0, 0, time.UTC))
				assertCopyTime(t, rows[0], 26, time.Date(2026, 4, 15, 9, 28, 0, 0, time.UTC))
				assertCopyBool(t, rows[1], 13, false)
				assertCopyNil(t, rows[1], 21)
				assertCopyNil(t, rows[1], 23)
				assertCopyNil(t, rows[1], 24)
				assertCopyNil(t, rows[1], 26)
			},
		},
		{
			name:     "clones",
			table:    "clones",
			columns:  []string{"project", "path", "host", "status", "issue", "branch", "updated_at"},
			wantRows: 2,
			copyFn:   copyClones,
			assertRow: func(t *testing.T, rows [][]any) {
				t.Helper()

				assertCopyString(t, rows[0], 1, "/clones/alpha-01")
				assertCopyString(t, rows[0], 2, "host-alpha")
				assertCopyTime(t, rows[0], 6, time.Date(2026, 4, 15, 9, 21, 0, 0, time.UTC))
			},
		},
		{
			name:     "events",
			table:    "events",
			columns:  []string{"id", "project", "kind", "issue", "worker_id", "message", "payload", "created_at"},
			wantRows: 3,
			copyFn:   copyEvents,
			assertRow: func(t *testing.T, rows [][]any) {
				t.Helper()

				assertCopyInt64(t, rows[0], 0, 7)
				assertCopyString(t, rows[0], 6, `{"branch":"lab-1304"}`)
				assertCopyNil(t, rows[1], 6)
				assertCopyString(t, rows[2], 6, "")
			},
		},
		{
			name:     "merge queue",
			table:    "merge_queue",
			columns:  []string{"project", "pr_number", "issue", "status", "created_at", "updated_at"},
			wantRows: 2,
			copyFn:   copyMergeQueue,
			assertRow: func(t *testing.T, rows [][]any) {
				t.Helper()

				assertCopyString(t, rows[0], 0, "/repo-alpha")
				assertCopyInt64(t, rows[0], 1, 104)
				assertCopyTime(t, rows[1], 5, time.Date(2026, 4, 15, 10, 5, 0, 0, time.UTC))
			},
		},
		{
			name:     "daemon status",
			table:    "daemon_statuses",
			columns:  []string{"host", "project", "session", "pid", "status", "started_at", "updated_at"},
			wantRows: 2,
			copyFn:   copyDaemonStatus,
			assertRow: func(t *testing.T, rows [][]any) {
				t.Helper()

				assertCopyString(t, rows[0], 0, "host-alpha")
				assertCopyString(t, rows[0], 1, "/repo-alpha")
				assertCopyInt64(t, rows[0], 3, 88)
				assertCopyTime(t, rows[0], 5, time.Date(2026, 4, 15, 9, 0, 0, 0, time.UTC))
				assertCopyTime(t, rows[1], 6, time.Date(2026, 4, 15, 8, 5, 0, 123456000, time.UTC))
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := newTestStore(t)
			seedSQLiteMigrationFixture(t, source)
			destination := newCopyFromRecorderTx()

			if err := tt.copyFn(context.Background(), source, destination); err != nil {
				t.Fatalf("%s() error = %v", tt.name, err)
			}

			if destination.execCalls != 0 {
				t.Fatalf("%s() used Exec %d times, want 0", tt.name, destination.execCalls)
			}
			if got, want := len(destination.copies), 1; got != want {
				t.Fatalf("recorded copies = %d, want %d", got, want)
			}

			copy, ok := destination.copies[tt.table]
			if !ok {
				t.Fatalf("copy destination missing table %q; got %#v", tt.table, destination.copyTables())
			}
			if !slices.Equal(copy.columns, tt.columns) {
				t.Fatalf("%s copy columns = %#v, want %#v", tt.table, copy.columns, tt.columns)
			}
			if got, want := len(copy.rows), tt.wantRows; got != want {
				t.Fatalf("%s copied rows = %d, want %d", tt.table, got, want)
			}

			tt.assertRow(t, copy.rows)
		})
	}
}

func TestCopyEventsUsesCopyFromForLargeEventTables(t *testing.T) {
	t.Parallel()

	source := newTestStore(t)
	seedSQLiteEventsFixture(t, source, 10_000)
	destination := newCopyFromRecorderTx()

	if err := copyEvents(context.Background(), source, destination); err != nil {
		t.Fatalf("copyEvents() error = %v", err)
	}
	if destination.execCalls != 0 {
		t.Fatalf("copyEvents() used Exec %d times, want 0", destination.execCalls)
	}

	copy := destination.copies["events"]
	if got, want := len(copy.rows), 10_000; got != want {
		t.Fatalf("large events copied rows = %d, want %d", got, want)
	}
	assertCopyInt64(t, copy.rows[0], 0, 1)
	assertCopyInt64(t, copy.rows[len(copy.rows)-1], 0, 10_000)
	assertCopyTime(t, copy.rows[len(copy.rows)-1], 7, time.Date(2026, 4, 20, 14, 46, 39, 0, time.UTC))
}

func seedSQLiteEventsFixture(t *testing.T, store *SQLiteStore, count int) {
	t.Helper()

	tx, err := store.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx() error = %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	stmt, err := tx.PrepareContext(context.Background(), `
		INSERT INTO events(id, project, kind, issue, worker_id, message, payload, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		t.Fatalf("PrepareContext() error = %v", err)
	}
	defer func() {
		_ = stmt.Close()
	}()

	start := time.Date(2026, 4, 20, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= count; i++ {
		var payload any
		if i%2 == 0 {
			payload = fmt.Sprintf(`{"seq":%d}`, i)
		}
		if _, err := stmt.ExecContext(
			context.Background(),
			i,
			"/repo-large",
			"task.progress",
			fmt.Sprintf("LAB-%d", 1400+i),
			"worker-large",
			fmt.Sprintf("event %d", i),
			payload,
			start.Add(time.Duration(i-1)*time.Second).Format(time.RFC3339Nano),
		); err != nil {
			t.Fatalf("ExecContext() error = %v", err)
		}
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
}

type copyFromRecorderTx struct {
	copies    map[string]recordedCopy
	execCalls int
}

type recordedCopy struct {
	columns []string
	rows    [][]any
}

func newCopyFromRecorderTx() *copyFromRecorderTx {
	return &copyFromRecorderTx{
		copies: make(map[string]recordedCopy),
	}
}

func (tx *copyFromRecorderTx) Begin(context.Context) (pgx.Tx, error) {
	return nil, errors.New("unexpected Begin")
}

func (tx *copyFromRecorderTx) Commit(context.Context) error {
	return errors.New("unexpected Commit")
}

func (tx *copyFromRecorderTx) Rollback(context.Context) error {
	return errors.New("unexpected Rollback")
}

func (tx *copyFromRecorderTx) CopyFrom(_ context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	table := strings.Join(tableName, ".")
	if _, exists := tx.copies[table]; exists {
		return 0, fmt.Errorf("duplicate copy for table %q", table)
	}

	record := recordedCopy{
		columns: slices.Clone(columnNames),
	}
	for rowSrc.Next() {
		values, err := rowSrc.Values()
		if err != nil {
			return 0, err
		}
		record.rows = append(record.rows, slices.Clone(values))
	}
	if err := rowSrc.Err(); err != nil {
		return 0, err
	}

	tx.copies[table] = record
	return int64(len(record.rows)), nil
}

func (tx *copyFromRecorderTx) SendBatch(context.Context, *pgx.Batch) pgx.BatchResults {
	panic("unexpected SendBatch")
}

func (tx *copyFromRecorderTx) LargeObjects() pgx.LargeObjects {
	panic("unexpected LargeObjects")
}

func (tx *copyFromRecorderTx) Prepare(context.Context, string, string) (*pgconn.StatementDescription, error) {
	return nil, errors.New("unexpected Prepare")
}

func (tx *copyFromRecorderTx) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	tx.execCalls++
	return pgconn.CommandTag{}, errors.New("unexpected Exec")
}

func (tx *copyFromRecorderTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("unexpected Query")
}

func (tx *copyFromRecorderTx) QueryRow(context.Context, string, ...any) pgx.Row {
	return nil
}

func (tx *copyFromRecorderTx) Conn() *pgx.Conn {
	return nil
}

func (tx *copyFromRecorderTx) copyTables() []string {
	tables := make([]string, 0, len(tx.copies))
	for table := range tx.copies {
		tables = append(tables, table)
	}
	slices.Sort(tables)
	return tables
}

func assertCopyString(t *testing.T, row []any, index int, want string) {
	t.Helper()

	got, ok := row[index].(string)
	if !ok {
		t.Fatalf("row[%d] type = %T, want string", index, row[index])
	}
	if got != want {
		t.Fatalf("row[%d] = %q, want %q", index, got, want)
	}
}

func assertCopyInt64(t *testing.T, row []any, index int, want int64) {
	t.Helper()

	got, ok := row[index].(int64)
	if !ok {
		t.Fatalf("row[%d] type = %T, want int64", index, row[index])
	}
	if got != want {
		t.Fatalf("row[%d] = %d, want %d", index, got, want)
	}
}

func assertCopyBool(t *testing.T, row []any, index int, want bool) {
	t.Helper()

	got, ok := row[index].(bool)
	if !ok {
		t.Fatalf("row[%d] type = %T, want bool", index, row[index])
	}
	if got != want {
		t.Fatalf("row[%d] = %t, want %t", index, got, want)
	}
}

func assertCopyTime(t *testing.T, row []any, index int, want time.Time) {
	t.Helper()

	got, ok := row[index].(time.Time)
	if !ok {
		t.Fatalf("row[%d] type = %T, want time.Time", index, row[index])
	}
	if !got.Equal(want) {
		t.Fatalf("row[%d] = %s, want %s", index, got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
}

func assertCopyNil(t *testing.T, row []any, index int) {
	t.Helper()

	if row[index] != nil {
		t.Fatalf("row[%d] = %#v, want nil", index, row[index])
	}
}
