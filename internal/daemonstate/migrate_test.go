package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMigrateSQLiteToPostgresCopiesAllRows(t *testing.T) {
	t.Parallel()

	source := newTestStore(t)
	destination := mustPostgresStore(t)
	wantCounts := seedSQLiteMigrationFixture(t, source)

	summary, err := migrateSQLiteToPostgres(context.Background(), source, destination, MigrationOptions{})
	if err != nil {
		t.Fatalf("migrateSQLiteToPostgres() error = %v", err)
	}

	assertMigrationCounts(t, summary, wantCounts)
	assertMigrationTablesEqual(t, source, destination)

	event, err := destination.AppendEvent(context.Background(), Event{
		Project:   "/repo-alpha",
		Kind:      "migration.check",
		Issue:     "LAB-1304",
		WorkerID:  "worker-01",
		Message:   "sequence advanced",
		CreatedAt: time.Date(2026, 4, 16, 13, 30, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("AppendEvent() after migrate error = %v", err)
	}
	if got, want := event.ID, int64(10); got != want {
		t.Fatalf("AppendEvent() id = %d, want %d", got, want)
	}
}

func TestMigrateValidatesStoreTypes(t *testing.T) {
	t.Parallel()

	source := newTestStore(t)
	destination := mustPostgresStore(t)

	_, err := Migrate(context.Background(), destination, destination, MigrationOptions{})
	if err == nil || !strings.Contains(err.Error(), "source must be SQLite") {
		t.Fatalf("Migrate() source error = %v, want source type failure", err)
	}

	_, err = Migrate(context.Background(), source, source, MigrationOptions{})
	if err == nil || !strings.Contains(err.Error(), "destination must be Postgres") {
		t.Fatalf("Migrate() destination error = %v, want destination type failure", err)
	}
}

func TestMigrateSQLiteToPostgresDryRunAndTruncate(t *testing.T) {
	t.Parallel()

	source := newTestStore(t)
	destination := mustPostgresStore(t)
	wantCounts := seedSQLiteMigrationFixture(t, source)
	seedPostgresMigrationDestinationJunk(t, destination)

	beforeTasks, err := postgresTableCount(context.Background(), destination.pool, "tasks")
	if err != nil {
		t.Fatalf("postgresTableCount(tasks) before dry run error = %v", err)
	}
	beforeEvents, err := postgresTableCount(context.Background(), destination.pool, "events")
	if err != nil {
		t.Fatalf("postgresTableCount(events) before dry run error = %v", err)
	}

	dryRunSummary, err := Migrate(context.Background(), source, destination, MigrationOptions{
		DryRun:   true,
		Truncate: true,
	})
	if err != nil {
		t.Fatalf("Migrate() dry run error = %v", err)
	}
	if !dryRunSummary.DryRun {
		t.Fatal("dry run summary did not preserve DryRun=true")
	}
	if !dryRunSummary.Truncate {
		t.Fatal("dry run summary did not preserve Truncate=true")
	}

	afterDryRunTasks, err := postgresTableCount(context.Background(), destination.pool, "tasks")
	if err != nil {
		t.Fatalf("postgresTableCount(tasks) after dry run error = %v", err)
	}
	afterDryRunEvents, err := postgresTableCount(context.Background(), destination.pool, "events")
	if err != nil {
		t.Fatalf("postgresTableCount(events) after dry run error = %v", err)
	}
	if got, want := afterDryRunTasks, beforeTasks; got != want {
		t.Fatalf("tasks rows after dry run = %d, want %d", got, want)
	}
	if got, want := afterDryRunEvents, beforeEvents; got != want {
		t.Fatalf("events rows after dry run = %d, want %d", got, want)
	}

	summary, err := Migrate(context.Background(), source, destination, MigrationOptions{
		Truncate: true,
	})
	if err != nil {
		t.Fatalf("Migrate() with truncate error = %v", err)
	}

	assertMigrationCounts(t, summary, wantCounts)
	assertMigrationTablesEqual(t, source, destination)
}

func TestMigrationCounts(t *testing.T) {
	t.Parallel()

	source := newTestStore(t)
	destination := mustPostgresStore(t)
	seedSQLiteMigrationFixture(t, source)
	seedPostgresMigrationDestinationJunk(t, destination)

	summary, err := migrationCounts(context.Background(), source, destination)
	if err != nil {
		t.Fatalf("migrationCounts() error = %v", err)
	}

	if got, want := summary.TotalSourceRows(), int64(13); got != want {
		t.Fatalf("TotalSourceRows() = %d, want %d", got, want)
	}
	if got, want := summary.TotalDestinationRowsBefore(), int64(2); got != want {
		t.Fatalf("TotalDestinationRowsBefore() = %d, want %d", got, want)
	}

	for _, table := range summary.Tables {
		switch table.Table {
		case "tasks":
			if got, want := table.DestinationRowsBefore, int64(1); got != want {
				t.Fatalf("tasks destination rows before = %d, want %d", got, want)
			}
		case "events":
			if got, want := table.DestinationRowsBefore, int64(1); got != want {
				t.Fatalf("events destination rows before = %d, want %d", got, want)
			}
		default:
			if got := table.DestinationRowsBefore; got != 0 {
				t.Fatalf("%s destination rows before = %d, want 0", table.Table, got)
			}
		}
	}
}

func TestMigrateSQLiteToPostgresRetriesEventSequenceSyncAfterEOF(t *testing.T) {
	t.Parallel()

	source := newTestStore(t)
	destination := mustPostgresStore(t)
	wantCounts := seedSQLiteMigrationFixture(t, source)

	deps := defaultMigrationDeps()
	deps.wait = func(context.Context, time.Duration) error { return nil }

	syncCalls := 0
	deps.syncEventSequence = func(ctx context.Context, queryer pgxQueryExecer) error {
		syncCalls++
		if syncCalls == 1 {
			return fmt.Errorf("sync event id sequence: unexpected EOF")
		}
		return syncPostgresEventSequence(ctx, queryer)
	}

	summary, err := migrateSQLiteToPostgresWithDeps(context.Background(), source, destination, MigrationOptions{}, deps)
	if err != nil {
		t.Fatalf("migrateSQLiteToPostgresWithDeps() error = %v", err)
	}
	if got, want := syncCalls, 2; got != want {
		t.Fatalf("sync event sequence calls = %d, want %d", got, want)
	}

	assertMigrationCounts(t, summary, wantCounts)
	assertMigrationTablesEqual(t, source, destination)

	event, err := destination.AppendEvent(context.Background(), Event{
		Project:   "/repo-alpha",
		Kind:      "migration.retry.check",
		Issue:     "LAB-1407",
		WorkerID:  "worker-01",
		Message:   "sequence advanced after retry",
		CreatedAt: time.Date(2026, 4, 20, 9, 0, 0, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("AppendEvent() after retrying sequence sync error = %v", err)
	}
	if got, want := event.ID, int64(10); got != want {
		t.Fatalf("AppendEvent() id = %d, want %d", got, want)
	}
}

func TestMigrateSQLiteToPostgresCommitsCopiedRowsBeforeSequenceSync(t *testing.T) {
	source := newTestStore(t)
	destination := mustPostgresStore(t)
	wantCounts := seedSQLiteMigrationFixture(t, source)

	deps := defaultMigrationDeps()
	deps.wait = func(context.Context, time.Duration) error { return nil }
	deps.syncEventSequence = func(context.Context, pgxQueryExecer) error {
		return fmt.Errorf("sync event id sequence: unexpected EOF")
	}

	_, err := migrateSQLiteToPostgresWithDeps(context.Background(), source, destination, MigrationOptions{}, deps)
	if err == nil || !strings.Contains(err.Error(), "unexpected EOF") {
		t.Fatalf("migrateSQLiteToPostgresWithDeps() error = %v, want unexpected EOF", err)
	}

	for table, want := range wantCounts {
		got, countErr := postgresTableCount(context.Background(), destination.pool, table)
		if countErr != nil {
			t.Fatalf("postgresTableCount(%s) after failed sequence sync error = %v", table, countErr)
		}
		if got != want {
			t.Fatalf("%s rows after failed sequence sync = %d, want %d", table, got, want)
		}
	}
}

func TestRetryPostgresTableCountRetriesEOF(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	queryer := &retryCountQueryerStub{
		scanErrs: []error{
			fmt.Errorf("count postgres events rows: unexpected EOF"),
			fmt.Errorf("count postgres events rows: unexpected EOF"),
		},
		count: 7,
	}

	var progress []MigrationProgress
	ctx = WithMigrationProgress(ctx, func(event MigrationProgress) {
		progress = append(progress, event)
	})

	count, err := retryPostgresTableCount(ctx, queryer, "events", func(context.Context, time.Duration) error { return nil })
	if err != nil {
		t.Fatalf("retryPostgresTableCount() error = %v", err)
	}
	if got, want := count, int64(7); got != want {
		t.Fatalf("retryPostgresTableCount() = %d, want %d", got, want)
	}
	if got, want := queryer.calls, 3; got != want {
		t.Fatalf("retryPostgresTableCount() calls = %d, want %d", got, want)
	}
	if got, want := len(progress), 3; got != want {
		t.Fatalf("progress events = %d, want %d", got, want)
	}
	for i, event := range progress {
		if got, want := event.Phase, MigrationProgressVerifyTable; got != want {
			t.Fatalf("progress[%d].Phase = %q, want %q", i, got, want)
		}
		if got, want := event.Table, "events"; got != want {
			t.Fatalf("progress[%d].Table = %q, want %q", i, got, want)
		}
		if got, want := event.Attempt, i+1; got != want {
			t.Fatalf("progress[%d].Attempt = %d, want %d", i, got, want)
		}
		if got, want := event.MaxAttempts, postCopyRetryMaxAttempts; got != want {
			t.Fatalf("progress[%d].MaxAttempts = %d, want %d", i, got, want)
		}
	}
}

func TestIsRetryablePostCopyError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "io EOF", err: io.EOF, want: true},
		{name: "wrapped unexpected EOF", err: fmt.Errorf("scan failed: %w", io.ErrUnexpectedEOF), want: true},
		{name: "text unexpected EOF", err: errors.New("sync event id sequence: unexpected EOF"), want: true},
		{name: "context canceled", err: context.Canceled, want: false},
		{name: "other error", err: errors.New("boom"), want: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := isRetryablePostCopyError(tt.err); got != tt.want {
				t.Fatalf("isRetryablePostCopyError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestWaitForPostCopyRetry(t *testing.T) {
	t.Parallel()

	if err := waitForPostCopyRetry(context.Background(), 0); err != nil {
		t.Fatalf("waitForPostCopyRetry() zero delay error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForPostCopyRetry(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForPostCopyRetry() canceled error = %v, want %v", err, context.Canceled)
	}
}

func TestRetryPostCopyStepReturnsNonRetryableError(t *testing.T) {
	t.Parallel()

	calls := 0
	err := retryPostCopyStep(context.Background(), MigrationProgressSyncSequence, "", func(context.Context, time.Duration) error {
		t.Fatal("wait should not be called for non-retryable errors")
		return nil
	}, func() error {
		calls++
		return errors.New("boom")
	})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("retryPostCopyStep() error = %v, want boom", err)
	}
	if got, want := calls, 1; got != want {
		t.Fatalf("retryPostCopyStep() calls = %d, want %d", got, want)
	}
}

func TestWithMigrationProgressNilReporterLeavesContextUntouched(t *testing.T) {
	t.Parallel()

	base := context.Background()
	if got := WithMigrationProgress(base, nil); got != base {
		t.Fatal("WithMigrationProgress(nil) should return the original context")
	}
}

func TestEmitMigrationProgressIgnoresNilContext(t *testing.T) {
	t.Parallel()

	EmitMigrationProgress(nil, MigrationProgress{Phase: MigrationProgressCommit})
}

func TestMigrationSummaryTotals(t *testing.T) {
	t.Parallel()

	summary := MigrationSummary{
		Tables: []TableMigrationSummary{
			{Table: "tasks", SourceRows: 3, DestinationRowsBefore: 1, DestinationRowsAfter: 3},
			{Table: "events", SourceRows: 5, DestinationRowsBefore: 2, DestinationRowsAfter: 5},
		},
	}

	if got, want := summary.TotalSourceRows(), int64(8); got != want {
		t.Fatalf("TotalSourceRows() = %d, want %d", got, want)
	}
	if got, want := summary.TotalDestinationRowsBefore(), int64(3); got != want {
		t.Fatalf("TotalDestinationRowsBefore() = %d, want %d", got, want)
	}
	if got, want := summary.TotalDestinationRowsAfter(), int64(8); got != want {
		t.Fatalf("TotalDestinationRowsAfter() = %d, want %d", got, want)
	}
}

func TestTruncateMigrationTables(t *testing.T) {
	t.Parallel()

	destination := mustPostgresStore(t)
	seedPostgresMigrationDestinationJunk(t, destination)

	tx, err := destination.pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	if err := truncateMigrationTables(context.Background(), tx); err != nil {
		t.Fatalf("truncateMigrationTables() error = %v", err)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	for _, table := range []string{"tasks", "events"} {
		got, countErr := postgresTableCount(context.Background(), destination.pool, table)
		if countErr != nil {
			t.Fatalf("postgresTableCount(%s) error = %v", table, countErr)
		}
		if got != 0 {
			t.Fatalf("%s rows after truncate = %d, want 0", table, got)
		}
	}
}

func TestSourceRowsForMigrationTable(t *testing.T) {
	t.Parallel()

	summary := MigrationSummary{
		Tables: []TableMigrationSummary{
			{Table: "tasks", SourceRows: 3},
		},
	}

	if got, want := sourceRowsForMigrationTable(summary, "tasks"), int64(3); got != want {
		t.Fatalf("sourceRowsForMigrationTable(tasks) = %d, want %d", got, want)
	}
	if got := sourceRowsForMigrationTable(summary, "events"); got != 0 {
		t.Fatalf("sourceRowsForMigrationTable(events) = %d, want 0", got)
	}
}

func TestParseMigrationTimes(t *testing.T) {
	t.Parallel()

	required, err := parseRequiredMigrationTime("events", "created_at", "2026-04-20T10:11:12.123456789Z")
	if err != nil {
		t.Fatalf("parseRequiredMigrationTime() error = %v", err)
	}
	if got, want := required, time.Date(2026, 4, 20, 10, 11, 12, 123456789, time.UTC); got != want {
		t.Fatalf("parseRequiredMigrationTime() = %v, want %v", got, want)
	}

	if _, err := parseRequiredMigrationTime("events", "created_at", ""); err == nil || !strings.Contains(err.Error(), "empty value") {
		t.Fatalf("parseRequiredMigrationTime() empty error = %v, want empty value", err)
	}

	if _, err := parseRequiredMigrationTime("events", "created_at", "not-a-time"); err == nil || !strings.Contains(err.Error(), "not-a-time") {
		t.Fatalf("parseRequiredMigrationTime() invalid error = %v, want parse failure", err)
	}

	optional, err := parseOptionalMigrationTime("workers", "last_seen_at", sql.NullString{Valid: true, String: "2026-04-20T10:11:12Z"})
	if err != nil {
		t.Fatalf("parseOptionalMigrationTime() error = %v", err)
	}
	if got, want := optional.(time.Time), time.Date(2026, 4, 20, 10, 11, 12, 0, time.UTC); got != want {
		t.Fatalf("parseOptionalMigrationTime() = %v, want %v", got, want)
	}

	optional, err = parseOptionalMigrationTime("workers", "last_seen_at", sql.NullString{})
	if err != nil {
		t.Fatalf("parseOptionalMigrationTime() null error = %v", err)
	}
	if optional != nil {
		t.Fatalf("parseOptionalMigrationTime() = %v, want nil", optional)
	}

	if _, err := parseOptionalMigrationTime("workers", "last_seen_at", sql.NullString{Valid: true, String: "bad"}); err == nil || !strings.Contains(err.Error(), "bad") {
		t.Fatalf("parseOptionalMigrationTime() invalid error = %v, want parse failure", err)
	}
}

func seedSQLiteMigrationFixture(t *testing.T, store *SQLiteStore) map[string]int64 {
	t.Helper()

	ctx := context.Background()

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO daemon_statuses(host, project, session, pid, status, started_at, updated_at)
		VALUES
			('host-global', '', 'global-session', 77, 'running', '2026-04-15T08:00:00.123456Z', '2026-04-15T08:05:00.123456Z'),
			('host-alpha', '/repo-alpha', 'alpha-session', 88, 'running', '2026-04-15T09:00:00Z', '2026-04-15T09:30:00Z')
	`); err != nil {
		t.Fatalf("insert daemon_statuses fixture error = %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO tasks(project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES
			('/repo-alpha', 'LAB-1304', 'host-alpha', 'active', 'pr_detected', 'codex', 'Migrate SQLite state to Postgres', 'pane-lead', 'worker-01', '/clones/alpha-01', 'lab-1304', 104, '2026-04-15T09:10:00Z', '2026-04-15T09:20:00Z'),
			('/repo-beta', 'LAB-1305', 'host-beta', 'queued', 'assigned', 'claude', 'Queue follow-up cleanup', 'pane-queue', '', '', '', NULL, '2026-04-15T10:00:00Z', '2026-04-15T10:01:00Z')
	`); err != nil {
		t.Fatalf("insert tasks fixture error = %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO workers(project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at)
		VALUES
			('/repo-alpha', 'worker-01', 'host-alpha', 'codex', 'pane-101', 'healthy', 'LAB-1304', '/clones/alpha-01', 4, 2, 1, 'comment-7', 3, 1, 'failed', 2, 5, 1, 'dirty', 6, 'capture://alpha', '2026-04-15T09:25:00Z', 104, '2026-04-15T09:26:00Z', '2026-04-15T09:27:00Z', 1, '2026-04-15T09:28:00Z', '2026-04-15T09:00:00Z', '2026-04-15T09:29:00Z'),
			('/repo-beta', 'worker-02', 'host-beta', 'claude', '', 'healthy', '', '', 0, 0, 0, '', 0, 0, '', 0, 0, 0, '', 0, '', '', 0, '', '', 0, '', '2026-04-15T10:00:00Z', '2026-04-15T10:02:00Z')
	`); err != nil {
		t.Fatalf("insert workers fixture error = %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO clones(project, path, host, status, issue, branch, updated_at)
		VALUES
			('/repo-alpha', '/clones/alpha-01', 'host-alpha', 'occupied', 'LAB-1304', 'lab-1304', '2026-04-15T09:21:00Z'),
			('/repo-beta', '/clones/beta-01', 'host-beta', 'free', '', '', '2026-04-15T10:03:00Z')
	`); err != nil {
		t.Fatalf("insert clones fixture error = %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO events(id, project, kind, issue, worker_id, message, payload, created_at)
		VALUES
			(7, '/repo-alpha', 'task.assigned', 'LAB-1304', 'worker-01', 'assigned worker', '{"branch":"lab-1304"}', '2026-04-15T09:12:00Z'),
			(8, '/repo-alpha', 'pr.detected', 'LAB-1304', 'worker-01', 'detected PR 104', NULL, '2026-04-15T09:22:00Z'),
			(9, '/repo-beta', 'task.queued', 'LAB-1305', '', 'queued worker', '', '2026-04-15T10:01:30Z')
	`); err != nil {
		t.Fatalf("insert events fixture error = %v", err)
	}

	if _, err := store.db.ExecContext(ctx, `
		INSERT INTO merge_queue(project, pr_number, issue, status, created_at, updated_at)
		VALUES
			('/repo-alpha', 104, 'LAB-1304', 'queued', '2026-04-15T09:23:00Z', '2026-04-15T09:24:00Z'),
			('/repo-beta', 205, 'LAB-1305', 'landing', '2026-04-15T10:04:00Z', '2026-04-15T10:05:00Z')
	`); err != nil {
		t.Fatalf("insert merge_queue fixture error = %v", err)
	}

	return map[string]int64{
		"tasks":           2,
		"workers":         2,
		"clones":          2,
		"events":          3,
		"merge_queue":     2,
		"daemon_statuses": 2,
	}
}

func seedPostgresMigrationDestinationJunk(t *testing.T, store *PostgresStore) {
	t.Helper()

	ctx := context.Background()
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO tasks(project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES('/stale', 'LAB-1', 'junk-host', 'done', 'done', 'codex', 'stale row', '', '', '', '', NULL, '2026-04-01T00:00:00Z', '2026-04-01T00:00:00Z')
	`); err != nil {
		t.Fatalf("insert stale task error = %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO events(id, project, kind, issue, worker_id, message, payload, created_at)
		VALUES(3, '/stale', 'task.done', 'LAB-1', '', 'stale event', NULL, '2026-04-01T00:01:00Z')
	`); err != nil {
		t.Fatalf("insert stale event error = %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		SELECT setval(pg_get_serial_sequence('events', 'id'), 3, true)
	`); err != nil {
		t.Fatalf("set stale event sequence error = %v", err)
	}
}

func assertMigrationCounts(t *testing.T, summary MigrationSummary, wantCounts map[string]int64) {
	t.Helper()

	if got, want := len(summary.Tables), len(migrationTableOrder); got != want {
		t.Fatalf("summary tables = %d, want %d", got, want)
	}

	gotTables := make([]string, 0, len(summary.Tables))
	for _, table := range summary.Tables {
		gotTables = append(gotTables, table.Table)
		wantCount, ok := wantCounts[table.Table]
		if !ok {
			t.Fatalf("unexpected table in summary: %q", table.Table)
		}
		if got, want := table.SourceRows, wantCount; got != want {
			t.Fatalf("%s source rows = %d, want %d", table.Table, got, want)
		}
		if got, want := table.DestinationRowsAfter, wantCount; got != want {
			t.Fatalf("%s destination rows after = %d, want %d", table.Table, got, want)
		}
	}
	if !slices.Equal(gotTables, migrationTableOrder) {
		t.Fatalf("summary table order = %#v, want %#v", gotTables, migrationTableOrder)
	}
}

func assertMigrationTablesEqual(t *testing.T, source *SQLiteStore, destination *PostgresStore) {
	t.Helper()

	for _, spec := range migrationEqualityTableSpecs() {
		sourceRows := dumpSQLiteCanonicalRows(t, source.db, spec)
		destinationRows := dumpPostgresCanonicalRows(t, destination.pool, spec)
		if !slices.EqualFunc(sourceRows, destinationRows, func(a, b []string) bool {
			return slices.Equal(a, b)
		}) {
			t.Fatalf("%s rows differ:\nsource=%#v\ndestination=%#v", spec.name, sourceRows, destinationRows)
		}
	}
}

type migrationEqualityTableSpec struct {
	name    string
	orderBy string
	columns []migrationEqualityColumn
}

type migrationEqualityColumn struct {
	name string
	kind string
}

func migrationEqualityTableSpecs() []migrationEqualityTableSpec {
	return []migrationEqualityTableSpec{
		{
			name:    "tasks",
			orderBy: "project ASC, issue ASC",
			columns: []migrationEqualityColumn{
				{name: "project"},
				{name: "issue"},
				{name: "host"},
				{name: "status"},
				{name: "state"},
				{name: "agent"},
				{name: "prompt"},
				{name: "caller_pane"},
				{name: "worker_id"},
				{name: "clone_path"},
				{name: "branch"},
				{name: "pr_number", kind: "int"},
				{name: "created_at", kind: "time"},
				{name: "updated_at", kind: "time"},
			},
		},
		{
			name:    "workers",
			orderBy: "project ASC, worker_id ASC",
			columns: []migrationEqualityColumn{
				{name: "project"},
				{name: "worker_id"},
				{name: "host"},
				{name: "agent_profile"},
				{name: "current_pane_id"},
				{name: "state"},
				{name: "issue"},
				{name: "clone_path"},
				{name: "last_review_count", kind: "int"},
				{name: "last_inline_review_comment_count", kind: "int"},
				{name: "last_issue_comment_count", kind: "int"},
				{name: "last_issue_comment_watermark"},
				{name: "review_nudge_count", kind: "int"},
				{name: "review_approved", kind: "bool"},
				{name: "last_ci_state"},
				{name: "ci_nudge_count", kind: "int"},
				{name: "ci_failure_poll_count", kind: "int"},
				{name: "ci_escalated", kind: "bool"},
				{name: "last_mergeable_state"},
				{name: "nudge_count", kind: "int"},
				{name: "last_capture"},
				{name: "last_activity_at", kind: "time"},
				{name: "last_pr_number", kind: "int"},
				{name: "last_push_at", kind: "time"},
				{name: "last_pr_poll_at", kind: "time"},
				{name: "restart_count", kind: "int"},
				{name: "first_crash_at", kind: "time"},
				{name: "created_at", kind: "time"},
				{name: "last_seen_at", kind: "time"},
			},
		},
		{
			name:    "clones",
			orderBy: "project ASC, path ASC",
			columns: []migrationEqualityColumn{
				{name: "project"},
				{name: "path"},
				{name: "host"},
				{name: "status"},
				{name: "issue"},
				{name: "branch"},
				{name: "updated_at", kind: "time"},
			},
		},
		{
			name:    "events",
			orderBy: "id ASC",
			columns: []migrationEqualityColumn{
				{name: "id", kind: "int"},
				{name: "project"},
				{name: "kind"},
				{name: "issue"},
				{name: "worker_id"},
				{name: "message"},
				{name: "payload"},
				{name: "created_at", kind: "time"},
			},
		},
		{
			name:    "merge_queue",
			orderBy: "project ASC, pr_number ASC",
			columns: []migrationEqualityColumn{
				{name: "project"},
				{name: "pr_number", kind: "int"},
				{name: "issue"},
				{name: "status"},
				{name: "created_at", kind: "time"},
				{name: "updated_at", kind: "time"},
			},
		},
		{
			name:    "daemon_statuses",
			orderBy: "host ASC, project ASC",
			columns: []migrationEqualityColumn{
				{name: "host"},
				{name: "project"},
				{name: "session"},
				{name: "pid", kind: "int"},
				{name: "status"},
				{name: "started_at", kind: "time"},
				{name: "updated_at", kind: "time"},
			},
		},
	}
}

func dumpSQLiteCanonicalRows(t *testing.T, db *sql.DB, spec migrationEqualityTableSpec) [][]string {
	t.Helper()

	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", joinMigrationColumns(spec.columns), spec.name, spec.orderBy)
	rows, err := db.QueryContext(context.Background(), query)
	if err != nil {
		t.Fatalf("query sqlite %s rows error = %v", spec.name, err)
	}
	defer rows.Close()

	return scanCanonicalRows(t, rows, spec)
}

func dumpPostgresCanonicalRows(t *testing.T, pool *pgxpool.Pool, spec migrationEqualityTableSpec) [][]string {
	t.Helper()

	query := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", joinMigrationColumns(spec.columns), spec.name, spec.orderBy)
	rows, err := pool.Query(context.Background(), query)
	if err != nil {
		t.Fatalf("query postgres %s rows error = %v", spec.name, err)
	}
	defer rows.Close()

	return scanCanonicalRows(t, rows, spec)
}

type migrationRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanCanonicalRows(t *testing.T, rows migrationRows, spec migrationEqualityTableSpec) [][]string {
	t.Helper()

	result := make([][]string, 0)
	for rows.Next() {
		values := make([]any, len(spec.columns))
		scanArgs := make([]any, len(spec.columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}
		if err := rows.Scan(scanArgs...); err != nil {
			t.Fatalf("scan %s row error = %v", spec.name, err)
		}
		result = append(result, canonicalMigrationRow(t, spec, values))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate %s rows error = %v", spec.name, err)
	}
	return result
}

func canonicalMigrationRow(t *testing.T, spec migrationEqualityTableSpec, values []any) []string {
	t.Helper()

	row := make([]string, 0, len(values))
	for i, value := range values {
		row = append(row, canonicalMigrationValue(t, spec.columns[i], value))
	}
	return row
}

func canonicalMigrationValue(t *testing.T, column migrationEqualityColumn, value any) string {
	t.Helper()

	switch column.kind {
	case "int":
		return canonicalMigrationInt(value)
	case "bool":
		return canonicalMigrationBool(value)
	case "time":
		return canonicalMigrationTime(t, column.name, value)
	default:
		return canonicalMigrationText(value)
	}
}

func canonicalMigrationText(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func canonicalMigrationInt(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case int:
		return strconv.Itoa(typed)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case string:
		return typed
	case []byte:
		return string(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func canonicalMigrationBool(value any) string {
	switch typed := value.(type) {
	case nil:
		return ""
	case bool:
		if typed {
			return "1"
		}
		return "0"
	case int:
		if typed != 0 {
			return "1"
		}
		return "0"
	case int32:
		if typed != 0 {
			return "1"
		}
		return "0"
	case int64:
		if typed != 0 {
			return "1"
		}
		return "0"
	case string:
		if typed == "true" || typed == "1" {
			return "1"
		}
		if typed == "false" || typed == "0" || typed == "" {
			return "0"
		}
	case []byte:
		return canonicalMigrationBool(string(typed))
	}
	return fmt.Sprint(value)
}

func canonicalMigrationTime(t *testing.T, column string, value any) string {
	t.Helper()

	switch typed := value.(type) {
	case nil:
		return ""
	case time.Time:
		if typed.IsZero() {
			return ""
		}
		return typed.UTC().Format(time.RFC3339Nano)
	case string:
		return parseCanonicalMigrationTime(t, column, typed)
	case []byte:
		return parseCanonicalMigrationTime(t, column, string(typed))
	default:
		t.Fatalf("unsupported time value type for %s: %T", column, value)
		return ""
	}
}

type retryCountQueryerStub struct {
	scanErrs []error
	count    int64
	calls    int
}

func (s *retryCountQueryerStub) QueryRow(context.Context, string, ...any) pgx.Row {
	index := s.calls
	s.calls++

	row := retryCountRowStub{count: s.count}
	if index < len(s.scanErrs) {
		row.err = s.scanErrs[index]
	}
	return row
}

type retryCountRowStub struct {
	count int64
	err   error
}

func (r retryCountRowStub) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("scan dest len = %d, want 1", len(dest))
	}

	target, ok := dest[0].(*int64)
	if !ok {
		return fmt.Errorf("scan dest type = %T, want *int64", dest[0])
	}
	*target = r.count
	return nil
}

func parseCanonicalMigrationTime(t *testing.T, column, value string) string {
	t.Helper()

	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}

	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err != nil {
		t.Fatalf("parse canonical time %s=%q error = %v", column, value, err)
	}
	return parsed.UTC().Format(time.RFC3339Nano)
}

func joinMigrationColumns(columns []migrationEqualityColumn) string {
	names := make([]string, 0, len(columns))
	for _, column := range columns {
		names = append(names, column.name)
	}
	return strings.Join(names, ", ")
}
