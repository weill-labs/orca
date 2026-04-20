package state

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var migrationTableOrder = []string{
	"tasks",
	"workers",
	"clones",
	"events",
	"merge_queue",
	"daemon_status",
}

type MigrationOptions struct {
	DryRun   bool
	Truncate bool
}

type TableMigrationSummary struct {
	Table                 string `json:"table"`
	SourceRows            int64  `json:"source_rows"`
	DestinationRowsBefore int64  `json:"destination_rows_before"`
	DestinationRowsAfter  int64  `json:"destination_rows_after"`
}

type MigrationSummary struct {
	DryRun   bool                    `json:"dry_run"`
	Truncate bool                    `json:"truncate"`
	Tables   []TableMigrationSummary `json:"tables"`
}

func (s MigrationSummary) TotalSourceRows() int64 {
	var total int64
	for _, table := range s.Tables {
		total += table.SourceRows
	}
	return total
}

func (s MigrationSummary) TotalDestinationRowsBefore() int64 {
	var total int64
	for _, table := range s.Tables {
		total += table.DestinationRowsBefore
	}
	return total
}

func (s MigrationSummary) TotalDestinationRowsAfter() int64 {
	var total int64
	for _, table := range s.Tables {
		total += table.DestinationRowsAfter
	}
	return total
}

func Migrate(ctx context.Context, from Store, to Store, options MigrationOptions) (MigrationSummary, error) {
	source, ok := from.(*SQLiteStore)
	if !ok {
		return MigrationSummary{}, fmt.Errorf("migrate state source must be SQLite, got %T", from)
	}

	destination, ok := to.(*PostgresStore)
	if !ok {
		return MigrationSummary{}, fmt.Errorf("migrate state destination must be Postgres, got %T", to)
	}

	return migrateSQLiteToPostgres(ctx, source, destination, options)
}

func migrateSQLiteToPostgres(ctx context.Context, source *SQLiteStore, destination *PostgresStore, options MigrationOptions) (MigrationSummary, error) {
	summary, err := migrationCounts(ctx, source, destination)
	if err != nil {
		return MigrationSummary{}, err
	}
	summary.DryRun = options.DryRun
	summary.Truncate = options.Truncate

	if options.DryRun {
		for i := range summary.Tables {
			summary.Tables[i].DestinationRowsAfter = summary.Tables[i].DestinationRowsBefore
		}
		return summary, nil
	}

	if !options.Truncate {
		for _, table := range summary.Tables {
			if table.DestinationRowsBefore != 0 {
				return MigrationSummary{}, fmt.Errorf("destination table %s is not empty; rerun with --truncate to replace existing rows", table.Table)
			}
		}
	}

	tx, err := destination.pool.Begin(ctx)
	if err != nil {
		return MigrationSummary{}, fmt.Errorf("begin destination migration tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if options.Truncate {
		if err := truncateMigrationTables(ctx, tx); err != nil {
			return MigrationSummary{}, err
		}
	}

	for _, table := range migrationTableOrder {
		if err := copyMigrationTable(ctx, source, tx, table); err != nil {
			return MigrationSummary{}, err
		}
	}

	if err := syncPostgresEventSequence(ctx, tx); err != nil {
		return MigrationSummary{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return MigrationSummary{}, fmt.Errorf("commit destination migration tx: %w", err)
	}

	for i := range summary.Tables {
		count, err := postgresTableCount(ctx, destination.pool, summary.Tables[i].Table)
		if err != nil {
			return MigrationSummary{}, err
		}
		summary.Tables[i].DestinationRowsAfter = count
		if count != summary.Tables[i].SourceRows {
			return MigrationSummary{}, fmt.Errorf("destination table %s row count = %d, want %d", summary.Tables[i].Table, count, summary.Tables[i].SourceRows)
		}
	}

	return summary, nil
}

func migrationCounts(ctx context.Context, source *SQLiteStore, destination *PostgresStore) (MigrationSummary, error) {
	summary := MigrationSummary{
		Tables: make([]TableMigrationSummary, 0, len(migrationTableOrder)),
	}

	for _, table := range migrationTableOrder {
		sourceRows, err := sqliteTableCount(ctx, source.db, table)
		if err != nil {
			return MigrationSummary{}, err
		}
		destinationRows, err := postgresTableCount(ctx, destination.pool, table)
		if err != nil {
			return MigrationSummary{}, err
		}
		summary.Tables = append(summary.Tables, TableMigrationSummary{
			Table:                 table,
			SourceRows:            sourceRows,
			DestinationRowsBefore: destinationRows,
		})
	}

	return summary, nil
}

func truncateMigrationTables(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `TRUNCATE TABLE daemon_status, tasks, workers, clones, events, merge_queue RESTART IDENTITY`); err != nil {
		return fmt.Errorf("truncate destination tables: %w", err)
	}
	return nil
}

func copyMigrationTable(ctx context.Context, source *SQLiteStore, destination pgx.Tx, table string) error {
	switch table {
	case "tasks":
		return copyTasks(ctx, source, destination)
	case "workers":
		return copyWorkers(ctx, source, destination)
	case "clones":
		return copyClones(ctx, source, destination)
	case "events":
		return copyEvents(ctx, source, destination)
	case "merge_queue":
		return copyMergeQueue(ctx, source, destination)
	case "daemon_status":
		return copyDaemonStatus(ctx, source, destination)
	default:
		return fmt.Errorf("unsupported migration table %q", table)
	}
}

func syncPostgresEventSequence(ctx context.Context, tx pgx.Tx) error {
	var maxID sql.NullInt64
	if err := tx.QueryRow(ctx, `SELECT MAX(id) FROM events`).Scan(&maxID); err != nil {
		return fmt.Errorf("query max event id: %w", err)
	}

	sequenceValue := int64(1)
	sequenceCalled := false
	if maxID.Valid {
		sequenceValue = maxID.Int64
		sequenceCalled = true
	}

	if _, err := tx.Exec(ctx, `SELECT setval(pg_get_serial_sequence('events', 'id'), $1, $2)`, sequenceValue, sequenceCalled); err != nil {
		return fmt.Errorf("sync event id sequence: %w", err)
	}
	return nil
}

func sqliteTableCount(ctx context.Context, db *sql.DB, table string) (int64, error) {
	var count int64
	if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		return 0, fmt.Errorf("count sqlite %s rows: %w", table, err)
	}
	return count, nil
}

type pgxCountQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func postgresTableCount(ctx context.Context, queryer pgxCountQueryer, table string) (int64, error) {
	var count int64
	if err := queryer.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&count); err != nil {
		return 0, fmt.Errorf("count postgres %s rows: %w", table, err)
	}
	return count, nil
}

func parseRequiredMigrationTime(table, column, value string) (time.Time, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, fmt.Errorf("parse %s.%s time: empty value", table, column)
	}

	parsed, err := time.Parse(time.RFC3339Nano, trimmed)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse %s.%s time %q: %w", table, column, value, err)
	}
	return parsed.UTC(), nil
}

func parseOptionalMigrationTime(table, column string, value sql.NullString) (any, error) {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil, nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value.String))
	if err != nil {
		return nil, fmt.Errorf("parse %s.%s time %q: %w", table, column, value.String, err)
	}
	return parsed.UTC(), nil
}
