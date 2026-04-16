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

func copyTasks(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	rows, err := source.db.QueryContext(ctx, `
		SELECT project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at
		FROM tasks
		ORDER BY project ASC, issue ASC
	`)
	if err != nil {
		return fmt.Errorf("query sqlite tasks: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var project string
		var issue string
		var host string
		var status string
		var taskState string
		var agent string
		var prompt string
		var callerPane string
		var workerID string
		var clonePath string
		var branch string
		var prNumber sql.NullInt64
		var createdAtText string
		var updatedAtText string

		if err := rows.Scan(
			&project,
			&issue,
			&host,
			&status,
			&taskState,
			&agent,
			&prompt,
			&callerPane,
			&workerID,
			&clonePath,
			&branch,
			&prNumber,
			&createdAtText,
			&updatedAtText,
		); err != nil {
			return fmt.Errorf("scan sqlite task: %w", err)
		}

		createdAt, err := parseRequiredMigrationTime("tasks", "created_at", createdAtText)
		if err != nil {
			return err
		}
		updatedAt, err := parseRequiredMigrationTime("tasks", "updated_at", updatedAtText)
		if err != nil {
			return err
		}

		var prNumberArg any
		if prNumber.Valid {
			prNumberArg = prNumber.Int64
		}

		if _, err := destination.Exec(ctx, `
			INSERT INTO tasks(project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
			VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		`, project, issue, host, status, taskState, agent, prompt, callerPane, workerID, clonePath, branch, prNumberArg, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert postgres task %s/%s: %w", project, issue, err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite tasks: %w", err)
	}
	return nil
}

func copyWorkers(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	rows, err := source.db.QueryContext(ctx, `
		SELECT project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		ORDER BY project ASC, worker_id ASC
	`)
	if err != nil {
		return fmt.Errorf("query sqlite workers: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var project string
		var workerID string
		var host string
		var agentProfile string
		var currentPaneID string
		var workerState string
		var issue string
		var clonePath string
		var lastReviewCount int64
		var lastInlineReviewCommentCount int64
		var lastIssueCommentCount int64
		var lastIssueCommentWatermark string
		var reviewNudgeCount int64
		var reviewApproved int64
		var lastCIState string
		var ciNudgeCount int64
		var ciFailurePollCount int64
		var ciEscalated int64
		var lastMergeableState string
		var nudgeCount int64
		var lastCapture string
		var lastActivityAt sql.NullString
		var lastPRNumber int64
		var lastPushAt sql.NullString
		var lastPRPollAt sql.NullString
		var restartCount int64
		var firstCrashAt sql.NullString
		var createdAtText string
		var lastSeenAtText string

		if err := rows.Scan(
			&project,
			&workerID,
			&host,
			&agentProfile,
			&currentPaneID,
			&workerState,
			&issue,
			&clonePath,
			&lastReviewCount,
			&lastInlineReviewCommentCount,
			&lastIssueCommentCount,
			&lastIssueCommentWatermark,
			&reviewNudgeCount,
			&reviewApproved,
			&lastCIState,
			&ciNudgeCount,
			&ciFailurePollCount,
			&ciEscalated,
			&lastMergeableState,
			&nudgeCount,
			&lastCapture,
			&lastActivityAt,
			&lastPRNumber,
			&lastPushAt,
			&lastPRPollAt,
			&restartCount,
			&firstCrashAt,
			&createdAtText,
			&lastSeenAtText,
		); err != nil {
			return fmt.Errorf("scan sqlite worker: %w", err)
		}

		createdAt, err := parseRequiredMigrationTime("workers", "created_at", createdAtText)
		if err != nil {
			return err
		}
		lastSeenAt, err := parseRequiredMigrationTime("workers", "last_seen_at", lastSeenAtText)
		if err != nil {
			return err
		}
		lastActivityAtArg, err := parseOptionalMigrationTime("workers", "last_activity_at", lastActivityAt)
		if err != nil {
			return err
		}
		lastPushAtArg, err := parseOptionalMigrationTime("workers", "last_push_at", lastPushAt)
		if err != nil {
			return err
		}
		lastPRPollAtArg, err := parseOptionalMigrationTime("workers", "last_pr_poll_at", lastPRPollAt)
		if err != nil {
			return err
		}
		firstCrashAtArg, err := parseOptionalMigrationTime("workers", "first_crash_at", firstCrashAt)
		if err != nil {
			return err
		}

		if _, err := destination.Exec(ctx, `
			INSERT INTO workers(project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at)
			VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
		`,
			project,
			workerID,
			host,
			agentProfile,
			currentPaneID,
			workerState,
			issue,
			clonePath,
			lastReviewCount,
			lastInlineReviewCommentCount,
			lastIssueCommentCount,
			lastIssueCommentWatermark,
			reviewNudgeCount,
			reviewApproved != 0,
			lastCIState,
			ciNudgeCount,
			ciFailurePollCount,
			ciEscalated != 0,
			lastMergeableState,
			nudgeCount,
			lastCapture,
			lastActivityAtArg,
			lastPRNumber,
			lastPushAtArg,
			lastPRPollAtArg,
			restartCount,
			firstCrashAtArg,
			createdAt,
			lastSeenAt,
		); err != nil {
			return fmt.Errorf("insert postgres worker %s/%s: %w", project, workerID, err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite workers: %w", err)
	}
	return nil
}

func copyClones(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	rows, err := source.db.QueryContext(ctx, `
		SELECT project, path, status, issue, branch, updated_at
		FROM clones
		ORDER BY project ASC, path ASC
	`)
	if err != nil {
		return fmt.Errorf("query sqlite clones: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var project string
		var path string
		var cloneStatus string
		var issue string
		var branch string
		var updatedAtText string

		if err := rows.Scan(&project, &path, &cloneStatus, &issue, &branch, &updatedAtText); err != nil {
			return fmt.Errorf("scan sqlite clone: %w", err)
		}

		updatedAt, err := parseRequiredMigrationTime("clones", "updated_at", updatedAtText)
		if err != nil {
			return err
		}

		if _, err := destination.Exec(ctx, `
			INSERT INTO clones(project, path, status, issue, branch, updated_at)
			VALUES($1, $2, $3, $4, $5, $6)
		`, project, path, cloneStatus, issue, branch, updatedAt); err != nil {
			return fmt.Errorf("insert postgres clone %s/%s: %w", project, path, err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite clones: %w", err)
	}
	return nil
}

func copyEvents(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	rows, err := source.db.QueryContext(ctx, `
		SELECT id, project, kind, issue, worker_id, message, payload, created_at
		FROM events
		ORDER BY id ASC
	`)
	if err != nil {
		return fmt.Errorf("query sqlite events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var project string
		var kind string
		var issue string
		var workerID string
		var message string
		var payload sql.NullString
		var createdAtText string

		if err := rows.Scan(&id, &project, &kind, &issue, &workerID, &message, &payload, &createdAtText); err != nil {
			return fmt.Errorf("scan sqlite event: %w", err)
		}

		createdAt, err := parseRequiredMigrationTime("events", "created_at", createdAtText)
		if err != nil {
			return err
		}

		var payloadArg any
		if payload.Valid {
			payloadArg = payload.String
		}

		if _, err := destination.Exec(ctx, `
			INSERT INTO events(id, project, kind, issue, worker_id, message, payload, created_at)
			VALUES($1, $2, $3, $4, $5, $6, $7, $8)
		`, id, project, kind, issue, workerID, message, payloadArg, createdAt); err != nil {
			return fmt.Errorf("insert postgres event %d: %w", id, err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite events: %w", err)
	}
	return nil
}

func copyMergeQueue(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	rows, err := source.db.QueryContext(ctx, `
		SELECT project, pr_number, issue, status, created_at, updated_at
		FROM merge_queue
		ORDER BY project ASC, pr_number ASC
	`)
	if err != nil {
		return fmt.Errorf("query sqlite merge queue: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var project string
		var prNumber int64
		var issue string
		var entryStatus string
		var createdAtText string
		var updatedAtText string

		if err := rows.Scan(&project, &prNumber, &issue, &entryStatus, &createdAtText, &updatedAtText); err != nil {
			return fmt.Errorf("scan sqlite merge queue entry: %w", err)
		}

		createdAt, err := parseRequiredMigrationTime("merge_queue", "created_at", createdAtText)
		if err != nil {
			return err
		}
		updatedAt, err := parseRequiredMigrationTime("merge_queue", "updated_at", updatedAtText)
		if err != nil {
			return err
		}

		if _, err := destination.Exec(ctx, `
			INSERT INTO merge_queue(project, pr_number, issue, status, created_at, updated_at)
			VALUES($1, $2, $3, $4, $5, $6)
		`, project, prNumber, issue, entryStatus, createdAt, updatedAt); err != nil {
			return fmt.Errorf("insert postgres merge queue entry %s/%d: %w", project, prNumber, err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite merge queue: %w", err)
	}
	return nil
}

func copyDaemonStatus(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	rows, err := source.db.QueryContext(ctx, `
		SELECT project, session, pid, status, started_at, updated_at
		FROM daemon_status
		ORDER BY project ASC
	`)
	if err != nil {
		return fmt.Errorf("query sqlite daemon status: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var project string
		var session string
		var pid int64
		var daemonState string
		var startedAtText string
		var updatedAtText string

		if err := rows.Scan(&project, &session, &pid, &daemonState, &startedAtText, &updatedAtText); err != nil {
			return fmt.Errorf("scan sqlite daemon status: %w", err)
		}

		startedAt, err := parseRequiredMigrationTime("daemon_status", "started_at", startedAtText)
		if err != nil {
			return err
		}
		updatedAt, err := parseRequiredMigrationTime("daemon_status", "updated_at", updatedAtText)
		if err != nil {
			return err
		}

		if _, err := destination.Exec(ctx, `
			INSERT INTO daemon_status(project, session, pid, status, started_at, updated_at)
			VALUES($1, $2, $3, $4, $5, $6)
		`, project, session, pid, daemonState, startedAt, updatedAt); err != nil {
			return fmt.Errorf("insert postgres daemon status %q: %w", project, err)
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate sqlite daemon status: %w", err)
	}
	return nil
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
