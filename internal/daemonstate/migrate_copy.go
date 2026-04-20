package state

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func copySQLiteRows(
	ctx context.Context,
	source *SQLiteStore,
	destination pgx.Tx,
	table string,
	label string,
	columns []string,
	query string,
	next func(*sql.Rows) ([]any, error),
) error {
	rows, err := source.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("query sqlite %s: %w", label, err)
	}
	defer rows.Close()

	if _, err := destination.CopyFrom(ctx, pgx.Identifier{table}, columns, pgx.CopyFromFunc(func() ([]any, error) {
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return nil, fmt.Errorf("iterate sqlite %s: %w", label, err)
			}
			return nil, nil
		}
		return next(rows)
	})); err != nil {
		return fmt.Errorf("copy postgres %s: %w", label, err)
	}

	return nil
}

func copyTasks(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	return copySQLiteRows(ctx, source, destination, "tasks", "tasks",
		[]string{"project", "issue", "host", "status", "state", "agent", "prompt", "caller_pane", "worker_id", "clone_path", "branch", "pr_number", "created_at", "updated_at"},
		`
		SELECT project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at
		FROM tasks
		ORDER BY project ASC, issue ASC
	`, func(rows *sql.Rows) ([]any, error) {
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
				return nil, fmt.Errorf("scan sqlite task: %w", err)
			}

			createdAt, err := parseRequiredMigrationTime("tasks", "created_at", createdAtText)
			if err != nil {
				return nil, err
			}
			updatedAt, err := parseRequiredMigrationTime("tasks", "updated_at", updatedAtText)
			if err != nil {
				return nil, err
			}

			var prNumberArg any
			if prNumber.Valid {
				prNumberArg = prNumber.Int64
			}

			return []any{
				project,
				issue,
				host,
				status,
				taskState,
				agent,
				prompt,
				callerPane,
				workerID,
				clonePath,
				branch,
				prNumberArg,
				createdAt,
				updatedAt,
			}, nil
		})
}

func copyWorkers(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	return copySQLiteRows(ctx, source, destination, "workers", "workers",
		[]string{"project", "worker_id", "host", "agent_profile", "current_pane_id", "state", "issue", "clone_path", "last_review_count", "last_inline_review_comment_count", "last_issue_comment_count", "last_issue_comment_watermark", "review_nudge_count", "review_approved", "last_ci_state", "ci_nudge_count", "ci_failure_poll_count", "ci_escalated", "last_mergeable_state", "nudge_count", "last_capture", "last_activity_at", "last_pr_number", "last_push_at", "last_pr_poll_at", "restart_count", "first_crash_at", "created_at", "last_seen_at"},
		`
		SELECT project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		ORDER BY project ASC, worker_id ASC
	`, func(rows *sql.Rows) ([]any, error) {
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
				return nil, fmt.Errorf("scan sqlite worker: %w", err)
			}

			createdAt, err := parseRequiredMigrationTime("workers", "created_at", createdAtText)
			if err != nil {
				return nil, err
			}
			lastSeenAt, err := parseRequiredMigrationTime("workers", "last_seen_at", lastSeenAtText)
			if err != nil {
				return nil, err
			}
			lastActivityAtArg, err := parseOptionalMigrationTime("workers", "last_activity_at", lastActivityAt)
			if err != nil {
				return nil, err
			}
			lastPushAtArg, err := parseOptionalMigrationTime("workers", "last_push_at", lastPushAt)
			if err != nil {
				return nil, err
			}
			lastPRPollAtArg, err := parseOptionalMigrationTime("workers", "last_pr_poll_at", lastPRPollAt)
			if err != nil {
				return nil, err
			}
			firstCrashAtArg, err := parseOptionalMigrationTime("workers", "first_crash_at", firstCrashAt)
			if err != nil {
				return nil, err
			}

			return []any{
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
			}, nil
		})
}

func copyClones(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	return copySQLiteRows(ctx, source, destination, "clones", "clones",
		[]string{"project", "path", "status", "issue", "branch", "updated_at"},
		`
		SELECT project, path, status, issue, branch, updated_at
		FROM clones
		ORDER BY project ASC, path ASC
	`, func(rows *sql.Rows) ([]any, error) {
			var project string
			var path string
			var cloneStatus string
			var issue string
			var branch string
			var updatedAtText string

			if err := rows.Scan(&project, &path, &cloneStatus, &issue, &branch, &updatedAtText); err != nil {
				return nil, fmt.Errorf("scan sqlite clone: %w", err)
			}

			updatedAt, err := parseRequiredMigrationTime("clones", "updated_at", updatedAtText)
			if err != nil {
				return nil, err
			}

			return []any{project, path, cloneStatus, issue, branch, updatedAt}, nil
		})
}

func copyEvents(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	return copySQLiteRows(ctx, source, destination, "events", "events",
		[]string{"id", "project", "kind", "issue", "worker_id", "message", "payload", "created_at"},
		`
		SELECT id, project, kind, issue, worker_id, message, payload, created_at
		FROM events
		ORDER BY id ASC
	`, func(rows *sql.Rows) ([]any, error) {
			var id int64
			var project string
			var kind string
			var issue string
			var workerID string
			var message string
			var payload sql.NullString
			var createdAtText string

			if err := rows.Scan(&id, &project, &kind, &issue, &workerID, &message, &payload, &createdAtText); err != nil {
				return nil, fmt.Errorf("scan sqlite event: %w", err)
			}

			createdAt, err := parseRequiredMigrationTime("events", "created_at", createdAtText)
			if err != nil {
				return nil, err
			}

			var payloadArg any
			if payload.Valid {
				payloadArg = payload.String
			}

			return []any{id, project, kind, issue, workerID, message, payloadArg, createdAt}, nil
		})
}

func copyMergeQueue(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	return copySQLiteRows(ctx, source, destination, "merge_queue", "merge queue",
		[]string{"project", "pr_number", "issue", "status", "created_at", "updated_at"},
		`
		SELECT project, pr_number, issue, status, created_at, updated_at
		FROM merge_queue
		ORDER BY project ASC, pr_number ASC
	`, func(rows *sql.Rows) ([]any, error) {
			var project string
			var prNumber int64
			var issue string
			var entryStatus string
			var createdAtText string
			var updatedAtText string

			if err := rows.Scan(&project, &prNumber, &issue, &entryStatus, &createdAtText, &updatedAtText); err != nil {
				return nil, fmt.Errorf("scan sqlite merge queue entry: %w", err)
			}

			createdAt, err := parseRequiredMigrationTime("merge_queue", "created_at", createdAtText)
			if err != nil {
				return nil, err
			}
			updatedAt, err := parseRequiredMigrationTime("merge_queue", "updated_at", updatedAtText)
			if err != nil {
				return nil, err
			}

			return []any{project, prNumber, issue, entryStatus, createdAt, updatedAt}, nil
		})
}

func copyDaemonStatus(ctx context.Context, source *SQLiteStore, destination pgx.Tx) error {
	return copySQLiteRows(ctx, source, destination, "daemon_status", "daemon status",
		[]string{"project", "session", "pid", "status", "started_at", "updated_at"},
		`
		SELECT project, session, pid, status, started_at, updated_at
		FROM daemon_status
		ORDER BY project ASC
	`, func(rows *sql.Rows) ([]any, error) {
			var project string
			var session string
			var pid int64
			var daemonState string
			var startedAtText string
			var updatedAtText string

			if err := rows.Scan(&project, &session, &pid, &daemonState, &startedAtText, &updatedAtText); err != nil {
				return nil, fmt.Errorf("scan sqlite daemon status: %w", err)
			}

			startedAt, err := parseRequiredMigrationTime("daemon_status", "started_at", startedAtText)
			if err != nil {
				return nil, err
			}
			updatedAt, err := parseRequiredMigrationTime("daemon_status", "updated_at", updatedAtText)
			if err != nil {
				return nil, err
			}

			return []any{project, session, pid, daemonState, startedAt, updatedAt}, nil
		})
}
