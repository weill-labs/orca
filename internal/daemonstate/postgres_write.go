package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	legacy "github.com/weill-labs/orca/internal/state"
)

func (s *PostgresStore) UpsertDaemon(ctx context.Context, project string, daemon DaemonStatus) error {
	if daemon.StartedAt.IsZero() {
		daemon.StartedAt = s.now()
	}
	if daemon.UpdatedAt.IsZero() {
		daemon.UpdatedAt = daemon.StartedAt
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO daemon_status(project, session, pid, status, started_at, updated_at)
		VALUES($1, $2, $3, $4, $5, $6)
		ON CONFLICT(project) DO UPDATE SET
			session = excluded.session,
			pid = excluded.pid,
			status = excluded.status,
			started_at = excluded.started_at,
			updated_at = excluded.updated_at
	`, project, daemon.Session, daemon.PID, daemon.Status, normalizeTime(daemon.StartedAt), normalizeTime(daemon.UpdatedAt))
	if err != nil {
		return fmt.Errorf("upsert daemon: %w", err)
	}
	return nil
}

func (s *PostgresStore) MarkDaemonStopped(ctx context.Context, project string, updatedAt time.Time) error {
	if updatedAt.IsZero() {
		updatedAt = s.now()
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO daemon_status(project, session, pid, status, started_at, updated_at)
		VALUES($1, '', 0, 'stopped', $2, $3)
		ON CONFLICT(project) DO UPDATE SET
			pid = 0,
			status = 'stopped',
			updated_at = excluded.updated_at
	`, project, normalizeTime(updatedAt), normalizeTime(updatedAt))
	if err != nil {
		return fmt.Errorf("mark daemon stopped: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpsertTask(ctx context.Context, project string, task Task) error {
	if task.CreatedAt.IsZero() {
		task.CreatedAt = s.now()
	}
	if task.UpdatedAt.IsZero() {
		task.UpdatedAt = task.CreatedAt
	}
	if task.State == "" {
		task.State = defaultPersistedTaskState(task.Status, task.PRNumber)
	}

	var prNumber any
	if task.PRNumber != nil {
		prNumber = *task.PRNumber
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO tasks(project, issue, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT(project, issue) DO UPDATE SET
			status = excluded.status,
			state = excluded.state,
			agent = excluded.agent,
			prompt = excluded.prompt,
			caller_pane = excluded.caller_pane,
			worker_id = excluded.worker_id,
			clone_path = excluded.clone_path,
			branch = excluded.branch,
			pr_number = excluded.pr_number,
			updated_at = excluded.updated_at
	`, project, task.Issue, task.Status, task.State, task.Agent, task.Prompt, task.CallerPane, task.WorkerID, task.ClonePath, task.Branch, prNumber, normalizeTime(task.CreatedAt), normalizeTime(task.UpdatedAt))
	if err != nil {
		return fmt.Errorf("upsert task: %w", err)
	}
	return nil
}

func (s *PostgresStore) UpsertWorker(ctx context.Context, project string, worker Worker) error {
	now := s.now()
	if worker.CreatedAt.IsZero() {
		worker.CreatedAt = now
	}
	if worker.LastSeenAt.IsZero() {
		worker.LastSeenAt = now
	}

	_, err := s.pool.Exec(ctx, `
		INSERT INTO workers(project, worker_id, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at)
		VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29)
		ON CONFLICT(project, worker_id) DO UPDATE SET
			agent_profile = excluded.agent_profile,
			current_pane_id = excluded.current_pane_id,
			state = excluded.state,
			issue = excluded.issue,
			clone_path = excluded.clone_path,
			last_review_count = excluded.last_review_count,
			last_inline_review_comment_count = excluded.last_inline_review_comment_count,
			last_issue_comment_count = excluded.last_issue_comment_count,
			last_issue_comment_watermark = excluded.last_issue_comment_watermark,
			last_review_updated_at = excluded.last_review_updated_at,
			review_nudge_count = excluded.review_nudge_count,
			review_approved = excluded.review_approved,
			last_ci_state = excluded.last_ci_state,
			ci_nudge_count = excluded.ci_nudge_count,
			ci_failure_poll_count = excluded.ci_failure_poll_count,
			ci_escalated = excluded.ci_escalated,
			last_mergeable_state = excluded.last_mergeable_state,
			nudge_count = excluded.nudge_count,
			last_capture = excluded.last_capture,
			last_activity_at = excluded.last_activity_at,
			last_pr_number = excluded.last_pr_number,
			last_push_at = excluded.last_push_at,
			last_pr_poll_at = excluded.last_pr_poll_at,
			restart_count = excluded.restart_count,
			first_crash_at = excluded.first_crash_at,
			last_seen_at = excluded.last_seen_at
	`, project, worker.WorkerID, worker.Agent, worker.CurrentPaneID, worker.State, worker.Issue, worker.ClonePath, worker.LastReviewCount, worker.LastInlineReviewCommentCount, worker.LastIssueCommentCount, worker.LastIssueCommentWatermark, optionalTimeArg(worker.LastReviewUpdatedAt), worker.ReviewNudgeCount, worker.ReviewApproved, worker.LastCIState, worker.CINudgeCount, worker.CIFailurePollCount, worker.CIEscalated, worker.LastMergeableState, worker.NudgeCount, worker.LastCapture, optionalTimeArg(worker.LastActivityAt), worker.LastPRNumber, optionalTimeArg(worker.LastPushAt), optionalTimeArg(worker.LastPRPollAt), worker.RestartCount, optionalTimeArg(worker.FirstCrashAt), normalizeTime(worker.CreatedAt), normalizeTime(worker.LastSeenAt))
	if err != nil {
		return fmt.Errorf("upsert worker: %w", err)
	}
	return nil
}

func (s *PostgresStore) ClaimWorker(ctx context.Context, project string, worker Worker) (Worker, error) {
	now := s.now()
	if worker.CreatedAt.IsZero() {
		worker.CreatedAt = now
	}
	if worker.LastSeenAt.IsZero() {
		worker.LastSeenAt = now
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Worker{}, fmt.Errorf("begin claim worker tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(8742, hashtext($1))`, project); err != nil {
		return Worker{}, fmt.Errorf("lock worker project: %w", err)
	}

	row := tx.QueryRow(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE project = $1 AND agent_profile = $2 AND issue = ''
		ORDER BY created_at ASC, worker_id ASC
		LIMIT 1
		FOR UPDATE
	`, project, worker.Agent)

	claimed, scanErr := scanPostgresWorker(row, false)
	switch {
	case scanErr == nil:
		claimed.Issue = worker.Issue
		claimed.State = worker.State
		claimed.LastSeenAt = worker.LastSeenAt
		if _, err := tx.Exec(ctx, `
			UPDATE workers
			SET issue = $1, state = $2, last_seen_at = $3
			WHERE project = $4 AND worker_id = $5
		`, claimed.Issue, claimed.State, normalizeTime(claimed.LastSeenAt), project, claimed.WorkerID); err != nil {
			return Worker{}, fmt.Errorf("claim existing worker: %w", err)
		}
	case errors.Is(scanErr, pgx.ErrNoRows):
		worker.WorkerID, err = nextPostgresWorkerIDTx(ctx, tx, project)
		if err != nil {
			return Worker{}, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO workers(project, worker_id, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at)
			VALUES($1, $2, $3, '', $4, $5, '', 0, 0, 0, '', NULL, 0, FALSE, '', 0, 0, FALSE, '', 0, '', NULL, 0, NULL, NULL, 0, NULL, $6, $7)
		`, project, worker.WorkerID, worker.Agent, worker.State, worker.Issue, normalizeTime(worker.CreatedAt), normalizeTime(worker.LastSeenAt)); err != nil {
			return Worker{}, fmt.Errorf("insert claimed worker: %w", err)
		}
		claimed = worker
	case scanErr != nil:
		return Worker{}, fmt.Errorf("load claimable worker: %w", scanErr)
	}

	if err := tx.Commit(ctx); err != nil {
		return Worker{}, fmt.Errorf("commit claim worker tx: %w", err)
	}
	return claimed, nil
}

func (s *PostgresStore) DeleteWorker(ctx context.Context, project, workerID string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM workers
		WHERE project = $1 AND worker_id = $2
	`, project, workerID)
	if err != nil {
		return fmt.Errorf("delete worker: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) DeleteTask(ctx context.Context, project, issue string) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM tasks
		WHERE project = $1 AND issue = $2
	`, project, issue)
	if err != nil {
		return fmt.Errorf("delete task: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) ClaimTask(ctx context.Context, project string, task Task) (*Task, error) {
	if task.CreatedAt.IsZero() {
		task.CreatedAt = s.now()
	}
	if task.UpdatedAt.IsZero() {
		task.UpdatedAt = task.CreatedAt
	}
	if task.State == "" {
		task.State = defaultPersistedTaskState(task.Status, task.PRNumber)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim task tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	tag, err := tx.Exec(ctx, `
		INSERT INTO tasks(project, issue, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES($1, $2, $3, $4, $5, $6, $7, '', '', $8, NULL, $9, $10)
		ON CONFLICT(project, issue) DO NOTHING
	`, project, task.Issue, task.Status, task.State, task.Agent, task.Prompt, task.CallerPane, task.Branch, normalizeTime(task.CreatedAt), normalizeTime(task.UpdatedAt))
	if err != nil {
		return nil, fmt.Errorf("insert claimed task: %w", err)
	}
	if tag.RowsAffected() == 1 {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit claimed task insert: %w", err)
		}
		return nil, nil
	}

	row := tx.QueryRow(ctx, `
		SELECT t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = $1 AND t.issue = $2
		FOR UPDATE OF t
	`, project, task.Issue)

	existing, err := scanPostgresTask(row, false)
	if err != nil {
		return nil, fmt.Errorf("load claimed task: %w", err)
	}
	if taskStatusBlocksClaim(existing.Status) {
		return nil, fmt.Errorf("task %s already assigned", task.Issue)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE tasks
		SET status = $1, state = $2, agent = $3, prompt = $4, caller_pane = $5, worker_id = '', clone_path = '', branch = $6, pr_number = NULL, updated_at = $7
		WHERE project = $8 AND issue = $9
	`, task.Status, task.State, task.Agent, task.Prompt, task.CallerPane, task.Branch, normalizeTime(task.UpdatedAt), project, task.Issue); err != nil {
		return nil, fmt.Errorf("update claimed task: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claimed task update: %w", err)
	}
	return &existing, nil
}

func (s *PostgresStore) EnqueueMergeEntry(ctx context.Context, entry MergeQueueEntry) (int, error) {
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = s.now()
	}
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = entry.CreatedAt
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin enqueue merge tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if _, err := tx.Exec(ctx, `
		INSERT INTO merge_queue(project, pr_number, issue, status, created_at, updated_at)
		VALUES($1, $2, $3, $4, $5, $6)
	`, entry.Project, entry.PRNumber, entry.Issue, entry.Status, normalizeTime(entry.CreatedAt), normalizeTime(entry.UpdatedAt)); err != nil {
		return 0, fmt.Errorf("enqueue merge entry: %w", err)
	}

	var position int
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM merge_queue
		WHERE project = $1
	`, entry.Project).Scan(&position); err != nil {
		return 0, fmt.Errorf("count merge queue entries: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit enqueue merge tx: %w", err)
	}
	return position, nil
}

func (s *PostgresStore) UpdateMergeEntry(ctx context.Context, entry MergeQueueEntry) error {
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = s.now()
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE merge_queue
		SET issue = $1, status = $2, updated_at = $3
		WHERE project = $4 AND pr_number = $5
	`, entry.Issue, entry.Status, normalizeTime(entry.UpdatedAt), entry.Project, entry.PRNumber)
	if err != nil {
		return fmt.Errorf("update merge entry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) DeleteMergeEntry(ctx context.Context, project string, prNumber int) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM merge_queue
		WHERE project = $1 AND pr_number = $2
	`, project, prNumber)
	if err != nil {
		return fmt.Errorf("delete merge entry: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) EnsureClone(ctx context.Context, project, path string) (legacy.CloneRecord, error) {
	now := s.now()
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO clones(project, path, status, issue, branch, updated_at)
		VALUES($1, $2, 'free', '', '', $3)
		ON CONFLICT(project, path) DO NOTHING
	`, project, path, normalizeTime(now)); err != nil {
		return legacy.CloneRecord{}, fmt.Errorf("ensure clone: %w", err)
	}

	record, err := s.lookupCloneRecord(ctx, project, path)
	if err != nil {
		return legacy.CloneRecord{}, fmt.Errorf("load clone: %w", err)
	}
	return record, nil
}

func (s *PostgresStore) TryOccupyClone(ctx context.Context, project, path, branch, task string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE clones
		SET status = 'occupied', issue = $1, branch = $2, updated_at = $3
		WHERE project = $4 AND path = $5 AND status = 'free'
	`, task, branch, normalizeTime(s.now()), project, path)
	if err != nil {
		return false, fmt.Errorf("occupy clone: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PostgresStore) MarkCloneFree(ctx context.Context, project, path string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE clones
		SET status = 'free', issue = '', branch = '', updated_at = $1
		WHERE project = $2 AND path = $3
	`, normalizeTime(s.now()), project, path)
	if err != nil {
		return fmt.Errorf("mark clone free: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return legacy.ErrCloneNotFound
	}
	return nil
}

func (s *PostgresStore) UpdateTaskStatus(ctx context.Context, project, issue, status string, updatedAt time.Time) (Task, error) {
	if updatedAt.IsZero() {
		updatedAt = s.now()
	}

	tag, err := s.pool.Exec(ctx, `
		UPDATE tasks
		SET status = $1, state = CASE WHEN $1 IN ('done', 'cancelled', 'failed') THEN 'done' ELSE state END, updated_at = $2
		WHERE project = $3 AND issue = $4
	`, status, normalizeTime(updatedAt), project, issue)
	if err != nil {
		return Task{}, fmt.Errorf("update task status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return Task{}, ErrNotFound
	}
	return s.lookupTask(ctx, project, issue)
}

func (s *PostgresStore) AppendEvent(ctx context.Context, event Event) (Event, error) {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now()
	}

	var payload any
	if len(event.Payload) > 0 {
		payload = string(event.Payload)
	}

	if err := s.pool.QueryRow(ctx, `
		INSERT INTO events(project, kind, issue, worker_id, message, payload, created_at)
		VALUES($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`, event.Project, event.Kind, event.Issue, event.WorkerID, event.Message, payload, normalizeTime(event.CreatedAt)).Scan(&event.ID); err != nil {
		return Event{}, fmt.Errorf("append event: %w", err)
	}
	event.CreatedAt = normalizeTime(event.CreatedAt)
	return event, nil
}

func nextPostgresWorkerIDTx(ctx context.Context, tx pgx.Tx, project string) (string, error) {
	rows, err := tx.Query(ctx, `
		SELECT worker_id
		FROM workers
		WHERE project = $1 AND worker_id LIKE 'worker-%'
	`, project)
	if err != nil {
		return "", fmt.Errorf("query next worker id: %w", err)
	}
	defer rows.Close()

	maxSequence := 0
	for rows.Next() {
		var workerID string
		if err := rows.Scan(&workerID); err != nil {
			return "", fmt.Errorf("scan worker id: %w", err)
		}
		var sequence int
		if _, err := fmt.Sscanf(workerID, "worker-%d", &sequence); err == nil && sequence > maxSequence {
			maxSequence = sequence
		}
	}
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("iterate worker ids: %w", err)
	}

	return workerIDForSequence(maxSequence + 1), nil
}
