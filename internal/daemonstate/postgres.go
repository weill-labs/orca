package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	legacy "github.com/weill-labs/orca/internal/state"
)

const postgresSchemaSQL = `
CREATE TABLE IF NOT EXISTS daemon_status (
	project TEXT PRIMARY KEY,
	session TEXT NOT NULL,
	pid INTEGER NOT NULL DEFAULT 0,
	status TEXT NOT NULL,
	started_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
	project TEXT NOT NULL,
	issue TEXT NOT NULL,
	status TEXT NOT NULL,
	state TEXT NOT NULL DEFAULT '',
	agent TEXT NOT NULL,
	prompt TEXT NOT NULL DEFAULT '',
	caller_pane TEXT NOT NULL DEFAULT '',
	worker_id TEXT NOT NULL DEFAULT '',
	clone_path TEXT NOT NULL DEFAULT '',
	pr_number INTEGER,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	PRIMARY KEY (project, issue)
);

CREATE TABLE IF NOT EXISTS workers (
	project TEXT NOT NULL,
	worker_id TEXT NOT NULL,
	agent_profile TEXT NOT NULL,
	current_pane_id TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL,
	issue TEXT NOT NULL DEFAULT '',
	clone_path TEXT NOT NULL DEFAULT '',
	last_review_count INTEGER NOT NULL DEFAULT 0,
	last_inline_review_comment_count INTEGER NOT NULL DEFAULT 0,
	last_issue_comment_count INTEGER NOT NULL DEFAULT 0,
	review_nudge_count INTEGER NOT NULL DEFAULT 0,
	last_ci_state TEXT NOT NULL DEFAULT '',
	ci_nudge_count INTEGER NOT NULL DEFAULT 0,
	ci_failure_poll_count INTEGER NOT NULL DEFAULT 0,
	ci_escalated INTEGER NOT NULL DEFAULT 0,
	last_mergeable_state TEXT NOT NULL DEFAULT '',
	nudge_count INTEGER NOT NULL DEFAULT 0,
	last_capture TEXT NOT NULL DEFAULT '',
	last_activity_at TEXT NOT NULL DEFAULT '',
	last_pr_number INTEGER NOT NULL DEFAULT 0,
	last_push_at TEXT NOT NULL DEFAULT '',
	last_pr_poll_at TEXT NOT NULL DEFAULT '',
	restart_count INTEGER NOT NULL DEFAULT 0,
	first_crash_at TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	last_seen_at TEXT NOT NULL,
	PRIMARY KEY (project, worker_id)
);

CREATE TABLE IF NOT EXISTS merge_queue (
	project TEXT NOT NULL,
	pr_number INTEGER NOT NULL,
	issue TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	PRIMARY KEY (project, pr_number)
);

CREATE TABLE IF NOT EXISTS clones (
	project TEXT NOT NULL,
	path TEXT NOT NULL,
	status TEXT NOT NULL,
	issue TEXT NOT NULL DEFAULT '',
	branch TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL,
	PRIMARY KEY (project, path)
);

CREATE TABLE IF NOT EXISTS events (
	id BIGSERIAL PRIMARY KEY,
	project TEXT NOT NULL,
	kind TEXT NOT NULL,
	issue TEXT NOT NULL DEFAULT '',
	worker_id TEXT NOT NULL DEFAULT '',
	message TEXT NOT NULL,
	payload TEXT,
	created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_project_updated ON tasks(project, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_workers_project_last_seen ON workers(project, last_seen_at DESC, worker_id ASC);
CREATE INDEX IF NOT EXISTS idx_clones_project_updated ON clones(project, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_project_id ON events(project, id);
CREATE INDEX IF NOT EXISTS idx_events_project_issue_id ON events(project, issue, id);
CREATE INDEX IF NOT EXISTS idx_events_project_worker_id ON events(project, worker_id, id);
CREATE INDEX IF NOT EXISTS idx_merge_queue_project_created ON merge_queue(project, created_at ASC, pr_number ASC);
`

type PostgresStore struct {
	db  *sql.DB
	now func() time.Time
}

var _ Backend = (*PostgresStore)(nil)

func OpenPostgres(dsn string) (*PostgresStore, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres database: %w", err)
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping postgres database: %w", err)
	}

	store := &PostgresStore{
		db:  db,
		now: func() time.Time { return time.Now().UTC() },
	}
	if err := store.EnsureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *PostgresStore) Close() error {
	return s.db.Close()
}

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	if err := execPostgresStatements(ctx, s.db, postgresSchemaSQL); err != nil {
		return fmt.Errorf("ensure postgres schema: %w", err)
	}
	return nil
}

func (s *PostgresStore) ProjectStatus(ctx context.Context, project string) (ProjectStatus, error) {
	status := ProjectStatus{
		Project: project,
		Tasks:   []Task{},
	}

	daemonStatus, err := s.lookupDaemon(ctx, project)
	if err != nil {
		return ProjectStatus{}, err
	}
	if daemonStatus == nil && !isGlobalProject(project) {
		daemonStatus, err = s.lookupDaemon(ctx, "")
		if err != nil {
			return ProjectStatus{}, err
		}
	}
	status.Daemon = daemonStatus

	tasks, err := s.listTasks(ctx, project)
	if err != nil {
		return ProjectStatus{}, err
	}
	status.Tasks = tasks

	workers, err := s.ListWorkers(ctx, project)
	if err != nil {
		return ProjectStatus{}, err
	}

	clones, err := s.ListClones(ctx, project)
	if err != nil {
		return ProjectStatus{}, err
	}

	for _, task := range tasks {
		status.Summary.Tasks++
		switch task.Status {
		case "queued":
			status.Summary.Queued++
		case "active":
			status.Summary.Active++
		case "done":
			status.Summary.Done++
		case "cancelled":
			status.Summary.Cancelled++
		}
	}

	for _, worker := range workers {
		status.Summary.Workers++
		switch worker.State {
		case "healthy":
			status.Summary.HealthyWorkers++
		case "stuck":
			status.Summary.StuckWorkers++
		}
	}

	for _, clone := range clones {
		status.Summary.Clones++
		if clone.Status == "free" {
			status.Summary.FreeClones++
		}
	}

	return status, nil
}

func (s *PostgresStore) TaskStatus(ctx context.Context, project, issue string) (TaskStatus, error) {
	task, err := s.lookupTask(ctx, project, issue)
	if err != nil {
		return TaskStatus{}, err
	}
	events, err := s.queryEvents(ctx, task.Project, task.Issue, 0)
	if err != nil {
		return TaskStatus{}, err
	}
	return TaskStatus{Task: task, Events: events}, nil
}

func (s *PostgresStore) ListWorkers(ctx context.Context, project string) ([]Worker, error) {
	if isGlobalProject(project) {
		return s.allWorkers(ctx)
	}

	rows, err := s.queryContext(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE project = ?
		ORDER BY last_seen_at DESC, worker_id ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		worker, err := scanWorker(rows, false)
		if err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		worker.Project = project
		workers = append(workers, worker)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workers: %w", err)
	}

	return workers, nil
}

func (s *PostgresStore) WorkerByID(ctx context.Context, project, workerID string) (Worker, error) {
	row := s.queryRowContext(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE project = ? AND worker_id = ?
	`, project, workerID)

	worker, err := scanWorker(row, false)
	if errors.Is(err, sql.ErrNoRows) {
		return Worker{}, ErrNotFound
	}
	if err != nil {
		return Worker{}, fmt.Errorf("lookup worker by id: %w", err)
	}
	worker.Project = project
	return worker, nil
}

func (s *PostgresStore) WorkerByPane(ctx context.Context, project, paneID string) (Worker, error) {
	query := `
		SELECT project, worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE current_pane_id = ?
	`
	args := []any{paneID}
	if !isGlobalProject(project) {
		query += ` AND project = ?`
		args = append(args, project)
	}

	row := s.queryRowContext(ctx, query, args...)
	worker, err := scanWorker(row, true)
	if errors.Is(err, sql.ErrNoRows) {
		return Worker{}, ErrNotFound
	}
	if err != nil {
		return Worker{}, fmt.Errorf("lookup worker by pane: %w", err)
	}
	return worker, nil
}

func (s *PostgresStore) ListClones(ctx context.Context, project string) ([]Clone, error) {
	if isGlobalProject(project) {
		return s.allClones(ctx)
	}

	rows, err := s.queryContext(ctx, `
		SELECT path, status, issue, branch, updated_at
		FROM clones
		WHERE project = ?
		ORDER BY updated_at DESC, path ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list clones: %w", err)
	}
	defer rows.Close()

	clones := make([]Clone, 0)
	for rows.Next() {
		var clone Clone
		var updatedAt string
		if err := rows.Scan(&clone.Path, &clone.Status, &clone.Issue, &clone.Branch, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan clone: %w", err)
		}
		clone.UpdatedAt = parseTime(updatedAt)
		clones = append(clones, clone)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clones: %w", err)
	}

	return clones, nil
}

func (s *PostgresStore) NonTerminalTasks(ctx context.Context, project string) ([]Task, error) {
	if isGlobalProject(project) {
		return s.AllNonTerminalTasks(ctx)
	}

	rows, err := s.queryContext(ctx, `
		SELECT t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = ? AND t.status IN ('starting', 'active')
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list non-terminal tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows, false)
		if err != nil {
			return nil, fmt.Errorf("scan non-terminal task: %w", err)
		}
		task.Project = project
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate non-terminal tasks: %w", err)
	}
	return tasks, nil
}

func (s *PostgresStore) StaleCloneOccupancies(ctx context.Context, project string) ([]CloneOccupancy, error) {
	query := `
		SELECT c.project, c.path, c.branch, c.issue, c.updated_at
		FROM clones c
		LEFT JOIN tasks t
			ON t.project = c.project
			AND t.issue = c.issue
			AND t.status IN ('starting', 'active')
		WHERE c.status = 'occupied'
			AND (c.issue = '' OR t.issue IS NULL)
	`
	args := make([]any, 0, 1)
	if !isGlobalProject(project) {
		query += ` AND c.project = ?`
		args = append(args, project)
	}
	query += ` ORDER BY c.updated_at DESC, c.project ASC, c.path ASC`

	rows, err := s.queryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list stale clone occupancies: %w", err)
	}
	defer rows.Close()

	occupancies := make([]CloneOccupancy, 0)
	for rows.Next() {
		var occupancy CloneOccupancy
		var updatedAt string
		if err := rows.Scan(&occupancy.Project, &occupancy.Path, &occupancy.CurrentBranch, &occupancy.AssignedTask, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan stale clone occupancy: %w", err)
		}
		occupancy.UpdatedAt = parseTime(updatedAt)
		if occupancy.Project == "" {
			occupancy.Project = project
		}
		occupancies = append(occupancies, occupancy)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate stale clone occupancies: %w", err)
	}
	return occupancies, nil
}

func (s *PostgresStore) Events(ctx context.Context, project string, afterID int64) (<-chan Event, <-chan error) {
	eventsCh := make(chan Event)
	errCh := make(chan error, 1)

	go func() {
		defer close(eventsCh)
		defer close(errCh)

		lastID := afterID
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for {
			events, err := s.queryEvents(ctx, project, "", lastID)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}

			for _, event := range events {
				lastID = event.ID
				select {
				case eventsCh <- event:
				case <-ctx.Done():
					return
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return eventsCh, errCh
}

func (s *PostgresStore) UpsertDaemon(ctx context.Context, project string, daemon DaemonStatus) error {
	if daemon.StartedAt.IsZero() {
		daemon.StartedAt = s.now()
	}
	if daemon.UpdatedAt.IsZero() {
		daemon.UpdatedAt = daemon.StartedAt
	}

	_, err := s.execContext(ctx, `
		INSERT INTO daemon_status(project, session, pid, status, started_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?)
		ON CONFLICT(project) DO UPDATE SET
			session = excluded.session,
			pid = excluded.pid,
			status = excluded.status,
			started_at = excluded.started_at,
			updated_at = excluded.updated_at
	`, project, daemon.Session, daemon.PID, daemon.Status, formatTime(daemon.StartedAt), formatTime(daemon.UpdatedAt))
	if err != nil {
		return fmt.Errorf("upsert daemon: %w", err)
	}
	return nil
}

func (s *PostgresStore) MarkDaemonStopped(ctx context.Context, project string, updatedAt time.Time) error {
	if updatedAt.IsZero() {
		updatedAt = s.now()
	}

	_, err := s.execContext(ctx, `
		INSERT INTO daemon_status(project, session, pid, status, started_at, updated_at)
		VALUES(?, '', 0, 'stopped', ?, ?)
		ON CONFLICT(project) DO UPDATE SET
			pid = 0,
			status = 'stopped',
			updated_at = excluded.updated_at
	`, project, formatTime(updatedAt), formatTime(updatedAt))
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

	var prNumber any
	if task.PRNumber != nil {
		prNumber = *task.PRNumber
	}

	_, err := s.execContext(ctx, `
		INSERT INTO tasks(project, issue, status, agent, prompt, caller_pane, worker_id, clone_path, pr_number, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, issue) DO UPDATE SET
			status = excluded.status,
			agent = excluded.agent,
			prompt = excluded.prompt,
			caller_pane = excluded.caller_pane,
			worker_id = excluded.worker_id,
			clone_path = excluded.clone_path,
			pr_number = excluded.pr_number,
			updated_at = excluded.updated_at
	`, project, task.Issue, task.Status, task.Agent, task.Prompt, task.CallerPane, task.WorkerID, task.ClonePath, prNumber, formatTime(task.CreatedAt), formatTime(task.UpdatedAt))
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

	_, err := s.execContext(ctx, `
		INSERT INTO workers(project, worker_id, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, worker_id) DO UPDATE SET
			agent_profile = excluded.agent_profile,
			current_pane_id = excluded.current_pane_id,
			state = excluded.state,
			issue = excluded.issue,
			clone_path = excluded.clone_path,
			last_review_count = excluded.last_review_count,
			last_inline_review_comment_count = excluded.last_inline_review_comment_count,
			last_issue_comment_count = excluded.last_issue_comment_count,
			review_nudge_count = excluded.review_nudge_count,
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
	`, project, worker.WorkerID, worker.Agent, worker.CurrentPaneID, worker.State, worker.Issue, worker.ClonePath, worker.LastReviewCount, worker.LastInlineReviewCommentCount, worker.LastIssueCommentCount, worker.ReviewNudgeCount, worker.LastCIState, worker.CINudgeCount, worker.CIFailurePollCount, boolToInt(worker.CIEscalated), worker.LastMergeableState, worker.NudgeCount, worker.LastCapture, formatTime(worker.LastActivityAt), worker.LastPRNumber, formatTime(worker.LastPushAt), formatTime(worker.LastPRPollAt), worker.RestartCount, formatTime(worker.FirstCrashAt), formatTime(worker.CreatedAt), formatTime(worker.LastSeenAt))
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Worker{}, fmt.Errorf("begin claim worker tx: %w", err)
	}
	ptx := postgresTx{Tx: tx}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := ptx.advisoryLock(ctx, "workers", project); err != nil {
		return Worker{}, err
	}

	row := ptx.queryRowContext(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE project = ? AND agent_profile = ? AND issue = ''
		ORDER BY created_at ASC, worker_id ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, project, worker.Agent)

	claimed, scanErr := scanWorker(row, false)
	switch {
	case scanErr == nil:
		claimed.Issue = worker.Issue
		claimed.State = worker.State
		claimed.LastSeenAt = worker.LastSeenAt
		if _, err := ptx.execContext(ctx, `
			UPDATE workers
			SET issue = ?, state = ?, last_seen_at = ?
			WHERE project = ? AND worker_id = ?
		`, claimed.Issue, claimed.State, formatTime(claimed.LastSeenAt), project, claimed.WorkerID); err != nil {
			return Worker{}, fmt.Errorf("claim existing worker: %w", err)
		}
	case errors.Is(scanErr, sql.ErrNoRows):
		worker.WorkerID, err = nextPostgresWorkerIDTx(ctx, ptx, project)
		if err != nil {
			return Worker{}, err
		}
		if _, err := ptx.execContext(ctx, `
			INSERT INTO workers(project, worker_id, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at)
			VALUES(?, ?, ?, '', ?, ?, '', 0, 0, 0, 0, '', 0, 0, 0, '', 0, '', '', 0, '', '', 0, '', ?, ?)
		`, project, worker.WorkerID, worker.Agent, worker.State, worker.Issue, formatTime(worker.CreatedAt), formatTime(worker.LastSeenAt)); err != nil {
			return Worker{}, fmt.Errorf("insert claimed worker: %w", err)
		}
		claimed = worker
	case scanErr != nil:
		return Worker{}, fmt.Errorf("load claimable worker: %w", scanErr)
	}

	if err := tx.Commit(); err != nil {
		return Worker{}, fmt.Errorf("commit claim worker tx: %w", err)
	}
	return claimed, nil
}

func (s *PostgresStore) DeleteWorker(ctx context.Context, project, workerID string) error {
	result, err := s.execContext(ctx, `
		DELETE FROM workers
		WHERE project = ? AND worker_id = ?
	`, project, workerID)
	if err != nil {
		return fmt.Errorf("delete worker: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete worker rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) DeleteTask(ctx context.Context, project, issue string) error {
	result, err := s.execContext(ctx, `
		DELETE FROM tasks
		WHERE project = ? AND issue = ?
	`, project, issue)
	if err != nil {
		return fmt.Errorf("delete task: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete task rows affected: %w", err)
	}
	if rowsAffected == 0 {
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin claim task tx: %w", err)
	}
	ptx := postgresTx{Tx: tx}
	defer func() {
		_ = tx.Rollback()
	}()

	if err := ptx.advisoryLock(ctx, "tasks", project+"\x00"+task.Issue); err != nil {
		return nil, err
	}

	row := ptx.queryRowContext(ctx, `
		SELECT t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = ? AND t.issue = ?
		FOR UPDATE OF t
	`, project, task.Issue)

	existing, err := scanTask(row, false)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if _, err := ptx.execContext(ctx, `
			INSERT INTO tasks(project, issue, status, agent, prompt, caller_pane, worker_id, clone_path, pr_number, created_at, updated_at)
			VALUES(?, ?, ?, ?, ?, ?, '', '', NULL, ?, ?)
		`, project, task.Issue, task.Status, task.Agent, task.Prompt, task.CallerPane, formatTime(task.CreatedAt), formatTime(task.UpdatedAt)); err != nil {
			return nil, fmt.Errorf("insert claimed task: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return nil, fmt.Errorf("commit claimed task insert: %w", err)
		}
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("load claimed task: %w", err)
	}

	if taskStatusBlocksClaim(existing.Status) {
		return nil, fmt.Errorf("task %s already assigned", task.Issue)
	}

	if _, err := ptx.execContext(ctx, `
		UPDATE tasks
		SET status = ?, agent = ?, prompt = ?, caller_pane = ?, worker_id = '', clone_path = '', pr_number = NULL, updated_at = ?
		WHERE project = ? AND issue = ?
	`, task.Status, task.Agent, task.Prompt, task.CallerPane, formatTime(task.UpdatedAt), project, task.Issue); err != nil {
		return nil, fmt.Errorf("update claimed task: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claimed task update: %w", err)
	}

	return &existing, nil
}

func (s *PostgresStore) ActiveAssignments(ctx context.Context, project string) ([]Assignment, error) {
	if isGlobalProject(project) {
		return s.AllActiveAssignments(ctx)
	}

	rows, err := s.queryContext(ctx, `
		SELECT
			t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = ? AND t.status = 'active'
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list active assignments: %w", err)
	}
	defer rows.Close()

	assignments := make([]Assignment, 0)
	for rows.Next() {
		assignment, err := scanAssignment(rows, false)
		if err != nil {
			return nil, fmt.Errorf("scan active assignment: %w", err)
		}
		assignment.Task.Project = project
		assignment.Worker.Project = project
		assignments = append(assignments, assignment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active assignments: %w", err)
	}
	return assignments, nil
}

func (s *PostgresStore) ActiveAssignmentByIssue(ctx context.Context, project, issue string) (Assignment, error) {
	query := `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.issue = ? AND t.status = 'active'
	`
	args := []any{issue}
	if !isGlobalProject(project) {
		query += ` AND t.project = ?`
		args = append(args, project)
	}

	row := s.queryRowContext(ctx, query, args...)
	assignment, err := scanAssignment(row, true)
	if errors.Is(err, sql.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lookup active assignment by issue: %w", err)
	}
	return assignment, nil
}

func (s *PostgresStore) ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (Assignment, error) {
	query := `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.pr_number = ? AND t.status = 'active'
	`
	args := []any{prNumber}
	if !isGlobalProject(project) {
		query += ` AND t.project = ?`
		args = append(args, project)
	}

	row := s.queryRowContext(ctx, query, args...)
	assignment, err := scanAssignment(row, true)
	if errors.Is(err, sql.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lookup active assignment by pr number: %w", err)
	}
	return assignment, nil
}

func (s *PostgresStore) EnqueueMergeEntry(ctx context.Context, entry MergeQueueEntry) (int, error) {
	if entry.CreatedAt.IsZero() {
		entry.CreatedAt = s.now()
	}
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = entry.CreatedAt
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin enqueue merge tx: %w", err)
	}
	ptx := postgresTx{Tx: tx}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := ptx.execContext(ctx, `
		INSERT INTO merge_queue(project, pr_number, issue, status, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?)
	`, entry.Project, entry.PRNumber, entry.Issue, entry.Status, formatTime(entry.CreatedAt), formatTime(entry.UpdatedAt)); err != nil {
		return 0, fmt.Errorf("enqueue merge entry: %w", err)
	}

	var position int
	if err := ptx.queryRowContext(ctx, `
		SELECT COUNT(*)
		FROM merge_queue
		WHERE project = ?
	`, entry.Project).Scan(&position); err != nil {
		return 0, fmt.Errorf("count merge queue entries: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit enqueue merge tx: %w", err)
	}
	return position, nil
}

func (s *PostgresStore) MergeEntry(ctx context.Context, project string, prNumber int) (*MergeQueueEntry, error) {
	query := `
		SELECT project, issue, pr_number, status, created_at, updated_at
		FROM merge_queue
		WHERE pr_number = ?
	`
	args := []any{prNumber}
	if !isGlobalProject(project) {
		query += ` AND project = ?`
		args = append(args, project)
	}

	row := s.queryRowContext(ctx, query, args...)
	entry, err := scanMergeQueueEntry(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup merge entry: %w", err)
	}
	return &entry, nil
}

func (s *PostgresStore) MergeEntries(ctx context.Context, project string) ([]MergeQueueEntry, error) {
	if isGlobalProject(project) {
		return s.AllMergeEntries(ctx)
	}

	rows, err := s.queryContext(ctx, `
		SELECT project, issue, pr_number, status, created_at, updated_at
		FROM merge_queue
		WHERE project = ?
		ORDER BY created_at ASC, pr_number ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list merge entries: %w", err)
	}
	defer rows.Close()

	entries := make([]MergeQueueEntry, 0)
	for rows.Next() {
		entry, err := scanMergeQueueEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("scan merge entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate merge entries: %w", err)
	}
	return entries, nil
}

func (s *PostgresStore) UpdateMergeEntry(ctx context.Context, entry MergeQueueEntry) error {
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = s.now()
	}

	result, err := s.execContext(ctx, `
		UPDATE merge_queue
		SET issue = ?, status = ?, updated_at = ?
		WHERE project = ? AND pr_number = ?
	`, entry.Issue, entry.Status, formatTime(entry.UpdatedAt), entry.Project, entry.PRNumber)
	if err != nil {
		return fmt.Errorf("update merge entry: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("update merge entry rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) DeleteMergeEntry(ctx context.Context, project string, prNumber int) error {
	result, err := s.execContext(ctx, `
		DELETE FROM merge_queue
		WHERE project = ? AND pr_number = ?
	`, project, prNumber)
	if err != nil {
		return fmt.Errorf("delete merge entry: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("delete merge entry rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *PostgresStore) EnsureClone(ctx context.Context, project, path string) (legacy.CloneRecord, error) {
	now := s.now()
	if _, err := s.execContext(ctx, `
		INSERT INTO clones(project, path, status, issue, branch, updated_at)
		VALUES(?, ?, 'free', '', '', ?)
		ON CONFLICT(project, path) DO NOTHING
	`, project, path, formatTime(now)); err != nil {
		return legacy.CloneRecord{}, fmt.Errorf("ensure clone: %w", err)
	}

	record, err := s.lookupCloneRecord(ctx, project, path)
	if err != nil {
		return legacy.CloneRecord{}, fmt.Errorf("load clone: %w", err)
	}
	return record, nil
}

func (s *PostgresStore) TryOccupyClone(ctx context.Context, project, path, branch, task string) (bool, error) {
	result, err := s.execContext(ctx, `
		UPDATE clones
		SET status = 'occupied', issue = ?, branch = ?, updated_at = ?
		WHERE project = ? AND path = ? AND status = 'free'
	`, task, branch, formatTime(s.now()), project, path)
	if err != nil {
		return false, fmt.Errorf("occupy clone: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("occupy clone rows affected: %w", err)
	}
	return rowsAffected == 1, nil
}

func (s *PostgresStore) MarkCloneFree(ctx context.Context, project, path string) error {
	result, err := s.execContext(ctx, `
		UPDATE clones
		SET status = 'free', issue = '', branch = '', updated_at = ?
		WHERE project = ? AND path = ?
	`, formatTime(s.now()), project, path)
	if err != nil {
		return fmt.Errorf("mark clone free: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("mark clone free rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return legacy.ErrCloneNotFound
	}
	return nil
}

func (s *PostgresStore) UpdateTaskStatus(ctx context.Context, project, issue, status string, updatedAt time.Time) (Task, error) {
	if updatedAt.IsZero() {
		updatedAt = s.now()
	}

	result, err := s.execContext(ctx, `
		UPDATE tasks
		SET status = ?, updated_at = ?
		WHERE project = ? AND issue = ?
	`, status, formatTime(updatedAt), project, issue)
	if err != nil {
		return Task{}, fmt.Errorf("update task status: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return Task{}, fmt.Errorf("update task status rows affected: %w", err)
	}
	if rowsAffected == 0 {
		return Task{}, ErrNotFound
	}

	return s.lookupTask(ctx, project, issue)
}

func (s *PostgresStore) TasksByPane(ctx context.Context, project, paneID string) ([]Task, error) {
	includeProject := isGlobalProject(project)
	query := `
		SELECT
	`
	if includeProject {
		query += `		t.project,`
	}
	query += `
			t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE w.current_pane_id = ?
	`
	args := []any{paneID}
	if !includeProject {
		query += ` AND t.project = ?`
		args = append(args, project)
	}
	query += ` ORDER BY t.created_at ASC, t.updated_at ASC, t.issue ASC`

	rows, err := s.queryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list tasks by pane: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows, includeProject)
		if err != nil {
			return nil, fmt.Errorf("scan task by pane: %w", err)
		}
		if !includeProject {
			task.Project = project
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tasks by pane: %w", err)
	}

	return tasks, nil
}

func (s *PostgresStore) AllNonTerminalTasks(ctx context.Context) ([]Task, error) {
	rows, err := s.queryContext(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE status IN ('starting', 'active')
		ORDER BY t.updated_at DESC, t.project ASC, t.issue ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all non-terminal tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows, true)
		if err != nil {
			return nil, fmt.Errorf("scan all non-terminal task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all non-terminal tasks: %w", err)
	}
	return tasks, nil
}

func (s *PostgresStore) allWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.queryContext(ctx, `
		SELECT project, worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		ORDER BY last_seen_at DESC, project ASC, worker_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all workers: %w", err)
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		worker, err := scanWorker(rows, true)
		if err != nil {
			return nil, fmt.Errorf("scan all worker: %w", err)
		}
		workers = append(workers, worker)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all workers: %w", err)
	}
	return workers, nil
}

func (s *PostgresStore) allClones(ctx context.Context) ([]Clone, error) {
	rows, err := s.queryContext(ctx, `
		SELECT path, status, issue, branch, updated_at
		FROM clones
		ORDER BY updated_at DESC, path ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all clones: %w", err)
	}
	defer rows.Close()

	clones := make([]Clone, 0)
	for rows.Next() {
		var clone Clone
		var updatedAt string
		if err := rows.Scan(&clone.Path, &clone.Status, &clone.Issue, &clone.Branch, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan all clone: %w", err)
		}
		clone.UpdatedAt = parseTime(updatedAt)
		clones = append(clones, clone)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all clones: %w", err)
	}
	return clones, nil
}

func (s *PostgresStore) AllActiveAssignments(ctx context.Context) ([]Assignment, error) {
	rows, err := s.queryContext(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.status = 'active'
		ORDER BY t.updated_at DESC, t.project ASC, t.issue ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all active assignments: %w", err)
	}
	defer rows.Close()

	assignments := make([]Assignment, 0)
	for rows.Next() {
		assignment, err := scanAssignment(rows, true)
		if err != nil {
			return nil, fmt.Errorf("scan all active assignment: %w", err)
		}
		assignments = append(assignments, assignment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all active assignments: %w", err)
	}
	return assignments, nil
}

func (s *PostgresStore) AllMergeEntries(ctx context.Context) ([]MergeQueueEntry, error) {
	rows, err := s.queryContext(ctx, `
		SELECT project, issue, pr_number, status, created_at, updated_at
		FROM merge_queue
		ORDER BY created_at ASC, project ASC, pr_number ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all merge entries: %w", err)
	}
	defer rows.Close()

	entries := make([]MergeQueueEntry, 0)
	for rows.Next() {
		entry, err := scanMergeQueueEntry(rows)
		if err != nil {
			return nil, fmt.Errorf("scan all merge entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all merge entries: %w", err)
	}
	return entries, nil
}

func (s *PostgresStore) AppendEvent(ctx context.Context, event Event) (Event, error) {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now()
	}

	var payload any
	if len(event.Payload) > 0 {
		payload = string(event.Payload)
	}

	if err := s.queryRowContext(ctx, `
		INSERT INTO events(project, kind, issue, worker_id, message, payload, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)
		RETURNING id
	`, event.Project, event.Kind, event.Issue, event.WorkerID, event.Message, payload, formatTime(event.CreatedAt)).Scan(&event.ID); err != nil {
		return Event{}, fmt.Errorf("append event: %w", err)
	}

	return event, nil
}

func (s *PostgresStore) lookupDaemon(ctx context.Context, project string) (*DaemonStatus, error) {
	row := s.queryRowContext(ctx, `
		SELECT session, pid, status, started_at, updated_at
		FROM daemon_status
		WHERE project = ?
	`, project)

	var daemon DaemonStatus
	var startedAt string
	var updatedAt string
	if err := row.Scan(&daemon.Session, &daemon.PID, &daemon.Status, &startedAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lookup daemon: %w", err)
	}

	daemon.StartedAt = parseTime(startedAt)
	daemon.UpdatedAt = parseTime(updatedAt)
	return &daemon, nil
}

func (s *PostgresStore) lookupTask(ctx context.Context, project, issue string) (Task, error) {
	includeProject := isGlobalProject(project)
	query := `
		SELECT
	`
	if includeProject {
		query += `		t.project,`
	}
	query += `
			t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.issue = ?
	`
	args := []any{issue}
	if !includeProject {
		query += ` AND t.project = ?`
		args = append(args, project)
	}

	row := s.queryRowContext(ctx, query, args...)
	task, err := scanTask(row, includeProject)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Task{}, ErrNotFound
		}
		return Task{}, fmt.Errorf("lookup task: %w", err)
	}
	if !includeProject {
		task.Project = project
	}
	return task, nil
}

func (s *PostgresStore) listTasks(ctx context.Context, project string) ([]Task, error) {
	if isGlobalProject(project) {
		return s.allTasks(ctx)
	}

	rows, err := s.queryContext(ctx, `
		SELECT t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = ?
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows, false)
		if err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		task.Project = project
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tasks: %w", err)
	}

	return tasks, nil
}

func (s *PostgresStore) allTasks(ctx context.Context) ([]Task, error) {
	rows, err := s.queryContext(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		ORDER BY t.updated_at DESC, t.project ASC, t.issue ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows, true)
		if err != nil {
			return nil, fmt.Errorf("scan all task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all tasks: %w", err)
	}

	return tasks, nil
}

func (s *PostgresStore) queryEvents(ctx context.Context, project, issue string, afterID int64) ([]Event, error) {
	const baseQuery = `
		SELECT id, project, kind, issue, worker_id, message, payload, created_at
		FROM events
		WHERE project = ? AND id > ?
	`

	query := baseQuery
	args := []any{project, afterID}
	if issue != "" {
		query += ` AND issue = ?`
		args = append(args, issue)
	}
	query += ` ORDER BY id ASC`

	rows, err := s.queryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var event Event
		var payload sql.NullString
		var createdAt string
		if err := rows.Scan(&event.ID, &event.Project, &event.Kind, &event.Issue, &event.WorkerID, &event.Message, &payload, &createdAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if payload.Valid {
			event.Payload = []byte(payload.String)
		}
		event.CreatedAt = parseTime(createdAt)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}

	return events, nil
}

func (s *PostgresStore) lookupCloneRecord(ctx context.Context, project, path string) (legacy.CloneRecord, error) {
	row := s.queryRowContext(ctx, `
		SELECT project, path, status, branch, issue
		FROM clones
		WHERE project = ? AND path = ?
	`, project, path)

	var record legacy.CloneRecord
	if err := row.Scan(&record.Project, &record.Path, &record.Status, &record.CurrentBranch, &record.AssignedTask); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return legacy.CloneRecord{}, legacy.ErrCloneNotFound
		}
		return legacy.CloneRecord{}, err
	}
	return record, nil
}

func (s *PostgresStore) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, rebindPostgres(query), args...)
}

func (s *PostgresStore) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, rebindPostgres(query), args...)
}

func (s *PostgresStore) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, rebindPostgres(query), args...)
}

type postgresTx struct {
	*sql.Tx
}

func (tx postgresTx) execContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return tx.ExecContext(ctx, rebindPostgres(query), args...)
}

func (tx postgresTx) queryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return tx.QueryContext(ctx, rebindPostgres(query), args...)
}

func (tx postgresTx) queryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return tx.QueryRowContext(ctx, rebindPostgres(query), args...)
}

func (tx postgresTx) advisoryLock(ctx context.Context, namespace, key string) error {
	if _, err := tx.execContext(ctx, `SELECT pg_advisory_xact_lock(hashtext(?), hashtext(?))`, namespace, key); err != nil {
		return fmt.Errorf("acquire advisory lock %s/%s: %w", namespace, key, err)
	}
	return nil
}

func nextPostgresWorkerIDTx(ctx context.Context, tx postgresTx, project string) (string, error) {
	var maxID sql.NullInt64
	if err := tx.queryRowContext(ctx, `
		SELECT MAX(CAST(SUBSTRING(worker_id FROM 8) AS INTEGER))
		FROM workers
		WHERE project = ? AND worker_id ~ '^worker-[0-9]+$'
	`, project).Scan(&maxID); err != nil {
		return "", fmt.Errorf("query next worker id: %w", err)
	}

	next := 1
	if maxID.Valid {
		next = int(maxID.Int64) + 1
	}
	return workerIDForSequence(next), nil
}

func execPostgresStatements(ctx context.Context, db *sql.DB, statements string) error {
	for _, statement := range strings.Split(statements, ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func rebindPostgres(query string) string {
	var builder strings.Builder
	builder.Grow(len(query) + 16)

	argIndex := 1
	for i := 0; i < len(query); i++ {
		if query[i] != '?' {
			builder.WriteByte(query[i])
			continue
		}
		builder.WriteByte('$')
		builder.WriteString(fmt.Sprintf("%d", argIndex))
		argIndex++
	}

	return builder.String()
}
