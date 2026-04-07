package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	legacy "github.com/weill-labs/orca/internal/state"
	_ "modernc.org/sqlite"
)

const schemaSQL = `
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
	agent TEXT NOT NULL,
	prompt TEXT NOT NULL DEFAULT '',
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
	id INTEGER PRIMARY KEY AUTOINCREMENT,
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

const schemaBootstrapSQL = `
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
	agent TEXT NOT NULL,
	prompt TEXT NOT NULL DEFAULT '',
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
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	project TEXT NOT NULL,
	kind TEXT NOT NULL,
	issue TEXT NOT NULL DEFAULT '',
	worker_id TEXT NOT NULL DEFAULT '',
	message TEXT NOT NULL,
	payload TEXT,
	created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_project_updated ON tasks(project, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_clones_project_updated ON clones(project, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_project_id ON events(project, id);
CREATE INDEX IF NOT EXISTS idx_events_project_issue_id ON events(project, issue, id);
CREATE INDEX IF NOT EXISTS idx_merge_queue_project_created ON merge_queue(project, created_at ASC, pr_number ASC);
`

type SQLiteStore struct {
	db  *sql.DB
	now func() time.Time
}

func isGlobalProject(project string) bool {
	return strings.TrimSpace(project) == ""
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	db.SetMaxOpenConns(1)

	store := &SQLiteStore{
		db:  db,
		now: func() time.Time { return time.Now().UTC() },
	}

	if err := store.execWithRetry(context.Background(), `PRAGMA busy_timeout = 5000`); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := store.execWithRetry(context.Background(), `PRAGMA journal_mode = WAL`); err != nil {
		_ = db.Close()
		return nil, err
	}

	if err := store.EnsureSchema(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) EnsureSchema(ctx context.Context) error {
	if err := s.execWithRetry(ctx, schemaBootstrapSQL); err != nil {
		return fmt.Errorf("ensure sqlite schema: %w", err)
	}
	if err := s.ensureWorkerIdentitySchema(ctx); err != nil {
		return err
	}
	if err := s.ensureWorkerTrackingColumns(ctx); err != nil {
		return err
	}
	if err := s.execWithRetry(ctx, schemaSQL); err != nil {
		return fmt.Errorf("ensure sqlite schema indexes: %w", err)
	}

	return nil
}

func (s *SQLiteStore) ProjectStatus(ctx context.Context, project string) (ProjectStatus, error) {
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

func (s *SQLiteStore) TaskStatus(ctx context.Context, project, issue string) (TaskStatus, error) {
	task, err := s.lookupTask(ctx, project, issue)
	if err != nil {
		return TaskStatus{}, err
	}

	events, err := s.queryEvents(ctx, project, issue, 0)
	if err != nil {
		return TaskStatus{}, err
	}

	return TaskStatus{
		Task:   task,
		Events: events,
	}, nil
}

func (s *SQLiteStore) ListWorkers(ctx context.Context, project string) ([]Worker, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, created_at, last_seen_at
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

func (s *SQLiteStore) WorkerByID(ctx context.Context, project, workerID string) (Worker, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, created_at, last_seen_at
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

func (s *SQLiteStore) WorkerByPane(ctx context.Context, project, paneID string) (Worker, error) {
	query := `
		SELECT project, worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, created_at, last_seen_at
		FROM workers
		WHERE current_pane_id = ?
	`
	args := []any{paneID}
	if !isGlobalProject(project) {
		query += ` AND project = ?`
		args = append(args, project)
	}

	row := s.db.QueryRowContext(ctx, query, args...)
	worker, err := scanWorker(row, true)
	if errors.Is(err, sql.ErrNoRows) {
		return Worker{}, ErrNotFound
	}
	if err != nil {
		return Worker{}, fmt.Errorf("lookup worker by pane: %w", err)
	}
	return worker, nil
}

func (s *SQLiteStore) ListClones(ctx context.Context, project string) ([]Clone, error) {
	rows, err := s.db.QueryContext(ctx, `
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

func (s *SQLiteStore) NonTerminalTasks(ctx context.Context, project string) ([]Task, error) {
	if isGlobalProject(project) {
		return s.AllNonTerminalTasks(ctx)
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
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

func (s *SQLiteStore) Events(ctx context.Context, project string, afterID int64) (<-chan Event, <-chan error) {
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

func (s *SQLiteStore) UpsertDaemon(ctx context.Context, project string, daemon DaemonStatus) error {
	if daemon.StartedAt.IsZero() {
		daemon.StartedAt = s.now()
	}
	if daemon.UpdatedAt.IsZero() {
		daemon.UpdatedAt = daemon.StartedAt
	}

	_, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) MarkDaemonStopped(ctx context.Context, project string, updatedAt time.Time) error {
	if updatedAt.IsZero() {
		updatedAt = s.now()
	}

	_, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) UpsertTask(ctx context.Context, project string, task Task) error {
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

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO tasks(project, issue, status, agent, prompt, worker_id, clone_path, pr_number, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, issue) DO UPDATE SET
			status = excluded.status,
			agent = excluded.agent,
			prompt = excluded.prompt,
			worker_id = excluded.worker_id,
			clone_path = excluded.clone_path,
			pr_number = excluded.pr_number,
			updated_at = excluded.updated_at
	`, project, task.Issue, task.Status, task.Agent, task.Prompt, task.WorkerID, task.ClonePath, prNumber, formatTime(task.CreatedAt), formatTime(task.UpdatedAt))
	if err != nil {
		return fmt.Errorf("upsert task: %w", err)
	}

	return nil
}

func (s *SQLiteStore) UpsertWorker(ctx context.Context, project string, worker Worker) error {
	now := s.now()
	if worker.CreatedAt.IsZero() {
		worker.CreatedAt = now
	}
	if worker.LastSeenAt.IsZero() {
		worker.LastSeenAt = now
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workers(project, worker_id, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, created_at, last_seen_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, worker_id) DO UPDATE SET
			agent_profile = excluded.agent_profile,
			current_pane_id = excluded.current_pane_id,
			state = excluded.state,
			issue = excluded.issue,
			clone_path = excluded.clone_path,
			last_review_count = excluded.last_review_count,
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
			last_seen_at = excluded.last_seen_at
	`, project, worker.WorkerID, worker.Agent, worker.CurrentPaneID, worker.State, worker.Issue, worker.ClonePath, worker.LastReviewCount, worker.LastIssueCommentCount, worker.ReviewNudgeCount, worker.LastCIState, worker.CINudgeCount, worker.CIFailurePollCount, boolToInt(worker.CIEscalated), worker.LastMergeableState, worker.NudgeCount, worker.LastCapture, formatTime(worker.LastActivityAt), formatTime(worker.CreatedAt), formatTime(worker.LastSeenAt))
	if err != nil {
		return fmt.Errorf("upsert worker: %w", err)
	}

	return nil
}

func (s *SQLiteStore) ClaimWorker(ctx context.Context, project string, worker Worker) (Worker, error) {
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
	defer func() {
		_ = tx.Rollback()
	}()

	row := tx.QueryRowContext(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, created_at, last_seen_at
		FROM workers
		WHERE project = ? AND agent_profile = ? AND issue = ''
		ORDER BY created_at ASC, worker_id ASC
		LIMIT 1
	`, project, worker.Agent)

	claimed, scanErr := scanWorker(row, false)
	switch {
	case scanErr == nil:
		claimed.Issue = worker.Issue
		claimed.State = worker.State
		claimed.LastSeenAt = worker.LastSeenAt
		if _, err := tx.ExecContext(ctx, `
			UPDATE workers
			SET issue = ?, state = ?, last_seen_at = ?
			WHERE project = ? AND worker_id = ?
		`, claimed.Issue, claimed.State, formatTime(claimed.LastSeenAt), project, claimed.WorkerID); err != nil {
			return Worker{}, fmt.Errorf("claim existing worker: %w", err)
		}
	case errors.Is(scanErr, sql.ErrNoRows):
		worker.WorkerID, err = nextWorkerIDTx(ctx, tx, project)
		if err != nil {
			return Worker{}, err
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO workers(project, worker_id, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, created_at, last_seen_at)
			VALUES(?, ?, ?, '', ?, ?, '', 0, 0, 0, '', 0, 0, 0, '', 0, '', '', ?, ?)
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

func (s *SQLiteStore) DeleteWorker(ctx context.Context, project, workerID string) error {
	result, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) DeleteTask(ctx context.Context, project, issue string) error {
	result, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) ClaimTask(ctx context.Context, project string, task Task) (*Task, error) {
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
	defer func() {
		_ = tx.Rollback()
	}()

	row := tx.QueryRowContext(ctx, `
		SELECT t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = ? AND t.issue = ?
	`, project, task.Issue)

	existing, err := scanTask(row, false)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = tx.ExecContext(ctx, `
			INSERT INTO tasks(project, issue, status, agent, prompt, worker_id, clone_path, pr_number, created_at, updated_at)
			VALUES(?, ?, ?, ?, ?, '', '', NULL, ?, ?)
		`, project, task.Issue, task.Status, task.Agent, task.Prompt, formatTime(task.CreatedAt), formatTime(task.UpdatedAt))
		if err != nil {
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

	_, err = tx.ExecContext(ctx, `
		UPDATE tasks
		SET status = ?, agent = ?, prompt = ?, worker_id = '', clone_path = '', pr_number = NULL, updated_at = ?
		WHERE project = ? AND issue = ?
	`, task.Status, task.Agent, task.Prompt, formatTime(task.UpdatedAt), project, task.Issue)
	if err != nil {
		return nil, fmt.Errorf("update claimed task: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit claimed task update: %w", err)
	}

	return &existing, nil
}

func (s *SQLiteStore) ActiveAssignments(ctx context.Context, project string) ([]Assignment, error) {
	if isGlobalProject(project) {
		return s.AllActiveAssignments(ctx)
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT
			t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.created_at, w.last_seen_at
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

func (s *SQLiteStore) ActiveAssignmentByIssue(ctx context.Context, project, issue string) (Assignment, error) {
	query := `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.created_at, w.last_seen_at
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

	row := s.db.QueryRowContext(ctx, query, args...)
	assignment, err := scanAssignment(row, true)
	if errors.Is(err, sql.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lookup active assignment by issue: %w", err)
	}
	return assignment, nil
}

func (s *SQLiteStore) ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (Assignment, error) {
	query := `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.created_at, w.last_seen_at
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

	row := s.db.QueryRowContext(ctx, query, args...)
	assignment, err := scanAssignment(row, true)
	if errors.Is(err, sql.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lookup active assignment by pr number: %w", err)
	}
	return assignment, nil
}

func (s *SQLiteStore) EnqueueMergeEntry(ctx context.Context, entry MergeQueueEntry) (int, error) {
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
	defer func() {
		_ = tx.Rollback()
	}()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO merge_queue(project, pr_number, issue, status, created_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?)
	`, entry.Project, entry.PRNumber, entry.Issue, entry.Status, formatTime(entry.CreatedAt), formatTime(entry.UpdatedAt))
	if err != nil {
		return 0, fmt.Errorf("enqueue merge entry: %w", err)
	}

	var position int
	if err := tx.QueryRowContext(ctx, `
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

func (s *SQLiteStore) MergeEntry(ctx context.Context, project string, prNumber int) (*MergeQueueEntry, error) {
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

	row := s.db.QueryRowContext(ctx, query, args...)

	entry, err := scanMergeQueueEntry(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup merge entry: %w", err)
	}
	return &entry, nil
}

func (s *SQLiteStore) MergeEntries(ctx context.Context, project string) ([]MergeQueueEntry, error) {
	if isGlobalProject(project) {
		return s.AllMergeEntries(ctx)
	}

	rows, err := s.db.QueryContext(ctx, `
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

func (s *SQLiteStore) UpdateMergeEntry(ctx context.Context, entry MergeQueueEntry) error {
	if entry.UpdatedAt.IsZero() {
		entry.UpdatedAt = s.now()
	}

	result, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) DeleteMergeEntry(ctx context.Context, project string, prNumber int) error {
	result, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) EnsureClone(ctx context.Context, project, path string) (legacy.CloneRecord, error) {
	now := s.now()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO clones(project, path, status, issue, branch, updated_at)
		VALUES(?, ?, 'free', '', '', ?)
		ON CONFLICT(project, path) DO NOTHING
	`, project, path, formatTime(now))
	if err != nil {
		return legacy.CloneRecord{}, fmt.Errorf("ensure clone: %w", err)
	}

	record, err := s.lookupCloneRecord(ctx, project, path)
	if err != nil {
		return legacy.CloneRecord{}, fmt.Errorf("load clone: %w", err)
	}
	return record, nil
}

func (s *SQLiteStore) TryOccupyClone(ctx context.Context, project, path, branch, task string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) MarkCloneFree(ctx context.Context, project, path string) error {
	result, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) UpdateTaskStatus(ctx context.Context, project, issue, status string, updatedAt time.Time) (Task, error) {
	if updatedAt.IsZero() {
		updatedAt = s.now()
	}

	result, err := s.db.ExecContext(ctx, `
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

func (s *SQLiteStore) TasksByPane(ctx context.Context, project, paneID string) ([]Task, error) {
	includeProject := isGlobalProject(project)
	query := `
		SELECT
	`
	if includeProject {
		query += `		t.project,`
	}
	query += `
			t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
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

	rows, err := s.db.QueryContext(ctx, query, args...)
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

func (s *SQLiteStore) AllNonTerminalTasks(ctx context.Context) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
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

func (s *SQLiteStore) AllActiveAssignments(ctx context.Context) ([]Assignment, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.created_at, w.last_seen_at
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

func (s *SQLiteStore) AllMergeEntries(ctx context.Context) ([]MergeQueueEntry, error) {
	rows, err := s.db.QueryContext(ctx, `
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

func (s *SQLiteStore) AppendEvent(ctx context.Context, event Event) (Event, error) {
	if event.CreatedAt.IsZero() {
		event.CreatedAt = s.now()
	}

	var payload any
	if len(event.Payload) > 0 {
		payload = string(event.Payload)
	}

	result, err := s.db.ExecContext(ctx, `
		INSERT INTO events(project, kind, issue, worker_id, message, payload, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)
	`, event.Project, event.Kind, event.Issue, event.WorkerID, event.Message, payload, formatTime(event.CreatedAt))
	if err != nil {
		return Event{}, fmt.Errorf("append event: %w", err)
	}

	eventID, err := result.LastInsertId()
	if err != nil {
		return Event{}, fmt.Errorf("append event last insert id: %w", err)
	}

	event.ID = eventID
	return event, nil
}

func (s *SQLiteStore) lookupDaemon(ctx context.Context, project string) (*DaemonStatus, error) {
	row := s.db.QueryRowContext(ctx, `
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

func (s *SQLiteStore) lookupTask(ctx context.Context, project, issue string) (Task, error) {
	includeProject := isGlobalProject(project)
	query := `
		SELECT
	`
	if includeProject {
		query += `		t.project,`
	}
	query += `
			t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
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

	row := s.db.QueryRowContext(ctx, query, args...)
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

func (s *SQLiteStore) listTasks(ctx context.Context, project string) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT t.issue, t.status, t.agent, t.prompt, t.worker_id, w.current_pane_id, t.clone_path, t.pr_number, t.created_at, t.updated_at
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

func (s *SQLiteStore) queryEvents(ctx context.Context, project, issue string, afterID int64) ([]Event, error) {
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

	rows, err := s.db.QueryContext(ctx, query, args...)
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

func (s *SQLiteStore) lookupCloneRecord(ctx context.Context, project, path string) (legacy.CloneRecord, error) {
	row := s.db.QueryRowContext(ctx, `
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

type rowScanner interface {
	Scan(dest ...any) error
}

func scanTask(scanner rowScanner, includeProject bool) (Task, error) {
	var task Task
	var prNumber sql.NullInt64
	var currentPaneID sql.NullString
	var createdAt string
	var updatedAt string

	fields := []any{
		&task.Issue,
		&task.Status,
		&task.Agent,
		&task.Prompt,
		&task.WorkerID,
		&currentPaneID,
		&task.ClonePath,
		&prNumber,
		&createdAt,
		&updatedAt,
	}
	if includeProject {
		fields = append([]any{&task.Project}, fields...)
	}
	if err := scanner.Scan(fields...); err != nil {
		return Task{}, err
	}

	if prNumber.Valid {
		value := int(prNumber.Int64)
		task.PRNumber = &value
	}
	if currentPaneID.Valid {
		task.CurrentPaneID = currentPaneID.String
	}
	task.CreatedAt = parseTime(createdAt)
	task.UpdatedAt = parseTime(updatedAt)
	return task, nil
}

func scanAssignment(scanner rowScanner, includeProject bool) (Assignment, error) {
	var task Task
	var prNumber sql.NullInt64
	var taskCreatedAt string
	var taskUpdatedAt string
	var worker Worker
	var ciEscalated int
	var lastActivityAt string
	var workerCreatedAt string
	var workerLastSeenAt string

	fields := []any{
		&task.Issue,
		&task.Status,
		&task.Agent,
		&task.Prompt,
		&task.WorkerID,
		&task.CurrentPaneID,
		&task.ClonePath,
		&prNumber,
		&taskCreatedAt,
		&taskUpdatedAt,
		&worker.WorkerID,
		&worker.CurrentPaneID,
		&worker.Agent,
		&worker.State,
		&worker.Issue,
		&worker.ClonePath,
		&worker.LastReviewCount,
		&worker.LastIssueCommentCount,
		&worker.ReviewNudgeCount,
		&worker.LastCIState,
		&worker.CINudgeCount,
		&worker.CIFailurePollCount,
		&ciEscalated,
		&worker.LastMergeableState,
		&worker.NudgeCount,
		&worker.LastCapture,
		&lastActivityAt,
		&workerCreatedAt,
		&workerLastSeenAt,
	}
	if includeProject {
		fields = append([]any{&task.Project}, fields...)
	}
	if err := scanner.Scan(fields...); err != nil {
		return Assignment{}, err
	}

	if prNumber.Valid {
		value := int(prNumber.Int64)
		task.PRNumber = &value
	}
	task.CreatedAt = parseTime(taskCreatedAt)
	task.UpdatedAt = parseTime(taskUpdatedAt)
	worker.Project = task.Project
	worker.CIEscalated = ciEscalated != 0
	worker.LastActivityAt = parseTime(lastActivityAt)
	worker.CreatedAt = parseTime(workerCreatedAt)
	worker.LastSeenAt = parseTime(workerLastSeenAt)

	return Assignment{
		Task:   task,
		Worker: worker,
	}, nil
}

func scanWorker(scanner rowScanner, includeProject bool) (Worker, error) {
	var worker Worker
	var ciEscalated int
	var lastActivityAt string
	var createdAt string
	var lastSeenAt string

	fields := []any{
		&worker.WorkerID,
		&worker.CurrentPaneID,
		&worker.Agent,
		&worker.State,
		&worker.Issue,
		&worker.ClonePath,
		&worker.LastReviewCount,
		&worker.LastIssueCommentCount,
		&worker.ReviewNudgeCount,
		&worker.LastCIState,
		&worker.CINudgeCount,
		&worker.CIFailurePollCount,
		&ciEscalated,
		&worker.LastMergeableState,
		&worker.NudgeCount,
		&worker.LastCapture,
		&lastActivityAt,
		&createdAt,
		&lastSeenAt,
	}
	if includeProject {
		fields = append([]any{&worker.Project}, fields...)
	}
	if err := scanner.Scan(fields...); err != nil {
		return Worker{}, err
	}

	worker.CIEscalated = ciEscalated != 0
	worker.LastActivityAt = parseTime(lastActivityAt)
	worker.CreatedAt = parseTime(createdAt)
	worker.LastSeenAt = parseTime(lastSeenAt)
	return worker, nil
}

func scanMergeQueueEntry(scanner rowScanner) (MergeQueueEntry, error) {
	var entry MergeQueueEntry
	var createdAt string
	var updatedAt string
	if err := scanner.Scan(&entry.Project, &entry.Issue, &entry.PRNumber, &entry.Status, &createdAt, &updatedAt); err != nil {
		return MergeQueueEntry{}, err
	}
	entry.CreatedAt = parseTime(createdAt)
	entry.UpdatedAt = parseTime(updatedAt)
	return entry, nil
}

func taskStatusBlocksClaim(status string) bool {
	switch status {
	case "", "done", "cancelled", "failed":
		return false
	default:
		return true
	}
}

func (s *SQLiteStore) ensureWorkerIdentitySchema(ctx context.Context) error {
	if err := s.addColumnIfMissing(ctx, "events", "worker_id", "TEXT NOT NULL DEFAULT ''"); err != nil {
		return err
	}

	hasWorkerID, err := s.tableHasColumn(ctx, "workers", "worker_id")
	if err != nil {
		return err
	}
	if !hasWorkerID {
		if err := s.ensureLegacyWorkerTrackingColumns(ctx); err != nil {
			return err
		}
		if err := s.migrateLegacyWorkerIdentitySchema(ctx); err != nil {
			return err
		}
	}

	type columnSpec struct {
		name       string
		definition string
	}
	for _, spec := range []columnSpec{
		{name: "agent_profile", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "current_pane_id", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "created_at", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "last_seen_at", definition: "TEXT NOT NULL DEFAULT ''"},
	} {
		if err := s.addColumnIfMissing(ctx, "workers", spec.name, spec.definition); err != nil {
			return err
		}
	}

	if err := s.backfillEventWorkerIDs(ctx); err != nil {
		return err
	}

	return nil
}

func (s *SQLiteStore) ensureWorkerTrackingColumns(ctx context.Context) error {
	type columnSpec struct {
		name       string
		definition string
	}

	specs := []columnSpec{
		{name: "last_review_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_issue_comment_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "review_nudge_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_ci_state", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "ci_nudge_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "ci_failure_poll_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "ci_escalated", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_mergeable_state", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "nudge_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_capture", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "last_activity_at", definition: "TEXT NOT NULL DEFAULT ''"},
	}

	for _, spec := range specs {
		if err := s.addColumnIfMissing(ctx, "workers", spec.name, spec.definition); err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLiteStore) ensureLegacyWorkerTrackingColumns(ctx context.Context) error {
	type columnSpec struct {
		name       string
		definition string
	}

	specs := []columnSpec{
		{name: "last_review_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_issue_comment_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "review_nudge_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_ci_state", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "ci_nudge_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "ci_failure_poll_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "ci_escalated", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_mergeable_state", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "nudge_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_capture", definition: "TEXT NOT NULL DEFAULT ''"},
		{name: "last_activity_at", definition: "TEXT NOT NULL DEFAULT ''"},
	}

	for _, spec := range specs {
		if err := s.addColumnIfMissing(ctx, "workers", spec.name, spec.definition); err != nil {
			return err
		}
	}

	return nil
}

func (s *SQLiteStore) migrateLegacyWorkerIdentitySchema(ctx context.Context) error {
	type legacyWorker struct {
		project               string
		paneID                string
		agent                 string
		state                 string
		issue                 string
		clonePath             string
		lastReviewCount       int
		lastIssueCommentCount int
		reviewNudgeCount      int
		lastCIState           string
		ciNudgeCount          int
		ciFailurePollCount    int
		ciEscalated           int
		lastMergeableState    string
		nudgeCount            int
		lastCapture           string
		lastActivityAt        string
		updatedAt             string
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT project, pane_id, agent, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, updated_at
		FROM workers
		ORDER BY project ASC, pane_id ASC
	`)
	if err != nil {
		return fmt.Errorf("load legacy workers: %w", err)
	}
	defer rows.Close()

	legacyWorkers := make([]legacyWorker, 0)
	workerIDsByPane := make(map[string]string)
	sequences := make(map[string]int)
	for rows.Next() {
		var worker legacyWorker
		if err := rows.Scan(
			&worker.project,
			&worker.paneID,
			&worker.agent,
			&worker.state,
			&worker.issue,
			&worker.clonePath,
			&worker.lastReviewCount,
			&worker.lastIssueCommentCount,
			&worker.reviewNudgeCount,
			&worker.lastCIState,
			&worker.ciNudgeCount,
			&worker.ciFailurePollCount,
			&worker.ciEscalated,
			&worker.lastMergeableState,
			&worker.nudgeCount,
			&worker.lastCapture,
			&worker.lastActivityAt,
			&worker.updatedAt,
		); err != nil {
			return fmt.Errorf("scan legacy worker: %w", err)
		}
		sequences[worker.project]++
		workerIDsByPane[worker.project+"\x00"+worker.paneID] = workerIDForSequence(sequences[worker.project])
		legacyWorkers = append(legacyWorkers, worker)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate legacy workers: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin worker identity migration tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.ExecContext(ctx, `ALTER TABLE workers RENAME TO workers_legacy`); err != nil {
		return fmt.Errorf("rename legacy workers table: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		CREATE TABLE workers (
			project TEXT NOT NULL,
			worker_id TEXT NOT NULL,
			agent_profile TEXT NOT NULL,
			current_pane_id TEXT NOT NULL DEFAULT '',
			state TEXT NOT NULL,
			issue TEXT NOT NULL DEFAULT '',
			clone_path TEXT NOT NULL DEFAULT '',
			last_review_count INTEGER NOT NULL DEFAULT 0,
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
			created_at TEXT NOT NULL,
			last_seen_at TEXT NOT NULL,
			PRIMARY KEY (project, worker_id)
		)
	`); err != nil {
		return fmt.Errorf("create migrated workers table: %w", err)
	}

	for _, worker := range legacyWorkers {
		workerID := workerIDsByPane[worker.project+"\x00"+worker.paneID]
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO workers(project, worker_id, agent_profile, current_pane_id, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, created_at, last_seen_at)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, worker.project, workerID, worker.agent, worker.paneID, worker.state, worker.issue, worker.clonePath, worker.lastReviewCount, worker.lastIssueCommentCount, worker.reviewNudgeCount, worker.lastCIState, worker.ciNudgeCount, worker.ciFailurePollCount, worker.ciEscalated, worker.lastMergeableState, worker.nudgeCount, worker.lastCapture, worker.lastActivityAt, worker.updatedAt, worker.updatedAt); err != nil {
			return fmt.Errorf("insert migrated worker %s/%s: %w", worker.project, worker.paneID, err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE tasks
			SET worker_id = ?
			WHERE project = ? AND worker_id = ?
		`, workerID, worker.project, worker.paneID); err != nil {
			return fmt.Errorf("rewrite task worker ids for %s/%s: %w", worker.project, worker.paneID, err)
		}
	}

	if err := backfillEventWorkerIDsTx(ctx, tx, workerIDsByPane); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DROP TABLE workers_legacy`); err != nil {
		return fmt.Errorf("drop legacy workers table: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit worker identity migration tx: %w", err)
	}

	if err := s.execWithRetry(ctx, schemaSQL); err != nil {
		return fmt.Errorf("re-ensure schema after worker identity migration: %w", err)
	}

	return nil
}

func (s *SQLiteStore) backfillEventWorkerIDs(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `
		SELECT project, worker_id, current_pane_id
		FROM workers
		WHERE current_pane_id <> ''
	`)
	if err != nil {
		return fmt.Errorf("load worker ids for event backfill: %w", err)
	}
	defer rows.Close()

	workerIDsByPane := make(map[string]string)
	for rows.Next() {
		var project string
		var workerID string
		var currentPaneID string
		if err := rows.Scan(&project, &workerID, &currentPaneID); err != nil {
			return fmt.Errorf("scan worker id backfill row: %w", err)
		}
		workerIDsByPane[project+"\x00"+currentPaneID] = workerID
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate worker ids for event backfill: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin event worker backfill tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := backfillEventWorkerIDsTx(ctx, tx, workerIDsByPane); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit event worker backfill tx: %w", err)
	}
	return nil
}

func backfillEventWorkerIDsTx(ctx context.Context, tx *sql.Tx, workerIDsByPane map[string]string) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, project, payload
		FROM events
		WHERE worker_id = ''
	`)
	if err != nil {
		return fmt.Errorf("load events for worker id backfill: %w", err)
	}
	defer rows.Close()

	type eventRow struct {
		id      int64
		project string
		payload sql.NullString
	}
	events := make([]eventRow, 0)
	for rows.Next() {
		var event eventRow
		if err := rows.Scan(&event.id, &event.project, &event.payload); err != nil {
			return fmt.Errorf("scan event backfill row: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate events for worker id backfill: %w", err)
	}

	for _, event := range events {
		if !event.payload.Valid || strings.TrimSpace(event.payload.String) == "" {
			continue
		}

		payload := make(map[string]any)
		if err := json.Unmarshal([]byte(event.payload.String), &payload); err != nil {
			continue
		}
		paneID, _ := payload["pane_id"].(string)
		if strings.TrimSpace(paneID) == "" {
			continue
		}
		workerID := workerIDsByPane[event.project+"\x00"+paneID]
		if workerID == "" {
			continue
		}
		payload["worker_id"] = workerID
		encoded, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal event payload with worker id: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE events
			SET worker_id = ?, payload = ?
			WHERE id = ?
		`, workerID, string(encoded), event.id); err != nil {
			return fmt.Errorf("update event worker id %d: %w", event.id, err)
		}
	}

	return nil
}

func nextWorkerIDTx(ctx context.Context, tx *sql.Tx, project string) (string, error) {
	var maxID sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT MAX(CAST(SUBSTR(worker_id, 8) AS INTEGER))
		FROM workers
		WHERE project = ? AND worker_id GLOB 'worker-[0-9]*'
	`, project).Scan(&maxID); err != nil {
		return "", fmt.Errorf("query next worker id: %w", err)
	}

	next := 1
	if maxID.Valid {
		next = int(maxID.Int64) + 1
	}
	return workerIDForSequence(next), nil
}

func workerIDForSequence(sequence int) string {
	if sequence < 1 {
		sequence = 1
	}
	return fmt.Sprintf("worker-%02d", sequence)
}

func (s *SQLiteStore) tableHasColumn(ctx context.Context, table, column string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, fmt.Errorf("query table info for %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var columnType string
		var notNull int
		var defaultValue sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &pk); err != nil {
			return false, fmt.Errorf("scan table info for %s: %w", table, err)
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate table info for %s: %w", table, err)
	}
	return false, nil
}

func (s *SQLiteStore) addColumnIfMissing(ctx context.Context, table, column, definition string) error {
	hasColumn, err := s.tableHasColumn(ctx, table, column)
	if err != nil {
		return err
	}
	if hasColumn {
		return nil
	}

	if err := s.execWithRetry(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition)); err != nil {
		return fmt.Errorf("add %s.%s column: %w", table, column, err)
	}
	return nil
}

func formatTime(timestamp time.Time) string {
	return timestamp.UTC().Format(time.RFC3339Nano)
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func parseTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}

	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}

	return parsed
}

func (s *SQLiteStore) execWithRetry(ctx context.Context, query string, args ...any) error {
	const maxAttempts = 20

	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
			lastErr = err
			if !isBusyError(err) {
				return err
			}
			if err := amux.Wait(ctx, 50*time.Millisecond); err != nil {
				return err
			}
			continue
		}
		return nil
	}

	return lastErr
}

func isBusyError(err error) bool {
	if err == nil {
		return false
	}

	message := err.Error()
	return strings.Contains(message, "SQLITE_BUSY") || strings.Contains(message, "database is locked")
}
