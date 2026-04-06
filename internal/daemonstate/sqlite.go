package state

import (
	"context"
	"database/sql"
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
	pane_id TEXT NOT NULL,
	agent TEXT NOT NULL,
	state TEXT NOT NULL,
	issue TEXT NOT NULL DEFAULT '',
	clone_path TEXT NOT NULL DEFAULT '',
	last_review_count INTEGER NOT NULL DEFAULT 0,
	last_issue_comment_count INTEGER NOT NULL DEFAULT 0,
	review_nudge_count INTEGER NOT NULL DEFAULT 0,
	last_ci_state TEXT NOT NULL DEFAULT '',
	last_mergeable_state TEXT NOT NULL DEFAULT '',
	nudge_count INTEGER NOT NULL DEFAULT 0,
	last_capture TEXT NOT NULL DEFAULT '',
	last_activity_at TEXT NOT NULL DEFAULT '',
	updated_at TEXT NOT NULL,
	PRIMARY KEY (project, pane_id)
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
	message TEXT NOT NULL,
	payload TEXT,
	created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_project_updated ON tasks(project, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_workers_project_updated ON workers(project, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_clones_project_updated ON clones(project, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_events_project_id ON events(project, id);
CREATE INDEX IF NOT EXISTS idx_events_project_issue_id ON events(project, issue, id);
CREATE INDEX IF NOT EXISTS idx_merge_queue_project_created ON merge_queue(project, created_at ASC, pr_number ASC);
`

type SQLiteStore struct {
	db  *sql.DB
	now func() time.Time
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
	if err := s.execWithRetry(ctx, schemaSQL); err != nil {
		return fmt.Errorf("ensure sqlite schema: %w", err)
	}
	if err := s.ensureWorkersColumns(ctx); err != nil {
		return err
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
		SELECT pane_id, agent, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, last_mergeable_state, nudge_count, last_capture, last_activity_at, updated_at
		FROM workers
		WHERE project = ?
		ORDER BY updated_at DESC, pane_id ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		var worker Worker
		var lastActivityAt string
		var updatedAt string
		if err := rows.Scan(
			&worker.PaneID,
			&worker.Agent,
			&worker.State,
			&worker.Issue,
			&worker.ClonePath,
			&worker.LastReviewCount,
			&worker.LastIssueCommentCount,
			&worker.ReviewNudgeCount,
			&worker.LastCIState,
			&worker.LastMergeableState,
			&worker.NudgeCount,
			&worker.LastCapture,
			&lastActivityAt,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		worker.LastActivityAt = parseTime(lastActivityAt)
		worker.UpdatedAt = parseTime(updatedAt)
		workers = append(workers, worker)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workers: %w", err)
	}

	return workers, nil
}

func (s *SQLiteStore) WorkerByPane(ctx context.Context, project, paneID string) (Worker, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT pane_id, agent, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, last_mergeable_state, nudge_count, last_capture, last_activity_at, updated_at
		FROM workers
		WHERE project = ? AND pane_id = ?
	`, project, paneID)

	worker, err := scanWorker(row)
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
	rows, err := s.db.QueryContext(ctx, `
		SELECT issue, status, agent, prompt, worker_id, clone_path, pr_number, created_at, updated_at
		FROM tasks
		WHERE project = ? AND status IN ('starting', 'active')
		ORDER BY updated_at DESC, issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list non-terminal tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan non-terminal task: %w", err)
		}
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
	if worker.UpdatedAt.IsZero() {
		worker.UpdatedAt = s.now()
	}

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO workers(project, pane_id, agent, state, issue, clone_path, last_review_count, last_issue_comment_count, review_nudge_count, last_ci_state, last_mergeable_state, nudge_count, last_capture, last_activity_at, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, pane_id) DO UPDATE SET
			agent = excluded.agent,
			state = excluded.state,
			issue = excluded.issue,
			clone_path = excluded.clone_path,
			last_review_count = excluded.last_review_count,
			last_issue_comment_count = excluded.last_issue_comment_count,
			review_nudge_count = excluded.review_nudge_count,
			last_ci_state = excluded.last_ci_state,
			last_mergeable_state = excluded.last_mergeable_state,
			nudge_count = excluded.nudge_count,
			last_capture = excluded.last_capture,
			last_activity_at = excluded.last_activity_at,
			updated_at = excluded.updated_at
	`, project, worker.PaneID, worker.Agent, worker.State, worker.Issue, worker.ClonePath, worker.LastReviewCount, worker.LastIssueCommentCount, worker.ReviewNudgeCount, worker.LastCIState, worker.LastMergeableState, worker.NudgeCount, worker.LastCapture, formatTime(worker.LastActivityAt), formatTime(worker.UpdatedAt))
	if err != nil {
		return fmt.Errorf("upsert worker: %w", err)
	}

	return nil
}

func (s *SQLiteStore) DeleteWorker(ctx context.Context, project, paneID string) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM workers
		WHERE project = ? AND pane_id = ?
	`, project, paneID)
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
		SELECT issue, status, agent, prompt, worker_id, clone_path, pr_number, created_at, updated_at
		FROM tasks
		WHERE project = ? AND issue = ?
	`, project, task.Issue)

	existing, err := scanTask(row)
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
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			t.issue, t.status, t.agent, t.prompt, t.worker_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.pane_id, w.agent, w.state, w.issue, w.clone_path, w.last_review_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.updated_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.pane_id = t.worker_id
		WHERE t.project = ? AND t.status = 'active'
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list active assignments: %w", err)
	}
	defer rows.Close()

	assignments := make([]Assignment, 0)
	for rows.Next() {
		assignment, err := scanAssignment(rows)
		if err != nil {
			return nil, fmt.Errorf("scan active assignment: %w", err)
		}
		assignments = append(assignments, assignment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active assignments: %w", err)
	}
	return assignments, nil
}

func (s *SQLiteStore) ActiveAssignmentByIssue(ctx context.Context, project, issue string) (Assignment, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT
			t.issue, t.status, t.agent, t.prompt, t.worker_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.pane_id, w.agent, w.state, w.issue, w.clone_path, w.last_review_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.updated_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.pane_id = t.worker_id
		WHERE t.project = ? AND t.issue = ? AND t.status = 'active'
	`, project, issue)

	assignment, err := scanAssignment(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lookup active assignment by issue: %w", err)
	}
	return assignment, nil
}

func (s *SQLiteStore) ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (Assignment, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT
			t.issue, t.status, t.agent, t.prompt, t.worker_id, t.clone_path, t.pr_number, t.created_at, t.updated_at,
			w.pane_id, w.agent, w.state, w.issue, w.clone_path, w.last_review_count, w.last_issue_comment_count, w.review_nudge_count, w.last_ci_state, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.updated_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.pane_id = t.worker_id
		WHERE t.project = ? AND t.pr_number = ? AND t.status = 'active'
	`, project, prNumber)

	assignment, err := scanAssignment(row)
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

func (s *SQLiteStore) NextMergeEntry(ctx context.Context, project string) (*MergeQueueEntry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT project, issue, pr_number, status, created_at, updated_at
		FROM merge_queue
		WHERE project = ?
		ORDER BY created_at ASC, pr_number ASC
		LIMIT 1
	`, project)

	entry, err := scanMergeQueueEntry(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("next merge entry: %w", err)
	}
	return &entry, nil
}

func (s *SQLiteStore) MergeEntry(ctx context.Context, project string, prNumber int) (*MergeQueueEntry, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT project, issue, pr_number, status, created_at, updated_at
		FROM merge_queue
		WHERE project = ? AND pr_number = ?
	`, project, prNumber)

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
	rows, err := s.db.QueryContext(ctx, `
		SELECT issue, status, agent, prompt, worker_id, clone_path, pr_number, created_at, updated_at
		FROM tasks
		WHERE project = ? AND worker_id = ?
		ORDER BY created_at ASC, updated_at ASC, issue ASC
	`, project, paneID)
	if err != nil {
		return nil, fmt.Errorf("list tasks by pane: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task by pane: %w", err)
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tasks by pane: %w", err)
	}

	return tasks, nil
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
		INSERT INTO events(project, kind, issue, message, payload, created_at)
		VALUES(?, ?, ?, ?, ?, ?)
	`, event.Project, event.Kind, event.Issue, event.Message, payload, formatTime(event.CreatedAt))
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
	row := s.db.QueryRowContext(ctx, `
		SELECT issue, status, agent, prompt, worker_id, clone_path, pr_number, created_at, updated_at
		FROM tasks
		WHERE project = ? AND issue = ?
	`, project, issue)

	task, err := scanTask(row)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Task{}, ErrNotFound
		}
		return Task{}, fmt.Errorf("lookup task: %w", err)
	}

	return task, nil
}

func (s *SQLiteStore) listTasks(ctx context.Context, project string) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT issue, status, agent, prompt, worker_id, clone_path, pr_number, created_at, updated_at
		FROM tasks
		WHERE project = ?
		ORDER BY updated_at DESC, issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tasks: %w", err)
	}

	return tasks, nil
}

func (s *SQLiteStore) queryEvents(ctx context.Context, project, issue string, afterID int64) ([]Event, error) {
	const baseQuery = `
		SELECT id, project, kind, issue, message, payload, created_at
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
		if err := rows.Scan(&event.ID, &event.Project, &event.Kind, &event.Issue, &event.Message, &payload, &createdAt); err != nil {
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

func scanTask(scanner rowScanner) (Task, error) {
	var task Task
	var prNumber sql.NullInt64
	var createdAt string
	var updatedAt string
	if err := scanner.Scan(&task.Issue, &task.Status, &task.Agent, &task.Prompt, &task.WorkerID, &task.ClonePath, &prNumber, &createdAt, &updatedAt); err != nil {
		return Task{}, err
	}

	if prNumber.Valid {
		value := int(prNumber.Int64)
		task.PRNumber = &value
	}
	task.CreatedAt = parseTime(createdAt)
	task.UpdatedAt = parseTime(updatedAt)
	return task, nil
}

func scanAssignment(scanner rowScanner) (Assignment, error) {
	var task Task
	var prNumber sql.NullInt64
	var taskCreatedAt string
	var taskUpdatedAt string
	var worker Worker
	var lastActivityAt string
	var workerUpdatedAt string
	if err := scanner.Scan(
		&task.Issue,
		&task.Status,
		&task.Agent,
		&task.Prompt,
		&task.WorkerID,
		&task.ClonePath,
		&prNumber,
		&taskCreatedAt,
		&taskUpdatedAt,
		&worker.PaneID,
		&worker.Agent,
		&worker.State,
		&worker.Issue,
		&worker.ClonePath,
		&worker.LastReviewCount,
		&worker.LastIssueCommentCount,
		&worker.ReviewNudgeCount,
		&worker.LastCIState,
		&worker.LastMergeableState,
		&worker.NudgeCount,
		&worker.LastCapture,
		&lastActivityAt,
		&workerUpdatedAt,
	); err != nil {
		return Assignment{}, err
	}
	if prNumber.Valid {
		value := int(prNumber.Int64)
		task.PRNumber = &value
	}
	task.CreatedAt = parseTime(taskCreatedAt)
	task.UpdatedAt = parseTime(taskUpdatedAt)
	worker.LastActivityAt = parseTime(lastActivityAt)
	worker.UpdatedAt = parseTime(workerUpdatedAt)

	return Assignment{
		Task:   task,
		Worker: worker,
	}, nil
}

func scanWorker(scanner rowScanner) (Worker, error) {
	var worker Worker
	var lastActivityAt string
	var updatedAt string
	if err := scanner.Scan(
		&worker.PaneID,
		&worker.Agent,
		&worker.State,
		&worker.Issue,
		&worker.ClonePath,
		&worker.LastReviewCount,
		&worker.LastIssueCommentCount,
		&worker.ReviewNudgeCount,
		&worker.LastCIState,
		&worker.LastMergeableState,
		&worker.NudgeCount,
		&worker.LastCapture,
		&lastActivityAt,
		&updatedAt,
	); err != nil {
		return Worker{}, err
	}
	worker.LastActivityAt = parseTime(lastActivityAt)
	worker.UpdatedAt = parseTime(updatedAt)
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

func (s *SQLiteStore) ensureWorkersColumns(ctx context.Context) error {
	type columnSpec struct {
		name       string
		definition string
	}

	specs := []columnSpec{
		{name: "last_review_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_issue_comment_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "review_nudge_count", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "last_ci_state", definition: "TEXT NOT NULL DEFAULT ''"},
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

func (s *SQLiteStore) addColumnIfMissing(ctx context.Context, table, column, definition string) error {
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return fmt.Errorf("query table info for %s: %w", table, err)
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
			return fmt.Errorf("scan table info for %s: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate table info for %s: %w", table, err)
	}

	if err := s.execWithRetry(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition)); err != nil {
		return fmt.Errorf("add %s.%s column: %w", table, column, err)
	}
	return nil
}

func formatTime(timestamp time.Time) string {
	return timestamp.UTC().Format(time.RFC3339Nano)
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
