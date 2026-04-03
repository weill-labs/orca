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
	updated_at TEXT NOT NULL,
	PRIMARY KEY (project, pane_id)
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
		SELECT pane_id, agent, state, issue, clone_path, updated_at
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
		var updatedAt string
		if err := rows.Scan(&worker.PaneID, &worker.Agent, &worker.State, &worker.Issue, &worker.ClonePath, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan worker: %w", err)
		}
		worker.UpdatedAt = parseTime(updatedAt)
		workers = append(workers, worker)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workers: %w", err)
	}

	return workers, nil
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
		INSERT INTO workers(project, pane_id, agent, state, issue, clone_path, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, pane_id) DO UPDATE SET
			agent = excluded.agent,
			state = excluded.state,
			issue = excluded.issue,
			clone_path = excluded.clone_path,
			updated_at = excluded.updated_at
	`, project, worker.PaneID, worker.Agent, worker.State, worker.Issue, worker.ClonePath, formatTime(worker.UpdatedAt))
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
