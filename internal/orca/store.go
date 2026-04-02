package orca

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

type Store struct {
	db *sql.DB
}

func OpenStore(path string) (*Store, error) {
	if path == "" {
		return nil, errors.New("orca: state path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create state directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite store: %w", err)
	}
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(`PRAGMA busy_timeout = 5000;`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("set sqlite busy timeout: %w", err)
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Init(ctx context.Context, project, session string, startedAt time.Time) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS daemon_state (
			project TEXT PRIMARY KEY,
			session TEXT NOT NULL,
			started_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS clones (
			project TEXT NOT NULL,
			path TEXT NOT NULL,
			status TEXT NOT NULL,
			branch TEXT NOT NULL,
			task_id TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (project, path)
		);`,
		`CREATE TABLE IF NOT EXISTS tasks (
			project TEXT NOT NULL,
			issue_id TEXT NOT NULL,
			status TEXT NOT NULL,
			branch TEXT NOT NULL,
			clone_path TEXT NOT NULL,
			worker_id TEXT NOT NULL,
			prompt TEXT NOT NULL,
			pr_number INTEGER NOT NULL DEFAULT 0,
			nudge_count INTEGER NOT NULL DEFAULT 0,
			last_capture TEXT NOT NULL DEFAULT '',
			last_activity_at TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (project, issue_id)
		);`,
		`CREATE TABLE IF NOT EXISTS workers (
			project TEXT NOT NULL,
			pane_id TEXT NOT NULL,
			pane_name TEXT NOT NULL,
			agent TEXT NOT NULL,
			task_id TEXT NOT NULL,
			clone_path TEXT NOT NULL,
			state TEXT NOT NULL,
			last_capture TEXT NOT NULL DEFAULT '',
			last_activity_at TEXT NOT NULL,
			nudge_count INTEGER NOT NULL DEFAULT 0,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (project, pane_id)
		);`,
		`CREATE TABLE IF NOT EXISTS events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			project TEXT NOT NULL,
			type TEXT NOT NULL,
			task_id TEXT NOT NULL,
			worker_id TEXT NOT NULL,
			clone_path TEXT NOT NULL,
			message TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
	}

	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("initialize sqlite schema: %w", err)
		}
	}

	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO daemon_state (project, session, started_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(project) DO UPDATE SET session = excluded.session, started_at = excluded.started_at`,
		project,
		session,
		formatTime(startedAt),
	)
	if err != nil {
		return fmt.Errorf("record daemon state: %w", err)
	}

	return nil
}

func (s *Store) EnsureClone(ctx context.Context, clone Clone) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO clones (project, path, status, branch, task_id, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(project, path) DO NOTHING`,
		clone.Project,
		clone.Path,
		clone.Status,
		clone.Branch,
		clone.TaskID,
		formatTime(clone.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("ensure clone %s: %w", clone.Path, err)
	}
	return nil
}

func (s *Store) ReserveClone(ctx context.Context, project, branch, issueID string, now time.Time) (Clone, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Clone{}, fmt.Errorf("begin clone reservation: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	row := tx.QueryRowContext(
		ctx,
		`SELECT path, status, branch, task_id, updated_at
		 FROM clones
		 WHERE project = ? AND status = ?
		 ORDER BY path
		 LIMIT 1`,
		project,
		CloneStatusFree,
	)

	var clone Clone
	var updatedAt string
	if scanErr := row.Scan(&clone.Path, &clone.Status, &clone.Branch, &clone.TaskID, &updatedAt); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			err = ErrNoFreeClones
			return Clone{}, err
		}
		err = fmt.Errorf("select free clone: %w", scanErr)
		return Clone{}, err
	}

	clone.Project = project
	clone.Status = CloneStatusOccupied
	clone.Branch = branch
	clone.TaskID = issueID
	clone.UpdatedAt = now

	if _, execErr := tx.ExecContext(
		ctx,
		`UPDATE clones
		 SET status = ?, branch = ?, task_id = ?, updated_at = ?
		 WHERE project = ? AND path = ?`,
		clone.Status,
		clone.Branch,
		clone.TaskID,
		formatTime(clone.UpdatedAt),
		project,
		clone.Path,
	); execErr != nil {
		err = fmt.Errorf("update reserved clone: %w", execErr)
		return Clone{}, err
	}

	if commitErr := tx.Commit(); commitErr != nil {
		err = fmt.Errorf("commit clone reservation: %w", commitErr)
		return Clone{}, err
	}

	return clone, nil
}

func (s *Store) PutTask(ctx context.Context, task Task) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO tasks (
			project, issue_id, status, branch, clone_path, worker_id, prompt, pr_number,
			nudge_count, last_capture, last_activity_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, issue_id) DO UPDATE SET
			status = excluded.status,
			branch = excluded.branch,
			clone_path = excluded.clone_path,
			worker_id = excluded.worker_id,
			prompt = excluded.prompt,
			pr_number = excluded.pr_number,
			nudge_count = excluded.nudge_count,
			last_capture = excluded.last_capture,
			last_activity_at = excluded.last_activity_at,
			updated_at = excluded.updated_at`,
		task.Project,
		task.IssueID,
		task.Status,
		task.Branch,
		task.ClonePath,
		task.WorkerID,
		task.Prompt,
		task.PRNumber,
		task.NudgeCount,
		task.LastCapture,
		formatTime(task.LastActivityAt),
		formatTime(task.CreatedAt),
		formatTime(task.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("put task %s: %w", task.IssueID, err)
	}
	return nil
}

func (s *Store) PutWorker(ctx context.Context, worker Worker) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO workers (
			project, pane_id, pane_name, agent, task_id, clone_path, state, last_capture,
			last_activity_at, nudge_count, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(project, pane_id) DO UPDATE SET
			pane_name = excluded.pane_name,
			agent = excluded.agent,
			task_id = excluded.task_id,
			clone_path = excluded.clone_path,
			state = excluded.state,
			last_capture = excluded.last_capture,
			last_activity_at = excluded.last_activity_at,
			nudge_count = excluded.nudge_count,
			updated_at = excluded.updated_at`,
		worker.Project,
		worker.PaneID,
		worker.PaneName,
		worker.Agent,
		worker.TaskID,
		worker.ClonePath,
		worker.State,
		worker.LastCapture,
		formatTime(worker.LastActivityAt),
		worker.NudgeCount,
		formatTime(worker.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("put worker %s: %w", worker.PaneID, err)
	}
	return nil
}

func (s *Store) PutClone(ctx context.Context, clone Clone) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO clones (project, path, status, branch, task_id, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT(project, path) DO UPDATE SET
			status = excluded.status,
			branch = excluded.branch,
			task_id = excluded.task_id,
			updated_at = excluded.updated_at`,
		clone.Project,
		clone.Path,
		clone.Status,
		clone.Branch,
		clone.TaskID,
		formatTime(clone.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("put clone %s: %w", clone.Path, err)
	}
	return nil
}

func (s *Store) Task(ctx context.Context, project, issueID string) (Task, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT status, branch, clone_path, worker_id, prompt, pr_number, nudge_count,
		        last_capture, last_activity_at, created_at, updated_at
		 FROM tasks
		 WHERE project = ? AND issue_id = ?`,
		project,
		issueID,
	)

	var task Task
	var lastActivityAt, createdAt, updatedAt string
	task.Project = project
	task.IssueID = issueID
	if err := row.Scan(
		&task.Status,
		&task.Branch,
		&task.ClonePath,
		&task.WorkerID,
		&task.Prompt,
		&task.PRNumber,
		&task.NudgeCount,
		&task.LastCapture,
		&lastActivityAt,
		&createdAt,
		&updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Task{}, ErrTaskNotFound
		}
		return Task{}, fmt.Errorf("load task %s: %w", issueID, err)
	}

	task.LastActivityAt = parseTime(lastActivityAt)
	task.CreatedAt = parseTime(createdAt)
	task.UpdatedAt = parseTime(updatedAt)
	return task, nil
}

func (s *Store) Worker(ctx context.Context, project, paneID string) (Worker, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT pane_name, agent, task_id, clone_path, state, last_capture,
		        last_activity_at, nudge_count, updated_at
		 FROM workers
		 WHERE project = ? AND pane_id = ?`,
		project,
		paneID,
	)

	var worker Worker
	var lastActivityAt, updatedAt string
	worker.Project = project
	worker.PaneID = paneID
	if err := row.Scan(
		&worker.PaneName,
		&worker.Agent,
		&worker.TaskID,
		&worker.ClonePath,
		&worker.State,
		&worker.LastCapture,
		&lastActivityAt,
		&worker.NudgeCount,
		&updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Worker{}, ErrWorkerNotFound
		}
		return Worker{}, fmt.Errorf("load worker %s: %w", paneID, err)
	}

	worker.LastActivityAt = parseTime(lastActivityAt)
	worker.UpdatedAt = parseTime(updatedAt)
	return worker, nil
}

func (s *Store) Clone(ctx context.Context, project, path string) (Clone, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT status, branch, task_id, updated_at
		 FROM clones
		 WHERE project = ? AND path = ?`,
		project,
		path,
	)

	var clone Clone
	var updatedAt string
	clone.Project = project
	clone.Path = path
	if err := row.Scan(&clone.Status, &clone.Branch, &clone.TaskID, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Clone{}, ErrCloneNotFound
		}
		return Clone{}, fmt.Errorf("load clone %s: %w", path, err)
	}

	clone.UpdatedAt = parseTime(updatedAt)
	return clone, nil
}

func (s *Store) Clones(ctx context.Context, project string) ([]Clone, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT path, status, branch, task_id, updated_at
		 FROM clones
		 WHERE project = ?
		 ORDER BY path`,
		project,
	)
	if err != nil {
		return nil, fmt.Errorf("list clones: %w", err)
	}
	defer rows.Close()

	var clones []Clone
	for rows.Next() {
		var clone Clone
		var updatedAt string
		clone.Project = project
		if err := rows.Scan(&clone.Path, &clone.Status, &clone.Branch, &clone.TaskID, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan clone row: %w", err)
		}
		clone.UpdatedAt = parseTime(updatedAt)
		clones = append(clones, clone)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clones: %w", err)
	}
	return clones, nil
}

func (s *Store) ActiveTasks(ctx context.Context, project string) ([]Task, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT issue_id, status, branch, clone_path, worker_id, prompt, pr_number, nudge_count,
		        last_capture, last_activity_at, created_at, updated_at
		 FROM tasks
		 WHERE project = ? AND status = ?
		 ORDER BY issue_id`,
		project,
		TaskStatusActive,
	)
	if err != nil {
		return nil, fmt.Errorf("list active tasks: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		var task Task
		var lastActivityAt, createdAt, updatedAt string
		task.Project = project
		if err := rows.Scan(
			&task.IssueID,
			&task.Status,
			&task.Branch,
			&task.ClonePath,
			&task.WorkerID,
			&task.Prompt,
			&task.PRNumber,
			&task.NudgeCount,
			&task.LastCapture,
			&lastActivityAt,
			&createdAt,
			&updatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan active task row: %w", err)
		}
		task.LastActivityAt = parseTime(lastActivityAt)
		task.CreatedAt = parseTime(createdAt)
		task.UpdatedAt = parseTime(updatedAt)
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active tasks: %w", err)
	}
	return tasks, nil
}

func (s *Store) LogEvent(ctx context.Context, event Event) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO events (project, type, task_id, worker_id, clone_path, message, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		event.Project,
		event.Type,
		event.TaskID,
		event.WorkerID,
		event.ClonePath,
		event.Message,
		formatTime(event.CreatedAt),
	)
	if err != nil {
		return fmt.Errorf("log event %s: %w", event.Type, err)
	}
	return nil
}

func (s *Store) Events(ctx context.Context, project string) ([]Event, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, type, task_id, worker_id, clone_path, message, created_at
		 FROM events
		 WHERE project = ?
		 ORDER BY id`,
		project,
	)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var event Event
		var createdAt string
		event.Project = project
		if err := rows.Scan(
			&event.ID,
			&event.Type,
			&event.TaskID,
			&event.WorkerID,
			&event.ClonePath,
			&event.Message,
			&createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan event row: %w", err)
		}
		event.CreatedAt = parseTime(createdAt)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}
