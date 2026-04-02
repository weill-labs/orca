package state

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

const schemaVersion = 2

var ErrNotFound = errors.New("state: not found")

type Store struct {
	db      *sql.DB
	project string
}

type Task struct {
	IssueID            string
	Status             string
	AssignedWorkerPane string
	AssignedClonePath  string
	PRNumber           *int
	CreatedAt          time.Time
	UpdatedAt          time.Time
	StartedAt          *time.Time
	CompletedAt        *time.Time
}

type Worker struct {
	PaneID           string
	PaneName         string
	AgentProfile     string
	CurrentTaskIssue string
	HealthState      string
	ClonePath        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	LastHeartbeatAt  *time.Time
}

type Clone struct {
	Path              string
	Status            string
	CurrentBranch     string
	AssignedTaskIssue string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

type Event struct {
	ID           int64
	TaskIssueID  string
	WorkerPaneID string
	ClonePath    string
	EventType    string
	Message      string
	CreatedAt    time.Time
}

type migration struct {
	version    int
	statements []string
}

func Open(path, project string) (*Store, error) {
	if project == "" {
		return nil, errors.New("state: project is required")
	}

	canonicalProject, err := canonicalizeProject(project)
	if err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create database directory: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	store := &Store{
		db:      db,
		project: canonicalProject,
	}

	if err := store.configure(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) CreateTask(ctx context.Context, task Task) error {
	task.CreatedAt = defaultTime(task.CreatedAt)
	task.UpdatedAt = defaultTime(task.UpdatedAt, task.CreatedAt)

	_, err := s.db.ExecContext(ctx, `
INSERT INTO tasks (
	project,
	issue_id,
	status,
	assigned_worker_pane,
	assigned_clone_path,
	pr_number,
	created_at,
	updated_at,
	started_at,
	completed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`,
		s.project,
		task.IssueID,
		task.Status,
		nullableString(task.AssignedWorkerPane),
		nullableString(task.AssignedClonePath),
		nullableInt(task.PRNumber),
		formatTime(task.CreatedAt),
		formatTime(task.UpdatedAt),
		nullableTime(task.StartedAt),
		nullableTime(task.CompletedAt),
	)
	if err != nil {
		return fmt.Errorf("create task %q: %w", task.IssueID, err)
	}

	return nil
}

func (s *Store) UpdateTask(ctx context.Context, task Task) error {
	task.UpdatedAt = defaultTime(task.UpdatedAt)

	result, err := s.db.ExecContext(ctx, `
UPDATE tasks
SET
	status = ?,
	assigned_worker_pane = ?,
	assigned_clone_path = ?,
	pr_number = ?,
	updated_at = ?,
	started_at = ?,
	completed_at = ?
WHERE project = ? AND issue_id = ?;
`,
		task.Status,
		nullableString(task.AssignedWorkerPane),
		nullableString(task.AssignedClonePath),
		nullableInt(task.PRNumber),
		formatTime(task.UpdatedAt),
		nullableTime(task.StartedAt),
		nullableTime(task.CompletedAt),
		s.project,
		task.IssueID,
	)
	if err != nil {
		return fmt.Errorf("update task %q: %w", task.IssueID, err)
	}

	return checkRowsAffected(result, "task", task.IssueID)
}

func (s *Store) GetTask(ctx context.Context, issueID string) (Task, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT
	issue_id,
	status,
	assigned_worker_pane,
	assigned_clone_path,
	pr_number,
	created_at,
	updated_at,
	started_at,
	completed_at
FROM tasks
WHERE project = ? AND issue_id = ?;
`, s.project, issueID)

	task, err := scanTask(row)
	if err != nil {
		return Task{}, err
	}
	return task, nil
}

func (s *Store) ListTasks(ctx context.Context) ([]Task, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT
	issue_id,
	status,
	assigned_worker_pane,
	assigned_clone_path,
	pr_number,
	created_at,
	updated_at,
	started_at,
	completed_at
FROM tasks
WHERE project = ?
ORDER BY issue_id;
`, s.project)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()

	var tasks []Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}

	return tasks, nil
}

func (s *Store) DeleteTask(ctx context.Context, issueID string) error {
	result, err := s.db.ExecContext(ctx, `
DELETE FROM tasks
WHERE project = ? AND issue_id = ?;
`, s.project, issueID)
	if err != nil {
		return fmt.Errorf("delete task %q: %w", issueID, err)
	}

	return checkRowsAffected(result, "task", issueID)
}

func (s *Store) CreateWorker(ctx context.Context, worker Worker) error {
	worker.CreatedAt = defaultTime(worker.CreatedAt)
	worker.UpdatedAt = defaultTime(worker.UpdatedAt, worker.CreatedAt)

	_, err := s.db.ExecContext(ctx, `
INSERT INTO workers (
	project,
	pane_id,
	pane_name,
	agent_profile,
	current_task_issue,
	health_state,
	clone_path,
	created_at,
	updated_at,
	last_heartbeat_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
`,
		s.project,
		worker.PaneID,
		worker.PaneName,
		worker.AgentProfile,
		nullableString(worker.CurrentTaskIssue),
		worker.HealthState,
		nullableString(worker.ClonePath),
		formatTime(worker.CreatedAt),
		formatTime(worker.UpdatedAt),
		nullableTime(worker.LastHeartbeatAt),
	)
	if err != nil {
		return fmt.Errorf("create worker %q: %w", worker.PaneID, err)
	}

	return nil
}

func (s *Store) UpdateWorker(ctx context.Context, worker Worker) error {
	worker.UpdatedAt = defaultTime(worker.UpdatedAt)

	result, err := s.db.ExecContext(ctx, `
UPDATE workers
SET
	pane_name = ?,
	agent_profile = ?,
	current_task_issue = ?,
	health_state = ?,
	clone_path = ?,
	updated_at = ?,
	last_heartbeat_at = ?
WHERE project = ? AND pane_id = ?;
`,
		worker.PaneName,
		worker.AgentProfile,
		nullableString(worker.CurrentTaskIssue),
		worker.HealthState,
		nullableString(worker.ClonePath),
		formatTime(worker.UpdatedAt),
		nullableTime(worker.LastHeartbeatAt),
		s.project,
		worker.PaneID,
	)
	if err != nil {
		return fmt.Errorf("update worker %q: %w", worker.PaneID, err)
	}

	return checkRowsAffected(result, "worker", worker.PaneID)
}

func (s *Store) GetWorker(ctx context.Context, paneID string) (Worker, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT
	pane_id,
	pane_name,
	agent_profile,
	current_task_issue,
	health_state,
	clone_path,
	created_at,
	updated_at,
	last_heartbeat_at
FROM workers
WHERE project = ? AND pane_id = ?;
`, s.project, paneID)

	worker, err := scanWorker(row)
	if err != nil {
		return Worker{}, err
	}
	return worker, nil
}

func (s *Store) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT
	pane_id,
	pane_name,
	agent_profile,
	current_task_issue,
	health_state,
	clone_path,
	created_at,
	updated_at,
	last_heartbeat_at
FROM workers
WHERE project = ?
ORDER BY pane_id;
`, s.project)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	var workers []Worker
	for rows.Next() {
		worker, err := scanWorker(rows)
		if err != nil {
			return nil, err
		}
		workers = append(workers, worker)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}

	return workers, nil
}

func (s *Store) DeleteWorker(ctx context.Context, paneID string) error {
	result, err := s.db.ExecContext(ctx, `
DELETE FROM workers
WHERE project = ? AND pane_id = ?;
`, s.project, paneID)
	if err != nil {
		return fmt.Errorf("delete worker %q: %w", paneID, err)
	}

	return checkRowsAffected(result, "worker", paneID)
}

func (s *Store) CreateClone(ctx context.Context, clone Clone) error {
	clone.CreatedAt = defaultTime(clone.CreatedAt)
	clone.UpdatedAt = defaultTime(clone.UpdatedAt, clone.CreatedAt)

	_, err := s.db.ExecContext(ctx, `
INSERT INTO clones (
	project,
	path,
	status,
	current_branch,
	assigned_task_issue,
	created_at,
	updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?);
`,
		s.project,
		clone.Path,
		clone.Status,
		clone.CurrentBranch,
		nullableString(clone.AssignedTaskIssue),
		formatTime(clone.CreatedAt),
		formatTime(clone.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("create clone %q: %w", clone.Path, err)
	}

	return nil
}

func (s *Store) UpdateClone(ctx context.Context, clone Clone) error {
	clone.UpdatedAt = defaultTime(clone.UpdatedAt)

	result, err := s.db.ExecContext(ctx, `
UPDATE clones
SET
	status = ?,
	current_branch = ?,
	assigned_task_issue = ?,
	updated_at = ?
WHERE project = ? AND path = ?;
`,
		clone.Status,
		clone.CurrentBranch,
		nullableString(clone.AssignedTaskIssue),
		formatTime(clone.UpdatedAt),
		s.project,
		clone.Path,
	)
	if err != nil {
		return fmt.Errorf("update clone %q: %w", clone.Path, err)
	}

	return checkRowsAffected(result, "clone", clone.Path)
}

func (s *Store) GetClone(ctx context.Context, path string) (Clone, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT
	path,
	status,
	current_branch,
	assigned_task_issue,
	created_at,
	updated_at
FROM clones
WHERE project = ? AND path = ?;
`, s.project, path)

	clone, err := scanClone(row)
	if err != nil {
		return Clone{}, err
	}
	return clone, nil
}

func (s *Store) ListClones(ctx context.Context) ([]Clone, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT
	path,
	status,
	current_branch,
	assigned_task_issue,
	created_at,
	updated_at
FROM clones
WHERE project = ?
ORDER BY path;
`, s.project)
	if err != nil {
		return nil, fmt.Errorf("list clones: %w", err)
	}
	defer rows.Close()

	var clones []Clone
	for rows.Next() {
		clone, err := scanClone(rows)
		if err != nil {
			return nil, err
		}
		clones = append(clones, clone)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list clones: %w", err)
	}

	return clones, nil
}

func (s *Store) DeleteClone(ctx context.Context, path string) error {
	result, err := s.db.ExecContext(ctx, `
DELETE FROM clones
WHERE project = ? AND path = ?;
`, s.project, path)
	if err != nil {
		return fmt.Errorf("delete clone %q: %w", path, err)
	}

	return checkRowsAffected(result, "clone", path)
}

func (s *Store) CreateEvent(ctx context.Context, event Event) (int64, error) {
	event.CreatedAt = defaultTime(event.CreatedAt)

	result, err := s.db.ExecContext(ctx, `
INSERT INTO event_log (
	project,
	task_issue_id,
	worker_pane_id,
	clone_path,
	event_type,
	message,
	created_at
) VALUES (?, ?, ?, ?, ?, ?, ?);
`,
		s.project,
		nullableString(event.TaskIssueID),
		nullableString(event.WorkerPaneID),
		nullableString(event.ClonePath),
		event.EventType,
		event.Message,
		formatTime(event.CreatedAt),
	)
	if err != nil {
		return 0, fmt.Errorf("create event %q: %w", event.EventType, err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read event id: %w", err)
	}

	return id, nil
}

func (s *Store) UpdateEvent(ctx context.Context, event Event) error {
	result, err := s.db.ExecContext(ctx, `
UPDATE event_log
SET
	task_issue_id = ?,
	worker_pane_id = ?,
	clone_path = ?,
	event_type = ?,
	message = ?,
	created_at = ?
WHERE project = ? AND id = ?;
`,
		nullableString(event.TaskIssueID),
		nullableString(event.WorkerPaneID),
		nullableString(event.ClonePath),
		event.EventType,
		event.Message,
		formatTime(defaultTime(event.CreatedAt)),
		s.project,
		event.ID,
	)
	if err != nil {
		return fmt.Errorf("update event %d: %w", event.ID, err)
	}

	return checkRowsAffected(result, "event", fmt.Sprintf("%d", event.ID))
}

func (s *Store) GetEvent(ctx context.Context, id int64) (Event, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT
	id,
	task_issue_id,
	worker_pane_id,
	clone_path,
	event_type,
	message,
	created_at
FROM event_log
WHERE project = ? AND id = ?;
`, s.project, id)

	event, err := scanEvent(row)
	if err != nil {
		return Event{}, err
	}
	return event, nil
}

func (s *Store) ListEvents(ctx context.Context) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT
	id,
	task_issue_id,
	worker_pane_id,
	clone_path,
	event_type,
	message,
	created_at
FROM event_log
WHERE project = ?
ORDER BY id;
`, s.project)
	if err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		event, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list events: %w", err)
	}

	return events, nil
}

func (s *Store) DeleteEvent(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, `
DELETE FROM event_log
WHERE project = ? AND id = ?;
`, s.project, id)
	if err != nil {
		return fmt.Errorf("delete event %d: %w", id, err)
	}

	return checkRowsAffected(result, "event", fmt.Sprintf("%d", id))
}

func (s *Store) configure() error {
	if _, err := s.db.Exec(`PRAGMA busy_timeout = 5000;`); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := s.db.Exec(`PRAGMA journal_mode = WAL;`); err != nil {
		return fmt.Errorf("enable WAL mode: %w", err)
	}

	if err := s.migrate(); err != nil {
		return err
	}

	return nil
}

func (s *Store) migrate() error {
	currentVersion, err := currentSchemaVersion(s.db)
	if err != nil {
		return err
	}

	for _, migration := range migrations() {
		if migration.version <= currentVersion {
			continue
		}

		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", migration.version, err)
		}

		for _, stmt := range migration.statements {
			if _, err := tx.Exec(stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply migration %d: %w", migration.version, err)
			}
		}

		if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d;`, migration.version)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("set user_version %d: %w", migration.version, err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", migration.version, err)
		}
	}

	return nil
}

func currentSchemaVersion(db *sql.DB) (int, error) {
	var version int
	if err := db.QueryRow(`PRAGMA user_version;`).Scan(&version); err != nil {
		return 0, fmt.Errorf("read user_version: %w", err)
	}
	return version, nil
}

func migrations() []migration {
	return []migration{
		{
			version: 1,
			statements: []string{
				`CREATE TABLE IF NOT EXISTS tasks (
					project TEXT NOT NULL,
					issue_id TEXT NOT NULL,
					status TEXT NOT NULL,
					assigned_worker_pane TEXT,
					assigned_clone_path TEXT,
					pr_number INTEGER,
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL,
					started_at TEXT,
					completed_at TEXT,
					PRIMARY KEY (project, issue_id)
				);`,
				`CREATE TABLE IF NOT EXISTS workers (
					project TEXT NOT NULL,
					pane_id TEXT NOT NULL,
					pane_name TEXT NOT NULL,
					agent_profile TEXT NOT NULL,
					current_task_issue TEXT,
					health_state TEXT NOT NULL,
					clone_path TEXT,
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL,
					last_heartbeat_at TEXT,
					PRIMARY KEY (project, pane_id)
				);`,
				`CREATE TABLE IF NOT EXISTS clones (
					project TEXT NOT NULL,
					path TEXT NOT NULL,
					status TEXT NOT NULL,
					current_branch TEXT NOT NULL,
					assigned_task_issue TEXT,
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL,
					PRIMARY KEY (project, path)
				);`,
				`CREATE TABLE IF NOT EXISTS event_log (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					project TEXT NOT NULL,
					task_issue_id TEXT,
					worker_pane_id TEXT,
					clone_path TEXT,
					event_type TEXT NOT NULL,
					message TEXT NOT NULL,
					created_at TEXT NOT NULL
				);`,
			},
		},
		{
			version: 2,
			statements: []string{
				`CREATE INDEX IF NOT EXISTS idx_tasks_project_status
				ON tasks(project, status);`,
				`CREATE INDEX IF NOT EXISTS idx_workers_project_health_state
				ON workers(project, health_state);`,
				`CREATE INDEX IF NOT EXISTS idx_clones_project_status
				ON clones(project, status);`,
				`CREATE INDEX IF NOT EXISTS idx_event_log_project_created_at
				ON event_log(project, created_at DESC);`,
			},
		},
	}
}

type scanner interface {
	Scan(dest ...any) error
}

func scanTask(s scanner) (Task, error) {
	var (
		task        Task
		workerPane  sql.NullString
		clonePath   sql.NullString
		prNumber    sql.NullInt64
		createdAt   string
		updatedAt   string
		startedAt   sql.NullString
		completedAt sql.NullString
	)

	err := s.Scan(
		&task.IssueID,
		&task.Status,
		&workerPane,
		&clonePath,
		&prNumber,
		&createdAt,
		&updatedAt,
		&startedAt,
		&completedAt,
	)
	if err != nil {
		return Task{}, mapNotFound(err, "task")
	}

	task.AssignedWorkerPane = workerPane.String
	task.AssignedClonePath = clonePath.String
	if prNumber.Valid {
		value := int(prNumber.Int64)
		task.PRNumber = &value
	}

	var parseErr error
	task.CreatedAt, parseErr = parseTime(createdAt)
	if parseErr != nil {
		return Task{}, parseErr
	}
	task.UpdatedAt, parseErr = parseTime(updatedAt)
	if parseErr != nil {
		return Task{}, parseErr
	}
	task.StartedAt, parseErr = parseOptionalTime(startedAt)
	if parseErr != nil {
		return Task{}, parseErr
	}
	task.CompletedAt, parseErr = parseOptionalTime(completedAt)
	if parseErr != nil {
		return Task{}, parseErr
	}

	return task, nil
}

func scanWorker(s scanner) (Worker, error) {
	var (
		worker        Worker
		currentTask   sql.NullString
		clonePath     sql.NullString
		createdAt     string
		updatedAt     string
		lastHeartbeat sql.NullString
	)

	err := s.Scan(
		&worker.PaneID,
		&worker.PaneName,
		&worker.AgentProfile,
		&currentTask,
		&worker.HealthState,
		&clonePath,
		&createdAt,
		&updatedAt,
		&lastHeartbeat,
	)
	if err != nil {
		return Worker{}, mapNotFound(err, "worker")
	}

	worker.CurrentTaskIssue = currentTask.String
	worker.ClonePath = clonePath.String

	var parseErr error
	worker.CreatedAt, parseErr = parseTime(createdAt)
	if parseErr != nil {
		return Worker{}, parseErr
	}
	worker.UpdatedAt, parseErr = parseTime(updatedAt)
	if parseErr != nil {
		return Worker{}, parseErr
	}
	worker.LastHeartbeatAt, parseErr = parseOptionalTime(lastHeartbeat)
	if parseErr != nil {
		return Worker{}, parseErr
	}

	return worker, nil
}

func scanClone(s scanner) (Clone, error) {
	var (
		clone        Clone
		assignedTask sql.NullString
		createdAt    string
		updatedAt    string
	)

	err := s.Scan(
		&clone.Path,
		&clone.Status,
		&clone.CurrentBranch,
		&assignedTask,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		return Clone{}, mapNotFound(err, "clone")
	}

	clone.AssignedTaskIssue = assignedTask.String

	var parseErr error
	clone.CreatedAt, parseErr = parseTime(createdAt)
	if parseErr != nil {
		return Clone{}, parseErr
	}
	clone.UpdatedAt, parseErr = parseTime(updatedAt)
	if parseErr != nil {
		return Clone{}, parseErr
	}

	return clone, nil
}

func scanEvent(s scanner) (Event, error) {
	var (
		event      Event
		taskIssue  sql.NullString
		workerPane sql.NullString
		clonePath  sql.NullString
		createdAt  string
	)

	err := s.Scan(
		&event.ID,
		&taskIssue,
		&workerPane,
		&clonePath,
		&event.EventType,
		&event.Message,
		&createdAt,
	)
	if err != nil {
		return Event{}, mapNotFound(err, "event")
	}

	event.TaskIssueID = taskIssue.String
	event.WorkerPaneID = workerPane.String
	event.ClonePath = clonePath.String

	parsedCreatedAt, err := parseTime(createdAt)
	if err != nil {
		return Event{}, err
	}
	event.CreatedAt = parsedCreatedAt

	return event, nil
}

func canonicalizeProject(project string) (string, error) {
	absPath, err := filepath.Abs(project)
	if err != nil {
		return "", fmt.Errorf("canonicalize project path: %w", err)
	}

	resolvedPath, err := filepath.EvalSymlinks(absPath)
	if err == nil {
		return resolvedPath, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return filepath.Clean(absPath), nil
	}

	return "", fmt.Errorf("canonicalize project path: %w", err)
}

func checkRowsAffected(result sql.Result, kind, id string) error {
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for %s %q: %w", kind, id, err)
	}
	if rowsAffected == 0 {
		return ErrNotFound
	}
	return nil
}

func mapNotFound(err error, kind string) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s: %w", kind, ErrNotFound)
	}
	return fmt.Errorf("scan %s: %w", kind, err)
}

func defaultTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Now().UTC()
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time %q: %w", value, err)
	}
	return parsed, nil
}

func parseOptionalTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}

	parsed, err := parseTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}
