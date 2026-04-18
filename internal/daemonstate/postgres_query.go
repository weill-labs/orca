package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	legacy "github.com/weill-labs/orca/internal/state"
)

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

	events, err := s.queryEvents(ctx, project, issue, 0)
	if err != nil {
		return TaskStatus{}, err
	}

	return TaskStatus{
		Task:   task,
		Events: events,
	}, nil
}

func (s *PostgresStore) ListWorkers(ctx context.Context, project string) ([]Worker, error) {
	if isGlobalProject(project) {
		return s.allWorkers(ctx)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE project = $1
		ORDER BY last_seen_at DESC, worker_id ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list workers: %w", err)
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		worker, err := scanPostgresWorker(rows, false)
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
	row := s.pool.QueryRow(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE project = $1 AND worker_id = $2
	`, project, workerID)

	worker, err := scanPostgresWorker(row, false)
	if errors.Is(err, pgx.ErrNoRows) {
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
		SELECT project, worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE current_pane_id = $1
	`
	args := []any{paneID}
	if !isGlobalProject(project) {
		query += ` AND project = $2`
		args = append(args, project)
	}

	row := s.pool.QueryRow(ctx, query, args...)
	worker, err := scanPostgresWorker(row, true)
	if errors.Is(err, pgx.ErrNoRows) {
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

	rows, err := s.pool.Query(ctx, `
		SELECT path, status, issue, branch, updated_at
		FROM clones
		WHERE project = $1
		ORDER BY updated_at DESC, path ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list clones: %w", err)
	}
	defer rows.Close()

	clones := make([]Clone, 0)
	for rows.Next() {
		var clone Clone
		if err := rows.Scan(&clone.Path, &clone.Status, &clone.Issue, &clone.Branch, &clone.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan clone: %w", err)
		}
		clone.UpdatedAt = normalizeTime(clone.UpdatedAt)
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

	rows, err := s.pool.Query(ctx, `
		SELECT t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = $1 AND t.status IN ('starting', 'active')
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list non-terminal tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanPostgresTask(rows, false)
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
		query += ` AND c.project = $1`
		args = append(args, project)
	}
	query += ` ORDER BY c.updated_at DESC, c.project ASC, c.path ASC`

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list stale clone occupancies: %w", err)
	}
	defer rows.Close()

	occupancies := make([]CloneOccupancy, 0)
	for rows.Next() {
		var occupancy CloneOccupancy
		if err := rows.Scan(&occupancy.Project, &occupancy.Path, &occupancy.CurrentBranch, &occupancy.AssignedTask, &occupancy.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan stale clone occupancy: %w", err)
		}
		occupancy.UpdatedAt = normalizeTime(occupancy.UpdatedAt)
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

func (s *PostgresStore) ActiveAssignments(ctx context.Context, project string) ([]Assignment, error) {
	if isGlobalProject(project) {
		return s.AllActiveAssignments(ctx)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = $1 AND t.status = 'active'
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list active assignments: %w", err)
	}
	defer rows.Close()

	assignments := make([]Assignment, 0)
	for rows.Next() {
		assignment, err := scanPostgresAssignment(rows, false)
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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.issue = $1 AND t.status = 'active'
	`
	args := []any{issue}
	if !isGlobalProject(project) {
		query += ` AND t.project = $2`
		args = append(args, project)
	}

	row := s.pool.QueryRow(ctx, query, args...)
	assignment, err := scanPostgresAssignment(row, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lookup active assignment by issue: %w", err)
	}
	return assignment, nil
}

func (s *PostgresStore) ActiveAssignmentByBranch(ctx context.Context, project, branch string) (Assignment, error) {
	query := `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.branch = $1 AND t.status = 'active'
	`
	args := []any{branch}
	if !isGlobalProject(project) {
		query += ` AND t.project = $2`
		args = append(args, project)
	}
	query += ` ORDER BY t.updated_at DESC, t.issue ASC LIMIT 1`

	row := s.pool.QueryRow(ctx, query, args...)
	assignment, err := scanPostgresAssignment(row, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lookup active assignment by branch: %w", err)
	}
	return assignment, nil
}

func (s *PostgresStore) ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (Assignment, error) {
	query := `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.pr_number = $1 AND t.status = 'active'
	`
	args := []any{prNumber}
	if !isGlobalProject(project) {
		query += ` AND t.project = $2`
		args = append(args, project)
	}

	row := s.pool.QueryRow(ctx, query, args...)
	assignment, err := scanPostgresAssignment(row, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return Assignment{}, ErrNotFound
	}
	if err != nil {
		return Assignment{}, fmt.Errorf("lookup active assignment by pr number: %w", err)
	}
	return assignment, nil
}

func (s *PostgresStore) MergeEntry(ctx context.Context, project string, prNumber int) (*MergeQueueEntry, error) {
	query := `
		SELECT project, issue, pr_number, status, created_at, updated_at
		FROM merge_queue
		WHERE pr_number = $1
	`
	args := []any{prNumber}
	if !isGlobalProject(project) {
		query += ` AND project = $2`
		args = append(args, project)
	}

	row := s.pool.QueryRow(ctx, query, args...)
	entry, err := scanPostgresMergeQueueEntry(row)
	if errors.Is(err, pgx.ErrNoRows) {
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

	rows, err := s.pool.Query(ctx, `
		SELECT project, issue, pr_number, status, created_at, updated_at
		FROM merge_queue
		WHERE project = $1
		ORDER BY created_at ASC, pr_number ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list merge entries: %w", err)
	}
	defer rows.Close()

	entries := make([]MergeQueueEntry, 0)
	for rows.Next() {
		entry, err := scanPostgresMergeQueueEntry(rows)
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

func (s *PostgresStore) lookupDaemon(ctx context.Context, project string) (*DaemonStatus, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT session, pid, status, started_at, updated_at
		FROM daemon_status
		WHERE project = $1
	`, project)

	var daemon DaemonStatus
	if err := row.Scan(&daemon.Session, &daemon.PID, &daemon.Status, &daemon.StartedAt, &daemon.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("lookup daemon: %w", err)
	}
	daemon.StartedAt = normalizeTime(daemon.StartedAt)
	daemon.UpdatedAt = normalizeTime(daemon.UpdatedAt)
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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.issue = $1
	`
	args := []any{issue}
	if !includeProject {
		query += ` AND t.project = $2`
		args = append(args, project)
	}

	row := s.pool.QueryRow(ctx, query, args...)
	task, err := scanPostgresTask(row, includeProject)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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

	rows, err := s.pool.Query(ctx, `
		SELECT t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = $1
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanPostgresTask(rows, false)
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
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at
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
		task, err := scanPostgresTask(rows, true)
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
	query := `
		SELECT id, project, kind, issue, worker_id, message, payload, created_at
		FROM events
		WHERE project = $1 AND id > $2
	`
	args := []any{project, afterID}
	if issue != "" {
		query += ` AND issue = $3`
		args = append(args, issue)
	}
	query += ` ORDER BY id ASC`

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query events: %w", err)
	}
	defer rows.Close()

	events := make([]Event, 0)
	for rows.Next() {
		var event Event
		var payload sql.NullString
		if err := rows.Scan(&event.ID, &event.Project, &event.Kind, &event.Issue, &event.WorkerID, &event.Message, &payload, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan event: %w", err)
		}
		if payload.Valid {
			event.Payload = []byte(payload.String)
		}
		event.CreatedAt = normalizeTime(event.CreatedAt)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate events: %w", err)
	}
	return events, nil
}

func (s *PostgresStore) lookupCloneRecord(ctx context.Context, project, path string) (legacy.CloneRecord, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT project, path, status, branch, issue
		FROM clones
		WHERE project = $1 AND path = $2
	`, project, path)

	var record legacy.CloneRecord
	if err := row.Scan(&record.Project, &record.Path, &record.Status, &record.CurrentBranch, &record.AssignedTask); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return legacy.CloneRecord{}, legacy.ErrCloneNotFound
		}
		return legacy.CloneRecord{}, err
	}
	return record, nil
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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE w.current_pane_id = $1
	`
	args := []any{paneID}
	if !includeProject {
		query += ` AND t.project = $2`
		args = append(args, project)
	}
	query += ` ORDER BY t.created_at ASC, t.updated_at ASC, t.issue ASC`

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list tasks by pane: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanPostgresTask(rows, includeProject)
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
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.status IN ('starting', 'active')
		ORDER BY t.updated_at DESC, t.project ASC, t.issue ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all non-terminal tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanPostgresTask(rows, true)
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
	rows, err := s.pool.Query(ctx, `
		SELECT project, worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		ORDER BY last_seen_at DESC, project ASC, worker_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list all workers: %w", err)
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		worker, err := scanPostgresWorker(rows, true)
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
	rows, err := s.pool.Query(ctx, `
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
		if err := rows.Scan(&clone.Path, &clone.Status, &clone.Issue, &clone.Branch, &clone.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan all clone: %w", err)
		}
		clone.UpdatedAt = normalizeTime(clone.UpdatedAt)
		clones = append(clones, clone)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all clones: %w", err)
	}
	return clones, nil
}

func (s *PostgresStore) AllActiveAssignments(ctx context.Context) ([]Assignment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
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
		assignment, err := scanPostgresAssignment(rows, true)
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
	rows, err := s.pool.Query(ctx, `
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
		entry, err := scanPostgresMergeQueueEntry(rows)
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
