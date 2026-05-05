package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
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

func (s *PostgresStore) KnownProjects(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project FROM tasks WHERE BTRIM(project) <> '' AND `+postgresHostMatch("host", 1)+`
		UNION
		SELECT project FROM workers WHERE BTRIM(project) <> '' AND `+postgresHostMatch("host", 1)+`
		UNION
		SELECT project FROM clones WHERE BTRIM(project) <> '' AND `+postgresHostMatch("host", 1)+`
		UNION
		SELECT project FROM daemon_statuses WHERE BTRIM(project) <> '' AND `+postgresHostMatch("host", 1)+`
		UNION
		SELECT project FROM merge_queue WHERE BTRIM(project) <> ''
		ORDER BY project ASC
	`, s.host)
	if err != nil {
		return nil, fmt.Errorf("list known projects: %w", err)
	}
	defer rows.Close()

	projects := make([]string, 0)
	for rows.Next() {
		var project string
		if err := rows.Scan(&project); err != nil {
			return nil, fmt.Errorf("scan known project: %w", err)
		}
		project = strings.TrimSpace(project)
		if project != "" {
			projects = append(projects, project)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate known projects: %w", err)
	}
	return projects, nil
}

func (s *PostgresStore) ProjectStatusAllHosts(ctx context.Context, project string) (ProjectStatus, error) {
	status, err := s.ProjectStatus(ctx, project)
	if err != nil {
		return ProjectStatus{}, err
	}

	daemons, err := s.lookupDaemonsAllHosts(ctx, project)
	if err != nil {
		return ProjectStatus{}, err
	}
	status.Daemons = daemons

	if isGlobalProject(project) {
		tasks, err := s.allTasks(ctx)
		if err != nil {
			return ProjectStatus{}, err
		}
		workers, err := s.allWorkers(ctx)
		if err != nil {
			return ProjectStatus{}, err
		}
		clones, err := s.allClones(ctx)
		if err != nil {
			return ProjectStatus{}, err
		}

		status.Tasks = tasks
		status.Summary = Summary{}
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

	if !isGlobalProject(project) {
		tasks, err := s.listTasksAllHostsForProject(ctx, project)
		if err != nil {
			return ProjectStatus{}, err
		}
		status.Tasks = tasks

		workers, err := s.allWorkers(ctx)
		if err != nil {
			return ProjectStatus{}, err
		}
		clones, err := s.listClonesAllHostsForProject(ctx, project)
		if err != nil {
			return ProjectStatus{}, err
		}

		status.Summary = Summary{}
		for _, task := range tasks {
			if task.Project != project {
				continue
			}
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
			if worker.Project != project {
				continue
			}
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

func (s *PostgresStore) TaskStatusAllHosts(ctx context.Context, project, issue string) (TaskStatus, error) {
	task, err := s.lookupTaskAllHosts(ctx, project, issue)
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
		return s.hostWorkers(ctx)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE project = $1 AND `+postgresHostMatch("host", 2)+`
		ORDER BY last_seen_at DESC, worker_id ASC
	`, project, s.host)
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
		WHERE project = $1 AND worker_id = $2 AND `+postgresHostMatch("host", 3)+`
	`, project, workerID, s.host)

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
		WHERE current_pane_id = $1 AND ` + postgresHostMatch("host", 2)
	args := []any{paneID, s.host}
	if !isGlobalProject(project) {
		query += ` AND project = $3`
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
		return s.hostClones(ctx)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT path, status, issue, branch, updated_at
		FROM clones
		WHERE project = $1 AND `+postgresHostMatch("host", 2)+`
		ORDER BY updated_at DESC, path ASC
	`, project, s.host)
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
		return s.hostNonTerminalTasks(ctx)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND `+postgresHostMatch("w.host", 2)+`
		WHERE t.project = $1 AND `+postgresHostMatch("t.host", 2)+` AND t.status IN ('starting', 'active')
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project, s.host)
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
			AND ` + postgresHostMatch("c.host", 1) + `
			AND (c.issue = '' OR t.issue IS NULL)
	`
	args := []any{s.host}
	if !isGlobalProject(project) {
		query += ` AND c.project = $2`
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
		return s.hostActiveAssignments(ctx)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND `+postgresHostMatch("w.host", 2)+`
		WHERE t.project = $1 AND `+postgresHostMatch("t.host", 2)+` AND t.status = 'active'
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project, s.host)
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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND ` + postgresHostMatch("w.host", 2) + `
		WHERE t.issue = $1 AND ` + postgresHostMatch("t.host", 2) + ` AND t.status = 'active'
	`
	args := []any{issue, s.host}
	if !isGlobalProject(project) {
		query += ` AND t.project = $3`
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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND ` + postgresHostMatch("w.host", 2) + `
		WHERE t.branch = $1 AND ` + postgresHostMatch("t.host", 2) + ` AND t.status = 'active'
	`
	args := []any{branch, s.host}
	if !isGlobalProject(project) {
		query += ` AND t.project = $3`
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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND ` + postgresHostMatch("w.host", 2) + `
		WHERE t.pr_number = $1 AND ` + postgresHostMatch("t.host", 2) + ` AND t.status = 'active'
	`
	args := []any{prNumber, s.host}
	if !isGlobalProject(project) {
		query += ` AND t.project = $3`
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
		SELECT host, session, pid, status, started_at, updated_at
		FROM daemon_statuses
		WHERE project = $1 AND `+postgresHostMatch("host", 2)+`
	`, project, s.host)

	var daemon DaemonStatus
	if err := row.Scan(&daemon.Host, &daemon.Session, &daemon.PID, &daemon.Status, &daemon.StartedAt, &daemon.UpdatedAt); err != nil {
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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND ` + postgresHostMatch("w.host", 2) + `
		WHERE t.issue = $1 AND ` + postgresHostMatch("t.host", 2) + `
	`
	args := []any{issue, s.host}
	if !includeProject {
		query += ` AND t.project = $3`
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
		return s.hostTasks(ctx)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND `+postgresHostMatch("w.host", 2)+`
		WHERE t.project = $1 AND `+postgresHostMatch("t.host", 2)+`
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project, s.host)
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

func (s *PostgresStore) lookupTaskAllHosts(ctx context.Context, project, issue string) (Task, error) {
	includeProject := isGlobalProject(project)
	query := `
		SELECT
	`
	if includeProject {
		query += `		t.project,`
	}
	query += `
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
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
		return Task{}, fmt.Errorf("lookup task all hosts: %w", err)
	}
	if !includeProject {
		task.Project = project
	}
	return task, nil
}

func (s *PostgresStore) listTasksAllHostsForProject(ctx context.Context, project string) ([]Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
		WHERE t.project = $1
		ORDER BY t.updated_at DESC, t.issue ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list tasks all hosts: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanPostgresTask(rows, false)
		if err != nil {
			return nil, fmt.Errorf("scan all-host task: %w", err)
		}
		task.Project = project
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all-host tasks: %w", err)
	}
	return tasks, nil
}

func (s *PostgresStore) hostTasks(ctx context.Context) ([]Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND `+postgresHostMatch("w.host", 1)+`
		WHERE `+postgresHostMatch("t.host", 1)+`
		ORDER BY t.updated_at DESC, t.project ASC, t.issue ASC
	`, s.host)
	if err != nil {
		return nil, fmt.Errorf("list host tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanPostgresTask(rows, true)
		if err != nil {
			return nil, fmt.Errorf("scan host task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate host tasks: %w", err)
	}
	return tasks, nil
}

func (s *PostgresStore) allTasks(ctx context.Context) ([]Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
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

func (s *PostgresStore) lookupDaemonsAllHosts(ctx context.Context, project string) ([]DaemonStatus, error) {
	if isGlobalProject(project) {
		return s.lookupDaemonRowsAllHosts(ctx, "")
	}

	specific, err := s.lookupDaemonRowsAllHosts(ctx, project)
	if err != nil {
		return nil, err
	}
	fallback, err := s.lookupDaemonRowsAllHosts(ctx, "")
	if err != nil {
		return nil, err
	}

	byHost := make(map[string]struct{}, len(specific))
	daemons := make([]DaemonStatus, 0, len(specific)+len(fallback))
	for _, daemon := range specific {
		byHost[daemon.Host] = struct{}{}
		daemons = append(daemons, daemon)
	}
	for _, daemon := range fallback {
		if _, ok := byHost[daemon.Host]; ok {
			continue
		}
		daemons = append(daemons, daemon)
	}
	sort.Slice(daemons, func(i, j int) bool {
		return daemons[i].Host < daemons[j].Host
	})
	return daemons, nil
}

func (s *PostgresStore) lookupDaemonRowsAllHosts(ctx context.Context, project string) ([]DaemonStatus, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT host, session, pid, status, started_at, updated_at
		FROM daemon_statuses
		WHERE project = $1
		ORDER BY host ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list daemon statuses: %w", err)
	}
	defer rows.Close()

	daemons := make([]DaemonStatus, 0)
	for rows.Next() {
		var daemon DaemonStatus
		if err := rows.Scan(&daemon.Host, &daemon.Session, &daemon.PID, &daemon.Status, &daemon.StartedAt, &daemon.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan daemon status: %w", err)
		}
		daemon.StartedAt = normalizeTime(daemon.StartedAt)
		daemon.UpdatedAt = normalizeTime(daemon.UpdatedAt)
		daemons = append(daemons, daemon)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate daemon statuses: %w", err)
	}
	return daemons, nil
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
		WHERE project = $1 AND path = $2 AND `+postgresHostMatch("host", 3)+`
	`, project, path, s.host)

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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND ` + postgresHostMatch("w.host", 2) + `
		WHERE w.current_pane_id = $1 AND ` + postgresHostMatch("t.host", 2) + `
	`
	args := []any{paneID, s.host}
	if !includeProject {
		query += ` AND t.project = $3`
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
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
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

func (s *PostgresStore) hostNonTerminalTasks(ctx context.Context) ([]Task, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at
		FROM tasks t
		LEFT JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND `+postgresHostMatch("w.host", 1)+`
		WHERE `+postgresHostMatch("t.host", 1)+` AND t.status IN ('starting', 'active')
		ORDER BY t.updated_at DESC, t.project ASC, t.issue ASC
	`, s.host)
	if err != nil {
		return nil, fmt.Errorf("list host non-terminal tasks: %w", err)
	}
	defer rows.Close()

	tasks := make([]Task, 0)
	for rows.Next() {
		task, err := scanPostgresTask(rows, true)
		if err != nil {
			return nil, fmt.Errorf("scan host non-terminal task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate host non-terminal tasks: %w", err)
	}
	return tasks, nil
}

func (s *PostgresStore) hostWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT project, worker_id, current_pane_id, agent_profile, state, issue, clone_path, last_review_count, last_inline_review_comment_count, last_issue_comment_count, last_issue_comment_watermark, last_review_updated_at, review_nudge_count, review_approved, last_ci_state, ci_nudge_count, ci_failure_poll_count, ci_escalated, last_mergeable_state, nudge_count, last_capture, last_activity_at, last_pr_number, last_push_at, last_pr_poll_at, restart_count, first_crash_at, created_at, last_seen_at
		FROM workers
		WHERE `+postgresHostMatch("host", 1)+`
		ORDER BY last_seen_at DESC, project ASC, worker_id ASC
	`, s.host)
	if err != nil {
		return nil, fmt.Errorf("list host workers: %w", err)
	}
	defer rows.Close()

	workers := make([]Worker, 0)
	for rows.Next() {
		worker, err := scanPostgresWorker(rows, true)
		if err != nil {
			return nil, fmt.Errorf("scan host worker: %w", err)
		}
		workers = append(workers, worker)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate host workers: %w", err)
	}
	return workers, nil
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

func (s *PostgresStore) hostClones(ctx context.Context) ([]Clone, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT path, status, issue, branch, updated_at
		FROM clones
		WHERE `+postgresHostMatch("host", 1)+`
		ORDER BY updated_at DESC, path ASC
	`, s.host)
	if err != nil {
		return nil, fmt.Errorf("list host clones: %w", err)
	}
	defer rows.Close()

	clones := make([]Clone, 0)
	for rows.Next() {
		var clone Clone
		if err := rows.Scan(&clone.Path, &clone.Status, &clone.Issue, &clone.Branch, &clone.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan host clone: %w", err)
		}
		clone.UpdatedAt = normalizeTime(clone.UpdatedAt)
		clones = append(clones, clone)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate host clones: %w", err)
	}
	return clones, nil
}

func (s *PostgresStore) listClonesAllHostsForProject(ctx context.Context, project string) ([]Clone, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT path, status, issue, branch, updated_at
		FROM clones
		WHERE project = $1
		ORDER BY updated_at DESC, path ASC
	`, project)
	if err != nil {
		return nil, fmt.Errorf("list all-host clones: %w", err)
	}
	defer rows.Close()

	clones := make([]Clone, 0)
	for rows.Next() {
		var clone Clone
		if err := rows.Scan(&clone.Path, &clone.Status, &clone.Issue, &clone.Branch, &clone.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan all-host clone: %w", err)
		}
		clone.UpdatedAt = normalizeTime(clone.UpdatedAt)
		clones = append(clones, clone)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate all-host clones: %w", err)
	}
	return clones, nil
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

func (s *PostgresStore) hostActiveAssignments(ctx context.Context) ([]Assignment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at,
			w.worker_id, w.current_pane_id, w.agent_profile, w.state, w.issue, w.clone_path, w.last_review_count, w.last_inline_review_comment_count, w.last_issue_comment_count, w.last_issue_comment_watermark, w.last_review_updated_at, w.review_nudge_count, w.review_approved, w.last_ci_state, w.ci_nudge_count, w.ci_failure_poll_count, w.ci_escalated, w.last_mergeable_state, w.nudge_count, w.last_capture, w.last_activity_at, w.last_pr_number, w.last_push_at, w.last_pr_poll_at, w.restart_count, w.first_crash_at, w.created_at, w.last_seen_at
		FROM tasks t
		INNER JOIN workers w
			ON w.project = t.project
			AND w.worker_id = t.worker_id
			AND `+postgresHostMatch("w.host", 1)+`
		WHERE `+postgresHostMatch("t.host", 1)+` AND t.status = 'active'
		ORDER BY t.updated_at DESC, t.project ASC, t.issue ASC
	`, s.host)
	if err != nil {
		return nil, fmt.Errorf("list host active assignments: %w", err)
	}
	defer rows.Close()

	assignments := make([]Assignment, 0)
	for rows.Next() {
		assignment, err := scanPostgresAssignment(rows, true)
		if err != nil {
			return nil, fmt.Errorf("scan host active assignment: %w", err)
		}
		assignments = append(assignments, assignment)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate host active assignments: %w", err)
	}
	return assignments, nil
}

func (s *PostgresStore) AllActiveAssignments(ctx context.Context) ([]Assignment, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.project,
			t.issue, t.status, t.state, t.agent, t.prompt, t.caller_pane, t.worker_id, w.current_pane_id, t.clone_path, t.branch, t.pr_number, t.pr_repo, t.created_at, t.updated_at,
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
