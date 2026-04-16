package state

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var postgresSchemaStatements = []string{
	`
CREATE TABLE IF NOT EXISTS daemon_status (
	project TEXT PRIMARY KEY,
	session TEXT NOT NULL,
	pid INTEGER NOT NULL DEFAULT 0,
	status TEXT NOT NULL,
	started_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL
)`,
	`
CREATE TABLE IF NOT EXISTS tasks (
	project TEXT NOT NULL,
	issue TEXT NOT NULL,
	host TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL,
	state TEXT NOT NULL DEFAULT '',
	agent TEXT NOT NULL,
	prompt TEXT NOT NULL DEFAULT '',
	caller_pane TEXT NOT NULL DEFAULT '',
	worker_id TEXT NOT NULL DEFAULT '',
	clone_path TEXT NOT NULL DEFAULT '',
	branch TEXT NOT NULL DEFAULT '',
	pr_number INTEGER,
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (project, issue)
)`,
	`
CREATE TABLE IF NOT EXISTS workers (
	project TEXT NOT NULL,
	worker_id TEXT NOT NULL,
	host TEXT NOT NULL DEFAULT '',
	agent_profile TEXT NOT NULL,
	current_pane_id TEXT NOT NULL DEFAULT '',
	state TEXT NOT NULL,
	issue TEXT NOT NULL DEFAULT '',
	clone_path TEXT NOT NULL DEFAULT '',
	last_review_count INTEGER NOT NULL DEFAULT 0,
	last_inline_review_comment_count INTEGER NOT NULL DEFAULT 0,
	last_issue_comment_count INTEGER NOT NULL DEFAULT 0,
	last_issue_comment_watermark TEXT NOT NULL DEFAULT '',
	review_nudge_count INTEGER NOT NULL DEFAULT 0,
	review_approved BOOLEAN NOT NULL DEFAULT FALSE,
	last_ci_state TEXT NOT NULL DEFAULT '',
	ci_nudge_count INTEGER NOT NULL DEFAULT 0,
	ci_failure_poll_count INTEGER NOT NULL DEFAULT 0,
	ci_escalated BOOLEAN NOT NULL DEFAULT FALSE,
	last_mergeable_state TEXT NOT NULL DEFAULT '',
	nudge_count INTEGER NOT NULL DEFAULT 0,
	last_capture TEXT NOT NULL DEFAULT '',
	last_activity_at TIMESTAMPTZ,
	last_pr_number INTEGER NOT NULL DEFAULT 0,
	last_push_at TIMESTAMPTZ,
	last_pr_poll_at TIMESTAMPTZ,
	restart_count INTEGER NOT NULL DEFAULT 0,
	first_crash_at TIMESTAMPTZ,
	created_at TIMESTAMPTZ NOT NULL,
	last_seen_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (project, worker_id)
)`,
	`
CREATE TABLE IF NOT EXISTS merge_queue (
	project TEXT NOT NULL,
	pr_number INTEGER NOT NULL,
	issue TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	updated_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (project, pr_number)
)`,
	`
CREATE TABLE IF NOT EXISTS clones (
	project TEXT NOT NULL,
	path TEXT NOT NULL,
	status TEXT NOT NULL,
	issue TEXT NOT NULL DEFAULT '',
	branch TEXT NOT NULL DEFAULT '',
	updated_at TIMESTAMPTZ NOT NULL,
	PRIMARY KEY (project, path)
)`,
	`
CREATE TABLE IF NOT EXISTS events (
	id BIGSERIAL PRIMARY KEY,
	project TEXT NOT NULL,
	kind TEXT NOT NULL,
	issue TEXT NOT NULL DEFAULT '',
	worker_id TEXT NOT NULL DEFAULT '',
	message TEXT NOT NULL,
	payload TEXT,
	created_at TIMESTAMPTZ NOT NULL
)`,
	`ALTER TABLE IF EXISTS tasks ADD COLUMN IF NOT EXISTS host TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE IF EXISTS tasks ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE IF EXISTS tasks ADD COLUMN IF NOT EXISTS state TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE IF EXISTS workers ADD COLUMN IF NOT EXISTS host TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE IF EXISTS workers ADD COLUMN IF NOT EXISTS last_issue_comment_watermark TEXT NOT NULL DEFAULT ''`,
	`ALTER TABLE IF EXISTS workers ADD COLUMN IF NOT EXISTS review_approved BOOLEAN NOT NULL DEFAULT FALSE`,
	`CREATE INDEX IF NOT EXISTS idx_tasks_project_updated ON tasks(project, updated_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_tasks_project_branch_status ON tasks(project, branch, status, updated_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_workers_project_last_seen ON workers(project, last_seen_at DESC, worker_id ASC)`,
	`CREATE INDEX IF NOT EXISTS idx_clones_project_updated ON clones(project, updated_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_events_project_id ON events(project, id)`,
	`CREATE INDEX IF NOT EXISTS idx_events_project_issue_id ON events(project, issue, id)`,
	`CREATE INDEX IF NOT EXISTS idx_events_project_worker_id ON events(project, worker_id, id)`,
	`CREATE INDEX IF NOT EXISTS idx_merge_queue_project_created ON merge_queue(project, created_at ASC, pr_number ASC)`,
	`
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_throughput AS
WITH completed_tasks AS (
	SELECT
		date_trunc('day', updated_at)::date AS day,
		project,
		COUNT(*) AS issues_completed
	FROM tasks
	WHERE status = 'done'
	GROUP BY 1, 2
),
merged_prs AS (
	SELECT
		date_trunc('day', created_at)::date AS day,
		project,
		COUNT(*) AS prs_merged
	FROM events
	WHERE kind = 'pr.merged'
	GROUP BY 1, 2
)
SELECT
	COALESCE(completed_tasks.day, merged_prs.day) AS day,
	COALESCE(completed_tasks.project, merged_prs.project) AS project,
	COALESCE(completed_tasks.issues_completed, 0)::bigint AS issues_completed,
	COALESCE(merged_prs.prs_merged, 0)::bigint AS prs_merged
FROM completed_tasks
FULL OUTER JOIN merged_prs
	ON completed_tasks.day = merged_prs.day
	AND completed_tasks.project = merged_prs.project
`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_throughput_day_project ON daily_throughput(day, project)`,
	`
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_cycle_time AS
SELECT
	date_trunc('day', updated_at)::date AS day,
	project,
	percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM updated_at - created_at) / 3600.0) AS median_cycle_time_hours,
	percentile_cont(0.9) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM updated_at - created_at) / 3600.0) AS p90_cycle_time_hours,
	NULL::double precision AS median_coding_hours,
	NULL::double precision AS median_ci_hours,
	NULL::double precision AS median_review_hours,
	NULL::double precision AS median_merge_queue_hours
FROM tasks
WHERE status = 'done'
GROUP BY 1, 2
`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_cycle_time_day_project ON daily_cycle_time(day, project)`,
	`COMMENT ON MATERIALIZED VIEW daily_cycle_time IS 'Phase breakdown columns remain NULL until richer task state transition timestamps are persisted.'`,
	`
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_quality AS
WITH task_counts AS (
	SELECT
		date_trunc('day', updated_at)::date AS day,
		project,
		COUNT(*) AS total_tasks,
		COUNT(*) FILTER (WHERE status = 'done') AS issues_completed
	FROM tasks
	GROUP BY 1, 2
),
stuck_counts AS (
	SELECT
		date_trunc('day', created_at)::date AS day,
		project,
		COUNT(*) AS stuck_tasks
	FROM events
	WHERE kind = 'worker.escalated'
	GROUP BY 1, 2
),
nudge_counts AS (
	SELECT
		date_trunc('day', created_at)::date AS day,
		project,
		COUNT(*) AS total_nudges
	FROM events
	WHERE kind IN ('worker.nudged', 'worker.nudged_ci', 'worker.nudged_review', 'worker.nudged_conflict')
	GROUP BY 1, 2
),
restart_counts AS (
	SELECT
		date_trunc('day', created_at)::date AS day,
		project,
		COUNT(*) AS total_restarts
	FROM events
	WHERE kind = 'worker.crash_report'
	GROUP BY 1, 2
),
keys AS (
	SELECT day, project FROM task_counts
	UNION
	SELECT day, project FROM stuck_counts
	UNION
	SELECT day, project FROM nudge_counts
	UNION
	SELECT day, project FROM restart_counts
)
SELECT
	keys.day,
	keys.project,
	COALESCE(task_counts.total_tasks, 0)::bigint AS total_tasks,
	COALESCE(stuck_counts.stuck_tasks, 0)::bigint AS stuck_tasks,
	COALESCE(stuck_counts.stuck_tasks, 0)::double precision / NULLIF(COALESCE(task_counts.total_tasks, 0), 0) AS stuck_rate,
	COALESCE(nudge_counts.total_nudges, 0)::bigint AS total_nudges,
	COALESCE(nudge_counts.total_nudges, 0)::double precision / NULLIF(COALESCE(task_counts.issues_completed, 0), 0) AS nudges_per_completed,
	COALESCE(restart_counts.total_restarts, 0)::bigint AS total_restarts,
	COALESCE(restart_counts.total_restarts, 0)::double precision / NULLIF(COALESCE(task_counts.total_tasks, 0), 0) AS restart_rate
FROM keys
LEFT JOIN task_counts
	ON task_counts.day = keys.day
	AND task_counts.project = keys.project
LEFT JOIN stuck_counts
	ON stuck_counts.day = keys.day
	AND stuck_counts.project = keys.project
LEFT JOIN nudge_counts
	ON nudge_counts.day = keys.day
	AND nudge_counts.project = keys.project
LEFT JOIN restart_counts
	ON restart_counts.day = keys.day
	AND restart_counts.project = keys.project
`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_quality_day_project ON daily_quality(day, project)`,
	`
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_workers AS
SELECT
	date_trunc('day', updated_at)::date AS day,
	project,
	agent,
	COUNT(DISTINCT NULLIF(worker_id, ''))::bigint AS active_workers,
	COUNT(*)::bigint AS tasks_completed,
	COUNT(*)::double precision / NULLIF(COUNT(DISTINCT NULLIF(worker_id, '')), 0) AS tasks_per_worker
FROM tasks
WHERE status = 'done'
GROUP BY 1, 2, 3
`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_workers_day_project_agent ON daily_workers(day, project, agent)`,
}

type PostgresStore struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func OpenPostgres(dsn string) (*PostgresStore, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("open postgres database: %w", err)
	}

	store := &PostgresStore{
		pool: pool,
		now:  func() time.Time { return time.Now().UTC() },
	}

	if err := store.EnsureSchema(context.Background()); err != nil {
		pool.Close()
		return nil, err
	}

	return store, nil
}

func (s *PostgresStore) Close() error {
	s.pool.Close()
	return nil
}

func (s *PostgresStore) EnsureSchema(ctx context.Context) error {
	for _, statement := range postgresSchemaStatements {
		if _, err := s.pool.Exec(ctx, statement); err != nil {
			return fmt.Errorf("ensure postgres schema: %w", err)
		}
	}
	return nil
}

func normalizeTime(timestamp time.Time) time.Time {
	if timestamp.IsZero() {
		return time.Time{}
	}
	return timestamp.UTC()
}

func optionalTimeArg(timestamp time.Time) any {
	if timestamp.IsZero() {
		return nil
	}
	return timestamp.UTC()
}
