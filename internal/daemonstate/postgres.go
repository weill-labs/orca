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
	`CREATE INDEX IF NOT EXISTS idx_tasks_project_updated ON tasks(project, updated_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_tasks_project_branch_status ON tasks(project, branch, status, updated_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_workers_project_last_seen ON workers(project, last_seen_at DESC, worker_id ASC)`,
	`CREATE INDEX IF NOT EXISTS idx_clones_project_updated ON clones(project, updated_at DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_events_project_id ON events(project, id)`,
	`CREATE INDEX IF NOT EXISTS idx_events_project_issue_id ON events(project, issue, id)`,
	`CREATE INDEX IF NOT EXISTS idx_events_project_worker_id ON events(project, worker_id, id)`,
	`CREATE INDEX IF NOT EXISTS idx_merge_queue_project_created ON merge_queue(project, created_at ASC, pr_number ASC)`,
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
