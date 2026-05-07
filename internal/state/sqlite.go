package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const (
	sqliteDriver = "sqlite"
)

var (
	ErrCloneNotFound       = errors.New("state: clone not found")
	ErrCloneNotQuarantined = errors.New("state: clone not quarantined")
)

type CloneStatus string

const (
	CloneStatusFree        CloneStatus = "free"
	CloneStatusOccupied    CloneStatus = "occupied"
	CloneStatusQuarantined CloneStatus = "quarantined"
)

type CloneRecord struct {
	Project       string
	Path          string
	Status        CloneStatus
	CurrentBranch string
	AssignedTask  string
	FailureCount  int
}

type SQLiteStore struct {
	db *sql.DB
}

func OpenSQLite(path string) (*SQLiteStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create sqlite directory: %w", err)
	}

	db, err := sql.Open(sqliteDriver, path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}
	db.SetMaxOpenConns(1)

	store := &SQLiteStore{db: db}
	if err := store.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteStore) EnsureClone(ctx context.Context, project, path string) (CloneRecord, error) {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO clones (project, path, status, current_branch, assigned_task, failure_count)
		 VALUES (?, ?, ?, '', '', 0)
		 ON CONFLICT(project, path) DO NOTHING`,
		project,
		path,
		CloneStatusFree,
	)
	if err != nil {
		return CloneRecord{}, fmt.Errorf("ensure clone: %w", err)
	}

	record, err := s.clone(ctx, project, path)
	if err != nil {
		return CloneRecord{}, fmt.Errorf("load clone: %w", err)
	}

	return record, nil
}

func (s *SQLiteStore) ListClones(ctx context.Context, project string) ([]CloneRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT project, path, status, current_branch, assigned_task, failure_count
		 FROM clones
		 WHERE project = ?
		 ORDER BY path`,
		project,
	)
	if err != nil {
		return nil, fmt.Errorf("list clones: %w", err)
	}
	defer rows.Close()

	var records []CloneRecord
	for rows.Next() {
		var record CloneRecord
		if err := rows.Scan(
			&record.Project,
			&record.Path,
			&record.Status,
			&record.CurrentBranch,
			&record.AssignedTask,
			&record.FailureCount,
		); err != nil {
			return nil, fmt.Errorf("scan clone: %w", err)
		}
		records = append(records, record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate clones: %w", err)
	}

	return records, nil
}

func (s *SQLiteStore) TryOccupyClone(ctx context.Context, project, path, branch, task string) (bool, error) {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE clones
		 SET status = ?, current_branch = ?, assigned_task = ?
		 WHERE project = ? AND path = ? AND status = ?`,
		CloneStatusOccupied,
		branch,
		task,
		project,
		path,
		CloneStatusFree,
	)
	if err != nil {
		return false, fmt.Errorf("occupy clone: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("read affected rows: %w", err)
	}

	return rows == 1, nil
}

func (s *SQLiteStore) RecordCloneFailure(ctx context.Context, project, path string, threshold int) (CloneRecord, error) {
	if threshold <= 0 {
		threshold = 1
	}
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE clones
		 SET failure_count = failure_count + 1,
		     status = CASE WHEN failure_count + 1 >= ? THEN ? ELSE status END,
		     current_branch = CASE WHEN failure_count + 1 >= ? THEN '' ELSE current_branch END,
		     assigned_task = CASE WHEN failure_count + 1 >= ? THEN '' ELSE assigned_task END
		 WHERE project = ? AND path = ? AND status != ?`,
		threshold,
		CloneStatusQuarantined,
		threshold,
		threshold,
		project,
		path,
		CloneStatusOccupied,
	)
	if err != nil {
		return CloneRecord{}, fmt.Errorf("record clone failure: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return CloneRecord{}, fmt.Errorf("read affected rows: %w", err)
	}
	if rows == 0 {
		record, loadErr := s.clone(ctx, project, path)
		if loadErr != nil {
			return CloneRecord{}, loadErr
		}
		if record.Status == CloneStatusOccupied {
			return record, nil
		}
		return CloneRecord{}, ErrCloneNotFound
	}
	return s.clone(ctx, project, path)
}

func (s *SQLiteStore) ResetCloneFailures(ctx context.Context, project, path string) error {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE clones
		 SET failure_count = 0
		 WHERE project = ? AND path = ?`,
		project,
		path,
	)
	if err != nil {
		return fmt.Errorf("reset clone failures: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read affected rows: %w", err)
	}
	if rows == 0 {
		return ErrCloneNotFound
	}
	return nil
}

func (s *SQLiteStore) UnquarantineClone(ctx context.Context, project, path string) error {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE clones
		 SET status = ?, current_branch = '', assigned_task = '', failure_count = 0
		 WHERE project = ? AND path = ? AND status = ?`,
		CloneStatusFree,
		project,
		path,
		CloneStatusQuarantined,
	)
	if err != nil {
		return fmt.Errorf("unquarantine clone: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read affected rows: %w", err)
	}
	if rows == 0 {
		if _, loadErr := s.clone(ctx, project, path); loadErr != nil {
			return loadErr
		}
		return ErrCloneNotQuarantined
	}
	return nil
}

func (s *SQLiteStore) MarkCloneFree(ctx context.Context, project, path string) error {
	result, err := s.db.ExecContext(
		ctx,
		`UPDATE clones
		 SET status = ?, current_branch = '', assigned_task = ''
		 WHERE project = ? AND path = ?`,
		CloneStatusFree,
		project,
		path,
	)
	if err != nil {
		return fmt.Errorf("mark clone free: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read affected rows: %w", err)
	}
	if rows == 0 {
		return ErrCloneNotFound
	}

	return nil
}

func (s *SQLiteStore) clone(ctx context.Context, project, path string) (CloneRecord, error) {
	var record CloneRecord
	err := s.db.QueryRowContext(
		ctx,
		`SELECT project, path, status, current_branch, assigned_task, failure_count
		 FROM clones
		 WHERE project = ? AND path = ?`,
		project,
		path,
	).Scan(
		&record.Project,
		&record.Path,
		&record.Status,
		&record.CurrentBranch,
		&record.AssignedTask,
		&record.FailureCount,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return CloneRecord{}, ErrCloneNotFound
	}
	if err != nil {
		return CloneRecord{}, err
	}

	return record, nil
}

func (s *SQLiteStore) init(ctx context.Context) error {
	_, err := s.db.ExecContext(
		ctx,
		`CREATE TABLE IF NOT EXISTS clones (
			project TEXT NOT NULL,
			path TEXT NOT NULL,
			status TEXT NOT NULL,
			current_branch TEXT NOT NULL DEFAULT '',
			assigned_task TEXT NOT NULL DEFAULT '',
			failure_count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (project, path)
		)`,
	)
	if err != nil {
		return fmt.Errorf("initialize clone schema: %w", err)
	}

	if err := addSQLiteColumnIfMissing(ctx, s.db, "clones", "failure_count", "INTEGER NOT NULL DEFAULT 0"); err != nil {
		return fmt.Errorf("ensure clone failure count schema: %w", err)
	}

	return nil
}

func addSQLiteColumnIfMissing(ctx context.Context, db *sql.DB, table, column, definition string) error {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			columnType string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultVal, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, column, definition))
	return err
}
