package state

import (
	"context"
	"database/sql"
)

func (s *SQLiteStore) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, query, args...)
}
