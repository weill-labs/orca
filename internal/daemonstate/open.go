package state

import (
	"os"
	"strings"
)

func Open(path string) (Backend, error) {
	if dsn := strings.TrimSpace(os.Getenv("ORCA_STATE_DSN")); dsn != "" {
		return OpenPostgres(dsn)
	}
	return OpenSQLite(path)
}
