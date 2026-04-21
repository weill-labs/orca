package state

import (
	"fmt"
	"os"
	"strings"
)

func defaultStoreHost() string {
	host, err := os.Hostname()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(host)
}

func normalizeStoreHost(host string) string {
	return strings.TrimSpace(host)
}

func postgresHostMatch(column string, argPosition int) string {
	return fmt.Sprintf("(%s = $%d OR %s = '')", column, argPosition, column)
}

func sqliteHostMatch(column string) string {
	return fmt.Sprintf("(%s = ? OR %s = '')", column, column)
}
