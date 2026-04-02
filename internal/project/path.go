package project

import (
	"fmt"
	"path/filepath"
	"strings"
)

// CanonicalPath resolves a project path to a stable absolute path for
// database scoping and PID-file lookup.
func CanonicalPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("project path is required")
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("canonicalize project path: %w", err)
	}

	return filepath.Clean(absPath), nil
}
