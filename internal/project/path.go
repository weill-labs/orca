package project

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CanonicalPath resolves a project path to a stable absolute path for
// database scoping and PID-file lookup. The canonical project identity is the
// resolved git repository root for the supplied path.
func CanonicalPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("project path is required")
	}

	resolvedPath, err := resolvePath(path)
	if err != nil {
		return "", err
	}

	return repoRoot(resolvedPath)
}

func NormalizeWorkerProjectPath(path string) string {
	if projectPath := ProjectRootFromPoolClonePath(path); projectPath != "" {
		return projectPath
	}
	return strings.TrimSpace(path)
}

func ProjectRootFromPoolClonePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}

	cleaned := filepath.Clean(path)
	marker := string(filepath.Separator) + filepath.Join(".orca", "pool") + string(filepath.Separator) + "clone-"
	idx := strings.Index(cleaned, marker)
	if idx <= 0 {
		return ""
	}
	projectPath := cleaned[:idx]
	if projectPath == "." || projectPath == string(filepath.Separator) {
		return ""
	}
	return projectPath
}

func resolvePath(path string) (string, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("canonicalize project path: %w", err)
	}

	resolvedPath, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return "", fmt.Errorf("canonicalize project path: %w", err)
	}

	info, err := os.Stat(resolvedPath)
	if err != nil {
		return "", fmt.Errorf("canonicalize project path: %w", err)
	}
	if !info.IsDir() {
		resolvedPath = filepath.Dir(resolvedPath)
	}

	return filepath.Clean(resolvedPath), nil
}

func repoRoot(path string) (string, error) {
	for current := filepath.Clean(path); ; current = filepath.Dir(current) {
		gitPath := filepath.Join(current, ".git")
		if _, err := os.Stat(gitPath); err == nil {
			return current, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("inspect repository root: %w", err)
		}

		parent := filepath.Dir(current)
		if parent == current {
			return "", fmt.Errorf("project path %q is not inside a git repository", path)
		}
	}
}
