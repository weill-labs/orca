package cli

import (
	"fmt"
	"path/filepath"

	"github.com/weill-labs/orca/internal/project"
)

func (a *App) resolveStartProject(projectPath string) (string, error) {
	resolvedProject, err := a.resolveProject(projectPath)
	if err != nil {
		return "", err
	}

	parentProject, ok, err := poolCloneParentProject(resolvedProject)
	if err != nil {
		return "", err
	}
	if ok {
		return parentProject, nil
	}

	return resolvedProject, nil
}

func poolCloneParentProject(projectPath string) (string, bool, error) {
	for current := filepath.Clean(projectPath); ; current = filepath.Dir(current) {
		parent := filepath.Dir(current)
		if parent == current {
			return "", false, nil
		}

		if filepath.Base(parent) != "pool" {
			continue
		}

		orcaDir := filepath.Dir(parent)
		if filepath.Base(orcaDir) != ".orca" {
			continue
		}

		parentProject, err := project.CanonicalPath(filepath.Dir(orcaDir))
		if err != nil {
			return "", false, fmt.Errorf("resolve pool clone parent project: %w", err)
		}
		return parentProject, true, nil
	}
}
