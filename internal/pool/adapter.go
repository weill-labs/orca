package pool

import (
	"context"
	"path/filepath"
)

type poolAdapter struct {
	manager *Manager
}

func NewAdapter(manager *Manager) *poolAdapter {
	return &poolAdapter{manager: manager}
}

func (a *poolAdapter) Acquire(ctx context.Context, project, issue string) (Clone, error) {
	if _, err := a.manager.Discover(ctx); err != nil {
		return Clone{}, err
	}

	clone, err := a.manager.Allocate(ctx, issue, issue)
	if err != nil {
		return Clone{}, err
	}
	clone.Name = filepath.Base(clone.Path)
	return clone, nil
}

func (a *poolAdapter) Release(ctx context.Context, project string, clone Clone) error {
	branch := clone.CurrentBranch
	if branch == "" {
		branch = clone.AssignedTask
	}
	return a.manager.Release(ctx, clone.Path, branch)
}
