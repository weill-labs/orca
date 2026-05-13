package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/project"
	legacy "github.com/weill-labs/orca/internal/state"
)

type multiProjectPool struct {
	store        pool.Store
	detectOrigin func(string) (string, error)
	amux         interface {
		ListPanes(ctx context.Context) ([]Pane, error)
	}
	runner pool.Runner
}

func newMultiProjectPool(store pool.Store, detectOrigin func(string) (string, error), amuxClient interface {
	ListPanes(ctx context.Context) ([]Pane, error)
}, runner pool.Runner) *multiProjectPool {
	return &multiProjectPool{
		store:        store,
		detectOrigin: detectOrigin,
		amux:         amuxClient,
		runner:       runner,
	}
}

func (p *multiProjectPool) Acquire(ctx context.Context, projectPath, issue string) (Clone, error) {
	manager, err := p.manager(projectPath)
	if err != nil {
		return Clone{}, err
	}

	clone, err := manager.Allocate(ctx, issue, issue)
	if err != nil {
		return Clone{}, err
	}
	clone.Name = filepath.Base(clone.Path)
	return clone, nil
}

func (p *multiProjectPool) Release(ctx context.Context, projectPath string, clone Clone) error {
	manager, err := p.manager(projectPath)
	if err != nil {
		return err
	}

	branch := clone.CurrentBranch
	if branch == "" {
		branch = clone.AssignedTask
	}
	return manager.Release(ctx, clone.Path, branch)
}

func (p *multiProjectPool) Adopt(ctx context.Context, projectPath string, clone Clone) error {
	projectPath, err := project.CanonicalPath(projectPath)
	if err != nil {
		return err
	}
	clonePath, err := pool.ValidateClonePath(filepath.Join(projectPath, OrcaPoolSubdir), clone.Path)
	if err != nil {
		return err
	}

	branch := clone.CurrentBranch
	if branch == "" {
		branch = clone.AssignedTask
	}
	record, err := p.store.EnsureClone(ctx, projectPath, clonePath)
	if err != nil {
		return fmt.Errorf("ensure adopted clone %q: %w", clonePath, err)
	}
	if record.Status == legacy.CloneStatusOccupied && record.AssignedTask == clone.AssignedTask {
		return nil
	}

	ok, err := p.store.TryOccupyClone(ctx, projectPath, clonePath, branch, clone.AssignedTask)
	if err != nil {
		return fmt.Errorf("occupy adopted clone %q: %w", clonePath, err)
	}
	if !ok {
		return fmt.Errorf("adopted clone %q is not free", clonePath)
	}
	return nil
}

func (p *multiProjectPool) RecordCloneFailure(ctx context.Context, projectPath string, clone Clone) error {
	manager, err := p.manager(projectPath)
	if err != nil {
		return err
	}
	return manager.RecordCloneFailure(ctx, clone.Path)
}

func (p *multiProjectPool) RecordCloneSuccess(ctx context.Context, projectPath string, clone Clone) error {
	manager, err := p.manager(projectPath)
	if err != nil {
		return err
	}
	return manager.RecordCloneSuccess(ctx, clone.Path)
}

func (p *multiProjectPool) manager(projectPath string) (*pool.Manager, error) {
	projectPath, err := project.CanonicalPath(projectPath)
	if err != nil {
		return nil, err
	}

	origin, err := p.detectOrigin(projectPath)
	if err != nil {
		return nil, fmt.Errorf("detect origin: %w", err)
	}

	poolDir := filepath.Join(projectPath, OrcaPoolSubdir)
	if err := os.MkdirAll(poolDir, 0o755); err != nil {
		return nil, fmt.Errorf("create pool directory: %w", err)
	}

	options := []pool.Option{
		pool.WithCWDUsageChecker(amuxCWDUsageChecker{amux: p.amux}),
	}
	if p.runner != nil {
		options = append(options, pool.WithRunner(p.runner))
	}

	manager, err := pool.New(projectPath, internalPoolConfig{poolDir: poolDir, origin: origin}, p.store, options...)
	if err != nil {
		return nil, fmt.Errorf("create pool manager: %w", err)
	}
	return manager, nil
}
