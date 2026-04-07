package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/project"
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

func (p *multiProjectPool) manager(projectPath string) (*pool.Manager, error) {
	projectPath, err := project.CanonicalPath(projectPath)
	if err != nil {
		return nil, err
	}

	origin, err := p.detectOrigin(projectPath)
	if err != nil {
		return nil, fmt.Errorf("detect origin: %w", err)
	}

	poolDir := filepath.Join(projectPath, orcaPoolSubdir)
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
