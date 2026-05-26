package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/worksource"
)

type workSourceVerifier interface {
	Verify(context.Context) error
}

type cloneLister interface {
	ListClones(context.Context, string) ([]state.Clone, error)
}

func (d *Daemon) verifyWorkSource(ctx context.Context) error {
	if !d.workSourceEnabled {
		return nil
	}
	verifier, ok := d.workSource.(workSourceVerifier)
	if !ok {
		return nil
	}
	if err := verifier.Verify(ctx); err != nil {
		return fmt.Errorf("verify work source: %w", err)
	}
	return nil
}

func (d *Daemon) runPullTick(ctx context.Context) {
	if err := d.pullReadyWork(ctx); err != nil && d.logf != nil {
		d.logf("worksource pull tick failed: %v", err)
	}
}

func (d *Daemon) pullReadyWork(ctx context.Context) error {
	if !d.workSourceEnabled {
		return nil
	}

	projectPath := d.pullProject()
	if projectPath == "" {
		return errors.New("worksource pull requires a project")
	}

	freeClones, err := d.freeCloneCount(ctx, projectPath)
	if err != nil {
		return err
	}
	if freeClones == 0 {
		return nil
	}

	items, err := d.workSource.Ready(ctx, freeClones)
	if err != nil {
		return fmt.Errorf("list ready work: %w", err)
	}

	remaining := freeClones
	for _, item := range items {
		if remaining == 0 {
			return nil
		}

		id := strings.TrimSpace(item.ID)
		if id == "" {
			return errors.New("work source returned an item with empty id")
		}

		if err := d.workSource.Claim(ctx, id, d.workSourceClaimWorkerID()); err != nil {
			if errors.Is(err, worksource.ErrAlreadyClaimed) {
				continue
			}
			return fmt.Errorf("claim work item %s: %w", id, err)
		}

		if err := d.assign(ctx, projectPath, id, item.Body, d.workSourceAgentProfile(), "", item.Title); err != nil {
			if releaseErr := d.workSource.Release(context.WithoutCancel(ctx), id, "assign failed: "+err.Error()); releaseErr != nil && d.logf != nil {
				d.logf("worksource release after assign failure failed: id=%s error=%v", id, releaseErr)
			}
			return fmt.Errorf("dispatch work item %s: %w", id, err)
		}
		remaining--
	}

	return nil
}

func (d *Daemon) freeCloneCount(ctx context.Context, projectPath string) (int, error) {
	lister, ok := d.state.(cloneLister)
	if !ok {
		return 0, errors.New("state store does not support clone listing")
	}

	clones, err := lister.ListClones(ctx, projectPath)
	if err != nil {
		return 0, fmt.Errorf("list clones: %w", err)
	}

	free := 0
	for _, clone := range clones {
		if clone.Status == string(pool.StatusFree) {
			free++
		}
	}
	return free, nil
}

func (d *Daemon) pullProject() string {
	if project := strings.TrimSpace(d.workSourceProject); project != "" {
		return project
	}
	return strings.TrimSpace(d.project)
}

func (d *Daemon) workSourceAgentProfile() string {
	if agent := strings.TrimSpace(d.workSourceAgent); agent != "" {
		return agent
	}
	return defaultWorkSourceAgentProfile
}

func (d *Daemon) workSourceClaimWorkerID() string {
	hostname := strings.TrimSpace(d.hostname)
	if hostname == "" {
		return "orca"
	}
	return "orca:" + hostname
}
