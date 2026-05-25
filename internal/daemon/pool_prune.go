package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	state "github.com/weill-labs/orca/internal/daemonstate"
	legacy "github.com/weill-labs/orca/internal/state"
)

type poolPruneStateStore interface {
	ListClones(ctx context.Context, project string) ([]state.Clone, error)
	DeleteClone(ctx context.Context, project, path string) error
}

func (d *Daemon) pruneMissingPoolEntries(ctx context.Context) {
	store, ok := d.state.(poolPruneStateStore)
	if !ok {
		return
	}

	clones, err := store.ListClones(ctx, d.project)
	if err != nil {
		if d.logf != nil {
			d.logf("pool prune failed to list clones: %v", err)
		}
		return
	}

	for _, clone := range clones {
		clone.Path = strings.TrimSpace(clone.Path)
		if clone.Path == "" {
			continue
		}
		if _, err := os.Stat(clone.Path); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			if d.logf != nil {
				d.logf("pool prune failed to inspect clone %q: %v", clone.Path, err)
			}
			continue
		}

		if err := store.DeleteClone(ctx, d.project, clone.Path); err != nil && !isCloneAlreadyDeletedError(err) {
			if d.logf != nil {
				d.logf("pool prune failed to delete clone %q: %v", clone.Path, err)
			}
			continue
		}
		d.emit(ctx, Event{
			Time:      d.now(),
			Type:      EventPoolEntryPruned,
			Project:   d.project,
			CloneName: filepath.Base(clone.Path),
			ClonePath: clone.Path,
			Message:   fmt.Sprintf("pruned missing pool entry %s", filepath.Base(clone.Path)),
		})
	}
}

func isCloneAlreadyDeletedError(err error) bool {
	return errors.Is(err, state.ErrNotFound) || errors.Is(err, legacy.ErrCloneNotFound)
}
