package daemon

import (
	"context"
	"fmt"
	"sync"
)

type prLookupCloneEventContextKey struct{}

type prLookupCloneEventTracker struct {
	mu   sync.Mutex
	seen map[string]bool
}

func withPRLookupCloneEventTracker(ctx context.Context) context.Context {
	return context.WithValue(ctx, prLookupCloneEventContextKey{}, &prLookupCloneEventTracker{
		seen: make(map[string]bool),
	})
}

func (d *Daemon) emitPRLookupCloneMissingOnce(ctx context.Context, clonePath string) {
	if tracker, ok := ctx.Value(prLookupCloneEventContextKey{}).(*prLookupCloneEventTracker); ok && tracker != nil {
		tracker.mu.Lock()
		if tracker.seen[clonePath] {
			tracker.mu.Unlock()
			return
		}
		tracker.seen[clonePath] = true
		tracker.mu.Unlock()
	}

	d.emitPRLookupCloneMissing(ctx, clonePath)
}

func (d *Daemon) emitPRLookupCloneMissing(ctx context.Context, clonePath string) {
	message := prLookupCloneMissingMessage(clonePath)
	if d.logf != nil {
		d.logf("%s", message)
	}
	d.emit(ctx, Event{
		Time:      d.now(),
		Type:      EventPRPollCloneMissing,
		Project:   d.project,
		ClonePath: clonePath,
		Message:   message,
	})
}

func prLookupCloneMissingMessage(clonePath string) string {
	return fmt.Sprintf("PR lookup clone path %q is missing; skipping it as a discovery CWD until it is restored or pruned", clonePath)
}

func prLookupClonePathInspectionErrorMessage(clonePath string, err error) string {
	return fmt.Sprintf("PR lookup clone path %q could not be inspected: %v; fix filesystem access or prune the stale pool entry", clonePath, err)
}
