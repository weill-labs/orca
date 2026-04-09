package daemon

import (
	"context"
	"time"

	"github.com/weill-labs/orca/internal/amux"
)

const (
	exitedEventInitialBackoff = 250 * time.Millisecond
	exitedEventMaxBackoff     = 5 * time.Second
)

func (d *Daemon) runExitedEventLoop(ctx context.Context, done chan struct{}) {
	defer close(done)

	backoff := exitedEventInitialBackoff
	for {
		if ctx.Err() != nil {
			return
		}

		eventsCh, errCh := d.amux.Events(ctx, amux.EventsRequest{
			Filter:      []string{"exited"},
			NoReconnect: true,
		})

		if !d.consumeExitedEventStream(ctx, eventsCh, errCh) {
			return
		}

		if err := d.sleep(ctx, backoff); err != nil {
			return
		}
		backoff = nextBackoff(backoff, exitedEventMaxBackoff)
	}
}

func (d *Daemon) consumeExitedEventStream(ctx context.Context, eventsCh <-chan amux.Event, errCh <-chan error) bool {
	for eventsCh != nil || errCh != nil {
		select {
		case <-ctx.Done():
			return false
		case event, ok := <-eventsCh:
			if !ok {
				eventsCh = nil
				continue
			}
			d.handleExitedEvent(ctx, event)
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				return true
			}
		}
	}

	return ctx.Err() == nil
}

func (d *Daemon) handleExitedEvent(ctx context.Context, event amux.Event) {
	paneID := event.PaneRef()
	if paneID == "" {
		return
	}

	worker, err := d.state.WorkerByPane(ctx, d.project, paneID)
	if err != nil || worker.Issue == "" {
		return
	}

	active, err := d.state.ActiveAssignmentByIssue(ctx, worker.Project, worker.Issue)
	if err != nil {
		return
	}

	d.dispatchTaskMonitorCheck(ctx, active, taskMonitorCheckExitedEvent)
}
