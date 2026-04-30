package daemon

import (
	"context"
	"errors"
	"fmt"
)

type mergeQueueActor struct {
	commands CommandRunner
	updates  chan<- MergeQueueUpdate
}

func newMergeQueueActor(_ string, commands CommandRunner, updates chan<- MergeQueueUpdate) *mergeQueueActor {
	return &mergeQueueActor{
		commands: commands,
		updates:  updates,
	}
}

func (a *mergeQueueActor) run(ctx context.Context, inbox <-chan ProcessQueue, done chan<- struct{}) {
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-inbox:
			a.processQueue(ctx, msg)
			if msg.Ack != nil {
				close(msg.Ack)
			}
		}
	}
}

func (a *mergeQueueActor) processQueue(ctx context.Context, msg ProcessQueue) {
	for _, entry := range msg.Entries {
		entry := entry
		switch entry.Status {
		case "", MergeQueueStatusQueued:
			entry.Status = MergeQueueStatusRebasing
			a.sendUpdate(ctx, MergeQueueUpdate{
				Entry:        entry,
				EventType:    EventPRLandingStarted,
				EventMessage: "processing queued PR landing",
			})
			go a.processRebase(ctx, entry)
		case MergeQueueStatusAwaitingChecks:
			entry.Status = MergeQueueStatusCheckingCI
			a.sendUpdate(ctx, MergeQueueUpdate{Entry: entry})
			go a.processAwaitingChecks(ctx, entry)
		case MergeQueueStatusCheckingCI, MergeQueueStatusRebasing, MergeQueueStatusMerging:
			continue
		default:
			entry.Status = MergeQueueStatusQueued
			a.sendUpdate(ctx, MergeQueueUpdate{Entry: entry})
		}
	}
}

func (a *mergeQueueActor) processRebase(ctx context.Context, entry MergeQueueEntry) {
	if err := a.rebaseQueuedPR(ctx, entry); err != nil {
		a.sendUpdate(ctx, MergeQueueUpdate{
			Entry:         entry,
			Delete:        true,
			EventType:     EventPRLandingFailed,
			EventMessage:  err.Error(),
			FailurePrompt: mergeQueueRebaseConflictPrompt(entry.PRNumber),
		})
		return
	}

	entry.Status = MergeQueueStatusAwaitingChecks
	a.sendUpdate(ctx, MergeQueueUpdate{Entry: entry})
}

func (a *mergeQueueActor) processAwaitingChecks(ctx context.Context, entry MergeQueueEntry) {
	ciState, err := lookupPRChecksState(ctx, a.commands, entry.Project, entry.PRNumber)
	if err != nil {
		entry.Status = MergeQueueStatusAwaitingChecks
		a.sendUpdate(ctx, MergeQueueUpdate{Entry: entry})
		return
	}

	switch ciState {
	case ciStatePass, ciStateSkipping:
		entry.Status = MergeQueueStatusMerging
		a.sendUpdate(ctx, MergeQueueUpdate{Entry: entry})
		if err := a.mergeQueuedPR(ctx, entry); err != nil {
			a.sendUpdate(ctx, MergeQueueUpdate{
				Entry:         entry,
				Delete:        true,
				EventType:     EventPRLandingFailed,
				EventMessage:  err.Error(),
				FailurePrompt: mergeQueueMergeFailedPrompt(entry.PRNumber),
			})
			return
		}
		a.sendUpdate(ctx, MergeQueueUpdate{
			Entry:  entry,
			Delete: true,
		})
	case ciStateFail, ciStateCancel:
		a.sendUpdate(ctx, MergeQueueUpdate{
			Entry:         entry,
			Delete:        true,
			EventType:     EventPRLandingFailed,
			EventMessage:  fmt.Sprintf("required checks state is %s", ciState),
			FailurePrompt: mergeQueueChecksFailedPrompt(entry.PRNumber),
		})
	default:
		entry.Status = MergeQueueStatusAwaitingChecks
		a.sendUpdate(ctx, MergeQueueUpdate{Entry: entry})
	}
}

func (a *mergeQueueActor) sendUpdate(ctx context.Context, update MergeQueueUpdate) {
	select {
	case <-ctx.Done():
	case a.updates <- update:
	}
}

func (a *mergeQueueActor) rebaseQueuedPR(ctx context.Context, entry MergeQueueEntry) error {
	_, err := a.commands.Run(ctx, entry.Project, "gh", "pr", "update-branch", fmt.Sprintf("%d", entry.PRNumber), "--rebase")
	return err
}

func (a *mergeQueueActor) mergeQueuedPR(ctx context.Context, entry MergeQueueEntry) error {
	_, err := a.commands.Run(ctx, entry.Project, "gh", "pr", "merge", fmt.Sprintf("%d", entry.PRNumber), "--squash")
	return err
}

func (d *Daemon) dispatchMergeQueue(ctx context.Context) {
	if d.mergeQueueInbox == nil {
		return
	}

	entries, err := d.state.MergeEntries(ctx, d.project)
	if err != nil || len(entries) == 0 {
		return
	}

	msg := ProcessQueue{
		Entries: make([]MergeQueueEntry, 0, len(entries)),
		Ack:     make(chan struct{}),
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}

		active, activeErr := d.state.ActiveAssignmentByPRNumber(ctx, entry.Project, entry.PRNumber)
		merged, err := d.isPRMerged(ctx, entry.Project, entry.PRNumber)
		if err == nil && merged {
			if activeErr == nil {
				d.dispatchTaskMonitorCheck(ctx, active, taskMonitorCheckPRPoll)
			}
			_ = d.state.DeleteMergeEntry(ctx, entry.Project, entry.PRNumber)
			continue
		}

		if activeErr != nil {
			event := d.mergeQueueEvent(nil, EventPRLandingFailed, entry.PRNumber, fmt.Sprintf("PR #%d is no longer tracked by an active assignment", entry.PRNumber), d.now())
			event.Project = entry.Project
			d.emit(ctx, event)
			_ = d.state.DeleteMergeEntry(ctx, entry.Project, entry.PRNumber)
			continue
		}

		switch entry.Status {
		case MergeQueueStatusCheckingCI, MergeQueueStatusRebasing, MergeQueueStatusMerging:
			continue
		default:
			msg.Entries = append(msg.Entries, entry)
		}
	}

	if len(msg.Entries) == 0 {
		return
	}

	select {
	case <-ctx.Done():
	case d.mergeQueueInbox <- msg:
	}
	select {
	case <-ctx.Done():
	case <-msg.Ack:
	}
}

func (d *Daemon) applyMergeQueueUpdates(ctx context.Context) {
	if d.mergeQueueUpdates == nil {
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case update := <-d.mergeQueueUpdates:
			d.applyMergeQueueUpdate(ctx, update)
		default:
			return
		}
	}
}

func (d *Daemon) runMergeQueueUpdateLoop(ctx context.Context, done chan<- struct{}) {
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			return
		case update := <-d.mergeQueueUpdates:
			d.applyMergeQueueUpdate(ctx, update)
		}
	}
}

func (d *Daemon) applyMergeQueueUpdate(ctx context.Context, update MergeQueueUpdate) {
	var persistenceErr error
	if !update.Delete {
		update.Entry.UpdatedAt = d.now()
		if err := d.state.UpdateMergeEntry(ctx, update.Entry); err != nil && !errors.Is(err, ErrTaskNotFound) {
			persistenceErr = err
		}
	} else {
		if err := d.state.DeleteMergeEntry(ctx, update.Entry.Project, update.Entry.PRNumber); err != nil && !errors.Is(err, ErrTaskNotFound) {
			persistenceErr = err
		}
	}

	if persistenceErr != nil && update.EventType == "" && update.FailurePrompt == "" {
		return
	}
	if update.EventType == "" {
		return
	}

	active, err := d.state.ActiveAssignmentByPRNumber(ctx, update.Entry.Project, update.Entry.PRNumber)
	if err != nil {
		event := d.mergeQueueEvent(nil, update.EventType, update.Entry.PRNumber, update.EventMessage, d.now())
		event.Project = update.Entry.Project
		d.emit(ctx, event)
		return
	}

	if update.FailurePrompt != "" {
		d.handleQueuedPRFailure(ctx, active, update.Entry.PRNumber, update.FailurePrompt, errors.New(update.EventMessage))
		return
	}

	d.emit(ctx, d.mergeQueueEvent(&active, update.EventType, update.Entry.PRNumber, update.EventMessage, d.now()))
}

func (d *Daemon) resetMergeQueueTransientStatuses(ctx context.Context) {
	entries, err := d.state.MergeEntries(ctx, d.project)
	if err != nil {
		return
	}

	for _, entry := range entries {
		nextStatus := ""
		switch entry.Status {
		case MergeQueueStatusCheckingCI:
			nextStatus = MergeQueueStatusAwaitingChecks
		case MergeQueueStatusRebasing:
			nextStatus = MergeQueueStatusQueued
		case MergeQueueStatusMerging:
			nextStatus = MergeQueueStatusAwaitingChecks
		}
		if nextStatus == "" {
			continue
		}

		entry.Status = nextStatus
		entry.UpdatedAt = d.now()
		_ = d.state.UpdateMergeEntry(ctx, entry)
	}
}
