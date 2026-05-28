package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type mergeQueueActor struct {
	commands                CommandRunner
	updates                 chan<- MergeQueueUpdate
	landingConfigForProject func(string) (LandingConfig, error)
}

func newMergeQueueActor(_ string, commands CommandRunner, updates chan<- MergeQueueUpdate, configLoaders ...func(string) (LandingConfig, error)) *mergeQueueActor {
	var configLoader func(string) (LandingConfig, error)
	if len(configLoaders) > 0 {
		configLoader = configLoaders[0]
	}
	return &mergeQueueActor{
		commands:                commands,
		updates:                 updates,
		landingConfigForProject: configLoader,
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
	directLandingStarted := false
	for _, entry := range msg.Entries {
		entry := entry
		switch entry.Status {
		case "", MergeQueueStatusQueued:
			entry.Status = MergeQueueStatusRebasing
			if mergeQueueEntryMode(entry) == LandingModeDirect {
				if directLandingStarted {
					continue
				}
				directLandingStarted = true
				a.sendUpdate(ctx, MergeQueueUpdate{
					Entry:        entry,
					EventType:    EventDirectLandingStarted,
					EventMessage: directLandingStartedMessage(entry),
				})
				go a.processDirectLanding(ctx, entry)
				continue
			}
			a.sendUpdate(ctx, MergeQueueUpdate{
				Entry:        entry,
				EventType:    EventPRLandingStarted,
				EventMessage: "processing queued PR landing",
			})
			go a.processRebase(ctx, entry)
		case MergeQueueStatusAwaitingChecks:
			if mergeQueueEntryMode(entry) == LandingModeDirect {
				entry.Status = MergeQueueStatusQueued
				a.sendUpdate(ctx, MergeQueueUpdate{Entry: entry})
				continue
			}
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

func (a *mergeQueueActor) processDirectLanding(ctx context.Context, entry MergeQueueEntry) {
	entry = a.fillDirectLandingConfig(entry)
	outcome := a.landDirect(ctx, entry)
	if outcome.err != nil {
		eventType := EventDirectLandingFailed
		eventMessage := directLandingFailedMessage(entry, outcome.failedAction, outcome.err)
		prompt := directLandingFailedPrompt(entry.Issue, entry.Branch, entry.BaseBranch, entry.QualityGate, outcome.failedAction)
		if outcome.conflict {
			eventType = EventDirectLandingConflict
			eventMessage = directLandingConflictMessage(entry.Issue, entry.Branch, entry.BaseBranch, outcome.conflictedFiles, outcome.failedAction)
			prompt = directLandingConflictPrompt(entry.Issue, entry.Branch, entry.BaseBranch, outcome.conflictedFiles)
		}
		a.sendUpdate(ctx, MergeQueueUpdate{
			Entry:                  entry,
			Delete:                 true,
			EventType:              eventType,
			EventMessage:           eventMessage,
			FailurePrompt:          prompt,
			ConflictedFiles:        strings.Join(outcome.conflictedFiles, "\n"),
			FailedAction:           outcome.failedAction,
			ConflictStatePreserved: outcome.conflictStatePreserved,
		})
		return
	}

	a.sendUpdate(ctx, MergeQueueUpdate{
		Entry:          entry,
		Delete:         true,
		CompleteDirect: true,
		EventType:      EventDirectLanded,
		EventMessage:   directLandingCompletedMessage(entry),
	})
}

type directLandingOutcome struct {
	err                    error
	conflict               bool
	conflictStatePreserved bool
	conflictedFiles        []string
	failedAction           string
}

func (a *mergeQueueActor) landDirect(ctx context.Context, entry MergeQueueEntry) directLandingOutcome {
	baseBranch := directLandingBaseBranch(entry)
	branch := directLandingBranch(entry)
	clonePath := strings.TrimSpace(entry.ClonePath)
	if clonePath == "" {
		return directLandingOutcome{err: errors.New("direct landing entry missing clone path"), failedAction: "load worker clone"}
	}
	commands := []struct {
		action string
		args   []string
	}{
		{action: "git fetch origin " + baseBranch, args: []string{"fetch", "origin", baseBranch}},
		{action: "git checkout " + branch, args: []string{"checkout", branch}},
		{action: "git rebase origin/" + baseBranch, args: []string{"rebase", "origin/" + baseBranch}},
	}

	for _, command := range commands {
		if _, err := a.commands.Run(ctx, clonePath, "git", command.args...); err != nil {
			outcome := directLandingOutcome{err: err, failedAction: command.action}
			if strings.HasPrefix(command.action, "git rebase ") {
				outcome.conflictedFiles = a.conflictedFiles(ctx, clonePath)
				if len(outcome.conflictedFiles) > 0 {
					outcome.conflict = true
					outcome.conflictStatePreserved = true
				}
			}
			return outcome
		}
	}

	if gate := strings.TrimSpace(entry.QualityGate); gate != "" {
		if _, err := a.commands.Run(ctx, clonePath, "sh", "-c", gate); err != nil {
			return directLandingOutcome{err: err, failedAction: "quality gate: " + gate}
		}
	}

	pushAction := "git push origin HEAD:" + baseBranch
	if _, err := a.commands.Run(ctx, clonePath, "git", "push", "origin", "HEAD:"+baseBranch); err != nil {
		return directLandingOutcome{err: err, failedAction: pushAction}
	}
	return directLandingOutcome{}
}

func (a *mergeQueueActor) conflictedFiles(ctx context.Context, projectPath string) []string {
	out, err := a.commands.Run(ctx, projectPath, "git", "diff", "--name-only", "--diff-filter=U")
	if err != nil {
		return nil
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	files := make([]string, 0, len(lines))
	for _, line := range lines {
		if file := strings.TrimSpace(line); file != "" {
			files = append(files, file)
		}
	}
	return files
}

func (a *mergeQueueActor) fillDirectLandingConfig(entry MergeQueueEntry) MergeQueueEntry {
	if mergeQueueEntryMode(entry) != LandingModeDirect || a.landingConfigForProject == nil {
		return entry
	}
	cfg, err := a.landingConfigForProject(entry.Project)
	if err != nil {
		return entry
	}
	if strings.TrimSpace(entry.BaseBranch) == "" {
		entry.BaseBranch = cfg.BaseBranch
	}
	if strings.TrimSpace(entry.QualityGate) == "" {
		entry.QualityGate = cfg.QualityGate
	}
	return entry
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

		active, activeErr := d.activeAssignmentForMergeEntry(ctx, entry)
		if mergeQueueEntryMode(entry) != LandingModeDirect {
			merged, err := d.isPRMerged(ctx, entry.Project, entry.PRNumber)
			if err == nil && merged {
				if activeErr == nil {
					d.dispatchTaskMonitorCheck(ctx, active, taskMonitorCheckPRPoll)
				}
				_ = d.state.DeleteMergeEntry(ctx, entry.Project, entry.PRNumber)
				continue
			}
		}

		if activeErr != nil {
			eventType := EventPRLandingFailed
			message := fmt.Sprintf("PR #%d is no longer tracked by an active assignment", entry.PRNumber)
			if mergeQueueEntryMode(entry) == LandingModeDirect {
				eventType = EventDirectLandingFailed
				message = fmt.Sprintf("%s is no longer tracked by an active assignment", directLandingBranch(entry))
			}
			event := d.mergeQueueEvent(nil, eventType, entry.PRNumber, message, d.now())
			event.Project = entry.Project
			event.Issue = entry.Issue
			event.LandingMode = mergeQueueEntryMode(entry)
			event.Target = entry.Target
			event.Branch = entry.Branch
			event.BaseBranch = entry.BaseBranch
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

func (d *Daemon) activeAssignmentForMergeEntry(ctx context.Context, entry MergeQueueEntry) (ActiveAssignment, error) {
	if mergeQueueEntryMode(entry) != LandingModeDirect {
		return d.state.ActiveAssignmentByPRNumber(ctx, entry.Project, entry.PRNumber)
	}
	if strings.TrimSpace(entry.Issue) != "" {
		if active, err := d.state.ActiveAssignmentByIssue(ctx, entry.Project, entry.Issue); err == nil {
			return active, nil
		}
	}
	return d.state.ActiveAssignmentByBranch(ctx, entry.Project, directLandingBranch(entry))
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

	active, err := d.activeAssignmentForMergeEntry(ctx, update.Entry)
	if err != nil {
		event := d.mergeQueueEvent(nil, update.EventType, update.Entry.PRNumber, update.EventMessage, d.now())
		event.Project = update.Entry.Project
		event.Issue = update.Entry.Issue
		applyMergeQueueEventMetadata(&event, update)
		d.emit(ctx, event)
		return
	}

	if update.FailurePrompt != "" {
		if mergeQueueEntryMode(update.Entry) == LandingModeDirect {
			d.handleDirectLandingFailure(ctx, active, update)
			return
		}
		d.handleQueuedPRFailure(ctx, active, update.Entry.PRNumber, update.FailurePrompt, errors.New(update.EventMessage))
		return
	}

	event := d.mergeQueueEvent(&active, update.EventType, update.Entry.PRNumber, update.EventMessage, d.now())
	applyMergeQueueEventMetadata(&event, update)
	d.emit(ctx, event)
	if update.CompleteDirect {
		d.completeDirectLanding(ctx, active)
	}
}

func applyMergeQueueEventMetadata(event *Event, update MergeQueueUpdate) {
	if event == nil {
		return
	}
	event.LandingMode = mergeQueueEntryMode(update.Entry)
	event.Target = update.Entry.Target
	if strings.TrimSpace(update.Entry.Branch) != "" {
		event.Branch = update.Entry.Branch
	}
	if event.ClonePath == "" {
		event.ClonePath = update.Entry.ClonePath
	}
	event.BaseBranch = update.Entry.BaseBranch
	event.QualityGate = update.Entry.QualityGate
	event.ConflictedFiles = mergeQueueUpdateConflictedFiles(update)
	event.FailedAction = update.FailedAction
	event.ConflictStatePreserved = update.ConflictStatePreserved
}

func mergeQueueUpdateConflictedFiles(update MergeQueueUpdate) []string {
	lines := strings.Split(strings.TrimSpace(update.ConflictedFiles), "\n")
	files := make([]string, 0, len(lines))
	for _, line := range lines {
		if file := strings.TrimSpace(line); file != "" {
			files = append(files, file)
		}
	}
	return files
}

func (d *Daemon) handleDirectLandingFailure(ctx context.Context, active ActiveAssignment, update MergeQueueUpdate) {
	if ctx.Err() != nil {
		return
	}

	_ = d.sendPromptAndEnter(ctx, active.Task.PaneID, update.FailurePrompt)
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}
	if update.EventType == EventDirectLandingConflict {
		stateUpdate := TaskStateUpdate{Active: active}
		d.escalateTaskState(&stateUpdate, profile, update.EventMessage, d.now())
		if stateUpdate.WorkerChanged {
			_ = d.state.PutWorker(ctx, stateUpdate.Active.Worker)
			active.Worker = stateUpdate.Active.Worker
		}
		if stateUpdate.TaskChanged {
			_ = d.state.PutTask(ctx, stateUpdate.Active.Task)
			active.Task = stateUpdate.Active.Task
		}
		for _, event := range stateUpdate.Events {
			d.emit(ctx, event)
		}
	}
	event := d.mergeQueueEvent(&active, update.EventType, update.Entry.PRNumber, update.EventMessage, d.now())
	applyMergeQueueEventMetadata(&event, update)
	d.emit(ctx, event)
}

func (d *Daemon) completeDirectLanding(ctx context.Context, active ActiveAssignment) {
	completionMessage := "task finished"
	if err := d.setIssueStatus(ctx, active.Task.Project, active.Task.Issue, IssueStateDone); err != nil {
		completionMessage = fmt.Sprintf("direct landing completed (failed to update Linear issue status: %v)", err)
	}
	if err := d.finishAssignmentWithMessageAndPrompt(ctx, active, TaskStatusDone, EventTaskCompleted, true, "Direct landing completed, wrap up.", completionMessage); err != nil {
		profile, profileErr := d.profileForTask(ctx, active.Task)
		if profileErr != nil {
			profile = AgentProfile{Name: active.Task.AgentProfile}
		}
		d.emit(ctx, d.assignmentEvent(active, profile, EventTaskCompletionFailed, err.Error()))
		return
	}
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
