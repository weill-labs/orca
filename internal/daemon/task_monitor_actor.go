package daemon

import (
	"context"
	"fmt"
	"strings"
	"sync"
)

type TaskMonitor struct {
	daemon *Daemon
	key    string

	inbox    chan taskMonitorRequest
	stopCh   chan struct{}
	doneCh   chan struct{}
	stopOnce sync.Once
}

type TaskStateUpdate struct {
	Active               ActiveAssignment
	TaskChanged          bool
	WorkerChanged        bool
	PaneMetadata         map[string]string
	PaneMetadataRemovals []string
	Events               []Event
	PRMerged             bool
	nudges               []taskMonitorNudge
}

type taskMonitorCheckKind int

const (
	taskMonitorCheckCapture taskMonitorCheckKind = iota
	taskMonitorCheckPRPoll
)

type taskMonitorRequest struct {
	ctx      context.Context
	kind     taskMonitorCheckKind
	active   ActiveAssignment
	response chan taskMonitorResult
}

type taskMonitorResult struct {
	key     string
	monitor *TaskMonitor
	update  TaskStateUpdate
}

func newTaskMonitor(daemon *Daemon, key string) *TaskMonitor {
	monitor := &TaskMonitor{
		daemon: daemon,
		key:    key,
		inbox:  make(chan taskMonitorRequest),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}
	go monitor.run()
	return monitor
}

func (m *TaskMonitor) run() {
	defer close(m.doneCh)

	for {
		select {
		case <-m.stopCh:
			return
		case request := <-m.inbox:
			update := m.handle(request.ctx, request.kind, request.active)
			request.response <- taskMonitorResult{
				key:     m.key,
				monitor: m,
				update:  update,
			}
		}
	}
}

func (m *TaskMonitor) handle(ctx context.Context, kind taskMonitorCheckKind, active ActiveAssignment) TaskStateUpdate {
	switch kind {
	case taskMonitorCheckCapture:
		return m.daemon.checkTaskCapture(ctx, active)
	case taskMonitorCheckPRPoll:
		return m.daemon.checkTaskPRPoll(ctx, active)
	default:
		return TaskStateUpdate{Active: active}
	}
}

func (m *TaskMonitor) dispatch(ctx context.Context, kind taskMonitorCheckKind, active ActiveAssignment) <-chan taskMonitorResult {
	response := make(chan taskMonitorResult, 1)
	request := taskMonitorRequest{
		ctx:      ctx,
		kind:     kind,
		active:   active,
		response: response,
	}

	select {
	case <-ctx.Done():
		return nil
	case <-m.stopCh:
		return nil
	case <-m.doneCh:
		return nil
	case m.inbox <- request:
		return response
	}
}

func (m *TaskMonitor) stop() {
	m.stopOnce.Do(func() {
		close(m.stopCh)
	})
}

func (m *TaskMonitor) wait() {
	<-m.doneCh
}

func (d *Daemon) ensureTaskMonitor(issue string) *TaskMonitor {
	return d.ensureTaskMonitorForProject(d.project, issue)
}

func (d *Daemon) ensureTaskMonitorForProject(projectPath, issue string) *TaskMonitor {
	key := taskMonitorKey(projectPath, issue)
	if key == "" {
		return nil
	}

	d.taskMonitorMu.Lock()
	defer d.taskMonitorMu.Unlock()

	if d.taskMonitors == nil {
		d.taskMonitors = make(map[string]*TaskMonitor)
	}
	if monitor := d.taskMonitors[key]; monitor != nil {
		return monitor
	}

	monitor := newTaskMonitor(d, key)
	d.taskMonitors[key] = monitor
	return monitor
}

func (d *Daemon) syncTaskMonitors(assignments []ActiveAssignment) map[string]*TaskMonitor {
	desired := make(map[string]struct{}, len(assignments))
	for _, active := range assignments {
		key := taskMonitorKey(active.Task.Project, active.Task.Issue)
		if key == "" {
			continue
		}
		desired[key] = struct{}{}
	}

	d.taskMonitorMu.Lock()
	if d.taskMonitors == nil {
		d.taskMonitors = make(map[string]*TaskMonitor)
	}

	monitors := make(map[string]*TaskMonitor, len(desired))
	for key := range desired {
		monitor := d.taskMonitors[key]
		if monitor == nil {
			monitor = newTaskMonitor(d, key)
			d.taskMonitors[key] = monitor
		}
		monitors[key] = monitor
	}

	var stale []*TaskMonitor
	for key, monitor := range d.taskMonitors {
		if _, ok := desired[key]; ok {
			continue
		}
		stale = append(stale, monitor)
		delete(d.taskMonitors, key)
	}
	d.taskMonitorMu.Unlock()

	for _, monitor := range stale {
		monitor.stop()
	}

	return monitors
}

func (d *Daemon) refreshTaskMonitors(ctx context.Context) {
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err != nil {
		return
	}
	d.syncTaskMonitors(assignments)
}

func (d *Daemon) stopTaskMonitor(issue string) {
	d.stopTaskMonitorForProject(d.project, issue)
}

func (d *Daemon) stopTaskMonitorForProject(projectPath, issue string) {
	key := taskMonitorKey(projectPath, issue)
	if key == "" {
		return
	}

	d.taskMonitorMu.Lock()
	monitor := d.taskMonitors[key]
	delete(d.taskMonitors, key)
	d.taskMonitorMu.Unlock()

	if monitor != nil {
		monitor.stop()
	}
}

func (d *Daemon) stopAllTaskMonitors(wait bool) {
	d.taskMonitorMu.Lock()
	monitors := make([]*TaskMonitor, 0, len(d.taskMonitors))
	for issue, monitor := range d.taskMonitors {
		delete(d.taskMonitors, issue)
		monitors = append(monitors, monitor)
	}
	d.taskMonitorMu.Unlock()

	for _, monitor := range monitors {
		monitor.stop()
	}
	if !wait {
		return
	}
	for _, monitor := range monitors {
		monitor.wait()
	}
}

func (d *Daemon) taskMonitorCount() int {
	d.taskMonitorMu.Lock()
	defer d.taskMonitorMu.Unlock()
	return len(d.taskMonitors)
}

func (d *Daemon) isCurrentTaskMonitor(key string, monitor *TaskMonitor) bool {
	d.taskMonitorMu.Lock()
	defer d.taskMonitorMu.Unlock()
	return d.taskMonitors[key] == monitor
}

func (d *Daemon) dispatchTaskMonitorChecks(ctx context.Context, assignments []ActiveAssignment, kind taskMonitorCheckKind) []taskMonitorResult {
	monitors := d.syncTaskMonitors(assignments)
	responses := make([]<-chan taskMonitorResult, 0, len(assignments))
	for _, active := range assignments {
		monitor := monitors[taskMonitorKey(active.Task.Project, active.Task.Issue)]
		if monitor == nil {
			continue
		}
		response := monitor.dispatch(ctx, kind, active)
		if response == nil {
			continue
		}
		responses = append(responses, response)
	}

	results := make([]taskMonitorResult, 0, len(responses))
	for _, response := range responses {
		select {
		case <-ctx.Done():
			return results
		case result := <-response:
			results = append(results, result)
		}
	}
	d.executeTaskMonitorNudges(ctx, results)
	return results
}

func (d *Daemon) applyTaskMonitorResults(ctx context.Context, results []taskMonitorResult) {
	for _, result := range results {
		if !d.isCurrentTaskMonitor(result.key, result.monitor) {
			continue
		}
		d.applyTaskStateUpdate(ctx, result.update)
	}
}

func (d *Daemon) applyTaskStateUpdate(ctx context.Context, update TaskStateUpdate) {
	active := update.Active
	if active.Task.Issue == "" {
		return
	}

	if update.PRMerged {
		for _, event := range update.Events {
			d.emit(ctx, event)
		}

		message := "pull request merged"
		if err := d.setIssueStatus(ctx, active.Task.Project, active.Task.Issue, IssueStateDone); err != nil {
			message = fmt.Sprintf("pull request merged (failed to update Linear issue status: %v)", err)
		}

		profile, err := d.profileForTask(ctx, active.Task)
		if err != nil {
			profile = AgentProfile{Name: active.Task.AgentProfile}
		}
		d.emit(ctx, d.assignmentEvent(active, profile, EventPRMerged, message))

		if err := d.finishAssignment(ctx, active, TaskStatusDone, EventTaskCompleted, true); err != nil {
			d.emit(ctx, d.assignmentEvent(active, profile, EventTaskCompletionFailed, err.Error()))
		}
		return
	}
	if update.WorkerChanged {
		_ = d.state.PutWorker(ctx, active.Worker)
	}
	if update.TaskChanged {
		_ = d.state.PutTask(ctx, active.Task)
	}
	if len(update.PaneMetadataRemovals) > 0 {
		_ = d.amux.RemoveMetadata(ctx, active.Task.PaneID, update.PaneMetadataRemovals...)
	}
	if len(update.PaneMetadata) > 0 {
		_ = d.setPaneMetadata(ctx, active.Task.PaneID, update.PaneMetadata)
	}
	for _, event := range update.Events {
		d.emit(ctx, event)
	}
}

func (d *Daemon) assignmentEvent(active ActiveAssignment, profile AgentProfile, eventType, message string) Event {
	event := Event{
		Time:         d.now(),
		Type:         eventType,
		Project:      active.Task.Project,
		Issue:        active.Task.Issue,
		PaneID:       active.Task.PaneID,
		PaneName:     assignmentPaneName(active.Task, active.Worker),
		CloneName:    active.Task.CloneName,
		ClonePath:    active.Task.ClonePath,
		Branch:       active.Task.Branch,
		AgentProfile: profile.Name,
		PRNumber:     active.Task.PRNumber,
		Message:      message,
	}
	if event.AgentProfile == "" {
		event.AgentProfile = active.Task.AgentProfile
	}
	return event
}

func taskMonitorKey(projectPath, issue string) string {
	issue = strings.TrimSpace(issue)
	if issue == "" {
		return ""
	}
	return strings.TrimSpace(projectPath) + "\x00" + issue
}
