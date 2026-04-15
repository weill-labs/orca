package daemon

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	relayInitialBackoff = time.Second
	relayMaxBackoff     = time.Minute
	relayReadTimeout    = 90 * time.Second
)

type relayConnection interface {
	ReadJSON(v any) error
	SetReadDeadline(time.Time) error
	WriteJSON(v any) error
	Close() error
}

type relayIdentifyMessage struct {
	Type        string                  `json:"type"`
	Hostname    string                  `json:"hostname"`
	Projects    []relayMonitoredProject `json:"projects,omitempty"`
	LastEventID string                  `json:"last_event_id,omitempty"`
}

type relayMonitoredProject struct {
	Path string `json:"path"`
	Repo string `json:"repo,omitempty"`
}

type relayEventMessage struct {
	ID             string         `json:"id,omitempty"`
	Type           string         `json:"type,omitempty"`
	EventType      string         `json:"event_type,omitempty"`
	Repo           string         `json:"repo,omitempty"`
	PRNumber       int            `json:"pr_number,omitempty"`
	Action         string         `json:"action,omitempty"`
	Merged         bool           `json:"merged,omitempty"`
	BaseBranch     string         `json:"base_branch,omitempty"`
	BaseRef        string         `json:"base_ref,omitempty"`
	HeadBranch     string         `json:"head_branch,omitempty"`
	HeadRef        string         `json:"head_ref,omitempty"`
	PayloadSummary map[string]any `json:"payload_summary,omitempty"`
}

func (d *Daemon) relayEnabled() bool {
	return strings.TrimSpace(d.relayURL) != ""
}

func (d *Daemon) runRelayLoop(ctx context.Context, done chan struct{}) {
	defer close(done)
	defer d.setRelayHealthy(false)

	if !d.relayEnabled() {
		return
	}

	backoff := relayInitialBackoff
	lastEventID := ""
	for {
		if ctx.Err() != nil {
			return
		}

		conn, err := d.connectRelay(ctx)
		if err != nil {
			d.logRelayError("connect relay", err)
			if err := d.sleep(ctx, backoff); err != nil {
				return
			}
			backoff = nextBackoff(backoff, relayMaxBackoff)
			continue
		}

		identify := d.buildRelayIdentifyMessage(ctx, lastEventID)
		if err := conn.WriteJSON(identify); err != nil {
			d.logRelayError("identify relay connection", err)
			_ = conn.Close()
			if d.consumeRelayReconnectRequest() {
				backoff = relayInitialBackoff
				continue
			}
			if err := d.sleep(ctx, backoff); err != nil {
				return
			}
			backoff = nextBackoff(backoff, relayMaxBackoff)
			continue
		}

		d.setRelayConn(conn)
		d.setRelayHealthy(true)
		backoff = relayInitialBackoff

		lastEventID, err = d.consumeRelayConnection(ctx, conn, lastEventID)
		d.clearRelayConn(conn)
		d.setRelayHealthy(false)
		_ = conn.Close()

		if ctx.Err() != nil {
			return
		}
		if d.consumeRelayReconnectRequest() {
			continue
		}
		if err != nil {
			d.logRelayError("relay disconnected", err)
		}
		if err := d.sleep(ctx, backoff); err != nil {
			return
		}
		backoff = nextBackoff(backoff, relayMaxBackoff)
	}
}

func (d *Daemon) connectRelay(ctx context.Context) (relayConnection, error) {
	headers := http.Header{}
	if token := strings.TrimSpace(d.relayToken); token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, d.relayURL, headers)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (d *Daemon) consumeRelayConnection(ctx context.Context, conn relayConnection, lastEventID string) (string, error) {
	for {
		if ctx.Err() != nil {
			return lastEventID, ctx.Err()
		}

		if err := conn.SetReadDeadline(time.Now().Add(relayReadTimeout)); err != nil {
			return lastEventID, err
		}

		var msg relayEventMessage
		if err := conn.ReadJSON(&msg); err != nil {
			return lastEventID, err
		}

		if msg.ID != "" {
			lastEventID = msg.ID
		}
		d.handleRelayEvent(ctx, msg)
	}
}

func (d *Daemon) handleRelayEvent(ctx context.Context, msg relayEventMessage) {
	if relayEventTriggersMergeConflictRefresh(msg) {
		go d.handleRelayMergeConflictRefresh(d.withMonitorCircuits(ctx), msg)
	}

	ctx = d.withMonitorCircuits(ctx)
	if d.handleRelayPullRequestLifecycleEvent(ctx, msg) {
		return
	}

	kind, ok := relayEventCheckKind(msg)
	if !ok || msg.PRNumber <= 0 {
		return
	}

	active, ok := d.activeAssignmentForRelayEvent(ctx, msg.Repo, msg.PRNumber)
	if !ok {
		return
	}
	d.dispatchTaskMonitorCheck(ctx, active, kind)
}

func relayEventCheckKind(msg relayEventMessage) (taskMonitorCheckKind, bool) {
	switch msg.eventType() {
	case "pull_request_review", "issue_comment":
		return taskMonitorCheckReviewPoll, true
	case "check_suite", "check_run":
		return taskMonitorCheckCIPoll, true
	case "pull_request_merge":
		return taskMonitorCheckMergePoll, true
	case "pull_request":
		if msg.merged() {
			return taskMonitorCheckMergePoll, true
		}
	}
	return 0, false
}

func (d *Daemon) activeAssignmentForRelayEvent(ctx context.Context, repo string, prNumber int) (ActiveAssignment, bool) {
	repo = strings.TrimSpace(repo)
	if prNumber <= 0 {
		return ActiveAssignment{}, false
	}

	active, err := d.state.ActiveAssignmentByPRNumber(ctx, d.project, prNumber)
	if err == nil && d.relayAssignmentMatchesRepo(active, repo) {
		return active, true
	}
	return d.activeAssignmentForRelayEventFallback(ctx, repo, func(active ActiveAssignment) bool {
		return active.Task.PRNumber == prNumber
	})
}

func (d *Daemon) activeAssignmentForBranch(ctx context.Context, repo, branch string) (ActiveAssignment, bool) {
	repo = strings.TrimSpace(repo)
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return ActiveAssignment{}, false
	}

	active, err := d.state.ActiveAssignmentByBranch(ctx, d.project, branch)
	if err == nil && d.relayAssignmentMatchesRepo(active, repo) {
		return active, true
	}
	return d.activeAssignmentForRelayEventFallback(ctx, repo, func(active ActiveAssignment) bool {
		return strings.EqualFold(strings.TrimSpace(active.Task.Branch), branch)
	})
}

func (d *Daemon) activeAssignmentForRelayEventFallback(ctx context.Context, repo string, match func(ActiveAssignment) bool) (ActiveAssignment, bool) {
	assignments, err := d.state.ActiveAssignments(ctx, d.project)
	if err != nil {
		return ActiveAssignment{}, false
	}

	for _, active := range assignments {
		if !match(active) || !d.relayAssignmentMatchesRepo(active, repo) {
			continue
		}
		return active, true
	}
	return ActiveAssignment{}, false
}

func (d *Daemon) relayAssignmentMatchesRepo(active ActiveAssignment, repo string) bool {
	repo = strings.TrimSpace(repo)
	return repo == "" || d.relayProjectMatches(active.Task.Project, repo)
}

func (d *Daemon) handleRelayPullRequestLifecycleEvent(ctx context.Context, msg relayEventMessage) bool {
	if msg.eventType() != "pull_request" {
		return false
	}

	switch action := msg.action(); {
	case strings.EqualFold(action, "opened"):
		active, ok := d.activeAssignmentForBranch(ctx, msg.Repo, msg.headBranch())
		if !ok {
			return false
		}
		active = d.bindRelayPRToAssignment(ctx, active, msg.PRNumber, true)
		d.dispatchTaskMonitorCheck(ctx, active, taskMonitorCheckPRPoll)
		return true
	case strings.EqualFold(action, "synchronize"):
		active, ok := d.activeAssignmentForRelayEvent(ctx, msg.Repo, msg.PRNumber)
		if !ok {
			active, ok = d.activeAssignmentForBranch(ctx, msg.Repo, msg.headBranch())
			if !ok {
				return false
			}
		}
		active = d.bindRelayPRToAssignment(ctx, active, msg.PRNumber, false)
		d.dispatchTaskMonitorCheck(ctx, active, taskMonitorCheckPRPoll)
		return true
	case strings.EqualFold(action, "closed") && msg.merged():
		active, ok := d.activeAssignmentForRelayEvent(ctx, msg.Repo, msg.PRNumber)
		if !ok {
			active, ok = d.activeAssignmentForBranch(ctx, msg.Repo, msg.headBranch())
			if !ok {
				return false
			}
		}
		active = d.bindRelayPRToAssignment(ctx, active, msg.PRNumber, false)
		d.dispatchTaskMonitorCheck(ctx, active, taskMonitorCheckMergePoll)
		return true
	default:
		return false
	}
}

func (d *Daemon) bindRelayPRToAssignment(ctx context.Context, active ActiveAssignment, prNumber int, emitDetected bool) ActiveAssignment {
	if prNumber <= 0 {
		return active
	}

	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		profile = AgentProfile{Name: active.Task.AgentProfile}
	}

	now := d.now()
	update := TaskStateUpdate{Active: active}
	prChanged := update.Active.Task.PRNumber != prNumber

	if prChanged {
		metadata, err := d.prPaneMetadata(ctx, update.Active, prNumber)
		if err == nil {
			update.PaneMetadata = mergeMetadata(update.PaneMetadata, metadata)
		}
		update.Active.Task.PRNumber = prNumber
		update.Active.Task.UpdatedAt = now
		update.TaskChanged = true
	}
	if update.Active.Worker.LastPRNumber != prNumber {
		update.Active.Worker.LastPRNumber = prNumber
		update.WorkerChanged = true
	}
	if update.Active.Worker.LastPushAt.IsZero() || !update.Active.Worker.LastPushAt.Equal(now) {
		update.Active.Worker.LastPushAt = now
		update.WorkerChanged = true
	}
	if update.WorkerChanged {
		update.Active.Worker.LastSeenAt = now
	}
	if prChanged && emitDetected {
		update.Events = append(update.Events, d.assignmentEvent(update.Active, profile, EventPRDetected, "pull request detected"))
	}

	if update.TaskChanged || update.WorkerChanged || len(update.PaneMetadata) > 0 || len(update.Events) > 0 {
		d.applyTaskStateUpdate(ctx, update)
		return update.Active
	}
	return active
}

func (msg relayEventMessage) eventType() string {
	return firstNonEmpty(strings.TrimSpace(msg.EventType), strings.TrimSpace(msg.Type))
}

func (msg relayEventMessage) action() string {
	return firstNonEmpty(strings.TrimSpace(msg.Action), relayPayloadSummaryString(msg.PayloadSummary, "action"))
}

func (msg relayEventMessage) merged() bool {
	if msg.Merged {
		return true
	}
	value, ok := relayPayloadSummaryBool(msg.PayloadSummary, "merged")
	return ok && value
}

func (msg relayEventMessage) headBranch() string {
	return firstNonEmpty(
		strings.TrimSpace(msg.HeadBranch),
		strings.TrimSpace(msg.HeadRef),
		relayPayloadSummaryString(msg.PayloadSummary, "head_branch"),
		relayPayloadSummaryString(msg.PayloadSummary, "head_ref"),
	)
}

func relayPayloadSummaryString(summary map[string]any, key string) string {
	if len(summary) == 0 {
		return ""
	}
	value, ok := summary[key]
	if !ok {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(text)
}

func relayPayloadSummaryBool(summary map[string]any, key string) (bool, bool) {
	if len(summary) == 0 {
		return false, false
	}
	value, ok := summary[key]
	if !ok {
		return false, false
	}
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		switch strings.TrimSpace(strings.ToLower(typed)) {
		case "true":
			return true, true
		case "false":
			return false, true
		}
	}
	return false, false
}

func (d *Daemon) relayProjectMatches(projectPath, repo string) bool {
	repoAliases := relayAliasSet(relayRepoAliases(repo))
	if len(repoAliases) == 0 {
		return false
	}
	for _, alias := range relayRepoAliases(projectPath) {
		if repoAliases[strings.ToLower(alias)] {
			return true
		}
	}
	if d.detectOrigin == nil {
		return false
	}
	origin, err := d.detectOrigin(projectPath)
	if err != nil {
		return false
	}
	for _, alias := range relayRepoAliases(origin) {
		if repoAliases[strings.ToLower(alias)] {
			return true
		}
	}
	return false
}

func relayAliasSet(aliases []string) map[string]bool {
	out := make(map[string]bool, len(aliases))
	for _, alias := range aliases {
		alias = strings.TrimSpace(alias)
		if alias == "" {
			continue
		}
		out[strings.ToLower(alias)] = true
	}
	return out
}

func relayRepoAliases(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	aliases := make([]string, 0, 6)
	seen := make(map[string]struct{}, 6)
	add := func(alias string) {
		alias = strings.TrimSpace(strings.TrimSuffix(alias, ".git"))
		alias = strings.TrimSpace(strings.Trim(alias, "/"))
		if alias == "" {
			return
		}
		key := strings.ToLower(alias)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}
		aliases = append(aliases, alias)
	}

	add(value)

	if parsed, err := url.Parse(value); err == nil && parsed.Host != "" {
		path := strings.Trim(strings.TrimSuffix(parsed.Path, ".git"), "/")
		add(path)
		if path != "" {
			add(strings.ToLower(parsed.Host) + "/" + path)
		}
		return aliases
	}

	if at := strings.Index(value, "@"); at >= 0 && strings.Contains(value, ":") && !strings.Contains(value, "://") {
		hostPath := value[at+1:]
		host, path, ok := strings.Cut(hostPath, ":")
		if ok {
			path = strings.Trim(strings.TrimSuffix(path, ".git"), "/")
			add(path)
			if path != "" {
				add(strings.ToLower(host) + "/" + path)
			}
		}
	}

	if strings.HasPrefix(value, "github.com/") {
		add(strings.TrimPrefix(value, "github.com/"))
	}
	if strings.HasPrefix(value, "/") {
		add(strings.TrimPrefix(value, "/"))
	}

	return aliases
}

func canonicalRelayRepo(value string) string {
	for _, alias := range relayRepoAliases(value) {
		if strings.Count(alias, "/") == 1 && !strings.Contains(alias, ":") {
			return alias
		}
	}
	aliases := relayRepoAliases(value)
	if len(aliases) == 0 {
		return ""
	}
	return aliases[0]
}

func (d *Daemon) buildRelayIdentifyMessage(ctx context.Context, lastEventID string) relayIdentifyMessage {
	projects := d.relayMonitoredProjects(ctx)
	return relayIdentifyMessage{
		Type:        "identify",
		Hostname:    d.hostname,
		Projects:    projects,
		LastEventID: strings.TrimSpace(lastEventID),
	}
}

func (d *Daemon) relayMonitoredProjects(ctx context.Context) []relayMonitoredProject {
	tasks, err := d.state.NonTerminalTasks(ctx, d.project)
	if err != nil {
		return nil
	}

	seen := make(map[string]struct{}, len(tasks)+1)
	projects := make([]relayMonitoredProject, 0, len(tasks)+1)
	addProject := func(projectPath string) {
		projectPath = strings.TrimSpace(projectPath)
		if projectPath == "" {
			return
		}
		if _, ok := seen[projectPath]; ok {
			return
		}
		seen[projectPath] = struct{}{}

		project := relayMonitoredProject{Path: projectPath}
		if d.detectOrigin != nil {
			if origin, err := d.detectOrigin(projectPath); err == nil {
				project.Repo = canonicalRelayRepo(origin)
			}
		}
		projects = append(projects, project)
	}

	addProject(d.project)
	for _, task := range tasks {
		projectPath := task.Project
		if projectPath == "" {
			projectPath = d.project
		}
		addProject(projectPath)
	}

	sort.Slice(projects, func(i, j int) bool {
		return projects[i].Path < projects[j].Path
	})
	return projects
}

func (d *Daemon) requestRelayReconnect() {
	if !d.relayEnabled() {
		return
	}

	d.relayConnMu.Lock()
	conn := d.relayConn
	d.relayConnMu.Unlock()
	if conn == nil {
		// A disconnected relay is already heading toward the next dial attempt,
		// so there is nothing to interrupt here.
		return
	}

	d.relayReconnect.Store(true)
	d.closeRelayConn()
}

func (d *Daemon) consumeRelayReconnectRequest() bool {
	return d.relayReconnect.Swap(false)
}

func (d *Daemon) setRelayHealthy(healthy bool) {
	if d.relayHealthy.Swap(healthy) == healthy {
		return
	}
	d.enqueuePollIntervalUpdate(d.currentPRPollInterval())
}

func (d *Daemon) enqueuePollIntervalUpdate(interval time.Duration) {
	if d.pollIntervalCh == nil {
		return
	}

	// Only the latest interval matters. If the channel is full, replace the
	// queued value with the most recent one instead of blocking the relay loop.
	select {
	case d.pollIntervalCh <- interval:
		return
	default:
	}

	select {
	case <-d.pollIntervalCh:
	default:
	}

	select {
	case d.pollIntervalCh <- interval:
	default:
	}
}

func (d *Daemon) currentPRPollInterval() time.Duration {
	if d.relayHealthy.Load() {
		return relayHealthyPollInterval
	}
	return d.pollInterval
}

func (d *Daemon) setRelayConn(conn relayConnection) {
	d.relayConnMu.Lock()
	defer d.relayConnMu.Unlock()
	d.relayConn = conn
}

func (d *Daemon) clearRelayConn(conn relayConnection) {
	d.relayConnMu.Lock()
	defer d.relayConnMu.Unlock()
	if d.relayConn == conn {
		d.relayConn = nil
	}
}

func (d *Daemon) closeRelayConn() {
	d.relayConnMu.Lock()
	conn := d.relayConn
	d.relayConn = nil
	d.relayConnMu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (d *Daemon) logRelayError(message string, err error) {
	if err == nil || d.logf == nil {
		return
	}
	d.logf("%s: %v", message, err)
}

func (d *Daemon) checkTaskImmediateReviewPoll(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	if active.Task.PRNumber == 0 {
		return TaskStateUpdate{Active: active}
	}
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return TaskStateUpdate{Active: active}
	}
	return d.checkTaskReviewPoll(ctx, active, profile)
}

func (d *Daemon) checkTaskImmediateCIPoll(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}
	if active.Task.PRNumber == 0 {
		return update
	}
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return update
	}
	d.handlePRChecksPoll(ctx, &update, profile)
	return update
}

func (d *Daemon) checkTaskImmediateMergePoll(ctx context.Context, active ActiveAssignment) TaskStateUpdate {
	update := TaskStateUpdate{Active: active}
	if active.Task.PRNumber == 0 {
		return update
	}
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		return update
	}
	merged, err := d.isPRMerged(ctx, active.Task.Project, active.Task.PRNumber)
	if err != nil {
		d.appendGitHubRateLimitEvent(&update, profile, err)
		return update
	}
	update.PRMerged = merged
	return update
}
