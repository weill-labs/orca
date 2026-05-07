package cli

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

const (
	eventsPostmortemFilter          = "postmortem"
	postmortemSuspiciousEventKind   = "worker.postmortem_suspicious"
	postmortemFollowOnWindow        = 5 * time.Minute
	postmortemExpiredSettleDuration = time.Second
)

// A healthy postmortem send is followed by finishAssignment emitting one of
// these terminal task lifecycle events after cleanup. task.failed skips
// postmortem today, so it is not an expected follow-on for a sent postmortem.
var postmortemExpectedFollowOnKinds = []string{
	daemon.EventTaskCompleted,
	daemon.EventTaskCancelled,
}

type eventsView struct {
	filter     string
	postmortem *postmortemEventTracker
}

func newEventsView(filter string) *eventsView {
	filter = strings.TrimSpace(filter)
	view := &eventsView{filter: filter}
	if isPostmortemFilter(filter) {
		view.postmortem = newPostmortemEventTracker(postmortemFollowOnWindow)
	}
	return view
}

func isPostmortemFilter(filter string) bool {
	return strings.EqualFold(filter, eventsPostmortemFilter) || filter == daemon.EventWorkerPostmortem
}

func (v *eventsView) process(event state.Event, now time.Time) []state.Event {
	if v.postmortem == nil {
		if v.filter == "" || event.Kind == v.filter {
			return []state.Event{event}
		}
		return nil
	}

	eventTime := event.CreatedAt
	if eventTime.IsZero() {
		eventTime = now
	}

	out := v.postmortem.flushDue(eventTime)
	if v.postmortem.includes(event) {
		out = append(out, event)
	}
	v.postmortem.observe(event)
	return out
}

func (v *eventsView) flushDue(now time.Time) []state.Event {
	if v.postmortem == nil {
		return nil
	}
	return v.postmortem.flushDue(now)
}

func (v *eventsView) nextDeadline() (time.Time, bool) {
	if v.postmortem == nil {
		return time.Time{}, false
	}
	return v.postmortem.nextDeadline()
}

type postmortemEventTracker struct {
	window  time.Duration
	seen    map[postmortemKey]bool
	pending map[postmortemKey]pendingPostmortem
}

type postmortemKey struct {
	project string
	issue   string
	worker  string
}

type pendingPostmortem struct {
	event    state.Event
	details  postmortemEventDetails
	deadline time.Time
}

type postmortemEventDetails struct {
	Project   string `json:"project,omitempty"`
	Issue     string `json:"issue,omitempty"`
	WorkerID  string `json:"worker_id,omitempty"`
	PaneID    string `json:"pane_id,omitempty"`
	PaneName  string `json:"pane_name,omitempty"`
	CloneName string `json:"clone_name,omitempty"`
	ClonePath string `json:"clone_path,omitempty"`
	Branch    string `json:"branch,omitempty"`
	PRNumber  int    `json:"pr_number,omitempty"`
}

type suspiciousPostmortemPayload struct {
	OriginalEventID int64                  `json:"original_event_id"`
	OriginalTime    time.Time              `json:"original_time"`
	ExpectedFollow  []string               `json:"expected_follow_on"`
	FollowOnWindow  string                 `json:"follow_on_window"`
	Details         postmortemEventDetails `json:"details"`
}

func newPostmortemEventTracker(window time.Duration) *postmortemEventTracker {
	return &postmortemEventTracker{
		window:  window,
		seen:    make(map[postmortemKey]bool),
		pending: make(map[postmortemKey]pendingPostmortem),
	}
}

func (t *postmortemEventTracker) includes(event state.Event) bool {
	switch event.Kind {
	case daemon.EventWorkerPostmortem:
		return true
	case daemon.EventTaskCompleted, daemon.EventTaskCancelled:
		return t.seen[postmortemEventKey(event)]
	default:
		return false
	}
}

func (t *postmortemEventTracker) observe(event state.Event) {
	key := postmortemEventKey(event)
	switch event.Kind {
	case daemon.EventWorkerPostmortem:
		t.seen[key] = true
		if !isSentPostmortem(event) {
			return
		}
		createdAt := event.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now().UTC()
		}
		t.pending[key] = pendingPostmortem{
			event:    event,
			details:  postmortemDetails(event),
			deadline: createdAt.Add(t.window),
		}
	case daemon.EventTaskCompleted, daemon.EventTaskCancelled:
		delete(t.pending, key)
	}
}

func (t *postmortemEventTracker) flushDue(now time.Time) []state.Event {
	if now.IsZero() || len(t.pending) == 0 {
		return nil
	}

	keys := make([]postmortemKey, 0, len(t.pending))
	for key, pending := range t.pending {
		if !pending.deadline.After(now) {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return t.pending[keys[i]].event.ID < t.pending[keys[j]].event.ID
	})

	events := make([]state.Event, 0, len(keys))
	for _, key := range keys {
		pending := t.pending[key]
		events = append(events, suspiciousPostmortemEvent(pending, now, t.window))
		delete(t.pending, key)
	}
	return events
}

func (t *postmortemEventTracker) nextDeadline() (time.Time, bool) {
	if len(t.pending) == 0 {
		return time.Time{}, false
	}

	var next time.Time
	for _, pending := range t.pending {
		if next.IsZero() || pending.deadline.Before(next) {
			next = pending.deadline
		}
	}
	return next, true
}

func postmortemEventKey(event state.Event) postmortemKey {
	if strings.TrimSpace(event.Issue) != "" {
		return postmortemKey{project: event.Project, issue: event.Issue}
	}
	return postmortemKey{project: event.Project, worker: event.WorkerID}
}

func isSentPostmortem(event state.Event) bool {
	if event.Kind != daemon.EventWorkerPostmortem {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(event.Message))
	return message == "postmortem sent" || strings.HasPrefix(message, "postmortem sent:")
}

func postmortemDetails(event state.Event) postmortemEventDetails {
	details := postmortemEventDetails{
		Project:  event.Project,
		Issue:    event.Issue,
		WorkerID: event.WorkerID,
	}

	if len(event.Payload) == 0 {
		return details
	}

	var payload daemon.Event
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return details
	}
	if details.Project == "" {
		details.Project = payload.Project
	}
	if details.Issue == "" {
		details.Issue = payload.Issue
	}
	if details.WorkerID == "" {
		details.WorkerID = payload.WorkerID
	}
	details.PaneID = payload.PaneID
	details.PaneName = payload.PaneName
	details.CloneName = payload.CloneName
	details.ClonePath = payload.ClonePath
	details.Branch = payload.Branch
	details.PRNumber = payload.PRNumber
	return details
}

func suspiciousPostmortemEvent(pending pendingPostmortem, now time.Time, window time.Duration) state.Event {
	payload, _ := json.Marshal(suspiciousPostmortemPayload{
		OriginalEventID: pending.event.ID,
		OriginalTime:    pending.event.CreatedAt,
		ExpectedFollow:  postmortemExpectedFollowOnKinds,
		FollowOnWindow:  window.String(),
		Details:         pending.details,
	})

	return state.Event{
		Project:   pending.event.Project,
		Kind:      postmortemSuspiciousEventKind,
		Issue:     pending.event.Issue,
		WorkerID:  pending.event.WorkerID,
		Message:   suspiciousPostmortemMessage(pending.details, window),
		Payload:   payload,
		CreatedAt: now,
	}
}

func suspiciousPostmortemMessage(details postmortemEventDetails, window time.Duration) string {
	subject := make([]string, 0, 4)
	if details.Issue != "" {
		subject = append(subject, "issue "+details.Issue)
	}
	if details.WorkerID != "" {
		subject = append(subject, "worker "+details.WorkerID)
	}
	if details.PaneID != "" {
		pane := "pane " + details.PaneID
		if details.PaneName != "" {
			pane += " (" + details.PaneName + ")"
		}
		subject = append(subject, pane)
	}
	if details.CloneName != "" {
		subject = append(subject, "clone "+details.CloneName)
	}
	if len(subject) == 0 {
		subject = append(subject, "worker assignment")
	}

	return fmt.Sprintf(
		"postmortem sent without %s or %s follow-on within %s; inspect %s",
		daemon.EventTaskCompleted,
		daemon.EventTaskCancelled,
		window,
		strings.Join(subject, " "),
	)
}
