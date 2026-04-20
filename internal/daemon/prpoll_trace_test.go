package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
)

type prPollTraceState struct {
	*fakeState
	tasksByPaneErr error
}

func (s *prPollTraceState) TasksByPane(ctx context.Context, project, paneID string) ([]Task, error) {
	if s.tasksByPaneErr != nil {
		return nil, s.tasksByPaneErr
	}
	return s.fakeState.TasksByPane(ctx, project, paneID)
}

func TestCheckTaskPRPollEmitsMetadataErrorTrace(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1415", "pane-1", 0)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1415", "--json", "number"}, `[{"number":42}]`, nil)

	metadataErr := errors.New("history unavailable")
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.State = &prPollTraceState{
			fakeState:      deps.state,
			tasksByPaneErr: metadataErr,
		}
	})

	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, "LAB-1415"))
	if got, want := len(update.Events), 1; got != want {
		t.Fatalf("len(update.Events) = %d, want %d", got, want)
	}
	if got, want := update.Events[0].Type, EventPRPollTrace; got != want {
		t.Fatalf("update.Events[0].Type = %q, want %q", got, want)
	}
	if got, want := update.Events[0].Message, `pr poll trace: issue=LAB-1415 pr_number=0 action=pr_metadata_error error="load pane task history: history unavailable"`; got != want {
		t.Fatalf("update.Events[0].Message = %q, want %q", got, want)
	}
	if got, want := update.Active.Worker.LastPRPollAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("worker.LastPRPollAt = %v, want %v", got, want)
	}
	if got := update.Active.Task.PRNumber; got != 0 {
		t.Fatalf("task.PRNumber = %d, want 0 when metadata sync fails", got)
	}
}

func TestTracePRPollAppendsEventAndLogs(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1415", "pane-1", 42)
	var logged []string
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = func(format string, args ...any) {
			logged = append(logged, formatMessage(format, args...))
		}
	})

	update := TaskStateUpdate{Active: activeTaskMonitorAssignment(t, deps, "LAB-1415")}
	traceErr := errors.New("github unavailable")

	d.tracePRPoll(&update, AgentProfile{Name: "codex"}, "follow_up_poll", traceErr)
	d.tracePRPoll(nil, AgentProfile{Name: "codex"}, "ignored", nil)

	if got, want := len(update.Events), 1; got != want {
		t.Fatalf("len(update.Events) = %d, want %d", got, want)
	}
	if got, want := update.Events[0].Message, `pr poll trace: issue=LAB-1415 pr_number=42 action=follow_up_poll error="github unavailable"`; got != want {
		t.Fatalf("update.Events[0].Message = %q, want %q", got, want)
	}
	if got, want := logged, []string{`pr poll trace: issue=LAB-1415 pr_number=42 action=follow_up_poll error="github unavailable"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("logged = %#v, want %#v", got, want)
	}
}

func TestEmitPRPollTaskTraceEmitsWithoutIssueWhenProfileLookupFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	d.emitPRPollTaskTrace(context.Background(), Task{
		Project:      "/tmp/project",
		PaneID:       "pane-1",
		PaneName:     "pane-1",
		ClonePath:    "/tmp/clone",
		Branch:       "LAB-1415",
		AgentProfile: "missing",
	}, Worker{
		Project:      "/tmp/project",
		PaneID:       "pane-1",
		PaneName:     "pane-1",
		ClonePath:    "/tmp/clone",
		AgentProfile: "missing",
	}, "monitor_missing", nil)

	event, ok := deps.events.lastEventOfType(EventPRPollTrace)
	if !ok {
		t.Fatal("pr poll trace event missing")
	}
	if got, want := event.Message, "pr poll trace: issue= pr_number=0 action=monitor_missing"; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
	if got, want := event.AgentProfile, "missing"; got != want {
		t.Fatalf("event.AgentProfile = %q, want %q", got, want)
	}
}

func TestEmitProjectPRPollTraceLogsAndTrimsFields(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	var logged []string
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = func(format string, args ...any) {
			logged = append(logged, formatMessage(format, args...))
		}
	})

	d.emitProjectPRPollTrace(context.Background(), " /tmp/project ", " list_non_terminal_tasks_error ", errors.New("postgres unavailable"))

	event, ok := deps.events.lastEventOfType(EventPRPollTrace)
	if !ok {
		t.Fatal("pr poll trace event missing")
	}
	if got, want := event.Project, "/tmp/project"; got != want {
		t.Fatalf("event.Project = %q, want %q", got, want)
	}
	if got, want := event.Message, `pr poll trace: action=list_non_terminal_tasks_error error="postgres unavailable"`; got != want {
		t.Fatalf("event.Message = %q, want %q", got, want)
	}
	if got, want := logged, []string{`pr poll trace: action=list_non_terminal_tasks_error error="postgres unavailable"`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("logged = %#v, want %#v", got, want)
	}
}

func formatMessage(format string, args ...any) string {
	return fmt.Sprintf(format, args...)
}
