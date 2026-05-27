package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/linear"
)

func TestAssignLinearStatusWriteBack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		issue       string
		statusErr   error
		wantSkipped bool
	}{
		{
			name:  "existing Linear issue updates status",
			issue: "LAB-1944",
		},
		{
			name:        "missing beads issue logs and continues",
			issue:       "orca-p32",
			statusErr:   errors.New("Entity not found: Issue (INPUT_ERROR)"),
			wantSkipped: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
			if tt.statusErr != nil {
				deps.issueTracker.errors = map[string]error{
					IssueStateInProgress: tt.statusErr,
				}
			}
			d := deps.newDaemon(t)
			ctx := context.Background()

			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			if err := d.Assign(ctx, tt.issue, "Implement status write-back handling.", "codex"); err != nil {
				t.Fatalf("Assign() error = %v, want success", err)
			}

			task, ok := deps.state.task(tt.issue)
			if !ok {
				t.Fatal("task missing after assign")
			}
			if got, want := task.Status, TaskStatusActive; got != want {
				t.Fatalf("task.Status = %q, want %q", got, want)
			}
			if _, ok := deps.state.worker("pane-1"); !ok {
				t.Fatal("worker missing after assign")
			}
			if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: tt.issue, State: IssueStateInProgress}}; !reflect.DeepEqual(got, want) {
				t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
			}
			if got := deps.events.countType(EventTaskAssignFailed); got != 0 {
				t.Fatalf("assign failure events = %d, want 0", got)
			}

			wantSkippedEvents := 0
			if tt.wantSkipped {
				wantSkippedEvents = 1
			}
			if got := deps.events.countType(EventIssueStatusSkipped); got != wantSkippedEvents {
				t.Fatalf("status skipped events = %d, want %d", got, wantSkippedEvents)
			}
			if tt.wantSkipped {
				event, ok := deps.events.lastEventOfType(EventIssueStatusSkipped)
				if !ok {
					t.Fatal("missing issue status skipped event")
				}
				if got, want := event.Issue, tt.issue; got != want {
					t.Fatalf("event.Issue = %q, want %q", got, want)
				}
				if !strings.Contains(event.Message, "INPUT_ERROR") {
					t.Fatalf("event.Message = %q, want INPUT_ERROR context", event.Message)
				}
			}
		})
	}
}

func TestAssignContinuesWhenLinearIssueLookupReturnsEntityNotFound(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	const issue = "fix-resume-sequence"
	deps.issueTracker.errors = map[string]error{
		IssueStateInProgress: fmt.Errorf("lookup issue %s: %w", issue, linear.ErrEntityNotFound),
	}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, issue, "Fix the resume sequence handling.", "codex"); err != nil {
		t.Fatalf("Assign() error = %v, want success", err)
	}

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after assign")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); !ok {
		t.Fatal("worker missing after assign")
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none", got)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: issue, State: IssueStateInProgress}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}

	event, ok := deps.events.lastEventOfType(EventIssueStatusSkipped)
	if !ok {
		t.Fatal("missing issue status skipped event")
	}
	if got, want := event.Issue, issue; got != want {
		t.Fatalf("event.Issue = %q, want %q", got, want)
	}
	if !strings.Contains(event.Message, IssueStateInProgress) {
		t.Fatalf("event.Message = %q, want state context", event.Message)
	}

	deps.events.requireTypes(t, EventDaemonStarted, EventIssueStatusSkipped, EventTaskAssigned)
}

func TestResumeExistingPaneContinuesWhenLinearIssueLookupReturnsEntityNotFound(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	const issue = "fix-resume-sequence"
	seedActiveAssignment(t, deps, issue, "pane-1")

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("seeded task missing")
	}
	task.Status = TaskStatusCancelled
	if err := deps.state.PutTask(context.Background(), task); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}

	deps.issueTracker.errors = map[string]error{
		IssueStateInProgress: fmt.Errorf("lookup issue %s: %w", issue, linear.ErrEntityNotFound),
	}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Resume(ctx, issue, "Continue from the latest review feedback"); err != nil {
		t.Fatalf("Resume() error = %v, want success", err)
	}

	resumedTask, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after resume")
	}
	if got, want := resumedTask.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := resumedTask.Prompt, "Continue from the latest review feedback"; got != want {
		t.Fatalf("task.Prompt = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); !ok {
		t.Fatal("worker missing after resume")
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: issue, State: IssueStateInProgress}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}

	event, ok := deps.events.lastEventOfType(EventIssueStatusSkipped)
	if !ok {
		t.Fatal("missing issue status skipped event")
	}
	if got, want := event.Issue, issue; got != want {
		t.Fatalf("event.Issue = %q, want %q", got, want)
	}
	if !strings.Contains(event.Message, IssueStateInProgress) {
		t.Fatalf("event.Message = %q, want state context", event.Message)
	}
}

func TestResumeFreshPaneContinuesWhenLinearIssueLookupReturnsEntityNotFound(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	const issue = "fix-resume-sequence"
	seedActiveAssignment(t, deps, issue, "pane-9")

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("seeded task missing")
	}
	task.Status = TaskStatusCancelled
	if err := deps.state.PutTask(context.Background(), task); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}
	if err := deps.state.DeleteWorker(context.Background(), task.Project, task.PaneID); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}

	deps.amux.spawnPane = Pane{ID: "pane-2", Name: "w-fix-resume-sequence"}
	deps.amux.paneExists = map[string]bool{"pane-9": false}
	deps.issueTracker.errors = map[string]error{
		IssueStateInProgress: fmt.Errorf("lookup issue %s: %w", issue, linear.ErrEntityNotFound),
	}
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Resume(ctx, issue, "Resume work after the cancellation"); err != nil {
		t.Fatalf("Resume() error = %v, want success", err)
	}

	resumedTask, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after resume")
	}
	if got, want := resumedTask.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := resumedTask.PaneID, "pane-2"; got != want {
		t.Fatalf("task.PaneID = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-2"); !ok {
		t.Fatal("worker missing after respawn")
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none", got)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: issue, State: IssueStateInProgress}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}

	event, ok := deps.events.lastEventOfType(EventIssueStatusSkipped)
	if !ok {
		t.Fatal("missing issue status skipped event")
	}
	if got, want := event.Issue, issue; got != want {
		t.Fatalf("event.Issue = %q, want %q", got, want)
	}
	if !strings.Contains(event.Message, IssueStateInProgress) {
		t.Fatalf("event.Message = %q, want state context", event.Message)
	}
}
