package daemon

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestCheckTaskPRPollTransitionsTaskState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialState string
		prNumber     int
		queue        func(*testDeps, string)
		wantState    string
		wantEvent    string
		wantCIState  string
	}{
		{
			name:         "assigned moves to pr detected when pull request appears",
			initialState: TaskStateAssigned,
			queue: func(deps *testDeps, issue string) {
				deps.commands.queue("gh", []string{"pr", "list", "--head", issue, "--json", "number"}, `[{"number":42}]`, nil)
				deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, ``, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}, ``, nil)
			},
			wantState: TaskStatePRDetected,
			wantEvent: EventPRDetected,
		},
		{
			name:         "pr detected moves to ci pending while checks are pending",
			initialState: TaskStatePRDetected,
			prNumber:     42,
			queue: func(deps *testDeps, _ string) {
				deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}, ``, nil)
			},
			wantState:   TaskStateCIPending,
			wantCIState: ciStatePending,
		},
		{
			name:         "ci pending moves to review pending once checks pass",
			initialState: TaskStateCIPending,
			prNumber:     42,
			queue: func(deps *testDeps, _ string) {
				deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":null}`, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}, ``, nil)
			},
			wantState:   TaskStateReviewPending,
			wantCIState: ciStatePass,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			issue := "LAB-1254"
			seedTaskMonitorAssignmentWithState(t, deps, issue, "pane-1", tt.prNumber, tt.initialState)
			tt.queue(deps, issue)

			d := deps.newDaemon(t)
			update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, issue))

			if got, want := update.Active.Task.State, tt.wantState; got != want {
				t.Fatalf("update.Active.Task.State = %q, want %q", got, want)
			}
			if !update.TaskChanged {
				t.Fatal("update.TaskChanged = false, want true")
			}
			if tt.wantEvent != "" {
				if len(update.Events) == 0 {
					t.Fatalf("len(update.Events) = 0, want %s event", tt.wantEvent)
				}
				if got := update.Events[0].Type; got != tt.wantEvent {
					t.Fatalf("update.Events[0].Type = %q, want %q", got, tt.wantEvent)
				}
			}
			if tt.wantCIState != "" {
				if got, want := update.Active.Worker.LastCIState, tt.wantCIState; got != want {
					t.Fatalf("update.Active.Worker.LastCIState = %q, want %q", got, want)
				}
			}
		})
	}
}

func TestDaemonCaptureTickEscalatesMissingPaneForThatTaskOnly(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignmentWithState(t, deps, "LAB-1254", "pane-1", 0, TaskStateAssigned)
	seedTaskMonitorAssignmentWithState(t, deps, "LAB-1255", "pane-2", 0, TaskStateAssigned)
	deps.amux.captureSequence("pane-2", []string{"worker output"})

	amux := &paneAwareCaptureAmux{
		fakeAmux: deps.amux,
		errByPane: map[string]error{
			"pane-1": errors.New("amux capture pane-1: exit status 1: pane not found"),
		},
	}

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Amux = amux
	})

	d.runCaptureTick(context.Background())
	d.runCaptureTick(context.Background())

	taskOne, ok := deps.state.task("LAB-1254")
	if !ok {
		t.Fatal("missing task LAB-1254 after capture tick")
	}
	if got, want := taskOne.State, TaskStateEscalated; got != want {
		t.Fatalf("taskOne.State = %q, want %q", got, want)
	}

	workerOne, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("missing worker pane-1 after capture tick")
	}
	if got, want := workerOne.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("workerOne.Health = %q, want %q", got, want)
	}

	taskTwo, ok := deps.state.task("LAB-1255")
	if !ok {
		t.Fatal("missing task LAB-1255 after capture tick")
	}
	if got, want := taskTwo.State, TaskStateAssigned; got != want {
		t.Fatalf("taskTwo.State = %q, want %q", got, want)
	}

	workerTwo, ok := deps.state.worker("pane-2")
	if !ok {
		t.Fatal("missing worker pane-2 after capture tick")
	}
	if got, want := workerTwo.Health, WorkerHealthHealthy; got != want {
		t.Fatalf("workerTwo.Health = %q, want %q", got, want)
	}

	if got, want := deps.events.countType(EventWorkerEscalated), 1; got != want {
		t.Fatalf("worker escalation event count = %d, want %d", got, want)
	}
}

func TestTaskStateForAssignmentPreservesMergedState(t *testing.T) {
	t.Parallel()

	active := ActiveAssignment{
		Task: Task{
			Issue:    "LAB-1258",
			State:    TaskStateMerged,
			Status:   TaskStatusActive,
			PRNumber: 42,
		},
		Worker: Worker{
			WorkerID:     "worker-01",
			Health:       WorkerHealthHealthy,
			LastCIState:  ciStatePass,
			LastPRNumber: 42,
		},
	}

	if got, want := taskStateForAssignment(active), TaskStateMerged; got != want {
		t.Fatalf("taskStateForAssignment(...) = %q, want %q", got, want)
	}
}

func TestCheckTaskPRPollTransitionsReviewPendingToMerged(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	issue := "LAB-1256"
	seedTaskMonitorAssignmentWithState(t, deps, issue, "pane-1", 42, TaskStateReviewPending)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":"2026-04-13T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, issue))

	if got, want := update.Active.Task.State, TaskStateMerged; got != want {
		t.Fatalf("update.Active.Task.State = %q, want %q", got, want)
	}
	if !update.PRMerged {
		t.Fatal("update.PRMerged = false, want true")
	}
}

func TestCheckTaskPRPollDistinguishesPRTerminalStates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		queue              func(*testDeps)
		wantState          string
		wantPRMerged       bool
		wantCompletionType string
		wantEventType      string
	}{
		{
			name: "merged pr marks merged terminal state",
			queue: func(deps *testDeps) {
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"state":"CLOSED","mergedAt":"2026-04-13T12:00:00Z","closedAt":"2026-04-13T12:00:00Z"}`, nil)
			},
			wantState:    TaskStateMerged,
			wantPRMerged: true,
		},
		{
			name: "closed pr marks failed completion",
			queue: func(deps *testDeps) {
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"state":"CLOSED","mergedAt":null,"closedAt":"2026-04-16T12:00:00Z"}`, nil)
			},
			wantState:          TaskStateDone,
			wantCompletionType: EventTaskFailed,
			wantEventType:      EventPRClosed,
		},
		{
			name: "open pr stays review pending",
			queue: func(deps *testDeps) {
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"state":"OPEN","mergedAt":null,"closedAt":null}`, nil)
			},
			wantState: TaskStateReviewPending,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			issue := "LAB-1315"
			seedTaskMonitorAssignmentWithState(t, deps, issue, "pane-1", 42, TaskStateReviewPending)
			seedMergeEntryForTest(t, deps, issue, 42)
			tt.queue(deps)

			d := deps.newDaemon(t)
			update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, issue))

			if got, want := update.Active.Task.State, tt.wantState; got != want {
				t.Fatalf("update.Active.Task.State = %q, want %q", got, want)
			}
			if got, want := update.PRMerged, tt.wantPRMerged; got != want {
				t.Fatalf("update.PRMerged = %v, want %v", got, want)
			}
			if got, want := update.CompletionEventType, tt.wantCompletionType; got != want {
				t.Fatalf("update.CompletionEventType = %q, want %q", got, want)
			}
			if tt.wantEventType == "" {
				if got, want := len(update.Events), 1; got != want {
					t.Fatalf("len(update.Events) = %d, want %d", got, want)
				}
			} else {
				if got, want := len(update.Events), 2; got != want {
					t.Fatalf("len(update.Events) = %d, want %d", got, want)
				}
				if got, want := update.Events[0].Type, tt.wantEventType; got != want {
					t.Fatalf("update.Events[0].Type = %q, want %q", got, want)
				}
			}
			if got, want := update.Events[len(update.Events)-1].Type, EventPRPollTrace; got != want {
				t.Fatalf("update.Events[last].Type = %q, want %q", got, want)
			}
		})
	}
}

func TestCheckTaskPRPollRegularPathDistinguishesPRTerminalStates(t *testing.T) {
	t.Parallel()

	until := time.Date(2026, 4, 2, 9, 2, 0, 0, time.UTC)
	tests := []struct {
		name                 string
		queue                func(*testDeps)
		wantState            string
		wantPRMerged         bool
		wantCompletionStatus string
		wantCompletionType   string
		wantEventType        string
		wantRateLimitedUntil time.Time
	}{
		{
			name: "merged pr terminal state marks merged",
			queue: func(deps *testDeps) {
				deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[]`, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"state":"MERGED","mergedAt":null,"closedAt":null}`, nil)
			},
			wantState:    TaskStateMerged,
			wantPRMerged: true,
		},
		{
			name: "closed pr terminal state fails task",
			queue: func(deps *testDeps) {
				deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[]`, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"state":"CLOSED","mergedAt":null,"closedAt":"2026-04-16T12:00:00Z"}`, nil)
			},
			wantState:            TaskStateDone,
			wantCompletionStatus: TaskStatusFailed,
			wantCompletionType:   EventTaskFailed,
			wantEventType:        EventPRClosed,
		},
		{
			name: "rate limited terminal state returns early",
			queue: func(deps *testDeps) {
				deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[]`, nil)
				deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, "HTTP 429: API rate limit exceeded\nRetry-After: 120\n", errors.New("gh: HTTP 429"))
			},
			wantState:            TaskStateReviewPending,
			wantRateLimitedUntil: until,
			wantEventType:        EventPRRateLimited,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			issue := "LAB-1315"
			seedTaskMonitorAssignmentWithState(t, deps, issue, "pane-1", 42, TaskStateReviewPending)
			tt.queue(deps)

			d := deps.newDaemon(t)
			update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, issue))

			if got, want := update.Active.Task.State, tt.wantState; got != want {
				t.Fatalf("update.Active.Task.State = %q, want %q", got, want)
			}
			if got, want := update.PRMerged, tt.wantPRMerged; got != want {
				t.Fatalf("update.PRMerged = %v, want %v", got, want)
			}
			if got, want := update.CompletionStatus, tt.wantCompletionStatus; got != want {
				t.Fatalf("update.CompletionStatus = %q, want %q", got, want)
			}
			if got, want := update.CompletionEventType, tt.wantCompletionType; got != want {
				t.Fatalf("update.CompletionEventType = %q, want %q", got, want)
			}
			if tt.wantEventType == "" {
				if got, want := len(update.Events), 1; got != want {
					t.Fatalf("len(update.Events) = %d, want %d", got, want)
				}
			} else {
				if got, want := len(update.Events), 2; got != want {
					t.Fatalf("len(update.Events) = %d, want %d", got, want)
				}
				if got, want := update.Events[0].Type, tt.wantEventType; got != want {
					t.Fatalf("update.Events[0].Type = %q, want %q", got, want)
				}
				if !tt.wantRateLimitedUntil.IsZero() && !update.Events[0].GitHubRateLimitedUntil.Equal(tt.wantRateLimitedUntil) {
					t.Fatalf("update.Events[0].GitHubRateLimitedUntil = %v, want %v", update.Events[0].GitHubRateLimitedUntil, tt.wantRateLimitedUntil)
				}
			}
			if got, want := update.Events[len(update.Events)-1].Type, EventPRPollTrace; got != want {
				t.Fatalf("update.Events[last].Type = %q, want %q", got, want)
			}
			if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 0; got != want {
				t.Fatalf("mergeable follow-up call count = %d, want %d", got, want)
			}
			if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}), 0; got != want {
				t.Fatalf("review follow-up call count = %d, want %d", got, want)
			}
		})
	}
}

func TestApplyTaskStateUpdateFailsTaskWhenPRClosesWithoutMerge(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	issue := "LAB-1315"
	seedTaskMonitorAssignmentWithState(t, deps, issue, "pane-1", 42, TaskStateReviewPending)
	setAssignmentWorkerID(t, deps, issue, "worker-01")
	seedMergeEntryForTest(t, deps, issue, 42)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"state":"CLOSED","mergedAt":null,"closedAt":"2026-04-16T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, issue))
	d.applyTaskStateUpdate(context.Background(), update)

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after closed PR completion")
	}
	if got, want := task.Status, TaskStatusFailed; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := task.State, TaskStateDone; got != want {
		t.Fatalf("task.State = %q, want %q", got, want)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          "clone-" + issue,
		Path:          "/tmp/" + issue,
		CurrentBranch: issue,
		AssignedTask:  issue,
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{closedWrapUpPrompt + "\n"})
	deps.events.requireTypes(t, EventPRClosed, EventTaskFailed)
}
func TestApplyTaskStateUpdateCompletesMergedTask(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	issue := "LAB-1257"
	seedTaskMonitorAssignmentWithState(t, deps, issue, "pane-1", 42, TaskStateMerged)

	d := deps.newDaemon(t)
	active := activeTaskMonitorAssignment(t, deps, issue)
	active.Task.State = TaskStateMerged
	active.Task.UpdatedAt = deps.clock.Now()

	d.applyTaskStateUpdate(context.Background(), TaskStateUpdate{
		Active:      active,
		TaskChanged: true,
		PRMerged:    true,
	})

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after merged completion")
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := task.State, TaskStateDone; got != want {
		t.Fatalf("task.State = %q, want %q", got, want)
	}
}

func seedTaskMonitorAssignmentWithState(t *testing.T, deps *testDeps, issue, paneID string, prNumber int, state string) {
	t.Helper()

	seedTaskMonitorAssignment(t, deps, issue, paneID, prNumber)
	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatalf("task %q not found", issue)
	}
	task.State = state
	deps.state.putTaskForTest(task)
}

func seedMergeEntryForTest(t *testing.T, deps *testDeps, issue string, prNumber int) {
	t.Helper()

	now := deps.clock.Now()
	if _, err := deps.state.EnqueueMerge(context.Background(), MergeQueueEntry{
		Project:   "/tmp/project",
		Issue:     issue,
		PRNumber:  prNumber,
		Status:    MergeQueueStatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("EnqueueMerge() error = %v", err)
	}
}

func setAssignmentWorkerID(t *testing.T, deps *testDeps, issue, workerID string) {
	t.Helper()

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatalf("task %q not found", issue)
	}
	task.WorkerID = workerID
	deps.state.putTaskForTest(task)

	worker, ok := deps.state.worker(task.PaneID)
	if !ok {
		t.Fatalf("worker %q not found", task.PaneID)
	}
	worker.WorkerID = workerID
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
}

type paneAwareCaptureAmux struct {
	*fakeAmux
	errByPane map[string]error
}

func (a *paneAwareCaptureAmux) CapturePane(ctx context.Context, paneID string) (PaneCapture, error) {
	if err := a.errByPane[paneID]; err != nil {
		return PaneCapture{}, err
	}
	return a.fakeAmux.CapturePane(ctx, paneID)
}
