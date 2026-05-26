package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/weill-labs/orca/internal/worksource"
)

func TestFinishAssignmentCompletesWorkSource(t *testing.T) {
	t.Parallel()

	completeErr := errors.New("bd unavailable")
	tests := []struct {
		name             string
		status           string
		eventType        string
		merged           bool
		manualSource     bool
		completeErr      error
		wantCalls        []finishCompleteCall
		wantLogSubstring string
	}{
		{
			name:      "merged task closes source work",
			status:    TaskStatusDone,
			eventType: EventTaskCompleted,
			merged:    true,
			wantCalls: []finishCompleteCall{{
				id:      "LAB-689",
				outcome: worksource.OutcomeMerged,
			}},
		},
		{
			name:      "cancelled task abandons source work",
			status:    TaskStatusCancelled,
			eventType: EventTaskCancelled,
			wantCalls: []finishCompleteCall{{
				id:      "LAB-689",
				outcome: worksource.OutcomeAbandoned,
			}},
		},
		{
			name:      "failed task marks source work failed",
			status:    TaskStatusFailed,
			eventType: EventTaskFailed,
			wantCalls: []finishCompleteCall{{
				id:      "LAB-689",
				outcome: worksource.OutcomeFailed,
			}},
		},
		{
			name:             "complete error logs and does not abort completion",
			status:           TaskStatusDone,
			eventType:        EventTaskCompleted,
			merged:           true,
			completeErr:      completeErr,
			wantLogSubstring: "bd unavailable",
			wantCalls: []finishCompleteCall{{
				id:      "LAB-689",
				outcome: worksource.OutcomeMerged,
			}},
		},
		{
			name:         "default manual source preserves completion behavior",
			status:       TaskStatusDone,
			eventType:    EventTaskCompleted,
			merged:       true,
			manualSource: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			setLifecyclePromptActiveAfterIdleProbes(deps, 0)

			var logs []string
			source := &fakeFinishWorkSource{completeErr: tt.completeErr}
			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.Logf = func(format string, args ...any) {
					logs = append(logs, fmt.Sprintf(format, args...))
				}
				if !tt.manualSource {
					opts.WorkSource = source
				}
			})

			active := newPostmortemAssignment(deps)
			active.Task.Status = TaskStatusActive
			if !tt.manualSource {
				source.completeHook = func(id string, outcome worksource.Outcome) {
					assertFinishCleanupBeforeComplete(t, deps, active, id, tt.status)
				}
			}
			seedFinishAssignmentState(t, deps, active)

			err := d.finishAssignment(context.Background(), active, tt.status, tt.eventType, tt.merged)
			if err != nil {
				t.Fatalf("finishAssignment() error = %v, want nil", err)
			}

			if !tt.manualSource {
				source.requireCompletes(t, tt.wantCalls)
			}
			task, err := deps.state.TaskByIssue(context.Background(), active.Task.Project, active.Task.Issue)
			if err != nil {
				t.Fatalf("TaskByIssue() error = %v", err)
			}
			if got := task.Status; got != tt.status {
				t.Fatalf("task status = %q, want %q", got, tt.status)
			}
			if got := task.State; got != TaskStateDone {
				t.Fatalf("task state = %q, want %q", got, TaskStateDone)
			}
			if got, want := deps.events.countType(tt.eventType), 1; got != want {
				t.Fatalf("%s event count = %d, want %d", tt.eventType, got, want)
			}
			if got := deps.events.countType(EventTaskCompletionFailed); got != 0 {
				t.Fatalf("completion failed event count = %d, want 0", got)
			}
			if tt.wantLogSubstring != "" && !logsContain(logs, tt.wantLogSubstring) {
				t.Fatalf("logs = %#v, want substring %q", logs, tt.wantLogSubstring)
			}
		})
	}
}

func TestCompleteWorkSourceSkipsNoOpCases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		status    string
		nilSource bool
	}{
		{
			name:      "nil source",
			status:    TaskStatusDone,
			nilSource: true,
		},
		{
			name:   "non terminal status",
			status: TaskStatusActive,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			source := &fakeFinishWorkSource{}
			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.WorkSource = source
			})
			if tt.nilSource {
				d.workSource = nil
			}

			d.completeWorkSource(context.Background(), newPostmortemAssignment(deps), tt.status)

			source.requireCompletes(t, nil)
		})
	}
}

func TestWorkSourceOutcomeForTerminalStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		status      string
		wantOutcome worksource.Outcome
		wantName    string
		wantOK      bool
	}{
		{
			name:        "done maps to merged",
			status:      TaskStatusDone,
			wantOutcome: worksource.OutcomeMerged,
			wantName:    "merged",
			wantOK:      true,
		},
		{
			name:        "cancelled maps to abandoned",
			status:      TaskStatusCancelled,
			wantOutcome: worksource.OutcomeAbandoned,
			wantName:    "abandoned",
			wantOK:      true,
		},
		{
			name:        "failed maps to failed",
			status:      TaskStatusFailed,
			wantOutcome: worksource.OutcomeFailed,
			wantName:    "failed",
			wantOK:      true,
		},
		{
			name:        "active is not terminal",
			status:      TaskStatusActive,
			wantOutcome: worksource.OutcomeUnknown,
			wantName:    "unknown",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := workSourceOutcomeForTerminalStatus(tt.status)
			if got != tt.wantOutcome || ok != tt.wantOK {
				t.Fatalf("workSourceOutcomeForTerminalStatus(%q) = (%v, %v), want (%v, %v)",
					tt.status, got, ok, tt.wantOutcome, tt.wantOK)
			}
			if gotName := workSourceOutcomeName(got); gotName != tt.wantName {
				t.Fatalf("workSourceOutcomeName(%v) = %q, want %q", got, gotName, tt.wantName)
			}
		})
	}
}

func assertFinishCleanupBeforeComplete(t *testing.T, deps *testDeps, active ActiveAssignment, id, status string) {
	t.Helper()

	task, err := deps.state.TaskByIssue(context.Background(), active.Task.Project, id)
	if err != nil {
		t.Errorf("TaskByIssue() during Complete error = %v, want nil", err)
		return
	}
	if got := task.Status; got != status {
		t.Errorf("task status during Complete = %q, want %q", got, status)
	}
	if got := task.State; got != TaskStateDone {
		t.Errorf("task state during Complete = %q, want %q", got, TaskStateDone)
	}

	worker, err := deps.state.WorkerByID(context.Background(), active.Task.Project, active.Worker.WorkerID)
	if err != nil {
		t.Errorf("WorkerByID() during Complete error = %v, want nil", err)
		return
	}
	if worker.Issue != "" || worker.PaneID != "" || worker.ClonePath != "" {
		t.Errorf("worker claim during Complete = issue %q pane %q clone %q, want released", worker.Issue, worker.PaneID, worker.ClonePath)
	}
	if got := deps.pool.releasedClones(); len(got) != 1 {
		t.Errorf("released clones during Complete = %#v, want one released clone", got)
	}
}

func logsContain(logs []string, substring string) bool {
	for _, log := range logs {
		if strings.Contains(log, substring) {
			return true
		}
	}
	return false
}

type fakeFinishWorkSource struct {
	mu           sync.Mutex
	completeErr  error
	completeHook func(string, worksource.Outcome)
	completes    []finishCompleteCall
}

type finishCompleteCall struct {
	id      string
	outcome worksource.Outcome
}

func (f *fakeFinishWorkSource) Ready(context.Context, int) ([]worksource.WorkItem, error) {
	return nil, nil
}

func (f *fakeFinishWorkSource) Get(context.Context, string) (worksource.WorkItem, error) {
	return worksource.WorkItem{}, worksource.ErrNotFound
}

func (f *fakeFinishWorkSource) Claim(context.Context, string, string) error {
	return nil
}

func (f *fakeFinishWorkSource) Release(context.Context, string, string) error {
	return nil
}

func (f *fakeFinishWorkSource) Complete(_ context.Context, id string, outcome worksource.Outcome) error {
	f.mu.Lock()
	f.completes = append(f.completes, finishCompleteCall{id: id, outcome: outcome})
	hook := f.completeHook
	err := f.completeErr
	f.mu.Unlock()

	if hook != nil {
		hook(id, outcome)
	}
	return err
}

func (f *fakeFinishWorkSource) requireCompletes(t *testing.T, want []finishCompleteCall) {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.completes) != len(want) {
		t.Fatalf("Complete() calls = %#v, want %#v", f.completes, want)
	}
	for i := range want {
		if got := f.completes[i]; got != want[i] {
			t.Fatalf("Complete()[%d] = %#v, want %#v", i, got, want[i])
		}
	}
}

var _ worksource.Source = (*fakeFinishWorkSource)(nil)
