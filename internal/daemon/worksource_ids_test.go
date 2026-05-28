package daemon

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/weill-labs/orca/internal/worksource"
)

func TestWorkSourceIDResolutionRoutesStatusAndCompletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		issue            string
		resolvePair      worksource.IDPair
		resolveErr       error
		wantStatuses     []issueStatusUpdate
		wantCompletes    []finishCompleteCall
		wantSkipped      int
		wantLogSubstring string
	}{
		{
			name:        "beads id uses external Linear ref for status and beads id for close",
			issue:       "orca-y5r.1",
			resolvePair: worksource.IDPair{BeadsID: "orca-y5r.1", LinearID: "LAB-1925"},
			wantStatuses: []issueStatusUpdate{{
				Issue: "LAB-1925",
				State: IssueStateInProgress,
			}},
			wantCompletes: []finishCompleteCall{{
				id:      "orca-y5r.1",
				outcome: worksource.OutcomeMerged,
			}},
		},
		{
			name:        "Linear id uses Linear id for status and resolved beads id for close",
			issue:       "LAB-1925",
			resolvePair: worksource.IDPair{BeadsID: "orca-y5r.1", LinearID: "LAB-1925"},
			wantStatuses: []issueStatusUpdate{{
				Issue: "LAB-1925",
				State: IssueStateInProgress,
			}},
			wantCompletes: []finishCompleteCall{{
				id:      "orca-y5r.1",
				outcome: worksource.OutcomeMerged,
			}},
		},
		{
			name:             "missing non-Linear mapping skips both side effects",
			issue:            "orca-missing",
			resolveErr:       worksource.ErrNotFound,
			wantStatuses:     []issueStatusUpdate{},
			wantSkipped:      1,
			wantLogSubstring: "worksource complete skipped",
		},
		{
			name:         "missing beads-shaped mapping still closes raw beads id",
			issue:        "orca-dxf",
			resolveErr:   worksource.ErrNotFound,
			wantStatuses: []issueStatusUpdate{},
			wantSkipped:  1,
			wantCompletes: []finishCompleteCall{{
				id:      "orca-dxf",
				outcome: worksource.OutcomeMerged,
			}},
			wantLogSubstring: "worksource complete using raw beads id",
		},
		{
			name:         "empty beads id for beads-shaped task closes raw beads id",
			issue:        "orca-dxf",
			wantStatuses: []issueStatusUpdate{},
			wantSkipped:  1,
			wantCompletes: []finishCompleteCall{{
				id:      "orca-dxf",
				outcome: worksource.OutcomeMerged,
			}},
			wantLogSubstring: "resolver returned no beads issue id",
		},
		{
			name:        "missing Linear reverse mapping keeps status and skips close",
			issue:       "LAB-404",
			resolvePair: worksource.IDPair{LinearID: "LAB-404"},
			resolveErr:  worksource.ErrNotFound,
			wantStatuses: []issueStatusUpdate{{
				Issue: "LAB-404",
				State: IssueStateInProgress,
			}},
			wantLogSubstring: "worksource complete skipped",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			var logs []string
			source := &fakeResolvingWorkSource{
				pair: tt.resolvePair,
				err:  tt.resolveErr,
			}
			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.WorkSource = source
				opts.Logf = func(format string, args ...any) {
					logs = append(logs, fmt.Sprintf(format, args...))
				}
			})

			ctx := context.Background()
			if err := d.setIssueStatus(ctx, "/tmp/project", tt.issue, IssueStateInProgress); err != nil {
				t.Fatalf("setIssueStatus() error = %v, want nil", err)
			}
			d.completeWorkSource(ctx, ActiveAssignment{
				Task: Task{Project: "/tmp/project", Issue: tt.issue},
			}, TaskStatusDone)

			if got := deps.issueTracker.statuses(); !reflect.DeepEqual(got, tt.wantStatuses) {
				t.Fatalf("issue tracker statuses = %#v, want %#v", got, tt.wantStatuses)
			}
			source.requireCompletes(t, tt.wantCompletes)
			if got := deps.events.countType(EventIssueStatusSkipped); got != tt.wantSkipped {
				t.Fatalf("status skipped events = %d, want %d", got, tt.wantSkipped)
			}
			if tt.wantLogSubstring != "" && !logsContain(logs, tt.wantLogSubstring) {
				t.Fatalf("logs = %#v, want substring %q", logs, tt.wantLogSubstring)
			}
		})
	}
}

type fakeResolvingWorkSource struct {
	mu        sync.Mutex
	pair      worksource.IDPair
	err       error
	completes []finishCompleteCall
}

func (f *fakeResolvingWorkSource) Ready(context.Context, int) ([]worksource.WorkItem, error) {
	return nil, nil
}

func (f *fakeResolvingWorkSource) Get(context.Context, string) (worksource.WorkItem, error) {
	return worksource.WorkItem{}, worksource.ErrNotFound
}

func (f *fakeResolvingWorkSource) Claim(context.Context, string, string) error {
	return nil
}

func (f *fakeResolvingWorkSource) Release(context.Context, string, string) error {
	return nil
}

func (f *fakeResolvingWorkSource) Complete(_ context.Context, id string, outcome worksource.Outcome) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.completes = append(f.completes, finishCompleteCall{id: id, outcome: outcome})
	return nil
}

func (f *fakeResolvingWorkSource) ResolveID(context.Context, string) (worksource.IDPair, error) {
	return f.pair, f.err
}

func (f *fakeResolvingWorkSource) requireCompletes(t *testing.T, want []finishCompleteCall) {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()
	if !reflect.DeepEqual(f.completes, want) {
		t.Fatalf("Complete() calls = %#v, want %#v", f.completes, want)
	}
}

var _ worksource.Source = (*fakeResolvingWorkSource)(nil)
var _ worksource.IDResolver = (*fakeResolvingWorkSource)(nil)
