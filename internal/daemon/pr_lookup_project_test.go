package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

func TestPRPollRepoDriftSkipsDeletedKnownPoolCloneForUnrelatedTask(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	const (
		issue  = "LAB-1873"
		branch = "lab-1873-fix-pr-discovery"
	)
	seedTaskMonitorAssignment(t, deps, issue, "pane-1873", 0)
	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after seed")
	}
	task.Branch = branch
	task.ClonePath = deps.pool.clone.Path
	deps.state.putTaskForTest(task)

	missingClonePath := filepath.Join("/tmp/project", orcaPoolSubdir, "deleted-discovery-clone")
	seedKnownProjectForPRLookup(t, deps, missingClonePath)

	branchLookupArgs := []string{"pr", "list", "--head", branch, "--json", "number"}
	deps.commands.queue("gh", branchLookupArgs, `[]`, nil)
	deps.commands.queue("gh", issueIDPRSearchArgs(issue), `[]`, nil)
	deps.commands.queue("gh", branchLookupArgs, ``, errors.New("chdir "+missingClonePath+": no such file or directory"))

	d := deps.newDaemon(t)
	update := d.checkTaskPRPoll(context.Background(), activeTaskMonitorAssignment(t, deps, issue))
	d.applyTaskStateUpdate(context.Background(), update)

	for _, call := range deps.commands.callsByName("gh") {
		if call.Dir == missingClonePath {
			t.Fatalf("unexpected GitHub lookup from deleted clone: %#v", call)
		}
	}
	if event, ok := deps.events.lastEventOfType(EventPRPollTrace); !ok {
		t.Fatal("PR poll trace missing")
	} else if strings.Contains(event.Message, "find_pr_repo_drift_error") {
		t.Fatalf("last PR poll trace = %q, want deleted discovery clone skipped", event.Message)
	}
}

func TestPRPollEmitsDeletedLookupCloneOncePerPollCycle(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	missingClonePath := filepath.Join("/tmp/project", orcaPoolSubdir, "deleted-discovery-clone")
	seedKnownProjectForPRLookup(t, deps, missingClonePath)

	tests := []struct {
		issue  string
		branch string
		paneID string
	}{
		{issue: "LAB-1874", branch: "lab-1874-first", paneID: "pane-1874"},
		{issue: "LAB-1875", branch: "lab-1875-second", paneID: "pane-1875"},
	}
	for _, tt := range tests {
		seedTaskMonitorAssignment(t, deps, tt.issue, tt.paneID, 0)
		task, ok := deps.state.task(tt.issue)
		if !ok {
			t.Fatalf("task %s missing after seed", tt.issue)
		}
		task.Branch = tt.branch
		task.ClonePath = deps.pool.clone.Path
		deps.state.putTaskForTest(task)

		deps.commands.queue("gh", []string{"pr", "list", "--head", tt.branch, "--json", "number"}, `[]`, nil)
		deps.commands.queue("gh", issueIDPRSearchArgs(tt.issue), `[]`, nil)
	}

	d := deps.newDaemon(t)
	d.runPollTick(context.Background())

	for _, call := range deps.commands.callsByName("gh") {
		if call.Dir == missingClonePath {
			t.Fatalf("unexpected GitHub lookup from deleted clone: %#v", call)
		}
	}
	if got, want := deps.events.countType(EventPRPollCloneMissing), 1; got != want {
		t.Fatalf("deleted lookup clone event count = %d, want %d", got, want)
	}
	event, ok := deps.events.lastEventOfType(EventPRPollCloneMissing)
	if !ok {
		t.Fatal("deleted lookup clone event missing")
	}
	if event.Issue != "" {
		t.Fatalf("deleted lookup clone event issue = %q, want empty project-level event", event.Issue)
	}
	if event.ClonePath != missingClonePath || !strings.Contains(event.Message, missingClonePath) {
		t.Fatalf("deleted lookup clone event = %#v, want clone path %q", event, missingClonePath)
	}
}

func TestReconcileReportsDeletedPRLookupClone(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	missingClonePath := filepath.Join("/tmp/project", orcaPoolSubdir, "deleted-discovery-clone")
	seedKnownProjectForPRLookup(t, deps, missingClonePath)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(result.Findings) != 1 {
		t.Fatalf("findings = %#v, want one deleted PR lookup clone finding", result.Findings)
	}
	finding := result.Findings[0]
	if got, want := finding.Kind, ReconcileCloneMissing; got != want {
		t.Fatalf("finding.Kind = %q, want %q", got, want)
	}
	if got, want := finding.ClonePath, missingClonePath; got != want {
		t.Fatalf("finding.ClonePath = %q, want %q", got, want)
	}
	if finding.Issue != "" {
		t.Fatalf("finding.Issue = %q, want empty project-level finding", finding.Issue)
	}
	if !strings.Contains(finding.Message, "PR lookup") || !strings.Contains(finding.Message, missingClonePath) {
		t.Fatalf("finding.Message = %q, want PR lookup missing clone message", finding.Message)
	}
}
