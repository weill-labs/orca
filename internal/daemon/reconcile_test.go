package daemon

import (
	"context"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestReconcileClassifiesTaskAndPaneDrift(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1487", "pane-1487", "worker-1487", 470)
	seedReconcileAssignment(t, deps, "LAB-1198", "pane-1198", "worker-1198", 0)
	seedReconcileAssignment(t, deps, "LAB-1500", "pane-1500", "worker-1500", 500)
	deps.amux.paneExists = map[string]bool{
		"pane-1487": false,
		"pane-1198": false,
		"pane-1500": true,
	}
	deps.amux.listPanes = []Pane{
		{ID: "906", Name: "w-LAB-1491"},
		{ID: "not-a-worker", Name: "scratch"},
	}
	queuePRSnapshot(deps, 470, `{"state":"MERGED","mergedAt":"2026-04-29T22:35:51Z"}`)
	queuePRSnapshot(deps, 500, `{"state":"MERGED","mergedAt":"2026-04-30T10:00:00Z"}`)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	got := findingKindsByIssue(result.Findings)
	want := map[string]string{
		"LAB-1487": ReconcileRecoverableGhost,
		"LAB-1198": ReconcileAbandoned,
		"LAB-1500": ReconcileStuckCleanup,
		"LAB-1491": ReconcileOrphanPane,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("finding kinds = %#v, want %#v", got, want)
	}
	if got := deps.events.countType(EventReconcileFinding); got != len(want) {
		t.Fatalf("reconcile finding event count = %d, want %d", got, len(want))
	}
}

func TestReconcileFixCompletesMergedGhostWithoutTouchingPanes(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1487", "pane-1487", "worker-1487", 470)
	seedReconcileAssignment(t, deps, "LAB-1198", "pane-1198", "worker-1198", 0)
	deps.amux.paneExists = map[string]bool{
		"pane-1487": false,
		"pane-1198": false,
	}
	deps.amux.listPanes = []Pane{{ID: "906", Name: "w-LAB-1491"}}
	queuePRSnapshot(deps, 470, `{"state":"MERGED","mergedAt":"2026-04-29T22:35:51Z"}`)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{
		Project: "/tmp/project",
		Fix:     true,
	})
	if err != nil {
		t.Fatalf("Reconcile(--fix) error = %v", err)
	}
	if got, want := result.Fixed, 1; got != want {
		t.Fatalf("result.Fixed = %d, want %d", got, want)
	}

	task, ok := deps.state.task("LAB-1487")
	if !ok {
		t.Fatal("LAB-1487 task missing")
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("LAB-1487 status = %q, want %q", got, want)
	}
	abandoned, ok := deps.state.task("LAB-1198")
	if !ok {
		t.Fatal("LAB-1198 task missing")
	}
	if got, want := abandoned.Status, TaskStatusActive; got != want {
		t.Fatalf("LAB-1198 status = %q, want %q", got, want)
	}

	worker, ok := deps.state.worker("worker-1487")
	if !ok {
		t.Fatal("worker-1487 missing")
	}
	if worker.PaneID != "" || worker.Issue != "" || worker.ClonePath != "" {
		t.Fatalf("worker after release = %#v, want released worker claim", worker)
	}

	if got := len(deps.pool.releasedClones()); got != 1 {
		t.Fatalf("released clone count = %d, want 1", got)
	}
	if len(deps.amux.killCalls) != 0 {
		t.Fatalf("kill calls = %#v, want none", deps.amux.killCalls)
	}
	if len(deps.amux.waitIdleCalls) != 0 {
		t.Fatalf("wait idle calls = %#v, want none for missing pane cleanup", deps.amux.waitIdleCalls)
	}
	deps.amux.requireSentKeys(t, "pane-1487", nil)

	deps.events.requireTypes(t, EventReconcileFinding, EventPRMerged, EventWorkerPostmortem, EventTaskCompleted)
	if got := deps.events.lastMessage(EventWorkerPostmortem); !strings.Contains(got, "postmortem skipped") {
		t.Fatalf("worker.postmortem message = %q, want skipped postmortem", got)
	}
}

func queuePRSnapshot(deps *testDeps, prNumber int, output string) {
	deps.commands.queue("gh", []string{"pr", "view", strconv.Itoa(prNumber), "--json", prSnapshotJSONFields}, output, nil)
}

func findingKindsByIssue(findings []ReconcileFinding) map[string]string {
	out := make(map[string]string, len(findings))
	for _, finding := range findings {
		out[finding.Issue] = finding.Kind
	}
	return out
}
