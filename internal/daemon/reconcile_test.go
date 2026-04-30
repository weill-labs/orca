package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	state "github.com/weill-labs/orca/internal/daemonstate"
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

func TestReconcileFixFailureContinuesFullScan(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1487", "pane-1487", "worker-1487", 470)
	seedReconcileAssignment(t, deps, "LAB-1198", "pane-1198", "worker-1198", 0)
	deps.amux.paneExists = map[string]bool{
		"pane-1487": false,
		"pane-1198": false,
	}
	deps.amux.listPanes = []Pane{{ID: "906", Name: "w-LAB-1491"}}
	deps.pool.releaseErr = errors.New("release failed")
	queuePRSnapshot(deps, 470, `{"state":"MERGED","mergedAt":"2026-04-29T22:35:51Z"}`)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{
		Project: "/tmp/project",
		Fix:     true,
	})
	if err == nil || !strings.Contains(err.Error(), "release failed") {
		t.Fatalf("Reconcile(--fix) error = %v, want release failure", err)
	}
	got := findingKindsByIssue(result.Findings)
	want := map[string]string{
		"LAB-1487": ReconcileRecoverableGhost,
		"LAB-1198": ReconcileAbandoned,
		"LAB-1491": ReconcileOrphanPane,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("finding kinds = %#v, want %#v", got, want)
	}
	for _, finding := range result.Findings {
		if finding.Issue == "LAB-1487" && finding.Action != reconcileActionFixFailed {
			t.Fatalf("LAB-1487 action = %q, want %q", finding.Action, reconcileActionFixFailed)
		}
	}
	if got := deps.events.countType(EventReconcileFinding); got != len(want) {
		t.Fatalf("reconcile finding event count = %d, want %d", got, len(want))
	}
}

func TestReconcileFixCompletesStuckCleanupWithLivePane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	setLifecyclePromptActiveAfterIdleProbes(deps, 0)
	seedReconcileAssignment(t, deps, "LAB-1500", "pane-1500", "worker-1500", 500)
	deps.amux.paneExists = map[string]bool{"pane-1500": true}
	queuePRSnapshot(deps, 500, `{"state":"MERGED","mergedAt":"2026-04-29T22:35:51Z"}`)

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
	if got, want := findingKindsByIssue(result.Findings), map[string]string{"LAB-1500": ReconcileStuckCleanup}; !reflect.DeepEqual(got, want) {
		t.Fatalf("finding kinds = %#v, want %#v", got, want)
	}
	task, ok := deps.state.task("LAB-1500")
	if !ok {
		t.Fatal("LAB-1500 task missing")
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if len(deps.amux.killCalls) != 0 {
		t.Fatalf("kill calls = %#v, want none", deps.amux.killCalls)
	}
	if got := len(deps.amux.waitIdleCalls); got < 2 {
		t.Fatalf("wait idle call count = %d, want wrapup and postmortem waits", got)
	}
	deps.amux.requireSentKeys(t, "pane-1500", []string{
		mergedWrapUpPrompt,
		"Enter",
		postmortemCommand,
		"Enter",
	})
	deps.events.requireTypes(t, EventReconcileFinding, EventPRMerged, EventWorkerPostmortem, EventTaskCompleted)
}

func TestReconcileDiscoversMergedPRByIssueID(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1322", "pane-1322", "worker-1322", 0)
	deps.amux.paneExists = map[string]bool{"pane-1322": false}
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1322", "--state", "all", "--json", "number,state"}, `[]`, nil)
	deps.commands.queue("gh", issueIDPRSearchArgs("LAB-1322"), `[{"number":456,"state":"MERGED","headRefName":"lab-1322-renamed","title":"LAB-1322: recover renamed branch"}]`, nil)
	queuePRSnapshot(deps, 456, `{"state":"MERGED","mergedAt":"2026-04-29T22:35:51Z"}`)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if got, want := len(result.Findings), 1; got != want {
		t.Fatalf("len(findings) = %d, want %d: %#v", got, want, result.Findings)
	}
	finding := result.Findings[0]
	if got, want := finding.Kind, ReconcileRecoverableGhost; got != want {
		t.Fatalf("finding.Kind = %q, want %q", got, want)
	}
	if got, want := finding.PRNumber, 456; got != want {
		t.Fatalf("finding.PRNumber = %d, want %d", got, want)
	}
	if got, want := finding.Branch, "lab-1322-renamed"; got != want {
		t.Fatalf("finding.Branch = %q, want %q", got, want)
	}
}

func TestReconcileFixReportsClosedPRWithoutCompleting(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1501", "pane-1501", "worker-1501", 501)
	deps.amux.paneExists = map[string]bool{"pane-1501": true}
	queuePRSnapshot(deps, 501, `{"state":"CLOSED","mergedAt":null,"closedAt":"2026-04-29T22:35:51Z"}`)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{
		Project: "/tmp/project",
		Fix:     true,
	})
	if err != nil {
		t.Fatalf("Reconcile(--fix) error = %v", err)
	}
	if got, want := result.Fixed, 0; got != want {
		t.Fatalf("result.Fixed = %d, want %d", got, want)
	}
	if got, want := len(result.Findings), 1; got != want {
		t.Fatalf("len(findings) = %d, want %d", got, want)
	}
	finding := result.Findings[0]
	if got, want := finding.Kind, ReconcileStuckCleanup; got != want {
		t.Fatalf("finding.Kind = %q, want %q", got, want)
	}
	if got, want := finding.PRState, "closed"; got != want {
		t.Fatalf("finding.PRState = %q, want %q", got, want)
	}
	if got, want := finding.Action, "reported"; got != want {
		t.Fatalf("finding.Action = %q, want %q", got, want)
	}
	task, ok := deps.state.task("LAB-1501")
	if !ok {
		t.Fatal("LAB-1501 task missing")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if len(deps.amux.killCalls) != 0 {
		t.Fatalf("kill calls = %#v, want none", deps.amux.killCalls)
	}
	deps.amux.requireSentKeys(t, "pane-1501", nil)
}

func TestReconcileSkipsLiveTaskWithOpenPR(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1502", "pane-1502", "worker-1502", 502)
	deps.amux.paneExists = map[string]bool{"pane-1502": true}
	queuePRSnapshot(deps, 502, `{"state":"OPEN","mergedAt":null,"closedAt":null}`)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(result.Findings) != 0 {
		t.Fatalf("findings = %#v, want none", result.Findings)
	}
	if got := deps.events.countType(EventReconcileFinding); got != 0 {
		t.Fatalf("reconcile finding event count = %d, want 0", got)
	}
}

func TestReconcileSkipsLiveTaskWithDiscoveredOpenPR(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1503", "pane-1503", "worker-1503", 0)
	deps.amux.paneExists = map[string]bool{"pane-1503": true}
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1503", "--state", "all", "--json", "number,state"}, `[{"number":503,"state":"OPEN"}]`, nil)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(result.Findings) != 0 {
		t.Fatalf("findings = %#v, want none", result.Findings)
	}
}

func TestReconcileSkipsNonActiveTaskWithoutPane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "starting", status: TaskStatusStarting},
		{name: "queued", status: "queued"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.state.putTaskForTest(Task{
				Project:      "/tmp/project",
				Issue:        "LAB-1504",
				Status:       tt.status,
				Branch:       "LAB-1504",
				AgentProfile: "codex",
				CreatedAt:    deps.clock.Now(),
				UpdatedAt:    deps.clock.Now(),
			})

			result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
			if err != nil {
				t.Fatalf("Reconcile() error = %v", err)
			}
			if len(result.Findings) != 0 {
				t.Fatalf("findings = %#v, want none", result.Findings)
			}
			if got := len(deps.commands.callsByName("gh")); got != 0 {
				t.Fatalf("gh call count = %d, want 0", got)
			}
		})
	}
}

func TestLocalControllerReconcileUsesRuntimeAdapters(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("LINEAR_API_KEY", "")

	projectPath := testProjectPath(t)
	stateDB := filepath.Join(t.TempDir(), "state.db")
	store, err := state.OpenSQLite(stateDB)
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	controller, err := NewLocalController(ControllerOptions{
		Store: store,
		Paths: Paths{
			StateDB: stateDB,
			PIDDir:  t.TempDir(),
		},
		Amux: &fakeAmux{listPanes: []Pane{{ID: "906", Name: "w-LAB-1491"}}},
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	result, err := controller.Reconcile(context.Background(), ReconcileRequest{Project: projectPath})
	if err != nil {
		t.Fatalf("controller.Reconcile() error = %v", err)
	}
	if got, want := result.Project, projectPath; got != want {
		t.Fatalf("result.Project = %q, want %q", got, want)
	}
	if got := findingKindsByIssue(result.Findings); !reflect.DeepEqual(got, map[string]string{"LAB-1491": ReconcileOrphanPane}) {
		t.Fatalf("finding kinds = %#v, want orphan pane", got)
	}
}

func TestLocalControllerReconcileSessionPrecedence(t *testing.T) {
	projectPath := filepath.Join(t.TempDir(), "repo")
	t.Setenv("AMUX_SESSION", "from-env")

	controller := &LocalController{
		store: &fakeStore{projectStatus: state.ProjectStatus{
			Daemon: &state.DaemonStatus{Session: "from-db"},
		}},
	}
	if got, want := controller.reconcileSession(context.Background(), projectPath), "from-db"; got != want {
		t.Fatalf("reconcileSession() = %q, want %q", got, want)
	}

	controller.store = &fakeStore{}
	if got, want := controller.reconcileSession(context.Background(), projectPath), "from-env"; got != want {
		t.Fatalf("reconcileSession() with env = %q, want %q", got, want)
	}

	t.Setenv("AMUX_SESSION", "")
	if got, want := controller.reconcileSession(context.Background(), projectPath), "repo"; got != want {
		t.Fatalf("reconcileSession() fallback = %q, want %q", got, want)
	}
	if got, want := controller.reconcileSession(context.Background(), ""), "orca"; got != want {
		t.Fatalf("reconcileSession() empty project = %q, want %q", got, want)
	}
}

func TestLocalControllerReconcileRejectsReaderOnlyStore(t *testing.T) {
	t.Parallel()

	controller, err := NewLocalController(ControllerOptions{
		Store: &fakeStore{},
		Paths: Paths{
			StateDB: filepath.Join(t.TempDir(), "state.db"),
			PIDDir:  t.TempDir(),
		},
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	_, err = controller.Reconcile(context.Background(), ReconcileRequest{Project: testProjectPath(t)})
	if err == nil || !strings.Contains(err.Error(), "task-capable state store") {
		t.Fatalf("controller.Reconcile() error = %v, want task-capable store error", err)
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
