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
	for _, finding := range result.Findings {
		if finding.Kind == ReconcileOrphanPane && !strings.Contains(finding.Message, "--adopt-orphans") {
			t.Fatalf("orphan message = %q, want adoption action", finding.Message)
		}
	}
}

func TestReconcileAdoptOrphansRecoversTask(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-02")
	deps.amux.listPanes = []Pane{{ID: "906", Name: "w-LAB-1816", CWD: clonePath}}
	deps.amux.metadata = map[string]map[string]string{
		"906": {
			"agent_profile": "codex",
			"branch":        "LAB-1816",
			"task":          "Implement LAB-1816",
		},
	}
	deps.amux.captureHistorySequence("906", []PaneCapture{{
		Content:        []string{"OpenAI Codex", "Working (12s - esc to interrupt)"},
		CWD:            clonePath,
		CurrentCommand: "codex",
	}})

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{
		Project:      "/tmp/project",
		AdoptOrphans: true,
	})
	if err != nil {
		t.Fatalf("Reconcile(--adopt-orphans) error = %v", err)
	}
	if got, want := result.Fixed, 1; got != want {
		t.Fatalf("result.Fixed = %d, want %d", got, want)
	}
	if got, want := len(result.Findings), 1; got != want {
		t.Fatalf("len(findings) = %d, want %d: %#v", got, want, result.Findings)
	}
	finding := result.Findings[0]
	if finding.Kind != ReconcileOrphanPane || finding.Action != reconcileActionFixed {
		t.Fatalf("finding = %#v, want fixed orphan pane", finding)
	}

	active, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1816")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if active.Task.Status != TaskStatusActive || active.Task.State != TaskStateAssigned {
		t.Fatalf("adopted task state = %s/%s, want active/assigned", active.Task.Status, active.Task.State)
	}
	if active.Task.PaneID != "906" || active.Task.PaneName != "w-LAB-1816" {
		t.Fatalf("adopted task pane = %q/%q, want 906/w-LAB-1816", active.Task.PaneID, active.Task.PaneName)
	}
	if active.Task.ClonePath != clonePath || active.Worker.ClonePath != clonePath {
		t.Fatalf("adopted clone paths = %q/%q, want %q", active.Task.ClonePath, active.Worker.ClonePath, clonePath)
	}
	if active.Task.AgentProfile != "codex" || active.Worker.AgentProfile != "codex" {
		t.Fatalf("adopted profiles = %q/%q, want codex", active.Task.AgentProfile, active.Worker.AgentProfile)
	}
	if !deps.pool.cloneAcquired(clonePath) {
		t.Fatalf("clone %q was not marked acquired", clonePath)
	}
	if len(deps.amux.killCalls) != 0 {
		t.Fatalf("kill calls = %#v, want none", deps.amux.killCalls)
	}
	deps.amux.requireSentKeys(t, "906", nil)
	deps.events.requireTypes(t, EventWorkerRecovered, EventReconcileFinding)

	_, err = deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:      ReconcileOrphanPane,
		Issue:     "LAB-1816",
		PaneID:    "906",
		PaneName:  "w-LAB-1816",
		ClonePath: clonePath,
	})
	if err == nil || !strings.Contains(err.Error(), "task LAB-1816 is already active") {
		t.Fatalf("second adoptOrphanPane() error = %v, want already active", err)
	}
}

func TestAdoptOrphanPaneUsesCaptureFallbacks(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-03")
	deps.amux.listPanes = []Pane{{ID: "907", Name: "w-LAB-1817"}}
	deps.amux.captureHistorySequence("907", []PaneCapture{{}})
	deps.amux.capturePaneSequence("907", []PaneCapture{{
		Content:        []string{"Claude Code", "working"},
		CWD:            clonePath,
		CurrentCommand: "claude",
	}})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-adopted",
		PaneID:       "907",
		PaneName:     "w-LAB-1817",
		AgentProfile: "claude",
		Health:       WorkerHealthHealthy,
		CreatedAt:    deps.clock.Now(),
		LastSeenAt:   deps.clock.Now(),
		UpdatedAt:    deps.clock.Now(),
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	message, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1817",
		PaneID:   "907",
		PaneName: "w-LAB-1817",
	})
	if err != nil {
		t.Fatalf("adoptOrphanPane() error = %v", err)
	}
	if !strings.Contains(message, "adopted") {
		t.Fatalf("message = %q, want adoption message", message)
	}

	active, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1817")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if got, want := active.Task.WorkerID, "worker-adopted"; got != want {
		t.Fatalf("worker id = %q, want %q", got, want)
	}
	if got, want := active.Task.AgentProfile, "claude"; got != want {
		t.Fatalf("agent profile = %q, want %q", got, want)
	}
	if got, want := active.Task.ClonePath, clonePath; got != want {
		t.Fatalf("clone path = %q, want %q", got, want)
	}
	if got, want := deps.amux.captureHistoryCount("907"), 1; got != want {
		t.Fatalf("history capture count = %d, want %d", got, want)
	}
	if got, want := deps.amux.captureCount("907"), 1; got != want {
		t.Fatalf("pane capture count = %d, want %d", got, want)
	}
}

func TestAdoptOrphanPaneClampsSubdirectoryCWDToCloneRoot(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-14")
	paneCWD := filepath.Join(clonePath, "src", "internal")
	deps.amux.listPanes = []Pane{{ID: "922", Name: "w-LAB-1836", CWD: paneCWD}}
	deps.amux.metadata = map[string]map[string]string{
		"922": {"agent_profile": "codex"},
	}
	deps.amux.captureHistorySequence("922", []PaneCapture{{
		Content:        []string{"OpenAI Codex"},
		CWD:            paneCWD,
		CurrentCommand: "codex",
	}})

	message, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1836",
		PaneID:   "922",
		PaneName: "w-LAB-1836",
	})
	if err != nil {
		t.Fatalf("adoptOrphanPane() error = %v", err)
	}
	if !strings.Contains(message, "adopted") {
		t.Fatalf("message = %q, want adoption message", message)
	}

	active, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1836")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if active.Task.ClonePath != clonePath || active.Worker.ClonePath != clonePath {
		t.Fatalf("adopted clone paths = %q/%q, want %q", active.Task.ClonePath, active.Worker.ClonePath, clonePath)
	}
	if !deps.pool.cloneAcquired(clonePath) {
		t.Fatalf("clone %q was not marked acquired", clonePath)
	}
	if deps.pool.cloneAcquired(paneCWD) {
		t.Fatalf("subdirectory %q was marked acquired", paneCWD)
	}
}

func TestAdoptOrphanPaneUsesPaneNameWhenPaneIDMissing(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-09")
	deps.amux.listPanes = []Pane{{Name: "w-LAB-1830", CWD: clonePath}}
	deps.amux.metadata = map[string]map[string]string{
		"w-LAB-1830": {"agent_profile": "codex"},
	}
	deps.amux.captureHistorySequence("w-LAB-1830", []PaneCapture{{CWD: clonePath, CurrentCommand: "codex"}})

	message, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1830",
		PaneName: "w-LAB-1830",
	})
	if err != nil {
		t.Fatalf("adoptOrphanPane() error = %v", err)
	}
	if !strings.Contains(message, "adopted") {
		t.Fatalf("message = %q, want adoption message", message)
	}
	active, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1830")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if got, want := active.Task.PaneName, "w-LAB-1830"; got != want {
		t.Fatalf("pane name = %q, want %q", got, want)
	}
	if got := active.Task.PaneID; got != "" {
		t.Fatalf("pane id = %q, want empty", got)
	}
}

func TestLiveOrphanPaneLooksUpCWDFromPaneList(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-04")
	deps.amux.listPanes = []Pane{{ID: "912", Name: "w-LAB-1822", CWD: clonePath}}

	pane, err := deps.newDaemon(t).liveOrphanPane(context.Background(), ReconcileFinding{
		Issue:    "LAB-1822",
		PaneID:   "912",
		PaneName: "w-LAB-1822",
	})
	if err != nil {
		t.Fatalf("liveOrphanPane() error = %v", err)
	}
	if got, want := pane.CWD, clonePath; got != want {
		t.Fatalf("pane.CWD = %q, want %q", got, want)
	}
	if got, want := deps.amux.paneExistsCalls, []string{"912"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pane exists calls = %#v, want %#v", got, want)
	}
}

func TestLiveOrphanPaneReportsListErrors(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.listPanesErr = errors.New("list failed")

	_, err := deps.newDaemon(t).liveOrphanPane(context.Background(), ReconcileFinding{
		Issue:    "LAB-1828",
		PaneName: "w-LAB-1828",
	})
	if err == nil || !strings.Contains(err.Error(), "list panes for orphan adoption") {
		t.Fatalf("liveOrphanPane() error = %v, want list failure", err)
	}
}

func TestLiveOrphanPaneReportsMissingListedPane(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.listPanes = []Pane{{ID: "other", Name: "w-LAB-0000"}}

	_, err := deps.newDaemon(t).liveOrphanPane(context.Background(), ReconcileFinding{
		Issue:    "LAB-1829",
		PaneName: "w-LAB-1829",
	})
	if err == nil || !strings.Contains(err.Error(), "is no longer listed") {
		t.Fatalf("liveOrphanPane() error = %v, want missing pane", err)
	}
}

func TestAdoptOrphanPaneReportsPaneLivenessFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.paneExistsErr = errors.New("amux unavailable")

	_, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1831",
		PaneID:   "918",
		PaneName: "w-LAB-1831",
	})
	if err == nil || !strings.Contains(err.Error(), "orphan pane 918 is no longer live") || !strings.Contains(err.Error(), "amux unavailable") {
		t.Fatalf("adoptOrphanPane() error = %v, want pane liveness failure", err)
	}
}

func TestAdoptOrphanPaneReportsTaskLookupFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.state.taskByIssueErr = errors.New("state unavailable")

	_, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1832",
		PaneName: "w-LAB-1832",
	})
	if err == nil || !strings.Contains(err.Error(), "lookup task LAB-1832 before adoption") || !strings.Contains(err.Error(), "state unavailable") {
		t.Fatalf("adoptOrphanPane() error = %v, want task lookup failure", err)
	}
}

func TestReconcileAdoptOrphansReportsAdoptionFailure(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-05")
	deps.amux.listPanes = []Pane{{ID: "913", Name: "w-LAB-1823", CWD: clonePath}}
	deps.amux.metadataErr = errors.New("metadata unavailable")

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{
		Project:      "/tmp/project",
		AdoptOrphans: true,
	})
	if err == nil || !strings.Contains(err.Error(), "metadata unavailable") {
		t.Fatalf("Reconcile(--adopt-orphans) error = %v, want metadata failure", err)
	}
	if got, want := result.Fixed, 0; got != want {
		t.Fatalf("result.Fixed = %d, want %d", got, want)
	}
	if got, want := len(result.Findings), 1; got != want {
		t.Fatalf("len(findings) = %d, want %d: %#v", got, want, result.Findings)
	}
	finding := result.Findings[0]
	if finding.Action != reconcileActionFixFailed {
		t.Fatalf("finding.Action = %q, want %q", finding.Action, reconcileActionFixFailed)
	}
	if !strings.Contains(finding.Message, "load orphan pane metadata") {
		t.Fatalf("finding.Message = %q, want metadata failure context", finding.Message)
	}
	deps.events.requireTypes(t, EventReconcileFinding)
}

func TestAdoptOrphanPaneWorkerWriteFailureLeavesRetryableOrphan(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-12")
	deps.amux.listPanes = []Pane{{ID: "920", Name: "w-LAB-1834", CWD: clonePath}}
	deps.amux.metadata = map[string]map[string]string{
		"920": {"agent_profile": "codex"},
	}
	deps.amux.captureHistorySequence("920", []PaneCapture{
		{Content: []string{"OpenAI Codex"}, CWD: clonePath, CurrentCommand: "codex"},
		{Content: []string{"OpenAI Codex"}, CWD: clonePath, CurrentCommand: "codex"},
	})
	deps.state.putWorkerErrs = []error{errors.New("worker store unavailable")}

	_, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:      ReconcileOrphanPane,
		Issue:     "LAB-1834",
		PaneID:    "920",
		PaneName:  "w-LAB-1834",
		ClonePath: clonePath,
	})
	if err == nil || !strings.Contains(err.Error(), "store adopted worker") || !strings.Contains(err.Error(), "worker store unavailable") {
		t.Fatalf("adoptOrphanPane() error = %v, want worker storage failure", err)
	}
	if _, err := deps.state.TaskByIssue(context.Background(), "/tmp/project", "LAB-1834"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("TaskByIssue() error = %v, want no task after worker write failure", err)
	}

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{
		Project:      "/tmp/project",
		AdoptOrphans: true,
	})
	if err != nil {
		t.Fatalf("retry Reconcile(--adopt-orphans) error = %v", err)
	}
	if got, want := result.Fixed, 1; got != want {
		t.Fatalf("retry result.Fixed = %d, want %d", got, want)
	}
	if got, want := len(result.Findings), 1; got != want {
		t.Fatalf("len(retry findings) = %d, want %d: %#v", got, want, result.Findings)
	}
	if finding := result.Findings[0]; finding.Action != reconcileActionFixed || !strings.Contains(finding.Message, "adopted") {
		t.Fatalf("retry finding = %#v, want fixed adoption", finding)
	}
	if _, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1834"); err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
}

func TestAdoptOrphanPaneTaskWriteFailureLeavesRetryableOrphan(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-13")
	deps.amux.listPanes = []Pane{{ID: "921", Name: "w-LAB-1835", CWD: clonePath}}
	deps.amux.metadata = map[string]map[string]string{
		"921": {"agent_profile": "codex"},
	}
	deps.amux.captureHistorySequence("921", []PaneCapture{
		{Content: []string{"OpenAI Codex"}, CWD: clonePath, CurrentCommand: "codex"},
		{Content: []string{"OpenAI Codex"}, CWD: clonePath, CurrentCommand: "codex"},
	})
	deps.state.putTaskErrs = []error{errors.New("task store unavailable")}

	_, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:      ReconcileOrphanPane,
		Issue:     "LAB-1835",
		PaneID:    "921",
		PaneName:  "w-LAB-1835",
		ClonePath: clonePath,
	})
	if err == nil || !strings.Contains(err.Error(), "store adopted task") || !strings.Contains(err.Error(), "task store unavailable") {
		t.Fatalf("adoptOrphanPane() error = %v, want task storage failure", err)
	}
	if _, err := deps.state.TaskByIssue(context.Background(), "/tmp/project", "LAB-1835"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("TaskByIssue() error = %v, want no task after task write failure", err)
	}
	if _, err := deps.state.WorkerByPane(context.Background(), "/tmp/project", "921"); err != nil {
		t.Fatalf("WorkerByPane() error = %v, want worker persisted for retry", err)
	}

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{
		Project:      "/tmp/project",
		AdoptOrphans: true,
	})
	if err != nil {
		t.Fatalf("retry Reconcile(--adopt-orphans) error = %v", err)
	}
	if got, want := result.Fixed, 1; got != want {
		t.Fatalf("retry result.Fixed = %d, want %d", got, want)
	}
	if got, want := len(result.Findings), 1; got != want {
		t.Fatalf("len(retry findings) = %d, want %d: %#v", got, want, result.Findings)
	}
	if finding := result.Findings[0]; finding.Action != reconcileActionFixed || !strings.Contains(finding.Message, "adopted") {
		t.Fatalf("retry finding = %#v, want fixed adoption", finding)
	}
	if _, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1835"); err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
}

func TestAdoptOrphanPaneReportsCaptureFailureWhenCWDUnknown(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.listPanes = []Pane{{ID: "919", Name: "w-LAB-1833"}}
	deps.amux.metadata = map[string]map[string]string{
		"919": {"agent_profile": "codex"},
	}
	deps.amux.captureHistoryErrors("919", []error{errors.New("history unavailable")})
	deps.amux.capturePaneErr = errors.New("capture unavailable")

	_, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1833",
		PaneID:   "919",
		PaneName: "w-LAB-1833",
	})
	if err == nil || !strings.Contains(err.Error(), "capture orphan pane cwd") || !strings.Contains(err.Error(), "history unavailable") || !strings.Contains(err.Error(), "capture unavailable") {
		t.Fatalf("adoptOrphanPane() error = %v, want capture cwd failure", err)
	}
}

func TestAdoptOrphanPaneReportsCaptureWarningWhenCWDKnown(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-06")
	deps.amux.metadata = map[string]map[string]string{
		"914": {"agent_profile": "claude"},
	}
	deps.amux.captureHistoryErrors("914", []error{errors.New("history unavailable")})
	deps.amux.capturePaneErr = errors.New("capture unavailable")

	message, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:      ReconcileOrphanPane,
		Issue:     "LAB-1824",
		PaneID:    "914",
		PaneName:  "w-LAB-1824",
		ClonePath: clonePath,
	})
	if err != nil {
		t.Fatalf("adoptOrphanPane() error = %v", err)
	}
	for _, want := range []string{"pane capture failed", "history unavailable", "capture unavailable"} {
		if !strings.Contains(message, want) {
			t.Fatalf("message = %q, want %q", message, want)
		}
	}

	active, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1824")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	if got, want := active.Task.AgentProfile, "claude"; got != want {
		t.Fatalf("agent profile = %q, want %q", got, want)
	}
}

func TestAdoptOrphanPaneReportsMetadataRefreshWarning(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-07")
	deps.amux.listPanes = []Pane{{ID: "915", Name: "w-LAB-1825", CWD: clonePath}}
	deps.amux.metadata = map[string]map[string]string{
		"915": {"agent_profile": "codex"},
	}
	deps.amux.captureHistorySequence("915", []PaneCapture{{CWD: clonePath, CurrentCommand: "codex"}})
	deps.amux.setMetadataErr = errors.New("metadata write failed")

	message, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1825",
		PaneID:   "915",
		PaneName: "w-LAB-1825",
	})
	if err != nil {
		t.Fatalf("adoptOrphanPane() error = %v", err)
	}
	if !strings.Contains(message, "metadata refresh failed") || !strings.Contains(message, "metadata write failed") {
		t.Fatalf("message = %q, want metadata refresh warning", message)
	}
	if _, err := deps.state.ActiveAssignmentByIssue(context.Background(), "/tmp/project", "LAB-1825"); err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
}

func TestAdoptOrphanPaneRejectsPoolWithoutAdopter(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	d.pool = nonAdoptingPool{}

	err := d.adoptOrphanClone(context.Background(), "/tmp/project", Clone{
		Path:         filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-08"),
		AssignedTask: "LAB-1826",
	})
	if err == nil || !strings.Contains(err.Error(), "requires pool implementation with Adopt") {
		t.Fatalf("adoptOrphanClone() error = %v, want missing adopter error", err)
	}
}

func TestAdoptOrphanPaneRejectsMissingClonePath(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.listPanes = []Pane{{ID: "908", Name: "w-LAB-1818"}}
	deps.amux.disableAutomaticReadyCapture = true

	_, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1818",
		PaneID:   "908",
		PaneName: "w-LAB-1818",
	})
	if err == nil || !strings.Contains(err.Error(), "validate orphan pane clone path") {
		t.Fatalf("adoptOrphanPane() error = %v, want clone path validation failure", err)
	}
}

func TestAdoptOrphanPaneRejectsClonePathOutsidePool(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.listPanes = []Pane{{ID: "916", Name: "w-LAB-1827", CWD: "/tmp/not-orca/clone"}}

	_, err := deps.newDaemon(t).adoptOrphanPane(context.Background(), "/tmp/project", ReconcileFinding{
		Kind:     ReconcileOrphanPane,
		Issue:    "LAB-1827",
		PaneID:   "916",
		PaneName: "w-LAB-1827",
	})
	if err == nil || !strings.Contains(err.Error(), "must stay inside pool root") {
		t.Fatalf("adoptOrphanPane() error = %v, want outside pool failure", err)
	}
}

func TestOrphanPaneAgentProfileFallbacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		metadata map[string]string
		snapshot PaneCapture
		want     string
	}{
		{
			name:     "metadata",
			metadata: map[string]string{"agent_profile": "codex"},
			want:     "codex",
		},
		{
			name:     "codex scrollback",
			snapshot: PaneCapture{Content: []string{"OpenAI Codex", "Working"}, CurrentCommand: "node"},
			want:     "codex",
		},
		{
			name:     "claude command",
			snapshot: PaneCapture{CurrentCommand: "/usr/local/bin/claude"},
			want:     "claude",
		},
		{
			name:     "claude output",
			snapshot: PaneCapture{Content: []string{"Claude Code"}},
			want:     "claude",
		},
		{
			name: "default",
			want: "codex",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := orphanPaneAgentProfile(tt.metadata, tt.snapshot); got != tt.want {
				t.Fatalf("orphanPaneAgentProfile() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOrphanPaneWorkerIDFallbacks(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	if got, want := d.orphanPaneWorkerID(context.Background(), "/tmp/project", Pane{ID: "909", Name: "w-LAB-1819"}, map[string]string{"worker_id": "worker-meta"}), "worker-meta"; got != want {
		t.Fatalf("metadata worker id = %q, want %q", got, want)
	}
	if got, want := d.orphanPaneWorkerID(context.Background(), "/tmp/project", Pane{Name: "w-LAB-1820"}, nil), "w-LAB-1820"; got != want {
		t.Fatalf("pane name worker id = %q, want %q", got, want)
	}
	if got, want := d.orphanPaneWorkerID(context.Background(), "/tmp/project", Pane{ID: "910"}, nil), "pane-910"; got != want {
		t.Fatalf("pane id worker id = %q, want %q", got, want)
	}
	if got, want := d.orphanPaneWorkerID(context.Background(), "/tmp/project", Pane{}, nil), "adopted-worker"; got != want {
		t.Fatalf("empty worker id = %q, want %q", got, want)
	}
}

func TestCaptureOrphanPaneReturnsCaptureErrors(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.captureHistoryErrors("911", []error{errors.New("history failed")})
	deps.amux.capturePaneErr = errors.New("pane failed")

	_, err := deps.newDaemon(t).captureOrphanPane(context.Background(), "911")
	if err == nil || !strings.Contains(err.Error(), "history failed") || !strings.Contains(err.Error(), "pane failed") {
		t.Fatalf("captureOrphanPane() error = %v, want both capture errors", err)
	}
}

func TestAppendAdoptionWarningIgnoresNilError(t *testing.T) {
	t.Parallel()

	if got, want := appendAdoptionWarning("adopted", "warning", nil), "adopted"; got != want {
		t.Fatalf("appendAdoptionWarning() = %q, want %q", got, want)
	}
}

type nonAdoptingPool struct{}

func (nonAdoptingPool) Acquire(context.Context, string, string) (Clone, error) {
	return Clone{}, errors.New("unused")
}

func (nonAdoptingPool) Release(context.Context, string, Clone) error {
	return errors.New("unused")
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
	seedReconcileAssignment(t, deps, "LAB-1500", "pane-1500", "worker-1500", 500)
	deps.amux.paneExists = map[string]bool{"pane-1500": true}
	deps.amux.listPanes = []Pane{{ID: "pane-1500", Name: "w-LAB-1500"}}
	deps.amux.waitIdleErr = errors.New("reconcile must not wait on a live pane")
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
	if got, want := len(result.Findings), 1; got != want {
		t.Fatalf("len(findings) = %d, want %d: %#v", got, want, result.Findings)
	}
	if got, want := findingKindsByIssue(result.Findings), map[string]string{"LAB-1500": ReconcileStuckCleanup}; !reflect.DeepEqual(got, want) {
		t.Fatalf("finding kinds = %#v, want %#v", got, want)
	}
	finding := result.Findings[0]
	if got, want := finding.Action, reconcileActionFixed; got != want {
		t.Fatalf("finding.Action = %q, want %q", got, want)
	}
	for _, want := range []string{"task marked done", "clone released", "live pane", "human"} {
		if !strings.Contains(finding.Message, want) {
			t.Fatalf("finding.Message = %q, want %q", finding.Message, want)
		}
	}
	task, ok := deps.state.task("LAB-1500")
	if !ok {
		t.Fatal("LAB-1500 task missing")
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	worker, ok := deps.state.worker("worker-1500")
	if !ok {
		t.Fatal("worker-1500 missing")
	}
	if worker.PaneID != "" || worker.Issue != "" || worker.ClonePath != "" {
		t.Fatalf("worker after release = %#v, want released worker claim", worker)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          "clone-LAB-1500",
		Path:          "/tmp/LAB-1500",
		CurrentBranch: "LAB-1500",
		AssignedTask:  "LAB-1500",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if len(deps.amux.killCalls) != 0 {
		t.Fatalf("kill calls = %#v, want none", deps.amux.killCalls)
	}
	if got := len(deps.amux.waitIdleCalls); got != 0 {
		t.Fatalf("wait idle calls = %#v, want none for reconcile live-pane cleanup", deps.amux.waitIdleCalls)
	}
	deps.amux.requireSentKeys(t, "pane-1500", nil)
	deps.amux.requireMetadata(t, "pane-1500", map[string]string{
		"status":         "done",
		"task":           "\x1b[2m\x1b[9mLAB-1500\x1b[29m\x1b[22m",
		"tracked_issues": `[{"id":"LAB-1500","status":"completed"}]`,
		"tracked_prs":    `[{"number":500,"status":"completed"}]`,
	})
	deps.events.requireTypes(t, EventReconcileFinding, EventPRMerged, EventWorkerPostmortem, EventTaskCompleted)
	if got := deps.events.lastMessage(EventWorkerPostmortem); !strings.Contains(got, "postmortem skipped") || !strings.Contains(got, "live pane") {
		t.Fatalf("worker.postmortem message = %q, want skipped live-pane postmortem", got)
	}
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

func TestReconcileReportsMissingClonePath(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	const issue = "LAB-1809"
	seedReconcileAssignment(t, deps, issue, "pane-1809", "worker-1809", 0)
	missingClonePath := filepath.Join("/tmp/project", orcaPoolSubdir, "deleted-clone")
	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatal("task missing after seed")
	}
	task.ClonePath = missingClonePath
	deps.state.putTaskForTest(task)
	worker, ok := deps.state.worker("worker-1809")
	if !ok {
		t.Fatal("worker missing after seed")
	}
	worker.ClonePath = missingClonePath
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	deps.amux.paneExists = map[string]bool{"pane-1809": true}

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if len(result.Findings) != 1 {
		t.Fatalf("findings = %#v, want one missing clone finding", result.Findings)
	}
	finding := result.Findings[0]
	if got, want := finding.Kind, "clone_missing"; got != want {
		t.Fatalf("finding.Kind = %q, want %q", got, want)
	}
	if got, want := finding.Issue, issue; got != want {
		t.Fatalf("finding.Issue = %q, want %q", got, want)
	}
	for _, want := range []string{missingClonePath, "restoring the directory alone does not resume polling", "orca cancel LAB-1809"} {
		if !strings.Contains(finding.Message, want) {
			t.Fatalf("finding.Message = %q, want to contain %q", finding.Message, want)
		}
	}
	if got := len(deps.commands.callsByName("gh")); got != 0 {
		t.Fatalf("gh call count = %d, want 0", got)
	}
}

func TestReconcileUsesDefaultProjectAndSortsFindings(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedReconcileAssignment(t, deps, "LAB-1506", "pane-1506", "worker-1506", 0)
	seedReconcileAssignment(t, deps, "LAB-1505", "pane-1505", "worker-1505", 0)
	deps.amux.paneExists = map[string]bool{
		"pane-1505": false,
		"pane-1506": false,
	}

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if got, want := result.Project, "/tmp/project"; got != want {
		t.Fatalf("result.Project = %q, want %q", got, want)
	}
	if got, want := findingIssues(result.Findings), []string{"LAB-1505", "LAB-1506"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("finding issues = %#v, want %#v", got, want)
	}
}

func TestReconcileReturnsScanErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		prepare func(*testing.T, *testDeps, context.Context) context.Context
		want    string
	}{
		{
			name: "non terminal task list",
			prepare: func(t *testing.T, deps *testDeps, ctx context.Context) context.Context {
				t.Helper()
				deps.state.rejectCanceledContext = true
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				return canceled
			},
			want: "list non-terminal tasks",
		},
		{
			name: "pane exists",
			prepare: func(t *testing.T, deps *testDeps, ctx context.Context) context.Context {
				t.Helper()
				seedReconcileAssignment(t, deps, "LAB-1507", "pane-1507", "worker-1507", 0)
				deps.amux.paneExistsErr = errors.New("amux unavailable")
				return ctx
			},
			want: "check pane for LAB-1507",
		},
		{
			name: "pane list",
			prepare: func(t *testing.T, deps *testDeps, ctx context.Context) context.Context {
				t.Helper()
				deps.amux.listPanesErr = errors.New("list failed")
				return ctx
			},
			want: "list amux panes",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			ctx := tt.prepare(t, deps, context.Background())
			_, err := deps.newDaemon(t).Reconcile(ctx, ReconcileRequest{Project: "/tmp/project"})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Reconcile() error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestReconcilePaneDriftReturnsTaskLookupError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.state.rejectCanceledContext = true
	deps.amux.listPanes = []Pane{{ID: "906", Name: "w-LAB-1491"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := deps.newDaemon(t).reconcilePaneDrift(ctx, "/tmp/project")
	if err == nil || !strings.Contains(err.Error(), "lookup task for pane") {
		t.Fatalf("reconcilePaneDrift() error = %v, want task lookup failure", err)
	}
}

func TestFixMergedReconcileFindingFillsFallbackMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.issueTracker.errors = map[string]error{IssueStateDone: errors.New("linear unavailable")}
	task := Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1508",
		Status:       TaskStatusActive,
		PaneID:       "missing-pane",
		ClonePath:    "/tmp/LAB-1508",
		AgentProfile: "unknown-profile",
		CreatedAt:    deps.clock.Now(),
		UpdatedAt:    deps.clock.Now(),
	}
	finding := ReconcileFinding{
		Kind:     ReconcileRecoverableGhost,
		Issue:    "LAB-1508",
		Branch:   "renamed-branch",
		PRNumber: 508,
		PRState:  reconcilePRStateMerged,
	}

	if err := deps.newDaemon(t).fixMergedReconcileFinding(context.Background(), task, finding); err != nil {
		t.Fatalf("fixMergedReconcileFinding() error = %v", err)
	}
	updated, ok := deps.state.task("LAB-1508")
	if !ok {
		t.Fatal("LAB-1508 task missing")
	}
	if updated.PRNumber != 508 || updated.Branch != "renamed-branch" || updated.Status != TaskStatusDone {
		t.Fatalf("updated task = %#v, want PR number, branch, and done status", updated)
	}
	if got := deps.events.lastMessage(EventPRMerged); !strings.Contains(got, "failed to update Linear issue status") {
		t.Fatalf("pr.merged message = %q, want Linear failure context", got)
	}
}

func TestReconcileFindingHelpersUseFallbacks(t *testing.T) {
	t.Parallel()

	finding := taskReconcileFinding(Task{
		Issue:  " LAB-1509 ",
		Status: " active ",
		Branch: " local-branch ",
	}, reconcilePRInfo{})
	if finding.PRState != reconcilePRStateNone {
		t.Fatalf("finding.PRState = %q, want none", finding.PRState)
	}

	overridden := taskReconcileFinding(Task{Branch: "local-branch"}, reconcilePRInfo{branch: "remote-branch"})
	if overridden.Branch != "remote-branch" {
		t.Fatalf("finding.Branch = %q, want remote branch", overridden.Branch)
	}

	if got := issueFromReconcileWorkerPane(Pane{Name: " "}); got != "" {
		t.Fatalf("issueFromReconcileWorkerPane(blank) = %q, want empty", got)
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

func TestLocalControllerReconcileAdoptsOrphanPaneForStatus(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	t.Setenv("LINEAR_API_KEY", "")

	projectPath := testProjectPath(t)
	clonePath := filepath.Join(projectPath, OrcaPoolSubdir, "clone-02")
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

	amuxClient := &fakeAmux{
		listPanes: []Pane{{ID: "906", Name: "w-LAB-1816", CWD: clonePath}},
		metadata: map[string]map[string]string{
			"906": {
				"agent_profile": "codex",
				"branch":        "LAB-1816",
				"task":          "Implement LAB-1816",
			},
		},
	}
	amuxClient.captureHistorySequence("906", []PaneCapture{{
		Content:        []string{"OpenAI Codex", "Working (12s - esc to interrupt)"},
		CWD:            clonePath,
		CurrentCommand: "codex",
	}})

	controller, err := NewLocalController(ControllerOptions{
		Store: store,
		Paths: Paths{
			StateDB: stateDB,
			PIDDir:  t.TempDir(),
		},
		DetectOrigin: func(string) (string, error) { return "", nil },
		Amux:         amuxClient,
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	result, err := controller.Reconcile(context.Background(), ReconcileRequest{
		Project:      projectPath,
		AdoptOrphans: true,
	})
	if err != nil {
		t.Fatalf("controller.Reconcile(--adopt-orphans) error = %v", err)
	}
	if got, want := result.Fixed, 1; got != want {
		t.Fatalf("result.Fixed = %d, want %d", got, want)
	}

	status, err := store.TaskStatus(context.Background(), projectPath, "LAB-1816")
	if err != nil {
		t.Fatalf("TaskStatus() error = %v", err)
	}
	if status.Task.Status != TaskStatusActive || status.Task.CurrentPaneID != "906" {
		t.Fatalf("status task = %#v, want active pane 906", status.Task)
	}
	if status.Task.ClonePath != clonePath || status.Task.Agent != "codex" {
		t.Fatalf("status task clone/agent = %q/%q, want %q/codex", status.Task.ClonePath, status.Task.Agent, clonePath)
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

func findingIssues(findings []ReconcileFinding) []string {
	issues := make([]string, 0, len(findings))
	for _, finding := range findings {
		issues = append(issues, finding.Issue)
	}
	return issues
}
