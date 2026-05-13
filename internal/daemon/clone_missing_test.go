package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTaskManagedClonePathUnavailableReportsPRRepoMissing(t *testing.T) {
	t.Parallel()

	existingClone := filepath.Join(t.TempDir(), orcaPoolSubdir, "clone-01")
	if err := os.MkdirAll(existingClone, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", existingClone, err)
	}
	missingPRRepo := filepath.Join(filepath.Dir(existingClone), "deleted-clone")

	gotPath, unavailable, err := taskManagedClonePathUnavailable(Task{
		ClonePath: existingClone,
		PRRepo:    missingPRRepo,
	})
	if err != nil {
		t.Fatalf("taskManagedClonePathUnavailable() error = %v", err)
	}
	if !unavailable {
		t.Fatal("taskManagedClonePathUnavailable() unavailable = false, want true")
	}
	if gotPath != missingPRRepo {
		t.Fatalf("taskManagedClonePathUnavailable() path = %q, want %q", gotPath, missingPRRepo)
	}
}

func TestTaskManagedClonePathUnavailableIgnoresUnmanagedPaths(t *testing.T) {
	t.Parallel()

	gotPath, unavailable, err := taskManagedClonePathUnavailable(Task{
		ClonePath: filepath.Join(t.TempDir(), "clone-01"),
		PRRepo:    "weill-labs/orca",
	})
	if err != nil {
		t.Fatalf("taskManagedClonePathUnavailable() error = %v", err)
	}
	if unavailable {
		t.Fatal("taskManagedClonePathUnavailable() unavailable = true, want false")
	}
	if gotPath != "" {
		t.Fatalf("taskManagedClonePathUnavailable() path = %q, want empty", gotPath)
	}
}

func TestTaskManagedClonePathUnavailableTreatsFilesAsUnavailable(t *testing.T) {
	t.Parallel()

	clonePath := filepath.Join(t.TempDir(), orcaPoolSubdir, "clone-file")
	if err := os.MkdirAll(filepath.Dir(clonePath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(clonePath), err)
	}
	if err := os.WriteFile(clonePath, []byte("not a directory"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", clonePath, err)
	}

	gotPath, unavailable, err := taskManagedClonePathUnavailable(Task{ClonePath: clonePath})
	if err != nil {
		t.Fatalf("taskManagedClonePathUnavailable() error = %v", err)
	}
	if !unavailable {
		t.Fatal("taskManagedClonePathUnavailable() unavailable = false, want true")
	}
	if gotPath != clonePath {
		t.Fatalf("taskManagedClonePathUnavailable() path = %q, want %q", gotPath, clonePath)
	}
}

func TestReconcileMissingClonePathDriftReportsStatError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := selfReferencingPoolSymlink(t)
	finding, ok, err := deps.newDaemon(t).reconcileMissingClonePathDrift(Task{
		Project:   "/tmp/project",
		Issue:     "LAB-1809",
		Status:    TaskStatusActive,
		State:     TaskStateAssigned,
		ClonePath: clonePath,
		Branch:    "LAB-1809",
	})
	if err != nil {
		t.Fatalf("reconcileMissingClonePathDrift() error = %v", err)
	}
	if !ok {
		t.Fatal("reconcileMissingClonePathDrift() ok = false, want true")
	}
	if got, want := finding.Kind, ReconcileClonePathError; got != want {
		t.Fatalf("finding.Kind = %q, want %q", got, want)
	}
	for _, want := range []string{clonePath, "could not be inspected", "orca cancel LAB-1809"} {
		if !strings.Contains(finding.Message, want) {
			t.Fatalf("finding.Message = %q, want to contain %q", finding.Message, want)
		}
	}
}

func TestReconcileReportsClonePathStatErrorAndContinues(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	statErrClonePath := selfReferencingPoolSymlink(t)
	missingClonePath := filepath.Join(t.TempDir(), orcaPoolSubdir, "deleted-clone")
	seedReconcileAssignment(t, deps, "LAB-1808", "pane-1808", "worker-1808", 0)
	seedReconcileAssignment(t, deps, "LAB-1809", "pane-1809", "worker-1809", 0)
	setReconcileAssignmentClonePath(t, deps, "LAB-1808", "worker-1808", statErrClonePath)
	setReconcileAssignmentClonePath(t, deps, "LAB-1809", "worker-1809", missingClonePath)

	result, err := deps.newDaemon(t).Reconcile(context.Background(), ReconcileRequest{Project: "/tmp/project"})
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}

	got := findingKindsByIssue(result.Findings)
	if len(got) != 2 || got["LAB-1808"] != ReconcileClonePathError || got["LAB-1809"] != ReconcileCloneMissing {
		t.Fatalf("finding kinds = %#v, want LAB-1808 clone path error and LAB-1809 clone missing", got)
	}
	var statErrFinding ReconcileFinding
	for _, finding := range result.Findings {
		if finding.Issue == "LAB-1808" {
			statErrFinding = finding
			break
		}
	}
	for _, want := range []string{statErrClonePath, "could not be inspected", "orca cancel LAB-1808"} {
		if !strings.Contains(statErrFinding.Message, want) {
			t.Fatalf("stat error finding message = %q, want to contain %q", statErrFinding.Message, want)
		}
	}
	if got := len(deps.commands.callsByName("gh")); got != 0 {
		t.Fatalf("gh call count = %d, want 0", got)
	}
	if got, want := deps.events.countType(EventReconcileFinding), 2; got != want {
		t.Fatalf("reconcile finding event count = %d, want %d", got, want)
	}
}

func TestReconcileMissingClonePathOnStartupLogsStatError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := selfReferencingPoolSymlink(t)
	var logs []string
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		}
	})

	if d.reconcileMissingClonePathOnStartup(context.Background(), Task{
		Project:   "/tmp/project",
		Issue:     "LAB-1809",
		Status:    TaskStatusActive,
		State:     TaskStateAssigned,
		ClonePath: clonePath,
		Branch:    "LAB-1809",
	}) {
		t.Fatal("reconcileMissingClonePathOnStartup() = true, want false")
	}
	if len(logs) != 1 || !strings.Contains(logs[0], "missing clone path reconciliation failed for LAB-1809") {
		t.Fatalf("logs = %#v, want missing clone path reconciliation log", logs)
	}
}

func selfReferencingPoolSymlink(t *testing.T) string {
	t.Helper()

	clonePath := filepath.Join(t.TempDir(), orcaPoolSubdir, "clone-loop")
	if err := os.MkdirAll(filepath.Dir(clonePath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(clonePath), err)
	}
	if err := os.Symlink(filepath.Base(clonePath), clonePath); err != nil {
		t.Fatalf("Symlink(%q) error = %v", clonePath, err)
	}
	return clonePath
}

func setReconcileAssignmentClonePath(t *testing.T, deps *testDeps, issue, workerID, clonePath string) {
	t.Helper()

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatalf("%s task missing after seed", issue)
	}
	task.ClonePath = clonePath
	deps.state.putTaskForTest(task)

	worker, ok := deps.state.worker(workerID)
	if !ok {
		t.Fatalf("%s worker missing after seed", workerID)
	}
	worker.ClonePath = clonePath
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
}
