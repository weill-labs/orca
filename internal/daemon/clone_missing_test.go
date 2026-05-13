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

func TestReconcileMissingClonePathDriftReturnsStatError(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := selfReferencingPoolSymlink(t)
	_, ok, err := deps.newDaemon(t).reconcileMissingClonePathDrift(Task{
		Project:   "/tmp/project",
		Issue:     "LAB-1809",
		Status:    TaskStatusActive,
		State:     TaskStateAssigned,
		ClonePath: clonePath,
		Branch:    "LAB-1809",
	})
	if err == nil {
		t.Fatal("reconcileMissingClonePathDrift() error = nil, want stat error")
	}
	if ok {
		t.Fatal("reconcileMissingClonePathDrift() ok = true, want false")
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
