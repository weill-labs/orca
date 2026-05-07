package pool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizePoolRootRelative(t *testing.T) {
	root := t.TempDir()
	poolDir := filepath.Join(root, "pool")
	t.Chdir(root)

	got, err := normalizePoolRoot("pool")
	if err != nil {
		t.Fatalf("normalizePoolRoot() error = %v", err)
	}
	if got != poolDir {
		t.Fatalf("normalizePoolRoot() = %q, want %q", got, poolDir)
	}
}

func TestPathValidationInternals(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	poolDir := filepath.Join(root, "pool")
	clonePath := filepath.Join(poolDir, "clone-01")
	if err := os.MkdirAll(clonePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", clonePath, err)
	}
	if err := ensureCloneMarker(clonePath); err != nil {
		t.Fatalf("ensureCloneMarker() setup error = %v", err)
	}

	tests := []struct {
		name    string
		run     func(*testing.T) error
		wantErr string
	}{
		{
			name: "empty pool root",
			run: func(*testing.T) error {
				_, err := normalizePoolRoot("")
				return err
			},
			wantErr: "pool directory path is required",
		},
		{
			name: "filesystem root pool",
			run: func(*testing.T) error {
				_, err := normalizePoolRoot(string(filepath.Separator))
				return err
			},
			wantErr: "resolves to filesystem root",
		},
		{
			name: "nul clone path",
			run: func(*testing.T) error {
				_, err := ValidateClonePath(poolDir, clonePath+"\x00")
				return err
			},
			wantErr: "contains NUL byte",
		},
		{
			name: "relative clone path",
			run: func(*testing.T) error {
				_, err := ValidateClonePath(poolDir, "clone-01")
				return err
			},
			wantErr: "must be absolute",
		},
		{
			name: "parent traversal clone path",
			run: func(*testing.T) error {
				_, err := ValidateClonePath(poolDir, poolDir+string(filepath.Separator)+".."+string(filepath.Separator)+"escape")
				return err
			},
			wantErr: "contains parent traversal",
		},
		{
			name: "pool root clone path",
			run: func(*testing.T) error {
				_, err := ValidateClonePath(poolDir, poolDir)
				return err
			},
			wantErr: "must be a child",
		},
		{
			name: "outside clone path",
			run: func(*testing.T) error {
				_, err := ValidateClonePath(poolDir, filepath.Join(root, "outside"))
				return err
			},
			wantErr: "must stay inside pool root",
		},
		{
			name: "missing marker",
			run: func(*testing.T) error {
				return requireCloneMarker(filepath.Join(poolDir, "missing"))
			},
			wantErr: "missing .orca-pool marker",
		},
		{
			name: "marker directory",
			run: func(t *testing.T) error {
				badClone := filepath.Join(poolDir, "clone-bad-marker")
				if err := os.MkdirAll(filepath.Join(badClone, ClonePoolMarker), 0o755); err != nil {
					t.Fatalf("MkdirAll(marker) error = %v", err)
				}
				_, err := cloneHasMarker(badClone)
				return err
			},
			wantErr: "must be a regular file",
		},
		{
			name: "marker write failure",
			run: func(*testing.T) error {
				return ensureCloneMarker(filepath.Join(root, "missing", "clone-01"))
			},
			wantErr: "write clone marker",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.run(t)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("%s error = %v, want substring %q", tt.name, err, tt.wantErr)
			}
		})
	}

	got, err := ValidateClonePath(poolDir, clonePath+string(filepath.Separator))
	if err != nil {
		t.Fatalf("ValidateClonePath() error = %v", err)
	}
	if got != clonePath {
		t.Fatalf("ValidateClonePath() = %q, want %q", got, clonePath)
	}

	manager := &Manager{poolDir: poolDir}
	got, err = manager.markedClonePath(clonePath)
	if err != nil {
		t.Fatalf("markedClonePath() error = %v", err)
	}
	if got != clonePath {
		t.Fatalf("markedClonePath() = %q, want %q", got, clonePath)
	}

	if hasMarker, err := cloneHasMarker(clonePath); err != nil || !hasMarker {
		t.Fatalf("cloneHasMarker() = %v, %v; want true, nil", hasMarker, err)
	}

	if got := statusLinePath("?? " + ClonePoolMarker); got != ClonePoolMarker {
		t.Fatalf("statusLinePath() = %q, want %q", got, ClonePoolMarker)
	}
	if got := statusLinePath("?"); got != "?" {
		t.Fatalf("statusLinePath(short) = %q, want ?", got)
	}
}

func TestActiveCWDSetNormalizesPoolCloneCWDs(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	poolDir := filepath.Join(root, "pool")
	clonePath := filepath.Join(poolDir, "clone-01")
	manager := &Manager{
		poolDir: poolDir,
		cwdUsageChecker: staticCWDUsageChecker{
			paths: []string{
				clonePath + string(filepath.Separator),
				filepath.Join(root, "outside"),
			},
		},
	}

	set, err := manager.activeCWDSet(context.Background())
	if err != nil {
		t.Fatalf("activeCWDSet() error = %v", err)
	}
	if _, ok := set[clonePath]; !ok {
		t.Fatalf("activeCWDSet() missing %q in %#v", clonePath, set)
	}
	if _, ok := set[filepath.Join(root, "outside")]; ok {
		t.Fatalf("activeCWDSet() included outside pool cwd: %#v", set)
	}
}

func TestActiveCWDSetSkipsMalformedCWD(t *testing.T) {
	t.Parallel()

	manager := &Manager{
		poolDir: t.TempDir(),
		cwdUsageChecker: staticCWDUsageChecker{
			paths: []string{"clone-01"},
		},
	}

	set, err := manager.activeCWDSet(context.Background())
	if err != nil {
		t.Fatalf("activeCWDSet() error = %v", err)
	}
	if len(set) != 0 {
		t.Fatalf("activeCWDSet() = %#v, want malformed cwd skipped", set)
	}
}

func TestRunCloneSetupHookRejectsInvalidCWD(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	manager := &Manager{poolDir: filepath.Join(root, "pool")}

	err := manager.runCloneSetupHook(context.Background(), filepath.Join(root, "outside"))
	if err == nil || !strings.Contains(err.Error(), "validate clone bootstrap hook cwd") {
		t.Fatalf("runCloneSetupHook() error = %v, want cwd validation error", err)
	}
}

func TestRunCloneSetupHookLogsDirectory(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	poolDir := filepath.Join(root, "pool")
	clonePath := filepath.Join(poolDir, "clone-01")
	if err := os.MkdirAll(filepath.Join(clonePath, ".orca", "setup.sh"), 0o755); err != nil {
		t.Fatalf("MkdirAll(setup hook dir) error = %v", err)
	}
	if err := ensureCloneMarker(clonePath); err != nil {
		t.Fatalf("ensureCloneMarker() setup error = %v", err)
	}

	var logs []string
	manager := &Manager{
		poolDir: poolDir,
		logf: func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	}

	if err := manager.runCloneSetupHook(context.Background(), clonePath); err != nil {
		t.Fatalf("runCloneSetupHook() error = %v", err)
	}
	if got := strings.Join(logs, "\n"); !strings.Contains(got, "path is a directory") {
		t.Fatalf("logs = %q, want directory hook message", got)
	}
}

func TestRunCloneSetupHookLogsStatError(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	poolDir := filepath.Join(root, "pool")
	clonePath := filepath.Join(poolDir, "clone-01")
	if err := os.MkdirAll(clonePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(clone) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(clonePath, ".orca"), []byte("not a directory\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(.orca) error = %v", err)
	}
	if err := ensureCloneMarker(clonePath); err != nil {
		t.Fatalf("ensureCloneMarker() setup error = %v", err)
	}

	var logs []string
	manager := &Manager{
		poolDir: poolDir,
		logf: func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	}

	if err := manager.runCloneSetupHook(context.Background(), clonePath); err != nil {
		t.Fatalf("runCloneSetupHook() error = %v", err)
	}
	got := strings.Join(logs, "\n")
	if !strings.Contains(got, "inspect clone bootstrap hook") || !strings.Contains(got, "not a directory") {
		t.Fatalf("logs = %q, want hook stat error", got)
	}
}

func TestCloneHasMarkerReturnsInspectErrors(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	clonePath := filepath.Join(root, "clone-file")
	if err := os.WriteFile(clonePath, []byte("not a directory\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(clone file) error = %v", err)
	}

	hasMarker, err := cloneHasMarker(clonePath)
	if err == nil || !strings.Contains(err.Error(), "inspect clone marker") {
		t.Fatalf("cloneHasMarker() error = %v, want inspect marker error", err)
	}
	if hasMarker {
		t.Fatal("cloneHasMarker() = true, want false")
	}
}

func TestCreateCloneReturnsMarkerWriteFailure(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	manager := &Manager{
		project:     filepath.Join(root, "project"),
		poolDir:     filepath.Join(root, "pool"),
		cloneOrigin: "origin",
		baseBranch:  "main",
		runner:      noOpPathRunner{},
	}

	_, err := manager.CreateClone(context.Background())
	if err == nil || !strings.Contains(err.Error(), "write clone marker") {
		t.Fatalf("CreateClone() error = %v, want marker write failure", err)
	}
}

type staticCWDUsageChecker struct {
	paths []string
}

func (c staticCWDUsageChecker) ActiveCWDs(context.Context) ([]string, error) {
	return append([]string(nil), c.paths...), nil
}

type noOpPathRunner struct{}

func (noOpPathRunner) Run(context.Context, string, string, ...string) error {
	return nil
}
