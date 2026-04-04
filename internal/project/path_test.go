package project

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCanonicalPathReturnsCanonicalRepoRoot(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	subdir := filepath.Join(repoRoot, "internal", "daemon")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", subdir, err)
	}

	got, err := CanonicalPath(subdir)
	if err != nil {
		t.Fatalf("CanonicalPath(%q) error = %v", subdir, err)
	}
	if got != repoRoot {
		t.Fatalf("CanonicalPath(%q) = %q, want %q", subdir, got, repoRoot)
	}
}

func TestCanonicalPathResolvesSymlinksBeforeFindingRepoRoot(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	realSubdir := filepath.Join(repoRoot, "cmd", "orca")
	if err := os.MkdirAll(realSubdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", realSubdir, err)
	}

	linkRoot := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(realSubdir, linkRoot); err != nil {
		t.Fatalf("Symlink(%q, %q): %v", realSubdir, linkRoot, err)
	}

	got, err := CanonicalPath(linkRoot)
	if err != nil {
		t.Fatalf("CanonicalPath(%q) error = %v", linkRoot, err)
	}
	if got != repoRoot {
		t.Fatalf("CanonicalPath(%q) = %q, want %q", linkRoot, got, repoRoot)
	}
}

func TestCanonicalPathRejectsPathsOutsideGitRepos(t *testing.T) {
	t.Parallel()

	path := t.TempDir()
	_, err := CanonicalPath(path)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "is not inside a git repository") {
		t.Fatalf("expected git repository error, got %v", err)
	}
}

func newRepoRoot(t *testing.T) string {
	t.Helper()

	root := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.git): %v", err)
	}

	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", root, err)
	}
	return resolvedRoot
}
