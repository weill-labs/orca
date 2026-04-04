package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestDetectOrigin(t *testing.T) {
	t.Parallel()

	t.Run("returns origin remote URL", func(t *testing.T) {
		t.Parallel()

		repo := newGitRepo(t)
		runGit(t, repo, "remote", "add", "origin", "git@github.com:weill-labs/orca.git")

		origin, err := DetectOrigin(repo)
		if err != nil {
			t.Fatalf("DetectOrigin() error = %v", err)
		}
		if got, want := origin, "git@github.com:weill-labs/orca.git"; got != want {
			t.Fatalf("DetectOrigin() = %q, want %q", got, want)
		}
	})

	t.Run("no origin remote returns error", func(t *testing.T) {
		t.Parallel()

		repo := newGitRepo(t)

		_, err := DetectOrigin(repo)
		if err == nil {
			t.Fatal("expected error for repo with no origin remote")
		}
		if !strings.Contains(err.Error(), "origin") {
			t.Fatalf("error should mention origin, got: %v", err)
		}
	})

	t.Run("non-git directory returns error", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()

		_, err := DetectOrigin(dir)
		if err == nil {
			t.Fatal("expected error for non-git directory")
		}
	})
}

func TestDetectOriginEnvVarOverride(t *testing.T) {
	repo := newGitRepo(t)
	runGit(t, repo, "remote", "add", "origin", "git@github.com:weill-labs/orca.git")
	t.Setenv("ORCA_CLONE_ORIGIN", "https://override.example.com/repo.git")

	origin, err := DetectOrigin(repo)
	if err != nil {
		t.Fatalf("DetectOrigin() error = %v", err)
	}
	if got, want := origin, "https://override.example.com/repo.git"; got != want {
		t.Fatalf("DetectOrigin() = %q, want %q", got, want)
	}
}

func newGitRepo(t *testing.T) string {
	t.Helper()

	dir := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	runGit(t, dir, "init")
	return dir
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()

	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v: %s", strings.Join(args, " "), err, out)
	}
}
