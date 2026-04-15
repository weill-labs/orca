package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
)

func TestAppRunStartResolvesPoolCloneToParentProject(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	poolClone := filepath.Join(repoRoot, ".orca", "pool", "clone-06")
	cwdPath := filepath.Join(poolClone, "internal", "cli")
	if err := os.MkdirAll(filepath.Join(poolClone, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", filepath.Join(poolClone, ".git"), err)
	}
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	d := &fakeDaemon{
		startResult: daemon.StartResult{
			Project:   repoRoot,
			Session:   "orca",
			PID:       321,
			StartedAt: time.Now().UTC(),
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	if err := app.Run(context.Background(), []string{"start"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.startRequest == nil {
		t.Fatal("expected start to be called")
	}
	if got, want := d.startRequest.Project, repoRoot; got != want {
		t.Fatalf("start project = %q, want %q", got, want)
	}
	if stderr.String() != "" {
		t.Fatalf("expected empty stderr, got %q", stderr.String())
	}
}
