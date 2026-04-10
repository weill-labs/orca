package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/daemon"
)

func TestAppRunReload(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	d := &fakeDaemon{
		reloadResult: daemon.ReloadResult{Project: repoRoot, PID: 321},
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

	if err := app.Run(context.Background(), []string{"reload"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if d.reloadRequest == nil {
		t.Fatal("expected reload to be called")
	}
	if got, want := d.reloadRequest.Project, repoRoot; got != want {
		t.Fatalf("reload project = %q, want %q", got, want)
	}
	if !strings.Contains(stdout.String(), "reloaded") {
		t.Fatalf("stdout = %q, want reload output", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAppRunReloadGlobal(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	d := &fakeDaemon{
		reloadResult: daemon.ReloadResult{PID: 321},
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

	if err := app.Run(context.Background(), []string{"reload", "--global"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if d.reloadRequest == nil {
		t.Fatal("expected reload to be called")
	}
	if got, want := d.reloadRequest.Project, ""; got != want {
		t.Fatalf("reload project = %q, want empty project for --global", got)
	}
	if !strings.Contains(stdout.String(), "reloaded global daemon") {
		t.Fatalf("stdout = %q, want global reload output", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAppRunReloadJSON(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	d := &fakeDaemon{
		reloadResult: daemon.ReloadResult{Project: repoRoot, PID: 321},
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

	if err := app.Run(context.Background(), []string{"reload", "--json"}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	var result daemon.ReloadResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("json.Unmarshal(stdout) error = %v", err)
	}
	if got, want := result, (daemon.ReloadResult{Project: repoRoot, PID: 321}); got != want {
		t.Fatalf("json result = %#v, want %#v", got, want)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAppRunReloadError(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	wantErr := errors.New("reload failed")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  &fakeDaemon{err: wantErr},
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	err := app.Run(context.Background(), []string{"reload"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Run() error = %v, want %v", err, wantErr)
	}
	if stdout.String() != "" {
		t.Fatalf("stdout = %q, want empty", stdout.String())
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAppRunReloadRejectsPositionalArguments(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon:  &fakeDaemon{},
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &stderr,
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	err := app.Run(context.Background(), []string{"reload", "extra"})
	if err == nil {
		t.Fatal("Run() error = nil, want parse error")
	}
	if !strings.Contains(err.Error(), "reload does not accept positional arguments") {
		t.Fatalf("Run() error = %v, want reload positional error", err)
	}
}
