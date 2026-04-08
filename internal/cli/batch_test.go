package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
)

func TestAppRunBatchParsesManifestAndDispatches(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	manifestPath := filepath.Join(repoRoot, "tasks.json")
	if err := os.WriteFile(manifestPath, []byte(`[
{"issue":"LAB-690","agent":"codex","prompt":"Implement CLI wiring","title":"Worker pane title"},
{"issue":"LAB-691","agent":"claude","prompt":"Write tests"}
]`), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
	}

	now := time.Date(2026, 4, 2, 15, 4, 5, 0, time.UTC)
	d := &fakeDaemon{
		batchResult: daemon.BatchResult{
			Project: repoRoot,
			Results: []daemon.TaskActionResult{
				{Project: repoRoot, Issue: "LAB-690", Status: "active", Agent: "codex", UpdatedAt: now},
				{Project: repoRoot, Issue: "LAB-691", Status: "active", Agent: "claude", UpdatedAt: now},
			},
		},
	}

	var stdout bytes.Buffer
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &bytes.Buffer{},
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	if err := app.Run(context.Background(), []string{"batch", "--delay", "7s", manifestPath}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.batchRequest == nil {
		t.Fatal("expected batch to be called")
	}
	if got, want := d.batchRequest.Project, repoRoot; got != want {
		t.Fatalf("batch project = %q, want %q", got, want)
	}
	if got, want := d.batchRequest.Delay, 7*time.Second; got != want {
		t.Fatalf("batch delay = %s, want %s", got, want)
	}
	if got, want := d.batchRequest.Entries, []daemon.BatchEntry{
		{Issue: "LAB-690", Agent: "codex", Prompt: "Implement CLI wiring", Title: "Worker pane title"},
		{Issue: "LAB-691", Agent: "claude", Prompt: "Write tests"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batch entries = %#v, want %#v", got, want)
	}
	if output := stdout.String(); !strings.Contains(output, "LAB-690 assigned to codex") || !strings.Contains(output, "LAB-691 assigned to claude") {
		t.Fatalf("stdout = %q, want per-task output", output)
	}
}

func TestAppRunBatchUsesDefaultDelay(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	manifestPath := filepath.Join(repoRoot, "tasks.json")
	if err := os.WriteFile(manifestPath, []byte(`[{"issue":"LAB-690","agent":"codex","prompt":"Implement CLI wiring"}]`), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
	}

	d := &fakeDaemon{
		batchResult: daemon.BatchResult{
			Project: repoRoot,
			Results: []daemon.TaskActionResult{{Project: repoRoot, Issue: "LAB-690", Status: "active", Agent: "codex", UpdatedAt: time.Now().UTC()}},
		},
	}

	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &bytes.Buffer{},
		Stderr:  &bytes.Buffer{},
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	if err := app.Run(context.Background(), []string{"batch", manifestPath}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.batchRequest == nil {
		t.Fatal("expected batch to be called")
	}
	if got, want := d.batchRequest.Delay, 5*time.Second; got != want {
		t.Fatalf("batch delay = %s, want %s", got, want)
	}
}

func TestAppRunBatchRejectsInvalidManifest(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	manifestPath := filepath.Join(repoRoot, "tasks.json")
	if err := os.WriteFile(manifestPath, []byte(`[{"agent":"codex","prompt":"Implement CLI wiring"}]`), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
	}

	d := &fakeDaemon{}
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &bytes.Buffer{},
		Stderr:  &bytes.Buffer{},
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	err := app.Run(context.Background(), []string{"batch", manifestPath})
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "batch manifest entry 1 requires issue") {
		t.Fatalf("error = %v, want manifest validation", err)
	}
	if d.batchRequest != nil {
		t.Fatal("expected batch not to be called")
	}
}

func TestAppRunBatchRejectsNegativeDelay(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	manifestPath := filepath.Join(repoRoot, "tasks.json")
	if err := os.WriteFile(manifestPath, []byte(`[{"issue":"LAB-690","agent":"codex","prompt":"Implement CLI wiring"}]`), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
	}

	app := New(Options{
		Daemon:  &fakeDaemon{},
		State:   &fakeState{},
		Stdout:  &bytes.Buffer{},
		Stderr:  &bytes.Buffer{},
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	err := app.Run(context.Background(), []string{"batch", "--delay", "-1s", manifestPath})
	if err == nil || !strings.Contains(err.Error(), "batch delay must be non-negative") {
		t.Fatalf("Run() error = %v, want negative delay validation", err)
	}
}

func TestAppRunBatchPropagatesManifestReadAndDecodeErrors(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	tests := []struct {
		name     string
		manifest string
		prepare  func(path string)
		wantErr  string
	}{
		{
			name:     "read error",
			manifest: filepath.Join(repoRoot, "missing.json"),
			wantErr:  "read batch manifest",
		},
		{
			name:     "decode error",
			manifest: filepath.Join(repoRoot, "broken.json"),
			prepare: func(path string) {
				if err := os.WriteFile(path, []byte(`{"issue":`), 0o644); err != nil {
					t.Fatalf("WriteFile(%q) error = %v", path, err)
				}
			},
			wantErr: "decode batch manifest",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if tt.prepare != nil {
				tt.prepare(tt.manifest)
			}

			d := &fakeDaemon{}
			app := New(Options{
				Daemon:  d,
				State:   &fakeState{},
				Stdout:  &bytes.Buffer{},
				Stderr:  &bytes.Buffer{},
				Version: "build-123",
				Cwd: func() (string, error) {
					return cwdPath, nil
				},
			})

			err := app.Run(context.Background(), []string{"batch", tt.manifest})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Run() error = %v, want %q", err, tt.wantErr)
			}
			if d.batchRequest != nil {
				t.Fatal("expected batch not to be called")
			}
		})
	}
}

func TestAppRunBatchPropagatesResolveProjectAndDaemonErrors(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	manifestPath := filepath.Join(repoRoot, "tasks.json")
	if err := os.WriteFile(manifestPath, []byte(`[{"issue":"LAB-690","agent":"codex","prompt":"Implement CLI wiring"}]`), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
	}

	t.Run("resolve project", func(t *testing.T) {
		t.Parallel()

		app := New(Options{
			Daemon:  &fakeDaemon{},
			State:   &fakeState{},
			Stdout:  &bytes.Buffer{},
			Stderr:  &bytes.Buffer{},
			Version: "build-123",
			Cwd: func() (string, error) {
				return cwdPath, nil
			},
		})

		err := app.Run(context.Background(), []string{"batch", "--project", t.TempDir(), manifestPath})
		if err == nil || !strings.Contains(err.Error(), "not inside a git repository") {
			t.Fatalf("Run() error = %v, want project resolution error", err)
		}
	})

	t.Run("daemon error", func(t *testing.T) {
		t.Parallel()

		d := &fakeDaemon{err: errors.New("daemon unavailable")}
		app := New(Options{
			Daemon:  d,
			State:   &fakeState{},
			Stdout:  &bytes.Buffer{},
			Stderr:  &bytes.Buffer{},
			Version: "build-123",
			Cwd: func() (string, error) {
				return cwdPath, nil
			},
		})

		err := app.Run(context.Background(), []string{"batch", manifestPath})
		if err == nil || !strings.Contains(err.Error(), "daemon unavailable") {
			t.Fatalf("Run() error = %v, want daemon error", err)
		}
	})
}

func TestAppRunBatchReportsPerAssignmentFailures(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	manifestPath := filepath.Join(repoRoot, "tasks.json")
	if err := os.WriteFile(manifestPath, []byte(`[{"issue":"LAB-690","agent":"codex","prompt":"Implement CLI wiring"}]`), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
	}

	d := &fakeDaemon{
		batchResult: daemon.BatchResult{
			Project: repoRoot,
			Results: []daemon.TaskActionResult{{Project: repoRoot, Issue: "LAB-690", Status: "active", Agent: "codex", UpdatedAt: time.Now().UTC()}},
			Failures: []daemon.BatchFailure{
				{Issue: "LAB-691", Error: `load agent profile "missing": unknown agent profile "missing" (available: claude, codex)`},
			},
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

	err := app.Run(context.Background(), []string{"batch", manifestPath})
	if err == nil {
		t.Fatal("Run() error = nil, want batch failure")
	}
	if !strings.Contains(err.Error(), "batch failed for 1 assignment") {
		t.Fatalf("Run() error = %v, want batch failure summary", err)
	}
	if output := stdout.String(); !strings.Contains(output, "LAB-690 assigned to codex") {
		t.Fatalf("stdout = %q, want successful assignment output", output)
	}
	if output := stderr.String(); !strings.Contains(output, `LAB-691 failed: load agent profile "missing"`) {
		t.Fatalf("stderr = %q, want per-assignment failure output", output)
	}
}

func TestAppRunBatchPropagatesStdoutWriteError(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	manifestPath := filepath.Join(repoRoot, "tasks.json")
	if err := os.WriteFile(manifestPath, []byte(`[{"issue":"LAB-690","agent":"codex","prompt":"Implement CLI wiring"}]`), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
	}

	d := &fakeDaemon{
		batchResult: daemon.BatchResult{
			Project: repoRoot,
			Results: []daemon.TaskActionResult{{Project: repoRoot, Issue: "LAB-690", Status: "active", Agent: "codex", UpdatedAt: time.Now().UTC()}},
		},
	}

	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  errWriter{},
		Stderr:  &bytes.Buffer{},
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	err := app.Run(context.Background(), []string{"batch", manifestPath})
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("Run() error = %v, want stdout write error", err)
	}
}

func TestAppRunBatchPropagatesStderrWriteError(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", cwdPath, err)
	}

	manifestPath := filepath.Join(repoRoot, "tasks.json")
	if err := os.WriteFile(manifestPath, []byte(`[{"issue":"LAB-690","agent":"codex","prompt":"Implement CLI wiring"}]`), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", manifestPath, err)
	}

	d := &fakeDaemon{
		batchResult: daemon.BatchResult{
			Project: repoRoot,
			Results: []daemon.TaskActionResult{{Project: repoRoot, Issue: "LAB-690", Status: "active", Agent: "codex", UpdatedAt: time.Now().UTC()}},
			Failures: []daemon.BatchFailure{
				{Issue: "LAB-691", Error: `load agent profile "missing": unknown agent profile "missing" (available: claude, codex)`},
			},
		},
	}

	var stdout bytes.Buffer
	app := New(Options{
		Daemon:  d,
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  errWriter{},
		Version: "build-123",
		Cwd: func() (string, error) {
			return cwdPath, nil
		},
	})

	err := app.Run(context.Background(), []string{"batch", manifestPath})
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("Run() error = %v, want stderr write error", err)
	}
	if output := stdout.String(); !strings.Contains(output, "LAB-690 assigned to codex") {
		t.Fatalf("stdout = %q, want successful assignment output before stderr failure", output)
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) {
	return 0, errors.New("write failed")
}
