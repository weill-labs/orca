package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
)

func TestAppRunAssignPlumbsNotifyPaneFlag(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	cwdPath := filepath.Join(repoRoot, "internal", "cli")
	if err := os.MkdirAll(cwdPath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", cwdPath, err)
	}

	d := &fakeDaemon{
		assignResult: daemon.TaskActionResult{
			Project:   repoRoot,
			Issue:     "LAB-1946",
			Status:    "queued",
			Agent:     "codex",
			UpdatedAt: time.Now().UTC(),
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

	err := app.Run(context.Background(), []string{
		"assign",
		"--notify-pane", "pane-13",
		"--prompt", "Implement worker comms",
		"LAB-1946",
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if d.assignRequest == nil {
		t.Fatal("expected assign to be called")
	}
	if got, want := d.assignRequest.NotifyPane, "pane-13"; got != want {
		t.Fatalf("assign notify pane = %q, want %q", got, want)
	}
	if stderr.String() != "" {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestAppRunAssignHelpMentionsNotifyPane(t *testing.T) {
	t.Parallel()

	usage, ok := HelpText([]string{"help", "assign"})
	if !ok {
		t.Fatal("HelpText(help assign) not found")
	}
	if !strings.Contains(usage, "--notify-pane") {
		t.Fatalf("assign help = %q, want --notify-pane", usage)
	}
}
