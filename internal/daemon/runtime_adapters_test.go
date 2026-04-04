package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestBuiltinConfigProviderAgentProfile(t *testing.T) {
	t.Parallel()

	provider := builtinConfigProvider{}

	t.Run("claude profile", func(t *testing.T) {
		t.Parallel()

		profile, err := provider.AgentProfile(context.Background(), "claude")
		if err != nil {
			t.Fatalf("AgentProfile(claude) error = %v", err)
		}
		if got, want := profile.StartCommand, "claude"; got != want {
			t.Fatalf("StartCommand = %q, want %q", got, want)
		}
		if got, want := profile.StuckTimeout, 8*time.Minute; got != want {
			t.Fatalf("StuckTimeout = %v, want %v", got, want)
		}
		if got, want := profile.NudgeCommand, "Enter"; got != want {
			t.Fatalf("NudgeCommand = %q, want %q", got, want)
		}
		if got, want := profile.MaxNudgeRetries, 3; got != want {
			t.Fatalf("MaxNudgeRetries = %d, want %d", got, want)
		}
	})

	t.Run("codex profile with quirks", func(t *testing.T) {
		t.Parallel()

		profile, err := provider.AgentProfile(context.Background(), "codex")
		if err != nil {
			t.Fatalf("AgentProfile(codex) error = %v", err)
		}
		if got, want := profile.StartCommand, "codex --yolo"; got != want {
			t.Fatalf("StartCommand = %q, want %q", got, want)
		}
		if got, want := profile.ResumeSequence, []string{"codex --yolo resume", "Enter", "."}; !reflect.DeepEqual(got, want) {
			t.Fatalf("ResumeSequence = %#v, want %#v", got, want)
		}
	})

	t.Run("case insensitive lookup", func(t *testing.T) {
		t.Parallel()

		_, err := provider.AgentProfile(context.Background(), "Claude")
		if err != nil {
			t.Fatalf("AgentProfile(Claude) error = %v", err)
		}
	})

	t.Run("unknown agent returns error", func(t *testing.T) {
		t.Parallel()

		_, err := provider.AgentProfile(context.Background(), "missing")
		if err == nil {
			t.Fatal("AgentProfile(missing) = nil error, want non-nil")
		}
		if !strings.Contains(err.Error(), "unknown agent") {
			t.Fatalf("error = %v, want 'unknown agent'", err)
		}
	})
}

func TestNormalizeCodexStartCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command string
		want    string
	}{
		{name: "empty command", command: "", want: "codex --yolo"},
		{name: "adds yolo", command: "codex", want: "codex --yolo"},
		{name: "preserves yolo", command: "codex --yolo", want: "codex --yolo"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := normalizeCodexStartCommand(tt.command); got != tt.want {
				t.Fatalf("normalizeCodexStartCommand(%q) = %q, want %q", tt.command, got, tt.want)
			}
		})
	}
}

func TestSQLiteStateAdapterRoundTrip(t *testing.T) {
	t.Parallel()

	store := openDaemonStateStore(t)
	adapter := newSQLiteStateAdapter(store)
	now := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)

	if err := adapter.PutTask(context.Background(), Task{
		Project:      "/repo",
		Issue:        "LAB-718",
		Status:       TaskStatusActive,
		PaneID:       "pane-1",
		ClonePath:    "/clone",
		AgentProfile: "codex",
		PRNumber:     17,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("PutTask() error = %v", err)
	}

	task, err := adapter.TaskByIssue(context.Background(), "/repo", "LAB-718")
	if err != nil {
		t.Fatalf("TaskByIssue() error = %v", err)
	}
	if got, want := task.PRNumber, 17; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	if _, err := adapter.TaskByIssue(context.Background(), "/repo", "missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("TaskByIssue() missing error = %v, want ErrTaskNotFound", err)
	}

	if err := adapter.PutWorker(context.Background(), Worker{
		Project:      "/repo",
		PaneID:       "pane-1",
		Issue:        "LAB-718",
		ClonePath:    "/clone",
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	if err := adapter.DeleteWorker(context.Background(), "/repo", "pane-1"); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}
	if err := adapter.DeleteWorker(context.Background(), "/repo", "pane-1"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("DeleteWorker() missing error = %v, want ErrWorkerNotFound", err)
	}

	if err := adapter.RecordEvent(context.Background(), Event{
		Time:    now,
		Type:    EventTaskAssigned,
		Project: "/repo",
		Issue:   "LAB-718",
		Message: "assigned",
	}); err != nil {
		t.Fatalf("RecordEvent() error = %v", err)
	}
	taskStatus, err := store.TaskStatus(context.Background(), "/repo", "LAB-718")
	if err != nil {
		t.Fatalf("store.TaskStatus() error = %v", err)
	}
	if got, want := len(taskStatus.Events), 1; got != want {
		t.Fatalf("len(taskStatus.Events) = %d, want %d", got, want)
	}
}

func TestExecCommandRunner(t *testing.T) {
	t.Parallel()

	runner := execCommandRunner{}
	out, err := runner.Run(context.Background(), t.TempDir(), "sh", "-c", "printf ok")
	if err != nil {
		t.Fatalf("Run() success error = %v", err)
	}
	if got, want := string(out), "ok"; got != want {
		t.Fatalf("Run() output = %q, want %q", got, want)
	}

	_, err = runner.Run(context.Background(), t.TempDir(), "sh", "-c", "echo boom; exit 1")
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("Run() failure error = %v, want stderr in message", err)
	}
}

func TestInternalPoolConfig(t *testing.T) {
	t.Parallel()

	cfg := internalPoolConfig{poolDir: "/tmp/pool", origin: "git@github.com:org/repo.git"}
	if got, want := cfg.PoolDir(), "/tmp/pool"; got != want {
		t.Fatalf("PoolDir() = %q, want %q", got, want)
	}
	if got, want := cfg.CloneOrigin(), "git@github.com:org/repo.git"; got != want {
		t.Fatalf("CloneOrigin() = %q, want %q", got, want)
	}
	if got := cfg.BaseBranch(); got != "" {
		t.Fatalf("BaseBranch() = %q, want empty string", got)
	}
}

func TestNewLinearIssueTrackerFromEnv(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	t.Run("missing key returns nil tracker", func(t *testing.T) {
		t.Setenv("LINEAR_API_KEY", "")

		tracker, err := newLinearIssueTrackerFromEnv()
		if err != nil {
			t.Fatalf("newLinearIssueTrackerFromEnv() error = %v", err)
		}
		if tracker != nil {
			t.Fatalf("newLinearIssueTrackerFromEnv() = %#v, want nil", tracker)
		}
	})

	t.Run("env key returns tracker", func(t *testing.T) {
		t.Setenv("LINEAR_API_KEY", "test-key")

		tracker, err := newLinearIssueTrackerFromEnv()
		if err != nil {
			t.Fatalf("newLinearIssueTrackerFromEnv() error = %v", err)
		}
		if tracker == nil {
			t.Fatal("newLinearIssueTrackerFromEnv() = nil, want tracker")
		}
	})
}

func TestAmuxCWDUsageChecker(t *testing.T) {
	t.Parallel()

	t.Run("returns active cwd values and skips blanks", func(t *testing.T) {
		t.Parallel()

		checker := amuxCWDUsageChecker{
			amux: staticPaneLister{panes: []Pane{
				{ID: "1", CWD: "/tmp/orca01"},
				{ID: "2", CWD: "   "},
				{ID: "3", CWD: "/tmp/orca03"},
			}},
		}

		got, err := checker.ActiveCWDs(context.Background())
		if err != nil {
			t.Fatalf("ActiveCWDs() error = %v", err)
		}
		want := []string{"/tmp/orca01", "/tmp/orca03"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("ActiveCWDs() = %#v, want %#v", got, want)
		}
	})

	t.Run("returns amux error", func(t *testing.T) {
		t.Parallel()

		checker := amuxCWDUsageChecker{
			amux: staticPaneLister{err: errors.New("list failed")},
		}

		_, err := checker.ActiveCWDs(context.Background())
		if err == nil || !strings.Contains(err.Error(), "list failed") {
			t.Fatalf("ActiveCWDs() error = %v, want list failed", err)
		}
	})
}

type staticPaneLister struct {
	panes []Pane
	err   error
}

func (s staticPaneLister) ListPanes(context.Context) ([]Pane, error) {
	if s.err != nil {
		return nil, s.err
	}
	return append([]Pane(nil), s.panes...), nil
}
func openDaemonStateStore(t *testing.T) *state.SQLiteStore {
	t.Helper()

	store, err := state.OpenSQLite(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("OpenSQLite() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return store
}
