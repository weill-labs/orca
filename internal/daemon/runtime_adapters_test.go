package daemon

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/config"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestConfigAdapterAgentProfile(t *testing.T) {
	t.Parallel()

	adapter := configAdapter{cfg: config.Config{
		Agents: map[string]config.AgentProfile{
			"codex": {
				StartCommand:      "codex",
				StuckTimeout:      5 * time.Minute,
				StuckTextPatterns: []string{"permission prompt"},
				NudgeCommand:      "y\n",
				MaxNudgeRetries:   3,
			},
		},
	}}

	profile, err := adapter.AgentProfile(context.Background(), "codex")
	if err != nil {
		t.Fatalf("AgentProfile() error = %v", err)
	}
	if got, want := profile.StartCommand, "codex --yolo"; got != want {
		t.Fatalf("profile.StartCommand = %q, want %q", got, want)
	}
	if got, want := profile.ResumeSequence, []string{"codex --yolo resume", "Enter", "."}; !reflect.DeepEqual(got, want) {
		t.Fatalf("profile.ResumeSequence = %#v, want %#v", got, want)
	}

	if _, err := adapter.AgentProfile(context.Background(), "missing"); err == nil {
		t.Fatal("AgentProfile() missing = nil error, want non-nil")
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

func TestExecCommandRunnerAndPoolConfigAdapter(t *testing.T) {
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

	cfg := config.Config{Pool: config.PoolConfig{Pattern: "/tmp/orca*"}}
	adapter := poolConfigAdapter{cfg: cfg}
	if got, want := adapter.PoolPattern(), "/tmp/orca*"; got != want {
		t.Fatalf("PoolPattern() = %q, want %q", got, want)
	}
	if got := adapter.BaseBranch(); got != "" {
		t.Fatalf("BaseBranch() = %q, want empty string", got)
	}
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
