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
		CallerPane:   "pane-13",
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
	if got, want := task.PaneName, "w-LAB-718"; got != want {
		t.Fatalf("task.PaneName = %q, want %q", got, want)
	}
	if got, want := task.PRNumber, 17; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	if got, want := task.CallerPane, "pane-13"; got != want {
		t.Fatalf("task.CallerPane = %q, want %q", got, want)
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

func TestSQLiteStateAdapterNonTerminalTasksAndWorkerByPane(t *testing.T) {
	t.Parallel()

	store := openDaemonStateStore(t)
	adapter := newSQLiteStateAdapter(store)
	now := time.Date(2026, 4, 4, 10, 0, 0, 0, time.UTC)

	for _, task := range []Task{
		{
			Project:      "/repo",
			Issue:        "LAB-740",
			Status:       TaskStatusStarting,
			PaneID:       "pane-1",
			ClonePath:    "/clones/clone-01",
			AgentProfile: "codex",
			CreatedAt:    now,
			UpdatedAt:    now,
		},
		{
			Project:      "/repo",
			Issue:        "LAB-741",
			Status:       TaskStatusActive,
			PaneID:       "pane-2",
			ClonePath:    "/clones/clone-02",
			AgentProfile: "codex",
			PRNumber:     42,
			CreatedAt:    now,
			UpdatedAt:    now.Add(time.Minute),
		},
		{
			Project:      "/repo",
			Issue:        "LAB-742",
			Status:       TaskStatusDone,
			PaneID:       "pane-3",
			ClonePath:    "/clones/clone-03",
			AgentProfile: "codex",
			CreatedAt:    now,
			UpdatedAt:    now.Add(2 * time.Minute),
		},
	} {
		if err := adapter.PutTask(context.Background(), task); err != nil {
			t.Fatalf("PutTask(%s) error = %v", task.Issue, err)
		}
	}

	if err := adapter.PutWorker(context.Background(), Worker{
		Project:               "/repo",
		PaneID:                "pane-2",
		Issue:                 "LAB-741",
		ClonePath:             "/clones/clone-02",
		AgentProfile:          "codex",
		Health:                WorkerHealthEscalated,
		LastReviewCount:       2,
		LastIssueCommentCount: 4,
		ReviewNudgeCount:      3,
		LastCIState:           "fail",
		CINudgeCount:          2,
		CIFailurePollCount:    1,
		CIEscalated:           true,
		LastMergeableState:    "CONFLICTING",
		NudgeCount:            3,
		LastCapture:           "permission prompt",
		LastActivityAt:        now,
		UpdatedAt:             now.Add(time.Minute),
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	tasks, err := adapter.NonTerminalTasks(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("NonTerminalTasks() error = %v", err)
	}
	if got, want := len(tasks), 2; got != want {
		t.Fatalf("len(tasks) = %d, want %d", got, want)
	}
	if got, want := tasks[0].Issue, "LAB-741"; got != want {
		t.Fatalf("tasks[0].Issue = %q, want %q", got, want)
	}
	if got, want := tasks[0].PRNumber, 42; got != want {
		t.Fatalf("tasks[0].PRNumber = %d, want %d", got, want)
	}
	if got, want := tasks[0].PaneName, "w-LAB-741"; got != want {
		t.Fatalf("tasks[0].PaneName = %q, want %q", got, want)
	}
	if got, want := tasks[0].CloneName, "clone-02"; got != want {
		t.Fatalf("tasks[0].CloneName = %q, want %q", got, want)
	}
	if got, want := tasks[1].Issue, "LAB-740"; got != want {
		t.Fatalf("tasks[1].Issue = %q, want %q", got, want)
	}
	if got, want := tasks[1].PaneName, "w-LAB-740"; got != want {
		t.Fatalf("tasks[1].PaneName = %q, want %q", got, want)
	}

	worker, err := adapter.WorkerByPane(context.Background(), "/repo", "pane-2")
	if err != nil {
		t.Fatalf("WorkerByPane() error = %v", err)
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got, want := worker.PaneName, "w-LAB-741"; got != want {
		t.Fatalf("worker.PaneName = %q, want %q", got, want)
	}
	if got, want := worker.LastReviewCount, 2; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
	}
	if got, want := worker.LastIssueCommentCount, 4; got != want {
		t.Fatalf("worker.LastIssueCommentCount = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 3; got != want {
		t.Fatalf("worker.ReviewNudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.LastCIState, "fail"; got != want {
		t.Fatalf("worker.LastCIState = %q, want %q", got, want)
	}
	if got, want := worker.CINudgeCount, 2; got != want {
		t.Fatalf("worker.CINudgeCount = %d, want %d", got, want)
	}
	if got, want := worker.CIFailurePollCount, 1; got != want {
		t.Fatalf("worker.CIFailurePollCount = %d, want %d", got, want)
	}
	if got, want := worker.CIEscalated, true; got != want {
		t.Fatalf("worker.CIEscalated = %t, want %t", got, want)
	}
	if got, want := worker.LastMergeableState, "CONFLICTING"; got != want {
		t.Fatalf("worker.LastMergeableState = %q, want %q", got, want)
	}
	if got, want := worker.NudgeCount, 3; got != want {
		t.Fatalf("worker.NudgeCount = %d, want %d", got, want)
	}
	if _, err := adapter.WorkerByPane(context.Background(), "/repo", "missing"); !errors.Is(err, ErrWorkerNotFound) {
		t.Fatalf("WorkerByPane() missing error = %v, want ErrWorkerNotFound", err)
	}
}

func TestSQLiteStateAdapterMergeQueueRoundTrip(t *testing.T) {
	t.Parallel()

	store := openDaemonStateStore(t)
	adapter := newSQLiteStateAdapter(store)
	now := time.Date(2026, 4, 6, 10, 0, 0, 0, time.UTC)

	position, err := adapter.EnqueueMerge(context.Background(), MergeQueueEntry{
		Project:   "/repo",
		Issue:     "LAB-750",
		PRNumber:  42,
		Status:    MergeQueueStatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("EnqueueMerge(42) error = %v", err)
	}
	if got, want := position, 1; got != want {
		t.Fatalf("position for PR 42 = %d, want %d", got, want)
	}

	position, err = adapter.EnqueueMerge(context.Background(), MergeQueueEntry{
		Project:   "/repo",
		Issue:     "LAB-751",
		PRNumber:  43,
		Status:    MergeQueueStatusAwaitingChecks,
		CreatedAt: now.Add(time.Minute),
		UpdatedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("EnqueueMerge(43) error = %v", err)
	}
	if got, want := position, 2; got != want {
		t.Fatalf("position for PR 43 = %d, want %d", got, want)
	}

	entry, err := adapter.MergeEntry(context.Background(), "/repo", 42)
	if err != nil {
		t.Fatalf("MergeEntry() error = %v", err)
	}
	if entry == nil {
		t.Fatal("MergeEntry() = nil, want entry")
	}
	if got, want := entry.Status, MergeQueueStatusQueued; got != want {
		t.Fatalf("entry.Status = %q, want %q", got, want)
	}

	entries, err := adapter.MergeEntries(context.Background(), "/repo")
	if err != nil {
		t.Fatalf("MergeEntries() error = %v", err)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("len(entries) = %d, want %d", got, want)
	}
	if got, want := entries[0].PRNumber, 42; got != want {
		t.Fatalf("entries[0].PRNumber = %d, want %d", got, want)
	}
	if got, want := entries[1].PRNumber, 43; got != want {
		t.Fatalf("entries[1].PRNumber = %d, want %d", got, want)
	}

	entry.Status = MergeQueueStatusCheckingCI
	entry.UpdatedAt = now.Add(2 * time.Minute)
	if err := adapter.UpdateMergeEntry(context.Background(), *entry); err != nil {
		t.Fatalf("UpdateMergeEntry() error = %v", err)
	}

	updatedEntry, err := adapter.MergeEntry(context.Background(), "/repo", 42)
	if err != nil {
		t.Fatalf("MergeEntry() after update error = %v", err)
	}
	if updatedEntry == nil {
		t.Fatal("MergeEntry() after update = nil, want entry")
	}
	if got, want := updatedEntry.Status, MergeQueueStatusCheckingCI; got != want {
		t.Fatalf("updatedEntry.Status = %q, want %q", got, want)
	}

	if err := adapter.DeleteMergeEntry(context.Background(), "/repo", 42); err != nil {
		t.Fatalf("DeleteMergeEntry() error = %v", err)
	}
	deletedEntry, err := adapter.MergeEntry(context.Background(), "/repo", 42)
	if err != nil {
		t.Fatalf("MergeEntry() after delete error = %v", err)
	}
	if deletedEntry != nil {
		t.Fatalf("MergeEntry() after delete = %#v, want nil", deletedEntry)
	}

	if err := adapter.DeleteMergeEntry(context.Background(), "/repo", 42); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("DeleteMergeEntry() missing error = %v, want ErrTaskNotFound", err)
	}
	if err := adapter.UpdateMergeEntry(context.Background(), MergeQueueEntry{
		Project:  "/repo",
		Issue:    "LAB-752",
		PRNumber: 99,
		Status:   MergeQueueStatusQueued,
	}); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("UpdateMergeEntry() missing error = %v, want ErrTaskNotFound", err)
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
