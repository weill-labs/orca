package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPRMergePollingReportsCompletionFailureWhenPostmortemNotRecorded(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-690", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-690", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task completion failure event", func() bool {
		return deps.events.countType(EventTaskCompletionFailed) == 1
	})

	task, ok := deps.state.task("LAB-690")
	if !ok {
		t.Fatal("task missing after merge completion failure")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); !ok {
		t.Fatal("worker removed after merge completion failure, want worker to remain active")
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "LAB-690", State: IssueStateInProgress},
		{Issue: "LAB-690", State: IssueStateDone},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if got, want := deps.amux.waitIdleCalls, []waitIdleCall{
		{PaneID: "pane-1", Timeout: 30 * time.Second},
		{PaneID: "pane-1", Timeout: 2 * time.Minute},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("wait idle calls = %#v, want %#v", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{
		"Implement daemon core\n",
		"$postmortem\n",
	})
	deps.events.requireTypes(t, EventDaemonStarted, EventTaskAssigned, EventPRDetected, EventPRMerged, EventTaskCompletionFailed)

	deps.events.mu.Lock()
	defer deps.events.mu.Unlock()
	var failure Event
	for _, event := range deps.events.events {
		if event.Type == EventTaskCompletionFailed {
			failure = event
			break
		}
	}
	if !strings.Contains(failure.Message, "postmortem not recorded") {
		t.Fatalf("failure.Message = %q, want to contain %q", failure.Message, "postmortem not recorded")
	}
}

func TestEnsureFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		command string
		flag    string
		want    string
	}{
		{name: "empty command", command: "", flag: "--yolo", want: ""},
		{name: "empty flag", command: "codex", flag: "", want: "codex"},
		{name: "appends missing flag", command: "codex", flag: "--yolo", want: "codex --yolo"},
		{name: "preserves existing flag", command: "codex --yolo --profile fast", flag: "--yolo", want: "codex --yolo --profile fast"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := ensureFlag(tt.command, tt.flag); got != tt.want {
				t.Fatalf("ensureFlag(%q, %q) = %q, want %q", tt.command, tt.flag, got, tt.want)
			}
		})
	}
}

func TestEnsurePostmortemErrorPathsAndCache(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	startedAt := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
	newActive := func() *assignment {
		return &assignment{
			profile:   AgentProfile{Name: "codex", PostmortemEnabled: true},
			task:      Task{Issue: "LAB-730", Branch: "LAB-730"},
			pane:      Pane{ID: "pane-1"},
			startedAt: startedAt,
		}
	}

	t.Run("request failure", func(t *testing.T) {
		deps := newTestDeps(t)
		deps.amux.sendKeysErr = errors.New("send failed")
		d := deps.newDaemon(t)

		err := d.ensurePostmortem(context.Background(), newActive())
		if err == nil || !strings.Contains(err.Error(), "request postmortem") {
			t.Fatalf("ensurePostmortem() error = %v, want request postmortem failure", err)
		}
	})

	t.Run("wait failure", func(t *testing.T) {
		deps := newTestDeps(t)
		deps.amux.waitIdleErr = errors.New("idle timeout")
		d := deps.newDaemon(t)

		err := d.ensurePostmortem(context.Background(), newActive())
		if err == nil || !strings.Contains(err.Error(), "wait for postmortem") {
			t.Fatalf("ensurePostmortem() error = %v, want wait for postmortem failure", err)
		}
	})

	t.Run("cached postmortem skips send", func(t *testing.T) {
		deps := newTestDeps(t)
		d := deps.newDaemon(t)
		d.postmortems["pane-1"] = startedAt

		if err := d.ensurePostmortem(context.Background(), newActive()); err != nil {
			t.Fatalf("ensurePostmortem() error = %v", err)
		}
		if got := deps.amux.sentKeys["pane-1"]; len(got) != 0 {
			t.Fatalf("sent keys = %#v, want none", got)
		}
		if got := deps.amux.waitIdleCalls; len(got) != 0 {
			t.Fatalf("waitIdle calls = %#v, want none", got)
		}
	})
}

func TestFindPostmortemHelpers(t *testing.T) {
	writeFixture := func(t *testing.T, dir, name, issue, branch string, modTime time.Time) {
		t.Helper()
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", dir, err)
		}
		path := filepath.Join(dir, name)
		content := strings.Join([]string{
			"### Metadata",
			"- **Repo**: orca",
			"- **Branch**: " + branch,
			"- **Issues**: " + issue,
			"",
		}, "\n")
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatalf("WriteFile(%q) error = %v", path, err)
		}
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatalf("Chtimes(%q) error = %v", path, err)
		}
	}

	t.Run("findPostmortemInDir filters by session and modtime", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		startedAt := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
		active := &assignment{
			startedAt: startedAt,
			task:      Task{Issue: "LAB-731", Branch: "LAB-731"},
		}

		writeFixture(t, dir, "stale.md", "LAB-731", "LAB-731", startedAt.Add(-time.Minute))
		writeFixture(t, dir, "wrong.md", "LAB-999", "LAB-999", startedAt.Add(time.Minute))
		writeFixture(t, dir, "match.md", "LAB-731", "LAB-731", startedAt.Add(2*time.Minute))

		recordedAt, ok, err := findPostmortemInDir(dir, active)
		if err != nil {
			t.Fatalf("findPostmortemInDir() error = %v", err)
		}
		if !ok {
			t.Fatal("findPostmortemInDir() ok = false, want true")
		}
		if got, want := recordedAt, startedAt.Add(2*time.Minute); !got.Equal(want) {
			t.Fatalf("recordedAt = %v, want %v", got, want)
		}
	})

	t.Run("findPostmortemInDir missing dir", func(t *testing.T) {
		t.Parallel()

		active := &assignment{startedAt: time.Now(), task: Task{Issue: "LAB-731", Branch: "LAB-731"}}
		recordedAt, ok, err := findPostmortemInDir(filepath.Join(t.TempDir(), "missing"), active)
		if err != nil {
			t.Fatalf("findPostmortemInDir() error = %v", err)
		}
		if ok {
			t.Fatal("findPostmortemInDir() ok = true, want false")
		}
		if !recordedAt.IsZero() {
			t.Fatalf("recordedAt = %v, want zero", recordedAt)
		}
	})

	t.Run("findPostmortemInDir unreadable target", func(t *testing.T) {
		t.Parallel()

		dir := t.TempDir()
		path := filepath.Join(dir, "broken.md")
		if err := os.Symlink(filepath.Join(dir, "missing-target"), path); err != nil {
			t.Fatalf("Symlink(%q) error = %v", path, err)
		}

		active := &assignment{
			startedAt: time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC),
			task:      Task{Issue: "LAB-731", Branch: "LAB-731"},
		}
		if _, _, err := findPostmortemInDir(dir, active); err == nil || !strings.Contains(err.Error(), "read postmortem") {
			t.Fatalf("findPostmortemInDir() error = %v, want read postmortem failure", err)
		}
	})

	t.Run("findPostmortem chooses newest known directory entry", func(t *testing.T) {
		home := t.TempDir()
		t.Setenv("HOME", home)

		startedAt := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)
		active := &assignment{
			startedAt: startedAt,
			task:      Task{Issue: "LAB-732", Branch: "LAB-732"},
		}

		writeFixture(t, filepath.Join(home, "sync", "postmortems"), "older.md", "LAB-732", "LAB-732", startedAt.Add(time.Minute))
		writeFixture(t, filepath.Join(home, ".local", "share", "postmortems"), "newer.md", "LAB-732", "LAB-732", startedAt.Add(2*time.Minute))

		recordedAt, ok, err := findPostmortem(active)
		if err != nil {
			t.Fatalf("findPostmortem() error = %v", err)
		}
		if !ok {
			t.Fatal("findPostmortem() ok = false, want true")
		}
		if got, want := recordedAt, startedAt.Add(2*time.Minute); !got.Equal(want) {
			t.Fatalf("recordedAt = %v, want %v", got, want)
		}
	})
}

func TestMatchesPostmortemSession(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
		issue   string
		branch  string
		want    bool
	}{
		{name: "matches issue and branch case-insensitively", content: "issue lab-issue-733 branch feature/task-733", issue: "LAB-ISSUE-733", branch: "feature/TASK-733", want: true},
		{name: "rejects missing issue", content: "branch feature/task-733", issue: "LAB-ISSUE-733", branch: "feature/TASK-733", want: false},
		{name: "rejects missing branch", content: "issue lab-issue-733", issue: "LAB-ISSUE-733", branch: "feature/TASK-733", want: false},
		{name: "branch-only match succeeds", content: "branch feature/task-733", issue: "", branch: "feature/TASK-733", want: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := matchesPostmortemSession(tt.content, tt.issue, tt.branch); got != tt.want {
				t.Fatalf("matchesPostmortemSession(%q, %q, %q) = %v, want %v", tt.content, tt.issue, tt.branch, got, tt.want)
			}
		})
	}
}

func TestCleanupCloneAndReleaseDefaultsCloneMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	clone := Clone{Name: deps.pool.clone.Name, Path: deps.pool.clone.Path}

	if err := d.cleanupCloneAndRelease(context.Background(), clone, "LAB-734"); err != nil {
		t.Fatalf("cleanupCloneAndRelease() error = %v", err)
	}

	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "LAB-734",
		AssignedTask:  "LAB-734",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
}
