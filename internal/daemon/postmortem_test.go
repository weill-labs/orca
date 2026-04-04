package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestNewUsesDefaultPostmortemConfig(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)

	deps := newTestDeps(t)
	daemon, err := New(Options{
		Project:          "/tmp/project",
		Session:          "test-session",
		PIDPath:          deps.pidPath,
		Config:           deps.config,
		State:            deps.state,
		Pool:             deps.pool,
		Amux:             deps.amux,
		IssueTracker:     deps.issueTracker,
		Commands:         deps.commands,
		Events:           deps.events,
		Now:              deps.clock.Now,
		NewTicker:        deps.tickers.NewTicker,
		CaptureInterval:  5 * time.Second,
		PollInterval:     30 * time.Second,
		MergeGracePeriod: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if got, want := daemon.postmortemDir, filepath.Join(homeDir, ".local", "share", "postmortems"); got != want {
		t.Fatalf("daemon.postmortemDir = %q, want %q", got, want)
	}
	if got, want := daemon.postmortemWindow, 10*time.Minute; got != want {
		t.Fatalf("daemon.postmortemWindow = %s, want %s", got, want)
	}
	if got, want := daemon.postmortemTimeout, 2*time.Minute; got != want {
		t.Fatalf("daemon.postmortemTimeout = %s, want %s", got, want)
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

func TestFindRecentPostmortemSkipsBrokenEntriesAndReturnsNewestMatch(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 4, 3, 18, 30, 0, 0, time.UTC)

	if err := os.Symlink("does-not-exist", filepath.Join(dir, "broken")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}

	oldMatch := writeRecentPostmortem(t, dir, now.Add(-11*time.Minute), filepath.Join(dir, "clone-01"), "LAB-689", "worker-old")
	newMatch := writeRecentPostmortem(t, dir, now, filepath.Join(dir, "clone-01"), "LAB-689", "worker-new")
	writeRecentPostmortem(t, dir, now, filepath.Join(dir, "clone-02"), "LAB-700", "worker-other")

	got, err := findRecentPostmortem(dir, []string{"clone-01"}, now, 10*time.Minute)
	if err != nil {
		t.Fatalf("findRecentPostmortem() error = %v", err)
	}
	if got != newMatch {
		t.Fatalf("findRecentPostmortem() = %q, want %q", got, newMatch)
	}
	if got == oldMatch {
		t.Fatalf("findRecentPostmortem() returned stale match %q", got)
	}
}

func TestPostmortemStatusDifferentiatesFailuresSkipsAndTriggers(t *testing.T) {
	now := time.Date(2026, 4, 3, 18, 30, 0, 0, time.UTC)

	testCases := []struct {
		name             string
		setup            func(t *testing.T, deps *testDeps, daemon *Daemon, active ActiveAssignment)
		active           func(deps *testDeps) ActiveAssignment
		allowTrigger     bool
		wantStatus       string
		wantErr          bool
		wantMessagePart  string
		wantPostmortemCt int
	}{
		{
			name: "skips without session metadata",
			active: func(_ *testDeps) ActiveAssignment {
				return ActiveAssignment{Task: Task{AgentProfile: "codex"}}
			},
			allowTrigger:    true,
			wantStatus:      "skipped",
			wantMessagePart: "no worker session metadata",
		},
		{
			name: "skips when profile disables postmortem",
			setup: func(_ *testing.T, deps *testDeps, _ *Daemon, _ ActiveAssignment) {
				profile := deps.config.profiles["codex"]
				profile.PostmortemEnabled = false
				deps.config.profiles["codex"] = profile
			},
			active:          func(deps *testDeps) ActiveAssignment { return newPostmortemAssignment(deps) },
			allowTrigger:    true,
			wantStatus:      "skipped",
			wantMessagePart: "postmortem disabled",
		},
		{
			name: "fails when initial scan errors",
			setup: func(t *testing.T, _ *testDeps, daemon *Daemon, _ ActiveAssignment) {
				path := filepath.Join(t.TempDir(), "postmortems-file")
				if err := os.WriteFile(path, []byte("not a directory"), 0o644); err != nil {
					t.Fatalf("WriteFile(%q) error = %v", path, err)
				}
				daemon.postmortemDir = path
			},
			active:           func(deps *testDeps) ActiveAssignment { return newPostmortemAssignment(deps) },
			allowTrigger:     true,
			wantStatus:       "failed",
			wantErr:          true,
			wantMessagePart:  "check failed",
			wantPostmortemCt: 0,
		},
		{
			name:             "skips when trigger already disallowed",
			active:           func(deps *testDeps) ActiveAssignment { return newPostmortemAssignment(deps) },
			allowTrigger:     false,
			wantStatus:       "skipped",
			wantMessagePart:  "cleanup already had an error",
			wantPostmortemCt: 0,
		},
		{
			name: "skips when pane id missing",
			active: func(deps *testDeps) ActiveAssignment {
				active := newPostmortemAssignment(deps)
				active.Task.PaneID = ""
				active.Worker.PaneID = ""
				return active
			},
			allowTrigger:     true,
			wantStatus:       "skipped",
			wantMessagePart:  "worker pane missing",
			wantPostmortemCt: 0,
		},
		{
			name: "fails when trigger send keys fails",
			setup: func(_ *testing.T, deps *testDeps, _ *Daemon, _ ActiveAssignment) {
				deps.amux.sendKeysErr = errors.New("send failed")
			},
			active:           func(deps *testDeps) ActiveAssignment { return newPostmortemAssignment(deps) },
			allowTrigger:     true,
			wantStatus:       "failed",
			wantErr:          true,
			wantMessagePart:  "trigger failed",
			wantPostmortemCt: 0,
		},
		{
			name: "triggered when file appears after prompt",
			setup: func(t *testing.T, deps *testDeps, daemon *Daemon, active ActiveAssignment) {
				deps.amux.waitIdleHook = func(_ string, _ time.Duration) {
					writeRecentPostmortem(t, daemon.postmortemDir, now, active.Task.ClonePath, active.Task.Issue, active.Task.PaneName)
				}
			},
			active:           func(deps *testDeps) ActiveAssignment { return newPostmortemAssignment(deps) },
			allowTrigger:     true,
			wantStatus:       "triggered",
			wantMessagePart:  "worker-1",
			wantPostmortemCt: 1,
		},
		{
			name: "triggered when wait idle errors but file appears",
			setup: func(t *testing.T, deps *testDeps, daemon *Daemon, active ActiveAssignment) {
				deps.amux.waitIdleErr = errors.New("wait idle failed")
				deps.amux.waitIdleHook = func(_ string, _ time.Duration) {
					writeRecentPostmortem(t, daemon.postmortemDir, now, active.Task.ClonePath, active.Task.Issue, active.Task.PaneName)
				}
			},
			active:           func(deps *testDeps) ActiveAssignment { return newPostmortemAssignment(deps) },
			allowTrigger:     true,
			wantStatus:       "triggered",
			wantMessagePart:  "wait idle: wait idle failed",
			wantPostmortemCt: 1,
		},
		{
			name: "fails when recheck errors after prompt",
			setup: func(t *testing.T, deps *testDeps, daemon *Daemon, _ ActiveAssignment) {
				deps.amux.waitIdleHook = func(_ string, _ time.Duration) {
					path := filepath.Join(t.TempDir(), "postmortems-file")
					if err := os.WriteFile(path, []byte("not a directory"), 0o644); err != nil {
						t.Fatalf("WriteFile(%q) error = %v", path, err)
					}
					daemon.postmortemDir = path
				}
			},
			active:           func(deps *testDeps) ActiveAssignment { return newPostmortemAssignment(deps) },
			allowTrigger:     true,
			wantStatus:       "failed",
			wantErr:          true,
			wantMessagePart:  "recheck failed",
			wantPostmortemCt: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			deps := newTestDeps(t)
			deps.clock.now = now
			daemon := deps.newDaemon(t)
			active := tc.active(deps)
			if tc.setup != nil {
				tc.setup(t, deps, daemon, active)
			}

			status, message, err := daemon.postmortemStatus(context.Background(), active, tc.allowTrigger)
			if got, want := status, tc.wantStatus; got != want {
				t.Fatalf("postmortemStatus() status = %q, want %q", got, want)
			}
			if tc.wantErr && err == nil {
				t.Fatal("postmortemStatus() error = nil, want non-nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("postmortemStatus() error = %v, want nil", err)
			}
			if !strings.Contains(message, tc.wantMessagePart) {
				t.Fatalf("postmortemStatus() message = %q, want substring %q", message, tc.wantMessagePart)
			}
			if got, want := deps.amux.countKey(active.Task.PaneID, "$postmortem\n"), tc.wantPostmortemCt; got != want {
				t.Fatalf("postmortem prompt count = %d, want %d", got, want)
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

func TestFinishAssignmentMergedCleanupSendsWrapUpThenEnter(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	profile := deps.config.profiles["codex"]
	profile.PostmortemEnabled = false
	deps.config.profiles["codex"] = profile

	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	deps.state.putTaskForTest(active.Task)
	if err := deps.state.PutWorker(context.Background(), active.Worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	var operations []string
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		operations = append(operations, "send:"+strings.Join(keys, "|"))
	}
	deps.amux.waitIdleHook = func(_ string, timeout time.Duration) {
		operations = append(operations, "wait:"+timeout.String())
	}

	if err := d.finishAssignment(context.Background(), active, TaskStatusDone, EventTaskCompleted, true); err != nil {
		t.Fatalf("finishAssignment() error = %v", err)
	}

	if got, want := operations, []string{
		"send:PR merged, wrap up.",
		"wait:2m0s",
		"send:Enter",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
}

func TestFinishAssignmentMergedCleanupSendsEnterBeforePostmortem(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	active := newPostmortemAssignment(deps)
	active.Task.Status = TaskStatusActive
	deps.state.putTaskForTest(active.Task)
	if err := deps.state.PutWorker(context.Background(), active.Worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	var operations []string
	deps.amux.sendKeysHook = func(_ string, keys []string) {
		operations = append(operations, "send:"+strings.Join(keys, "|"))
		if len(keys) > 0 && keys[0] == postmortemCommand {
			writeRecentPostmortem(t, d.postmortemDir, deps.clock.Now(), active.Task.ClonePath, active.Task.Issue, active.Task.PaneName)
		}
	}
	deps.amux.waitIdleHook = func(_ string, timeout time.Duration) {
		operations = append(operations, "wait:"+timeout.String())
	}

	if err := d.finishAssignment(context.Background(), active, TaskStatusDone, EventTaskCompleted, true); err != nil {
		t.Fatalf("finishAssignment() error = %v", err)
	}

	if got, want := operations, []string{
		"send:PR merged, wrap up.",
		"wait:2m0s",
		"send:Enter",
		"send:$postmortem|Enter",
		"wait:2m0s",
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("operations = %#v, want %#v", got, want)
	}
}

func writeRecentPostmortem(t *testing.T, dir string, now time.Time, clonePath, issue, workerName string) string {
	t.Helper()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", dir, err)
	}

	filename := now.Format("2006-01-02-150405") + "-" + workerName + ".md"
	path := filepath.Join(dir, filename)
	content := strings.Join([]string{
		"### Metadata",
		"- **Repo**: orca",
		"- **Clone**: " + clonePath,
		"- **Branch**: " + issue,
		"- **Issues**: " + issue,
		"- **Pane**: " + workerName,
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
	if err := os.Chtimes(path, now, now); err != nil {
		t.Fatalf("Chtimes(%q) error = %v", path, err)
	}
	return path
}

func newPostmortemAssignment(deps *testDeps) ActiveAssignment {
	return ActiveAssignment{
		Task: Task{
			Project:      "/tmp/project",
			Issue:        "LAB-689",
			Branch:       "LAB-689",
			PaneID:       deps.amux.spawnPane.ID,
			PaneName:     deps.amux.spawnPane.Name,
			CloneName:    deps.pool.clone.Name,
			ClonePath:    deps.pool.clone.Path,
			AgentProfile: deps.config.profiles["codex"].Name,
			CreatedAt:    deps.clock.Now(),
			UpdatedAt:    deps.clock.Now(),
		},
		Worker: Worker{
			Project:      "/tmp/project",
			PaneID:       deps.amux.spawnPane.ID,
			PaneName:     deps.amux.spawnPane.Name,
			ClonePath:    deps.pool.clone.Path,
			AgentProfile: deps.config.profiles["codex"].Name,
		},
	}
}

func TestFailStuckWorkerIncludesDiagnosticsErrorInEventMessage(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-710", "Capture diagnostics before kill", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-710")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		t.Fatalf("profileForTask() error = %v", err)
	}
	deps.amux.captureHistoryErrors("pane-1", []error{errors.New("history unavailable")})

	d.failStuckWorker(ctx, active, profile, "idle timeout exceeded")

	waitFor(t, "failed task", func() bool {
		task, ok := deps.state.task("LAB-710")
		return ok && task.Status == TaskStatusFailed
	})

	event, ok := deps.events.lastEventOfType(EventTaskFailed)
	if !ok {
		t.Fatalf("lastEventOfType(%q) = false, want true", EventTaskFailed)
	}
	if !strings.Contains(event.Message, "diagnostics error: capture pane history: history unavailable") {
		t.Fatalf("event.Message = %q, want diagnostics error context", event.Message)
	}
}

func TestFinishAssignmentDefaultsFailedEventMessageWhenEmpty(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-710", "Capture diagnostics before kill", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-710")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}

	if err := d.finishAssignmentWithMessage(ctx, active, TaskStatusFailed, EventTaskFailed, false, ""); err != nil {
		t.Fatalf("finishAssignmentWithMessage() error = %v", err)
	}

	event, ok := deps.events.lastEventOfType(EventTaskFailed)
	if !ok {
		t.Fatalf("lastEventOfType(%q) = false, want true", EventTaskFailed)
	}
	if got := event.Message; got != "task failed" {
		t.Fatalf("event.Message = %q, want %q", got, "task failed")
	}
}

func TestCaptureStuckWorkerDiagnosticsErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		historySequence   []PaneCapture
		historyErrors     []error
		signalErr         error
		wantErrSubstring  string
		wantLogPathSuffix string
	}{
		{
			name:             "initial capture failure",
			historyErrors:    []error{errors.New("history unavailable")},
			wantErrSubstring: "capture pane history: history unavailable",
		},
		{
			name: "sigquit failure still returns log path",
			historySequence: []PaneCapture{{
				Content:        []string{"stuck output"},
				CWD:            "/tmp/clone-01",
				CurrentCommand: "codex",
				ChildPIDs:      []int{4242},
			}},
			signalErr:         errors.New("no such process"),
			wantErrSubstring:  "send SIGQUIT to 4242: no such process",
			wantLogPathSuffix: "goroutine-dump-LAB-710.log",
		},
		{
			name: "post sigquit capture failure still returns log path",
			historySequence: []PaneCapture{{
				Content:        []string{"stuck output"},
				CWD:            "/tmp/clone-01",
				CurrentCommand: "codex",
				ChildPIDs:      []int{4242},
			}},
			historyErrors:     []error{nil, errors.New("capture failed after signal")},
			wantErrSubstring:  "capture pane history after SIGQUIT: capture failed after signal",
			wantLogPathSuffix: "goroutine-dump-LAB-710.log",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.config.profiles["codex"] = AgentProfile{
				Name:            "codex",
				StartCommand:    "codex --yolo",
				GoBased:         true,
				StuckTimeout:    time.Hour,
				NudgeCommand:    "Enter",
				MaxNudgeRetries: 0,
			}
			deps.amux.captureHistorySequence("pane-1", tt.historySequence)
			deps.amux.captureHistoryErrors("pane-1", tt.historyErrors)

			d := deps.newDaemon(t)
			if tt.signalErr != nil {
				d.signalProcess = func(int, syscall.Signal) error {
					return tt.signalErr
				}
			}

			active, profile := testActiveAssignment(t, deps, d, "codex")
			logPath, err := d.captureStuckWorkerDiagnostics(context.Background(), active, profile, "idle timeout exceeded")
			if tt.wantErrSubstring != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErrSubstring) {
					t.Fatalf("captureStuckWorkerDiagnostics() error = %v, want substring %q", err, tt.wantErrSubstring)
				}
			} else if err != nil {
				t.Fatalf("captureStuckWorkerDiagnostics() error = %v", err)
			}
			if tt.wantLogPathSuffix == "" {
				if logPath != "" {
					t.Fatalf("logPath = %q, want empty", logPath)
				}
				return
			}
			if !strings.HasSuffix(logPath, tt.wantLogPathSuffix) {
				t.Fatalf("logPath = %q, want suffix %q", logPath, tt.wantLogPathSuffix)
			}
		})
	}
}

func TestWriteStuckWorkerPostmortemErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		postmortemDir    func(t *testing.T) string
		issue            string
		wantErrSubstring string
	}{
		{
			name: "mkdir failure",
			postmortemDir: func(t *testing.T) string {
				path := filepath.Join(t.TempDir(), "postmortems-file")
				if err := os.WriteFile(path, []byte("not a dir"), 0o644); err != nil {
					t.Fatalf("WriteFile() error = %v", err)
				}
				return path
			},
			issue:            "LAB-710",
			wantErrSubstring: "create postmortem directory",
		},
		{
			name: "write failure",
			postmortemDir: func(t *testing.T) string {
				return t.TempDir()
			},
			issue:            "LAB-710/bad",
			wantErrSubstring: "write postmortem log",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			d := deps.newDaemon(t)
			d.postmortemDir = tt.postmortemDir(t)

			active := ActiveAssignment{
				Task: Task{
					Issue:     tt.issue,
					PaneID:    "pane-1",
					PaneName:  "worker-1",
					ClonePath: deps.pool.clone.Path,
				},
			}

			_, err := d.writeStuckWorkerPostmortem(active, AgentProfile{Name: "codex"}, "idle timeout exceeded", PaneCapture{Content: []string{"stuck output"}}, 0)
			if err == nil || !strings.Contains(err.Error(), tt.wantErrSubstring) {
				t.Fatalf("writeStuckWorkerPostmortem() error = %v, want substring %q", err, tt.wantErrSubstring)
			}
		})
	}
}

func TestMergePaneCaptureFillsMissingMetadata(t *testing.T) {
	t.Parallel()

	got := mergePaneCapture(
		PaneCapture{Content: []string{"after"}},
		PaneCapture{
			Content:        []string{"before"},
			CWD:            "/tmp/clone-01",
			CurrentCommand: "codex",
			ChildPIDs:      []int{4242},
		},
	)

	if got.CWD != "/tmp/clone-01" {
		t.Fatalf("CWD = %q, want %q", got.CWD, "/tmp/clone-01")
	}
	if got.CurrentCommand != "codex" {
		t.Fatalf("CurrentCommand = %q, want %q", got.CurrentCommand, "codex")
	}
	if !reflect.DeepEqual(got.ChildPIDs, []int{4242}) {
		t.Fatalf("ChildPIDs = %#v, want %#v", got.ChildPIDs, []int{4242})
	}
}

func TestMergePaneCaptureContent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		primary  []string
		fallback []string
		want     []string
	}{
		{
			name:     "primary empty",
			primary:  nil,
			fallback: []string{"before"},
			want:     []string{"before"},
		},
		{
			name:     "fallback empty",
			primary:  []string{"after"},
			fallback: nil,
			want:     []string{"after"},
		},
		{
			name:     "primary contains fallback",
			primary:  []string{"before", "after"},
			fallback: []string{"before"},
			want:     []string{"before", "after"},
		},
		{
			name:     "fallback contains primary",
			primary:  []string{"after"},
			fallback: []string{"before", "after"},
			want:     []string{"before", "after"},
		},
		{
			name:     "disjoint content",
			primary:  []string{"goroutine dump"},
			fallback: []string{"stuck output"},
			want:     []string{"stuck output", "", "goroutine dump"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := mergePaneCaptureContent(tt.primary, tt.fallback); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("mergePaneCaptureContent() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func testActiveAssignment(t *testing.T, deps *testDeps, d *Daemon, profileName string) (ActiveAssignment, AgentProfile) {
	t.Helper()

	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-710", "Capture diagnostics before kill", profileName); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	active, err := deps.state.ActiveAssignmentByIssue(ctx, d.project, "LAB-710")
	if err != nil {
		t.Fatalf("ActiveAssignmentByIssue() error = %v", err)
	}
	profile, err := d.profileForTask(ctx, active.Task)
	if err != nil {
		t.Fatalf("profileForTask() error = %v", err)
	}
	return active, profile
}
