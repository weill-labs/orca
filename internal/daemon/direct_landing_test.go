package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDirectLandingSucceedsWithLocalBareRemote(t *testing.T) {
	ctx := context.Background()
	remote, seed := newDirectLandingRemote(t, map[string]string{"README.md": "base\n"})
	projectPath, clonePath := cloneDirectLandingRepo(t, remote)
	deps, d := newDirectLandingDaemon(t, projectPath, clonePath, LandingConfig{
		Mode:        LandingModeDirect,
		BaseBranch:  "main",
		QualityGate: "test -f landed.txt",
	})

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	if err := d.Assign(ctx, "LAB-1969", "Implement direct landing", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	writeDirectLandingFile(t, clonePath, "landed.txt", "landed\n")
	git(t, clonePath, "add", "landed.txt")
	git(t, clonePath, "commit", "-m", "LAB-1969 direct landing")

	result, err := d.EnqueueTarget(ctx, "LAB-1969")
	if err != nil {
		t.Fatalf("EnqueueTarget() error = %v", err)
	}
	if got, want := result.Mode, LandingModeDirect; got != want {
		t.Fatalf("result.Mode = %q, want %q", got, want)
	}
	d.dispatchMergeQueue(ctx)

	waitFor(t, "direct landing completion", func() bool {
		task, ok := deps.state.task("LAB-1969")
		return ok && task.Status == TaskStatusDone
	})
	git(t, seed, "fetch", "origin", "main")
	if got, want := strings.TrimSpace(git(t, seed, "show", "origin/main:landed.txt")), "landed"; got != want {
		t.Fatalf("origin/main:landed.txt = %q, want %q", got, want)
	}
	if got := deps.events.countType(EventDirectLandingConflict); got != 0 {
		t.Fatalf("direct landing conflict event count = %d, want 0", got)
	}
	deps.events.requireTypes(t, EventDirectLandingStarted, EventDirectLanded, EventTaskCompleted)
}

func TestDirectLandingSucceedsWithLocalWorktreeOrigin(t *testing.T) {
	ctx := context.Background()
	projectPath, clonePath := newLocalDirectLandingRepo(t, map[string]string{"README.md": "base\n"})
	deps, d := newDirectLandingDaemon(t, projectPath, clonePath, LandingConfig{
		Mode:       LandingModeDirect,
		BaseBranch: "main",
	})

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	if err := d.Assign(ctx, "LAB-1973", "Land into a local worktree origin", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	writeDirectLandingFile(t, clonePath, "landed.txt", "landed\n")
	git(t, clonePath, "add", "landed.txt")
	git(t, clonePath, "commit", "-m", "LAB-1973 direct landing")
	git(t, clonePath, "push", "-u", "origin", "HEAD:LAB-1973")

	if _, err := d.EnqueueTarget(ctx, "LAB-1973"); err != nil {
		t.Fatalf("EnqueueTarget() error = %v", err)
	}
	d.dispatchMergeQueue(ctx)

	waitFor(t, "local direct landing completion", func() bool {
		task, ok := deps.state.task("LAB-1973")
		return ok && task.Status == TaskStatusDone
	})
	if got, want := strings.TrimSpace(git(t, projectPath, "show", "HEAD:landed.txt")), "landed"; got != want {
		t.Fatalf("manager HEAD:landed.txt = %q, want %q", got, want)
	}
	if got := strings.TrimSpace(git(t, projectPath, "status", "--short")); got != "" {
		t.Fatalf("manager status --short = %q, want clean", got)
	}
	deps.events.requireTypes(t, EventDirectLandingStarted, EventDirectLanded, EventTaskCompleted)
}

func TestDirectLandingLocalWorktreeOriginRollbackWhenMainAdvances(t *testing.T) {
	ctx := context.Background()
	projectPath, clonePath := newLocalDirectLandingRepo(t, map[string]string{"README.md": "base\n"})
	gate := strings.Join([]string{
		"printf 'manager advance\\n' > " + testShellQuote(filepath.Join(projectPath, "README.md")),
		"git -C " + testShellQuote(projectPath) + " add README.md",
		"git -C " + testShellQuote(projectPath) + " commit -m 'advance main'",
	}, " && ")
	deps, d := newDirectLandingDaemon(t, projectPath, clonePath, LandingConfig{
		Mode:        LandingModeDirect,
		BaseBranch:  "main",
		QualityGate: gate,
	})

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	if err := d.Assign(ctx, "LAB-1974", "Race local direct landing with main", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	writeDirectLandingFile(t, clonePath, "landed.txt", "landed\n")
	git(t, clonePath, "add", "landed.txt")
	git(t, clonePath, "commit", "-m", "LAB-1974 direct landing")
	git(t, clonePath, "push", "-u", "origin", "HEAD:LAB-1974")

	if _, err := d.EnqueueTarget(ctx, "LAB-1974"); err != nil {
		t.Fatalf("EnqueueTarget() error = %v", err)
	}
	d.dispatchMergeQueue(ctx)

	waitFor(t, "local direct landing failure", func() bool {
		return deps.events.countType(EventDirectLandingFailed) == 1
	})
	task, ok := deps.state.task("LAB-1974")
	if !ok {
		t.Fatal("task missing after local direct landing failure")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got := strings.TrimSpace(git(t, projectPath, "status", "--short")); got != "" {
		t.Fatalf("manager status --short = %q, want clean", got)
	}
	if got, want := strings.TrimSpace(git(t, projectPath, "show", "HEAD:README.md")), "manager advance"; got != want {
		t.Fatalf("manager HEAD:README.md = %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(projectPath, "landed.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manager landed.txt stat error = %v, want not exist", err)
	}
	if message := deps.events.lastMessage(EventDirectLandingFailed); !strings.Contains(message, "local origin main advanced") {
		t.Fatalf("direct landing failed message = %q, want local origin advancement detail", message)
	}
}

func TestDirectLandingReportsRefSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "rebased feature branch advances remote main"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			remote, seed := newDirectLandingRemote(t, map[string]string{"README.md": "base\n"})
			projectPath, clonePath := cloneDirectLandingRepo(t, remote)
			deps, d := newDirectLandingDaemon(t, projectPath, clonePath, LandingConfig{
				Mode:       LandingModeDirect,
				BaseBranch: "main",
			})

			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})
			if err := d.Assign(ctx, "LAB-2016", "Report direct landing refs", "codex"); err != nil {
				t.Fatalf("Assign() error = %v", err)
			}

			writeDirectLandingFile(t, clonePath, "worker.txt", "worker\n")
			git(t, clonePath, "add", "worker.txt")
			git(t, clonePath, "commit", "-m", "LAB-2016 worker change")
			featureBefore := strings.TrimSpace(git(t, clonePath, "rev-parse", "HEAD"))

			writeDirectLandingFile(t, seed, "main.txt", "main\n")
			git(t, seed, "add", "main.txt")
			git(t, seed, "commit", "-m", "advance main")
			git(t, seed, "push", "origin", "main")
			originMainBefore := strings.TrimSpace(git(t, seed, "rev-parse", "HEAD"))

			if _, err := d.EnqueueTarget(ctx, "LAB-2016"); err != nil {
				t.Fatalf("EnqueueTarget() error = %v", err)
			}
			d.dispatchMergeQueue(ctx)

			waitFor(t, "direct landing ref summary", func() bool {
				_, ok := deps.events.lastEventOfType(EventDirectLanded)
				return ok
			})

			event, ok := deps.events.lastEventOfType(EventDirectLanded)
			if !ok {
				t.Fatal("direct landed event missing")
			}
			payload, err := json.Marshal(event)
			if err != nil {
				t.Fatalf("Marshal(event) error = %v", err)
			}
			var details map[string]any
			if err := json.Unmarshal(payload, &details); err != nil {
				t.Fatalf("Unmarshal(event) error = %v", err)
			}

			git(t, seed, "fetch", "origin", "main")
			featureAfter := strings.TrimSpace(git(t, seed, "rev-parse", "origin/main"))
			if got := details["origin_main_before_sha"]; got != originMainBefore {
				t.Fatalf("origin_main_before_sha = %q, want %q", got, originMainBefore)
			}
			if got := details["origin_main_after_sha"]; got != featureAfter {
				t.Fatalf("origin_main_after_sha = %q, want %q", got, featureAfter)
			}
			if got := details["feature_branch_before_sha"]; got != featureBefore {
				t.Fatalf("feature_branch_before_sha = %q, want %q", got, featureBefore)
			}
			if got := details["feature_branch_after_sha"]; got != featureAfter {
				t.Fatalf("feature_branch_after_sha = %q, want %q", got, featureAfter)
			}
		})
	}
}

func TestDirectLandingConflictNotifiesWorkerWithConflictedFiles(t *testing.T) {
	ctx := context.Background()
	remote, seed := newDirectLandingRemote(t, map[string]string{"README.md": "base\n"})
	projectPath, clonePath := cloneDirectLandingRepo(t, remote)
	deps, d := newDirectLandingDaemon(t, projectPath, clonePath, LandingConfig{
		Mode:       LandingModeDirect,
		BaseBranch: "main",
	})

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	if err := d.Assign(ctx, "LAB-1970", "Create a conflicting direct landing", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	writeDirectLandingFile(t, clonePath, "README.md", "worker change\n")
	git(t, clonePath, "add", "README.md")
	git(t, clonePath, "commit", "-m", "LAB-1970 worker change")

	writeDirectLandingFile(t, seed, "README.md", "base changed\n")
	git(t, seed, "add", "README.md")
	git(t, seed, "commit", "-m", "advance main")
	git(t, seed, "push", "origin", "main")

	if _, err := d.EnqueueTarget(ctx, "LAB-1970"); err != nil {
		t.Fatalf("EnqueueTarget() error = %v", err)
	}
	d.dispatchMergeQueue(ctx)

	waitFor(t, "direct landing conflict event", func() bool {
		return deps.events.countType(EventDirectLandingConflict) == 1
	})
	task, ok := deps.state.task("LAB-1970")
	if !ok {
		t.Fatal("task missing after conflict")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := task.State, TaskStateEscalated; got != want {
		t.Fatalf("task.State = %q, want %q", got, want)
	}
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after conflict")
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}

	event, ok := deps.events.lastEventOfType(EventDirectLandingConflict)
	if !ok {
		t.Fatal("direct landing conflict event missing")
	}
	if got, want := event.ConflictedFiles, []string{"README.md"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("event.ConflictedFiles = %#v, want %#v", got, want)
	}
	if !event.ConflictStatePreserved {
		t.Fatal("event.ConflictStatePreserved = false, want true")
	}
	if !strings.Contains(event.Message, "README.md") || !strings.Contains(event.Message, "orca enqueue LAB-1970") {
		t.Fatalf("conflict event message = %q, want file and retry instructions", event.Message)
	}
	if got := deps.amux.countKey("pane-1", directLandingConflictPrompt("LAB-1970", "LAB-1970", "main", []string{"README.md"})+"\n"); got != 1 {
		t.Fatalf("conflict prompt count = %d, want 1", got)
	}
}

func TestDirectLandingQualityGateFailureKeepsTaskActive(t *testing.T) {
	ctx := context.Background()
	remote, _ := newDirectLandingRemote(t, map[string]string{"README.md": "base\n"})
	projectPath, clonePath := cloneDirectLandingRepo(t, remote)
	deps, d := newDirectLandingDaemon(t, projectPath, clonePath, LandingConfig{
		Mode:        LandingModeDirect,
		BaseBranch:  "main",
		QualityGate: "test -f missing-quality-gate-file",
	})

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	if err := d.Assign(ctx, "LAB-1971", "Fail the quality gate", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	writeDirectLandingFile(t, clonePath, "landed.txt", "landed\n")
	git(t, clonePath, "add", "landed.txt")
	git(t, clonePath, "commit", "-m", "LAB-1971 quality gate")

	if _, err := d.EnqueueTarget(ctx, "LAB-1971"); err != nil {
		t.Fatalf("EnqueueTarget() error = %v", err)
	}
	d.dispatchMergeQueue(ctx)

	waitFor(t, "direct landing quality failure", func() bool {
		return deps.events.countType(EventDirectLandingFailed) == 1
	})
	task, ok := deps.state.task("LAB-1971")
	if !ok {
		t.Fatal("task missing after quality gate failure")
	}
	if got, want := task.Status, TaskStatusActive; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if entries, err := deps.state.MergeEntries(ctx, projectPath); err != nil || len(entries) != 0 {
		t.Fatalf("merge entries after quality failure = %#v, %v, want none", entries, err)
	}
	if message := deps.events.lastMessage(EventDirectLandingFailed); !strings.Contains(message, "quality gate") {
		t.Fatalf("direct landing failed message = %q, want quality gate detail", message)
	}
}

func TestDirectLandingLinearFailureStillCompletesTask(t *testing.T) {
	ctx := context.Background()
	remote, _ := newDirectLandingRemote(t, map[string]string{"README.md": "base\n"})
	projectPath, clonePath := cloneDirectLandingRepo(t, remote)
	deps, d := newDirectLandingDaemon(t, projectPath, clonePath, LandingConfig{
		Mode:       LandingModeDirect,
		BaseBranch: "main",
	})
	deps.issueTracker.errors = map[string]error{
		IssueStateDone: errors.New("linear unavailable"),
	}

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})
	if err := d.Assign(ctx, "LAB-1972", "Land despite Linear failure", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	writeDirectLandingFile(t, clonePath, "landed.txt", "landed\n")
	git(t, clonePath, "add", "landed.txt")
	git(t, clonePath, "commit", "-m", "LAB-1972 direct landing")

	if _, err := d.EnqueueTarget(ctx, "LAB-1972"); err != nil {
		t.Fatalf("EnqueueTarget() error = %v", err)
	}
	d.dispatchMergeQueue(ctx)

	waitFor(t, "direct landing completion after Linear failure", func() bool {
		task, ok := deps.state.task("LAB-1972")
		return ok && task.Status == TaskStatusDone
	})
	if got := deps.events.countType(EventTaskCompletionFailed); got != 0 {
		t.Fatalf("task.completion_failed events = %d, want 0", got)
	}
	if message := deps.events.lastMessage(EventTaskCompleted); !strings.Contains(message, "failed to update Linear issue status") {
		t.Fatalf("task.completed message = %q, want Linear failure context", message)
	}
}

func newDirectLandingDaemon(t *testing.T, projectPath, clonePath string, landing LandingConfig) (*testDeps, *Daemon) {
	t.Helper()

	deps := newTestDeps(t)
	deps.pool.clone = Clone{Name: filepath.Base(clonePath), Path: clonePath}
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Project = projectPath
		opts.Commands = execCommandRunner{}
		opts.LandingConfig = landing
	})
	return deps, d
}

func newDirectLandingRemote(t *testing.T, files map[string]string) (string, string) {
	t.Helper()

	root := t.TempDir()
	remote := filepath.Join(root, "origin.git")
	git(t, root, "init", "--bare", remote)
	seed := filepath.Join(root, "seed")
	git(t, root, "clone", remote, seed)
	git(t, seed, "checkout", "-b", "main")
	git(t, seed, "config", "user.name", "Test User")
	git(t, seed, "config", "user.email", "test@example.invalid")
	for path, content := range files {
		writeDirectLandingFile(t, seed, path, content)
	}
	git(t, seed, "add", ".")
	git(t, seed, "commit", "-m", "initial commit")
	git(t, seed, "push", "origin", "main")
	git(t, remote, "symbolic-ref", "HEAD", "refs/heads/main")
	return remote, seed
}

func cloneDirectLandingRepo(t *testing.T, remote string) (string, string) {
	t.Helper()

	projectPath := filepath.Join(t.TempDir(), "project")
	clonePath := filepath.Join(projectPath, OrcaPoolSubdir, "clone-01")
	if err := os.MkdirAll(filepath.Dir(clonePath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(clonePath), err)
	}
	git(t, filepath.Dir(clonePath), "clone", remote, clonePath)
	markClonePathForTest(t, clonePath)
	return projectPath, clonePath
}

func newLocalDirectLandingRepo(t *testing.T, files map[string]string) (string, string) {
	t.Helper()

	projectPath := filepath.Join(t.TempDir(), "project")
	git(t, filepath.Dir(projectPath), "init", "--initial-branch=main", projectPath)
	git(t, projectPath, "config", "user.name", "Test User")
	git(t, projectPath, "config", "user.email", "test@example.invalid")
	if err := os.MkdirAll(filepath.Join(projectPath, ".git", "info"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.git/info) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(projectPath, ".git", "info", "exclude"), []byte(".orca/\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(.git/info/exclude) error = %v", err)
	}
	for path, content := range files {
		writeDirectLandingFile(t, projectPath, path, content)
	}
	git(t, projectPath, "add", ".")
	git(t, projectPath, "commit", "-m", "initial commit")

	clonePath := filepath.Join(projectPath, OrcaPoolSubdir, "clone-01")
	if err := os.MkdirAll(filepath.Dir(clonePath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(clonePath), err)
	}
	git(t, filepath.Dir(clonePath), "clone", projectPath, clonePath)
	markClonePathForTest(t, clonePath)
	return projectPath, clonePath
}

func writeDirectLandingFile(t *testing.T, root, name, content string) {
	t.Helper()

	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func git(t *testing.T, dir string, args ...string) string {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git -C %s %s failed: %v\n%s", dir, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output)
}

func testShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
