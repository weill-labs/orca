package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareAssignmentPromptForDeliverySkipsUnpromotedPrompts(t *testing.T) {
	t.Parallel()

	d := &Daemon{}
	task := Task{Issue: "LAB-1871", ClonePath: t.TempDir()}

	largePrompt := strings.Repeat("large prompt ", 80)
	got, err := d.prepareAssignmentPromptForDelivery(task, AgentProfile{Name: "claude"}, largePrompt)
	if err != nil {
		t.Fatalf("prepareAssignmentPromptForDelivery(non-codex) error = %v", err)
	}
	if got != largePrompt {
		t.Fatalf("delivery prompt = %q, want original prompt", got)
	}

	smallPrompt := "small prompt"
	got, err = d.prepareAssignmentPromptForDelivery(task, AgentProfile{Name: "codex"}, smallPrompt)
	if err != nil {
		t.Fatalf("prepareAssignmentPromptForDelivery(small codex) error = %v", err)
	}
	if got != smallPrompt {
		t.Fatalf("delivery prompt = %q, want original prompt", got)
	}
	if _, err := os.Stat(filepath.Join(task.ClonePath, assignmentPromptFileDir)); !os.IsNotExist(err) {
		t.Fatalf("prompt dir stat error = %v, want not exist", err)
	}
}

func TestPrepareAssignmentPromptForDeliveryRequiresClonePath(t *testing.T) {
	t.Parallel()

	d := &Daemon{}
	_, err := d.prepareAssignmentPromptForDelivery(Task{Issue: "LAB-1871"}, AgentProfile{Name: "codex"}, strings.Repeat("x", 600))
	if err == nil || !strings.Contains(err.Error(), "requires clone path") {
		t.Fatalf("prepareAssignmentPromptForDelivery() error = %v, want clone path error", err)
	}
}

func TestPrepareAssignmentPromptForDeliveryWritesGitIgnoredRelativeReference(t *testing.T) {
	t.Parallel()

	clonePath := t.TempDir()
	excludePath := filepath.Join(clonePath, ".git", "info", "exclude")
	if err := os.MkdirAll(filepath.Dir(excludePath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(excludePath), err)
	}
	if err := os.WriteFile(excludePath, []byte("# local excludes"), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", excludePath, err)
	}

	d := &Daemon{}
	task := Task{Issue: "LAB/1871: Large prompt?", ClonePath: clonePath}
	prompt := strings.Repeat("preserve this prompt ", 40) + "\n\n"
	got, err := d.prepareAssignmentPromptForDelivery(task, AgentProfile{Name: "Codex"}, prompt)
	if err != nil {
		t.Fatalf("prepareAssignmentPromptForDelivery() error = %v", err)
	}

	promptFile, err := assignmentPromptFilePath(task)
	if err != nil {
		t.Fatalf("assignmentPromptFilePath() error = %v", err)
	}
	if got != codexAssignmentPromptFileReference(assignmentPromptFileReferencePath(task, promptFile)) {
		t.Fatalf("delivery prompt = %q, want relative file reference", got)
	}
	if strings.Contains(got, clonePath) {
		t.Fatalf("delivery prompt = %q, want relative path without clone prefix", got)
	}
	content, err := os.ReadFile(promptFile)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", promptFile, err)
	}
	if string(content) != prompt {
		t.Fatalf("prompt file content = %q, want exact prompt", string(content))
	}

	if err := ensureAssignmentPromptFileIgnored(clonePath); err != nil {
		t.Fatalf("ensureAssignmentPromptFileIgnored() error = %v", err)
	}
	exclude, err := os.ReadFile(excludePath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", excludePath, err)
	}
	if count := strings.Count(string(exclude), assignmentPromptGitExclude); count != 1 {
		t.Fatalf("git exclude pattern count = %d, want 1 in %q", count, string(exclude))
	}
}

func TestAssignmentPromptFilePathSanitizesIssue(t *testing.T) {
	t.Parallel()

	path, err := assignmentPromptFilePath(Task{
		Issue:     " LAB/1871: prompt? ",
		ClonePath: "/tmp/clone",
	})
	if err != nil {
		t.Fatalf("assignmentPromptFilePath() error = %v", err)
	}
	if got, want := filepath.Base(path), "LAB-1871--prompt.md"; got != want {
		t.Fatalf("prompt file base = %q, want %q", got, want)
	}
}

func TestAssignmentPromptFileReferencePathFallsBackOutsideClone(t *testing.T) {
	t.Parallel()

	clonePath := t.TempDir()
	outsidePath := filepath.Join(t.TempDir(), "prompt.md")
	if got := assignmentPromptFileReferencePath(Task{ClonePath: clonePath}, outsidePath); got != outsidePath {
		t.Fatalf("reference path = %q, want absolute outside path %q", got, outsidePath)
	}
	if got := assignmentPromptFileReferencePath(Task{}, outsidePath); got != outsidePath {
		t.Fatalf("reference path without clone = %q, want %q", got, outsidePath)
	}
}

func TestEnsureAssignmentPromptFileIgnoredSkipsNonGitClone(t *testing.T) {
	t.Parallel()

	if err := ensureAssignmentPromptFileIgnored(t.TempDir()); err != nil {
		t.Fatalf("ensureAssignmentPromptFileIgnored(non-git) error = %v", err)
	}
}

func TestStoreFailedAssignmentTaskFillsMissingFields(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	d.storeFailedAssignmentTask(context.Background(), "/tmp/project", "LAB-1871", Task{}, Clone{
		Name: "clone-02",
		Path: "/tmp/project/.orca/pool/clone-02",
	}, Pane{
		ID:   "pane-2",
		Name: "w-LAB-1871",
	}, Worker{
		WorkerID: "worker-02",
	}, AgentProfile{Name: "codex"})

	task, ok := deps.state.task("LAB-1871")
	if !ok {
		t.Fatal("failed task missing")
	}
	if task.Status != TaskStatusFailed || task.Branch != "LAB-1871" {
		t.Fatalf("task status/branch = %q/%q, want failed/LAB-1871", task.Status, task.Branch)
	}
	if task.CloneName != "clone-02" || task.ClonePath != "/tmp/project/.orca/pool/clone-02" {
		t.Fatalf("task clone = %q/%q, want clone-02 path", task.CloneName, task.ClonePath)
	}
	if task.PaneID != "pane-2" || task.PaneName != "w-LAB-1871" {
		t.Fatalf("task pane = %q/%q, want pane-2/w-LAB-1871", task.PaneID, task.PaneName)
	}
	if task.WorkerID != "worker-02" || task.AgentProfile != "codex" {
		t.Fatalf("task worker/profile = %q/%q, want worker-02/codex", task.WorkerID, task.AgentProfile)
	}
	if task.CreatedAt.IsZero() || task.UpdatedAt.IsZero() {
		t.Fatalf("task timestamps = %v/%v, want set", task.CreatedAt, task.UpdatedAt)
	}
}
