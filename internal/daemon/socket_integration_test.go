package daemon

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/project"
)

func TestRunProcessAssignResumeAndCancelOverUnixSocket(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(filepath.Join(projectDir, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.git) error = %v", err)
	}

	poolDir := filepath.Join(projectDir, ".orca", "pool")
	initPoolClone(t, poolDir, "clone-01")

	projectPath, err := project.CanonicalPath(projectDir)
	if err != nil {
		t.Fatalf("CanonicalPath(%q) error = %v", projectDir, err)
	}

	configDir, err := os.MkdirTemp("/tmp", "orca-it-")
	if err != nil {
		t.Fatalf("MkdirTemp(/tmp) error = %v", err)
	}
	t.Cleanup(func() {
		_ = os.RemoveAll(configDir)
	})
	paths := Paths{
		ConfigDir: configDir,
		StateDB:   filepath.Join(configDir, "state.db"),
		PIDDir:    filepath.Join(configDir, "pids"),
	}
	stateDB := filepath.Join(configDir, "state.db")
	pidFile := paths.pidFile(projectPath)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	amuxClient := &fakeAmux{
		spawnPane: Pane{ID: "pane-1", Name: "worker-1"},
		captures:  make(map[string][]string),
	}
	commandRunner := newFakeCommands()
	errCh := make(chan error, 1)
	go func() {
		errCh <- runProcess(ctx, ServeRequest{
			Session: "test-session",
			Project: projectDir,
			StateDB: stateDB,
			PIDFile: pidFile,
		}, serveDeps{
			detectOrigin: func(_ string) (string, error) {
				return "git@github.com:weill-labs/orca.git", nil
			},
			amux:       amuxClient,
			commands:   commandRunner,
			poolRunner: stubPoolRunner{},
		})
	}()

	store, err := state.OpenSQLite(stateDB)
	if err != nil {
		t.Fatalf("OpenSQLite(%q) error = %v", stateDB, err)
	}
	defer store.Close()

	socketPath := paths.socketFile(projectPath)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case err := <-errCh:
			t.Fatalf("runProcess() returned early: %v", err)
		default:
		}

		if _, err := os.Stat(socketPath); err != nil {
			waitForDuration(t, 25*time.Millisecond)
			continue
		}
		status, err := store.ProjectStatus(context.Background(), projectPath)
		if err == nil && status.Daemon != nil && status.Daemon.Status == "running" {
			goto daemonReady
		}
		waitForDuration(t, 25*time.Millisecond)
	}
	t.Fatalf("timed out waiting for daemon socket %s", socketPath)

daemonReady:

	controller, err := NewLocalController(ControllerOptions{
		Store: store,
		Paths: paths,
	})
	if err != nil {
		t.Fatalf("NewLocalController() error = %v", err)
	}

	var assignResult TaskActionResult
	err = callRPC(context.Background(), socketPath, "assign", map[string]any{
		"issue":  "LAB-718",
		"prompt": "Implement Unix socket IPC.",
		"agent":  "claude",
		"title":  "Unix socket IPC title",
	}, &assignResult)
	if err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	if got, want := assignResult.Status, TaskStatusActive; got != want {
		t.Fatalf("assignResult.Status = %q, want %q", got, want)
	}

	waitFor(t, "active task state", func() bool {
		status, err := store.ProjectStatus(context.Background(), projectPath)
		if err != nil {
			return false
		}
		return status.Summary.Active == 1 && status.Summary.Workers == 1 && status.Summary.FreeClones == 0
	})

	taskStatus, err := store.TaskStatus(context.Background(), projectPath, "LAB-718")
	if err != nil {
		t.Fatalf("TaskStatus() error = %v", err)
	}
	wantClonePath := filepath.Join(projectPath, ".orca", "pool", "clone-01")
	if got := taskStatus.Task.ClonePath; got != wantClonePath {
		t.Fatalf("task.ClonePath = %q, want %q", got, wantClonePath)
	}
	if got, want := taskStatus.Task.WorkerID, "pane-1"; got != want {
		t.Fatalf("task.WorkerID = %q, want %q", got, want)
	}

	amuxClient.requireMetadata(t, "pane-1", map[string]string{
		"agent_profile":  "claude",
		"branch":         "LAB-718",
		"task":           "Unix socket IPC title",
		"tracked_issues": `[{"id":"LAB-718","status":"active"}]`,
	})

	resumer, ok := any(controller).(interface {
		Resume(context.Context, ResumeRequest) (TaskActionResult, error)
	})
	if !ok {
		t.Fatal("LocalController does not implement Resume")
	}

	resumeResult, err := resumer.Resume(context.Background(), ResumeRequest{
		Project: projectPath,
		Issue:   "LAB-718",
	})
	if err != nil {
		t.Fatalf("Resume() error = %v", err)
	}
	if got, want := resumeResult.Status, TaskStatusActive; got != want {
		t.Fatalf("resumeResult.Status = %q, want %q", got, want)
	}

	if got, want := amuxClient.paneExistsCalls, []string{"pane-1"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("paneExistsCalls = %#v, want %#v", got, want)
	}
	amuxClient.requireSentKeys(t, "pane-1", []string{"Implement Unix socket IPC.\n", "claude\n"})

	cancelResult, err := controller.Cancel(context.Background(), CancelRequest{
		Project: projectPath,
		Issue:   "LAB-718",
	})
	if err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if got, want := cancelResult.Status, TaskStatusCancelled; got != want {
		t.Fatalf("cancelResult.Status = %q, want %q", got, want)
	}

	waitFor(t, "cancelled task state", func() bool {
		status, err := store.ProjectStatus(context.Background(), projectPath)
		if err != nil {
			return false
		}
		return status.Summary.Cancelled == 1 && status.Summary.Workers == 0 && status.Summary.FreeClones == 1
	})

	if got, want := amuxClient.killCalls, []string{"pane-1"}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("killCalls = %#v, want %#v", amuxClient.killCalls, want)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runProcess() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for runProcess to exit")
	}

	if _, err := os.Stat(socketPath); !os.IsNotExist(err) {
		t.Fatalf("socket file still present after shutdown: %v", err)
	}
}

type stubPoolRunner struct{}

func (stubPoolRunner) Run(context.Context, string, string, ...string) error {
	return nil
}

func initPoolClone(t *testing.T, root, name string) string {
	t.Helper()

	clonePath := filepath.Join(root, name)
	if err := os.MkdirAll(clonePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", clonePath, err)
	}
	runGit(t, "", "init", clonePath)
	runGit(t, clonePath, "checkout", "-b", "main")
	if err := os.WriteFile(filepath.Join(clonePath, "README.md"), []byte("hello\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(README.md) error = %v", err)
	}
	runGit(t, clonePath, "add", "README.md")
	runGit(t, clonePath, "-c", "user.name=Orca Tests", "-c", "user.email=orca-tests@example.com", "commit", "-m", "initial commit")
	return clonePath
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()

	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, output)
	}
}
