package daemon

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/pool"
	projectpkg "github.com/weill-labs/orca/internal/project"
)

func TestLocalControllerSpawn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		amux         *fakeSpawnAmux
		store        func(t *testing.T) state.Store
		detect       func(string) (string, error)
		session      *string
		setupProject func(t *testing.T, project string)
		poolRunner   pool.Runner
		assert       func(t *testing.T, store state.Store, result SpawnPaneResult, amuxClient *fakeSpawnAmux, project string)
		wantErr      string
	}{
		{
			name: "allocates clone and opens pane without creating a task",
			amux: &fakeSpawnAmux{
				spawnPane: amux.Pane{ID: "pane-7", Name: "Scratch pane"},
			},
			assert: func(t *testing.T, store state.Store, result SpawnPaneResult, amuxClient *fakeSpawnAmux, project string) {
				t.Helper()
				queryableStore, ok := store.(interface {
					NonTerminalTasks(context.Context, string) ([]state.Task, error)
					ListClones(context.Context, string) ([]state.Clone, error)
				})
				if !ok {
					t.Fatal("store does not support spawn assertions")
				}

				if got, want := result.Project, project; got != want {
					t.Fatalf("result.Project = %q, want %q", got, want)
				}
				if got, want := result.PaneID, "pane-7"; got != want {
					t.Fatalf("result.PaneID = %q, want %q", got, want)
				}
				if got, want := len(amuxClient.spawnRequests), 1; got != want {
					t.Fatalf("len(spawnRequests) = %d, want %d", got, want)
				}

				req := amuxClient.spawnRequests[0]
				if got, want := req.Session, "orca-dev"; got != want {
					t.Fatalf("spawn session = %q, want %q", got, want)
				}
				if got, want := req.AtPane, "lead-pane"; got != want {
					t.Fatalf("spawn lead pane = %q, want %q", got, want)
				}
				if got, want := req.Name, "Scratch pane"; got != want {
					t.Fatalf("spawn name = %q, want %q", got, want)
				}
				if req.Command != "" {
					t.Fatalf("spawn command = %q, want empty", req.Command)
				}
				if got, want := req.CWD, result.ClonePath; got != want {
					t.Fatalf("spawn cwd = %q, want %q", got, want)
				}

				tasks, err := queryableStore.NonTerminalTasks(context.Background(), project)
				if err != nil {
					t.Fatalf("NonTerminalTasks() error = %v", err)
				}
				if len(tasks) != 0 {
					t.Fatalf("NonTerminalTasks() = %#v, want no tasks", tasks)
				}

				clones, err := queryableStore.ListClones(context.Background(), project)
				if err != nil {
					t.Fatalf("ListClones() error = %v", err)
				}
				if got, want := len(clones), 1; got != want {
					t.Fatalf("len(clones) = %d, want %d", got, want)
				}
				if got, want := clones[0].Status, "occupied"; got != want {
					t.Fatalf("clone status = %q, want %q", got, want)
				}
				if clones[0].Issue == "" || clones[0].Branch == "" {
					t.Fatalf("clone occupancy = %#v, want synthetic issue and branch", clones[0])
				}
			},
		},
		{
			name: "releases clone when pane creation fails",
			amux: &fakeSpawnAmux{
				spawnErr: errors.New("amux unavailable"),
			},
			wantErr: "spawn pane: amux unavailable",
			assert: func(t *testing.T, store state.Store, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, project string) {
				t.Helper()
				queryableStore, ok := store.(interface {
					ListClones(context.Context, string) ([]state.Clone, error)
				})
				if !ok {
					t.Fatal("store does not support clone assertions")
				}

				if got, want := len(amuxClient.spawnRequests), 1; got != want {
					t.Fatalf("len(spawnRequests) = %d, want %d", got, want)
				}

				clones, err := queryableStore.ListClones(context.Background(), project)
				if err != nil {
					t.Fatalf("ListClones() error = %v", err)
				}
				if got, want := len(clones), 1; got != want {
					t.Fatalf("len(clones) = %d, want %d", got, want)
				}
				if got, want := clones[0].Status, "free"; got != want {
					t.Fatalf("clone status = %q, want %q", got, want)
				}
				if clones[0].Issue != "" || clones[0].Branch != "" {
					t.Fatalf("clone occupancy = %#v, want released clone", clones[0])
				}
			},
		},
		{
			name: "returns detect origin errors before touching amux",
			amux: &fakeSpawnAmux{},
			detect: func(string) (string, error) {
				return "", errors.New("origin lookup failed")
			},
			wantErr: "detect origin: origin lookup failed",
			assert: func(t *testing.T, _ state.Store, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, _ string) {
				t.Helper()
				if got := len(amuxClient.spawnRequests); got != 0 {
					t.Fatalf("len(spawnRequests) = %d, want 0", got)
				}
			},
		},
		{
			name: "rejects stores that cannot track clone occupancy",
			amux: &fakeSpawnAmux{},
			store: func(t *testing.T) state.Store {
				t.Helper()
				return &fakeStore{}
			},
			wantErr: "spawn requires clone-capable state store",
			assert: func(t *testing.T, _ state.Store, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, _ string) {
				t.Helper()
				if got := len(amuxClient.spawnRequests); got != 0 {
					t.Fatalf("len(spawnRequests) = %d, want 0", got)
				}
			},
		},
		{
			name:       "returns clone allocation failures",
			amux:       &fakeSpawnAmux{},
			poolRunner: spawnFailingRunner{err: errors.New("clone failed")},
			assert: func(t *testing.T, _ state.Store, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, _ string) {
				t.Helper()
				if got := len(amuxClient.spawnRequests); got != 0 {
					t.Fatalf("len(spawnRequests) = %d, want 0", got)
				}
			},
			wantErr: "auto-create clone",
		},
		{
			name: "returns missing session pane creation failures",
			amux: &fakeSpawnAmux{
				spawnErr: errors.New("session not found"),
			},
			session: stringPtr(""),
			wantErr: "spawn pane: session not found",
			assert: func(t *testing.T, store state.Store, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, project string) {
				t.Helper()
				queryableStore, ok := store.(interface {
					ListClones(context.Context, string) ([]state.Clone, error)
				})
				if !ok {
					t.Fatal("store does not support clone assertions")
				}

				if got, want := len(amuxClient.spawnRequests), 1; got != want {
					t.Fatalf("len(spawnRequests) = %d, want %d", got, want)
				}
				if got, want := amuxClient.spawnRequests[0].Session, ""; got != want {
					t.Fatalf("spawn session = %q, want %q", got, want)
				}

				clones, err := queryableStore.ListClones(context.Background(), project)
				if err != nil {
					t.Fatalf("ListClones() error = %v", err)
				}
				if got, want := clones[0].Status, "free"; got != want {
					t.Fatalf("clone status = %q, want %q", got, want)
				}
			},
		},
		{
			name: "returns pool directory creation failures",
			amux: &fakeSpawnAmux{},
			setupProject: func(t *testing.T, project string) {
				t.Helper()
				mustWriteSpawnFile(t, filepath.Join(project, ".orca"), "not-a-directory\n")
			},
			wantErr: "create pool directory",
			assert: func(t *testing.T, _ state.Store, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, _ string) {
				t.Helper()
				if got := len(amuxClient.spawnRequests); got != 0 {
					t.Fatalf("len(spawnRequests) = %d, want 0", got)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			project, origin := newSpawnProject(t)
			canonicalProject, err := projectpkg.CanonicalPath(project)
			if err != nil {
				t.Fatalf("CanonicalPath(%q) error = %v", project, err)
			}
			store := newSpawnStore(t)
			if tt.store != nil {
				store = tt.store(t)
			}
			if tt.setupProject != nil {
				tt.setupProject(t, project)
			}

			detectOrigin := tt.detect
			if detectOrigin == nil {
				detectOrigin = func(string) (string, error) { return origin, nil }
			}
			session := "orca-dev"
			if tt.session != nil {
				session = *tt.session
			}

			controller, err := NewLocalController(ControllerOptions{
				Store:        store,
				Paths:        Paths{StateDB: filepath.Join(t.TempDir(), "state.db"), PIDDir: filepath.Join(t.TempDir(), "pids")},
				Now:          func() time.Time { return time.Date(2026, 4, 5, 12, 0, 0, 123, time.UTC) },
				DetectOrigin: detectOrigin,
				Amux:         tt.amux,
				PoolRunner:   tt.poolRunner,
			})
			if err != nil {
				t.Fatalf("NewLocalController() error = %v", err)
			}

			req := SpawnPaneRequest{
				Project:  project,
				Session:  session,
				LeadPane: "lead-pane",
				Title:    "Scratch pane",
			}

			result, err := controller.Spawn(context.Background(), req)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Spawn() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Spawn() error = %v", err)
			}

			tt.assert(t, store, result, tt.amux, canonicalProject)
		})
	}
}

func TestWorkerPaneSpawnName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		task      Task
		stableRef string
		want      string
	}{
		{
			name:      "uses shortened issue pane prefix",
			task:      Task{Issue: "LAB-948"},
			stableRef: "worker-01",
			want:      "w-LAB-948",
		},
		{
			name:      "falls back to stable ref without issue",
			task:      Task{},
			stableRef: "worker-01",
			want:      "worker-01",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := workerPaneSpawnName(tt.task, tt.stableRef); got != tt.want {
				t.Fatalf("workerPaneSpawnName() = %q, want %q", got, tt.want)
			}
		})
	}
}

type spawnFailingRunner struct {
	err error
}

func stringPtr(value string) *string {
	return &value
}

func (r spawnFailingRunner) Run(context.Context, string, string, ...string) error {
	return r.err
}

type fakeSpawnAmux struct {
	spawnRequests []amux.SpawnRequest
	spawnPane     amux.Pane
	spawnErr      error
}

func (f *fakeSpawnAmux) Spawn(_ context.Context, req amux.SpawnRequest) (amux.Pane, error) {
	f.spawnRequests = append(f.spawnRequests, req)
	if f.spawnErr != nil {
		return amux.Pane{}, f.spawnErr
	}
	return f.spawnPane, nil
}

func (f *fakeSpawnAmux) PaneExists(context.Context, string) (bool, error) { return false, nil }
func (f *fakeSpawnAmux) ListPanes(context.Context) ([]amux.Pane, error)   { return nil, nil }
func (f *fakeSpawnAmux) Metadata(context.Context, string) (map[string]string, error) {
	return nil, nil
}
func (f *fakeSpawnAmux) SendKeys(context.Context, string, ...string) error { return nil }
func (f *fakeSpawnAmux) Capture(context.Context, string) (string, error)   { return "", nil }
func (f *fakeSpawnAmux) CapturePane(context.Context, string) (amux.PaneCapture, error) {
	return amux.PaneCapture{}, nil
}
func (f *fakeSpawnAmux) CaptureHistory(context.Context, string) (amux.PaneCapture, error) {
	return amux.PaneCapture{}, nil
}
func (f *fakeSpawnAmux) SetMetadata(context.Context, string, map[string]string) error { return nil }
func (f *fakeSpawnAmux) RemoveMetadata(context.Context, string, ...string) error      { return nil }
func (f *fakeSpawnAmux) KillPane(context.Context, string) error                       { return nil }
func (f *fakeSpawnAmux) WaitIdle(context.Context, string, time.Duration) error        { return nil }
func (f *fakeSpawnAmux) WaitIdleSettle(context.Context, string, time.Duration, time.Duration) error {
	return nil
}
func (f *fakeSpawnAmux) WaitContent(context.Context, string, string, time.Duration) error { return nil }

func newSpawnProject(t *testing.T) (string, string) {
	t.Helper()

	root := t.TempDir()
	project := filepath.Join(root, "project")
	origin := filepath.Join(root, "origin.git")

	mustRunSpawnGit(t, root, "git", "init", "--bare", "--initial-branch=main", origin)
	mustRunSpawnGit(t, root, "git", "init", "--initial-branch=main", project)
	mustWriteSpawnFile(t, filepath.Join(project, "README.md"), "orca\n")
	mustRunSpawnGit(t, project, "git", "add", "README.md")
	mustRunSpawnGit(t, project, "git", "-c", "user.name=Test User", "-c", "user.email=test@example.com", "commit", "-m", "initial commit")
	mustRunSpawnGit(t, project, "git", "remote", "add", "origin", origin)
	mustRunSpawnGit(t, project, "git", "push", "origin", "main")

	return project, origin
}

func newSpawnStore(t *testing.T) state.Store {
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

func mustRunSpawnGit(t *testing.T, dir, name string, args ...string) {
	t.Helper()

	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %s error = %v\n%s", name, strings.Join(args, " "), err, output)
	}
}

func mustWriteSpawnFile(t *testing.T, path, contents string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

var _ amux.Client = (*fakeSpawnAmux)(nil)
