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
)

func TestLocalControllerSpawn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		amux    *fakeSpawnAmux
		assert  func(t *testing.T, store *state.SQLiteStore, result SpawnPaneResult, amuxClient *fakeSpawnAmux, project string)
		wantErr string
	}{
		{
			name: "allocates clone and opens pane without creating a task",
			amux: &fakeSpawnAmux{
				spawnPane: amux.Pane{ID: "pane-7", Name: "Scratch pane"},
			},
			assert: func(t *testing.T, store *state.SQLiteStore, result SpawnPaneResult, amuxClient *fakeSpawnAmux, project string) {
				t.Helper()

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

				tasks, err := store.NonTerminalTasks(context.Background(), project)
				if err != nil {
					t.Fatalf("NonTerminalTasks() error = %v", err)
				}
				if len(tasks) != 0 {
					t.Fatalf("NonTerminalTasks() = %#v, want no tasks", tasks)
				}

				clones, err := store.ListClones(context.Background(), project)
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
			assert: func(t *testing.T, store *state.SQLiteStore, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, project string) {
				t.Helper()

				if got, want := len(amuxClient.spawnRequests), 1; got != want {
					t.Fatalf("len(spawnRequests) = %d, want %d", got, want)
				}

				clones, err := store.ListClones(context.Background(), project)
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
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			project, origin := newSpawnProject(t)
			store := newSpawnStore(t)

			controller, err := NewLocalController(ControllerOptions{
				Store:        store,
				Paths:        Paths{StateDB: filepath.Join(t.TempDir(), "state.db"), PIDDir: filepath.Join(t.TempDir(), "pids")},
				Now:          func() time.Time { return time.Date(2026, 4, 5, 12, 0, 0, 123, time.UTC) },
				DetectOrigin: func(string) (string, error) { return origin, nil },
				Amux:         tt.amux,
			})
			if err != nil {
				t.Fatalf("NewLocalController() error = %v", err)
			}

			result, err := controller.Spawn(context.Background(), SpawnPaneRequest{
				Project:  project,
				Session:  "orca-dev",
				LeadPane: "lead-pane",
				Title:    "Scratch pane",
			})
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Spawn() error = %v, want substring %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatalf("Spawn() error = %v", err)
			}

			tt.assert(t, store, result, tt.amux, project)
		})
	}
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

func (f *fakeSpawnAmux) PaneExists(context.Context, string) (bool, error)                  { return false, nil }
func (f *fakeSpawnAmux) ListPanes(context.Context) ([]amux.Pane, error)                    { return nil, nil }
func (f *fakeSpawnAmux) SendKeys(context.Context, string, ...string) error                 { return nil }
func (f *fakeSpawnAmux) Capture(context.Context, string) (string, error)                   { return "", nil }
func (f *fakeSpawnAmux) CapturePane(context.Context, string) (amux.PaneCapture, error)     { return amux.PaneCapture{}, nil }
func (f *fakeSpawnAmux) CaptureHistory(context.Context, string) (amux.PaneCapture, error)  { return amux.PaneCapture{}, nil }
func (f *fakeSpawnAmux) SetMetadata(context.Context, string, map[string]string) error       { return nil }
func (f *fakeSpawnAmux) KillPane(context.Context, string) error                             { return nil }
func (f *fakeSpawnAmux) WaitIdle(context.Context, string, time.Duration) error              { return nil }
func (f *fakeSpawnAmux) WaitContent(context.Context, string, string, time.Duration) error   { return nil }

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

func newSpawnStore(t *testing.T) *state.SQLiteStore {
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
