package daemon

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
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
		config       ConfigProvider
		store        func(t *testing.T) state.Store
		detect       func(string) (string, error)
		session      *string
		agent        string
		prompt       string
		setupProject func(t *testing.T, project string)
		poolRunner   pool.Runner
		assert       func(t *testing.T, store state.Store, result SpawnPaneResult, amuxClient *fakeSpawnAmux, project string)
		wantErr      string
	}{
		{
			name: "allocates clone and opens pane without creating a task",
			amux: &fakeSpawnAmux{
				spawnPane: amux.Pane{ID: "pane-7", Name: "Scratch pane"},
				listPanes: []amux.Pane{{ID: "99", Name: "lead-pane", Window: "orca"}},
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
				if got, want := req.Window, "orca"; got != want {
					t.Fatalf("spawn window = %q, want %q", got, want)
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
			name: "starts requested agent from configured profile",
			amux: &fakeSpawnAmux{
				spawnPane: amux.Pane{ID: "pane-7", Name: "Scratch pane"},
			},
			config: &fakeConfig{
				profiles: map[string]AgentProfile{
					"scratch": {Name: "scratch", StartCommand: "codex --profile scratch"},
				},
			},
			agent: "scratch",
			assert: func(t *testing.T, _ state.Store, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, _ string) {
				t.Helper()
				if got, want := len(amuxClient.spawnRequests), 1; got != want {
					t.Fatalf("len(spawnRequests) = %d, want %d", got, want)
				}
				if got, want := amuxClient.spawnRequests[0].Command, "codex --profile scratch"; got != want {
					t.Fatalf("spawn command = %q, want %q", got, want)
				}
			},
		},
		{
			name: "sends prompt to spawned agent",
			amux: &fakeSpawnAmux{
				spawnPane:   amux.Pane{ID: "pane-7", Name: "Scratch pane"},
				waitIdleErr: errors.New("still working"),
			},
			config: &fakeConfig{
				profiles: map[string]AgentProfile{
					"scratch": {Name: "scratch", StartCommand: "codex --profile scratch"},
				},
			},
			agent:  "scratch",
			prompt: "explore the repo",
			assert: func(t *testing.T, _ state.Store, _ SpawnPaneResult, amuxClient *fakeSpawnAmux, _ string) {
				t.Helper()
				if got, want := len(amuxClient.spawnRequests), 1; got != want {
					t.Fatalf("len(spawnRequests) = %d, want %d", got, want)
				}
				if got, want := amuxClient.spawnRequests[0].Command, "codex --profile scratch"; got != want {
					t.Fatalf("spawn command = %q, want %q", got, want)
				}
				if got, want := amuxClient.sentKeys["pane-7"], []string{"explore the repo\n"}; !slices.Equal(got, want) {
					t.Fatalf("sent keys = %#v, want %#v", got, want)
				}
				if got, want := len(amuxClient.waitContentCalls), 1; got != want {
					t.Fatalf("len(waitContentCalls) = %d, want %d", got, want)
				}
				if got, want := amuxClient.waitContentCalls[0].Substring, codexWorkingText; got != want {
					t.Fatalf("wait content substring = %q, want %q", got, want)
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

			options := ControllerOptions{
				Store:        store,
				Paths:        Paths{StateDB: filepath.Join(t.TempDir(), "state.db"), PIDDir: filepath.Join(t.TempDir(), "pids")},
				Now:          func() time.Time { return time.Date(2026, 4, 5, 12, 0, 0, 123, time.UTC) },
				DetectOrigin: detectOrigin,
				Amux:         tt.amux,
				PoolRunner:   tt.poolRunner,
				Config:       tt.config,
			}
			controller, err := NewLocalController(options)
			if err != nil {
				t.Fatalf("NewLocalController() error = %v", err)
			}

			req := SpawnPaneRequest{
				Project:  project,
				Session:  session,
				LeadPane: "lead-pane",
				Title:    "Scratch pane",
				Agent:    tt.agent,
				Prompt:   tt.prompt,
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

func TestSpawnWorkerPaneKillsOrphanPaneWithTargetName(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.amux.listPanes = []Pane{{ID: "orphan-1", Name: "w-LAB-1115"}}
	d := deps.newDaemon(t)

	pane, err := d.spawnWorkerPane(context.Background(), Task{Project: "/tmp/project", Issue: "LAB-1115"}, "worker-01", deps.pool.clone.Path, deps.config.profiles["codex"])
	if err != nil {
		t.Fatalf("spawnWorkerPane() error = %v", err)
	}

	if got, want := len(deps.amux.killCalls), 1; got != want {
		t.Fatalf("kill calls = %d, want %d", got, want)
	}
	if got, want := deps.amux.killCalls[0], "orphan-1"; got != want {
		t.Fatalf("kill call = %q, want %q", got, want)
	}
	if got, want := len(deps.amux.spawnRequests), 1; got != want {
		t.Fatalf("spawn requests = %d, want %d", got, want)
	}
	if got, want := deps.amux.spawnRequests[0].Name, "w-LAB-1115"; got != want {
		t.Fatalf("spawn request name = %q, want %q", got, want)
	}

	panes, err := deps.amux.ListPanes(context.Background())
	if err != nil {
		t.Fatalf("ListPanes() error = %v", err)
	}
	if got, want := len(panes), 1; got != want {
		t.Fatalf("len(ListPanes()) = %d, want %d", got, want)
	}
	if got, want := panes[0].ID, pane.ID; got != want {
		t.Fatalf("pane ID = %q, want %q", got, want)
	}
	if got, want := panes[0].Name, "w-LAB-1115"; got != want {
		t.Fatalf("pane name = %q, want %q", got, want)
	}
}

func TestSpawnWorkerPaneFailsWhenTargetNameBelongsToActiveTask(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	now := deps.clock.Now()
	deps.amux.listPanes = []Pane{{ID: "live-pane", Name: "w-LAB-1115"}}
	deps.state.putTaskForTest(Task{
		Project:   "/tmp/project",
		Issue:     "LAB-1115",
		Status:    TaskStatusActive,
		WorkerID:  "worker-99",
		PaneID:    "live-pane",
		PaneName:  "w-LAB-1115",
		CreatedAt: now,
		UpdatedAt: now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-99",
		PaneID:       "live-pane",
		PaneName:     "w-LAB-1115",
		Issue:        "LAB-1115",
		AgentProfile: "codex",
		Health:       WorkerHealthHealthy,
		CreatedAt:    now,
		LastSeenAt:   now,
		UpdatedAt:    now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	d := deps.newDaemon(t)

	_, err := d.spawnWorkerPane(context.Background(), Task{Project: "/tmp/project", Issue: "LAB-1115"}, "worker-01", deps.pool.clone.Path, deps.config.profiles["codex"])
	if err == nil {
		t.Fatal("spawnWorkerPane() succeeded, want active task collision error")
	}
	if got, want := err.Error(), `pane "w-LAB-1115" already exists for active task LAB-1115`; !strings.Contains(got, want) {
		t.Fatalf("spawnWorkerPane() error = %q, want substring %q", got, want)
	}
	if got := len(deps.amux.killCalls); got != 0 {
		t.Fatalf("kill calls = %d, want 0", got)
	}
	if got := len(deps.amux.spawnRequests); got != 0 {
		t.Fatalf("spawn requests = %d, want 0", got)
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
	spawnRequests    []amux.SpawnRequest
	spawnPane        amux.Pane
	spawnErr         error
	listPanes        []amux.Pane
	waitIdleErr      error
	sentKeys         map[string][]string
	waitContentCalls []waitContentCall
}

func (f *fakeSpawnAmux) Spawn(_ context.Context, req amux.SpawnRequest) (amux.Pane, error) {
	f.spawnRequests = append(f.spawnRequests, req)
	if f.spawnErr != nil {
		return amux.Pane{}, f.spawnErr
	}
	return f.spawnPane, nil
}

func (f *fakeSpawnAmux) PaneExists(context.Context, string) (bool, error) { return false, nil }
func (f *fakeSpawnAmux) ListPanes(context.Context) ([]amux.Pane, error)   { return f.listPanes, nil }
func (f *fakeSpawnAmux) Events(context.Context, amux.EventsRequest) (<-chan amux.Event, <-chan error) {
	eventsCh := make(chan amux.Event)
	errCh := make(chan error)
	close(eventsCh)
	close(errCh)
	return eventsCh, errCh
}
func (f *fakeSpawnAmux) Metadata(context.Context, string) (map[string]string, error) {
	return nil, nil
}
func (f *fakeSpawnAmux) SendKeys(_ context.Context, paneID string, keys ...string) error {
	if f.sentKeys == nil {
		f.sentKeys = make(map[string][]string)
	}
	f.sentKeys[paneID] = appendNormalizedSentKeys(f.sentKeys[paneID], normalizeSentKeys(keys...))
	return nil
}
func (f *fakeSpawnAmux) Capture(context.Context, string) (string, error) { return "", nil }
func (f *fakeSpawnAmux) CapturePane(context.Context, string) (amux.PaneCapture, error) {
	return amux.PaneCapture{}, nil
}
func (f *fakeSpawnAmux) CaptureHistory(context.Context, string) (amux.PaneCapture, error) {
	return amux.PaneCapture{}, nil
}
func (f *fakeSpawnAmux) SetMetadata(context.Context, string, map[string]string) error { return nil }
func (f *fakeSpawnAmux) RemoveMetadata(context.Context, string, ...string) error      { return nil }
func (f *fakeSpawnAmux) KillPane(context.Context, string) error                       { return nil }
func (f *fakeSpawnAmux) WaitIdle(context.Context, string, time.Duration) error {
	return f.waitIdleErr
}
func (f *fakeSpawnAmux) WaitIdleSettle(context.Context, string, time.Duration, time.Duration) error {
	return nil
}
func (f *fakeSpawnAmux) WaitContent(_ context.Context, paneID, substring string, timeout time.Duration) error {
	f.waitContentCalls = append(f.waitContentCalls, waitContentCall{PaneID: paneID, Substring: substring, Timeout: timeout})
	return nil
}

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
