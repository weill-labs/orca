package pool_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/state"
)

type staticConfig struct {
	pattern    string
	baseBranch string
}

func (c staticConfig) PoolPattern() string {
	return c.pattern
}

func (c staticConfig) BaseBranch() string {
	return c.baseBranch
}

func TestManagerDiscover(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		setup        func(t *testing.T, root string, store *state.SQLiteStore, project string) string
		options      func(root string) []pool.Option
		wantPaths    []string
		wantStatuses map[string]pool.Status
	}{
		{
			name: "returns only marked directories matching glob",
			setup: func(t *testing.T, root string, store *state.SQLiteStore, project string) string {
				t.Helper()

				mustMkdir(t, filepath.Join(root, "orca01"))
				mustTouch(t, filepath.Join(root, "orca01", ".orca-pool"))
				mustMkdir(t, filepath.Join(root, "orca02"))
				mustMkdir(t, filepath.Join(root, "notes"))
				mustTouch(t, filepath.Join(root, "notes", ".orca-pool"))

				return filepath.Join(root, "orca*")
			},
			wantPaths: []string{
				"orca01",
			},
			wantStatuses: map[string]pool.Status{
				"orca01": pool.StatusFree,
			},
		},
		{
			name: "expands tilde in pool pattern",
			setup: func(t *testing.T, root string, store *state.SQLiteStore, project string) string {
				t.Helper()

				path := filepath.Join(root, "clones", "orca01")
				mustMkdir(t, path)
				mustTouch(t, filepath.Join(path, ".orca-pool"))

				return filepath.Join("~", "clones", "orca*")
			},
			options: func(root string) []pool.Option {
				return []pool.Option{pool.WithHomeDir(root)}
			},
			wantPaths: []string{
				"orca01",
			},
			wantStatuses: map[string]pool.Status{
				"orca01": pool.StatusFree,
			},
		},
		{
			name: "preserves occupied status from state",
			setup: func(t *testing.T, root string, store *state.SQLiteStore, project string) string {
				t.Helper()

				path := filepath.Join(root, "orca01")
				mustMkdir(t, path)
				mustTouch(t, filepath.Join(path, ".orca-pool"))

				if _, err := store.EnsureClone(context.Background(), project, path); err != nil {
					t.Fatalf("EnsureClone() setup error = %v", err)
				}
				ok, err := store.TryOccupyClone(context.Background(), project, path, "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("TryOccupyClone() setup error = %v", err)
				}
				if !ok {
					t.Fatal("TryOccupyClone() setup = false, want true")
				}

				return filepath.Join(root, "orca*")
			},
			wantPaths: []string{
				"orca01",
			},
			wantStatuses: map[string]pool.Status{
				"orca01": pool.StatusOccupied,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			project := filepath.Join(root, "project")
			store := newStore(t)

			pattern := tc.setup(t, root, store, project)
			var options []pool.Option
			if tc.options != nil {
				options = tc.options(root)
			}
			manager := newManager(t, project, staticConfig{pattern: pattern}, store, options...)

			clones, err := manager.Discover(context.Background())
			if err != nil {
				t.Fatalf("Discover() error = %v", err)
			}

			if len(clones) != len(tc.wantPaths) {
				t.Fatalf("len(clones) = %d, want %d", len(clones), len(tc.wantPaths))
			}

			for i, clone := range clones {
				gotBase := filepath.Base(clone.Path)
				if gotBase != tc.wantPaths[i] {
					t.Fatalf("clone[%d].Path base = %q, want %q", i, gotBase, tc.wantPaths[i])
				}
				if clone.Status != tc.wantStatuses[gotBase] {
					t.Fatalf("clone[%d].Status = %q, want %q", i, clone.Status, tc.wantStatuses[gotBase])
				}
			}
		})
	}
}

func TestManagerAllocate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		baseBranch string
		run        func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string)
	}{
		{
			name: "allocates free clone and creates issue branch",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() error = %v", err)
				}

				if clone.Path != clones[0] {
					t.Fatalf("clone.Path = %q, want %q", clone.Path, clones[0])
				}
				if clone.Status != pool.StatusOccupied {
					t.Fatalf("clone.Status = %q, want %q", clone.Status, pool.StatusOccupied)
				}

				if branch := gitCurrentBranch(t, clone.Path); branch != "LAB-687" {
					t.Fatalf("current branch = %q, want %q", branch, "LAB-687")
				}

				record := lookupClone(t, store, project, clone.Path)
				if record.Status != state.CloneStatusOccupied {
					t.Fatalf("record.Status = %q, want %q", record.Status, state.CloneStatusOccupied)
				}
				if record.CurrentBranch != "LAB-687" {
					t.Fatalf("record.CurrentBranch = %q, want %q", record.CurrentBranch, "LAB-687")
				}
				if record.AssignedTask != "LAB-687" {
					t.Fatalf("record.AssignedTask = %q, want %q", record.AssignedTask, "LAB-687")
				}
			},
		},
		{
			name: "returns no free clone error when pool is exhausted",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				for _, clonePath := range clones {
					if _, err := store.EnsureClone(context.Background(), project, clonePath); err != nil {
						t.Fatalf("EnsureClone() setup error = %v", err)
					}
					ok, err := store.TryOccupyClone(context.Background(), project, clonePath, "LAB-600", "LAB-600")
					if err != nil {
						t.Fatalf("TryOccupyClone() setup error = %v", err)
					}
					if !ok {
						t.Fatal("TryOccupyClone() setup = false, want true")
					}
				}

				_, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if !errors.Is(err, pool.ErrNoFreeClones) {
					t.Fatalf("Allocate() error = %v, want ErrNoFreeClones", err)
				}
			},
		},
		{
			name: "repairs unhealthy clone before creating issue branch",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				mustRun(t, clones[0], "git", "checkout", "-b", "scratch")
				mustWriteFile(t, filepath.Join(clones[0], "README.md"), "dirty")
				mustWriteFile(t, filepath.Join(clones[0], "junk.txt"), "junk")

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() error = %v", err)
				}

				if got, want := gitCurrentBranch(t, clone.Path), "LAB-687"; got != want {
					t.Fatalf("current branch = %q, want %q", got, want)
				}
				if got := gitStatusPorcelain(t, clone.Path); !gitStatusClean(got) {
					t.Fatalf("git status --porcelain = %q, want clean worktree except %s", got, ".orca-pool")
				}
				if _, err := os.Stat(filepath.Join(clone.Path, "junk.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("junk.txt stat error = %v, want not exist", err)
				}
				if _, err := os.Stat(filepath.Join(clone.Path, ".orca-pool")); err != nil {
					t.Fatalf(".orca-pool stat error = %v", err)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			project := filepath.Join(root, "project")
			origin := newOrigin(t, defaultBaseBranch(tc.baseBranch))
			clones := []string{
				newClone(t, origin, filepath.Join(root, "orca01")),
				newClone(t, origin, filepath.Join(root, "orca02")),
			}
			store := newStore(t)
			manager := newManager(t, project, staticConfig{
				pattern:    filepath.Join(root, "orca*"),
				baseBranch: tc.baseBranch,
			}, store)

			tc.run(t, manager, store, project, clones)
		})
	}
}

func TestManagerHealthCheck(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		baseBranch string
		setup      func(t *testing.T, clonePath string)
		verify     func(t *testing.T, clonePath string)
		wantErr    string
	}{
		{
			name: "repairs dirty clone back to base branch",
			setup: func(t *testing.T, clonePath string) {
				t.Helper()

				mustRun(t, clonePath, "git", "checkout", "-b", "scratch")
				mustWriteFile(t, filepath.Join(clonePath, "README.md"), "dirty")
				mustWriteFile(t, filepath.Join(clonePath, "junk.txt"), "junk")
			},
			verify: func(t *testing.T, clonePath string) {
				t.Helper()

				if got, want := gitCurrentBranch(t, clonePath), "main"; got != want {
					t.Fatalf("current branch = %q, want %q", got, want)
				}
				if got := gitStatusPorcelain(t, clonePath); !gitStatusClean(got) {
					t.Fatalf("git status --porcelain = %q, want clean worktree except %s", got, ".orca-pool")
				}
				if _, err := os.Stat(filepath.Join(clonePath, "junk.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("junk.txt stat error = %v, want not exist", err)
				}
				if _, err := os.Stat(filepath.Join(clonePath, ".orca-pool")); err != nil {
					t.Fatalf(".orca-pool stat error = %v", err)
				}
			},
		},
		{
			name:       "repairs clone to configured base branch",
			baseBranch: "trunk",
			setup: func(t *testing.T, clonePath string) {
				t.Helper()

				mustRun(t, clonePath, "git", "checkout", "-b", "scratch")
			},
			verify: func(t *testing.T, clonePath string) {
				t.Helper()

				if got, want := gitCurrentBranch(t, clonePath), "trunk"; got != want {
					t.Fatalf("current branch = %q, want %q", got, want)
				}
			},
		},
		{
			name: "returns error when remote is unreachable",
			setup: func(t *testing.T, clonePath string) {
				t.Helper()

				mustRun(t, clonePath, "git", "remote", "set-url", "origin", filepath.Join(t.TempDir(), "missing-origin.git"))
			},
			wantErr: "remote",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			project := filepath.Join(root, "project")
			origin := newOrigin(t, defaultBaseBranch(tc.baseBranch))
			clonePath := newClone(t, origin, filepath.Join(root, "orca01"))
			store := newStore(t)
			manager := newManager(t, project, staticConfig{
				pattern:    filepath.Join(root, "orca*"),
				baseBranch: tc.baseBranch,
			}, store)

			if tc.setup != nil {
				tc.setup(t, clonePath)
			}

			err := manager.HealthCheck(context.Background(), clonePath)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("HealthCheck() error = nil, want substring %q", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("HealthCheck() error = %v, want substring %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("HealthCheck() error = %v", err)
			}

			if tc.verify != nil {
				tc.verify(t, clonePath)
			}
		})
	}
}

func TestManagerRelease(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		baseBranch string
		run        func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string)
	}{
		{
			name: "cleans clone and marks it free",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() setup error = %v", err)
				}

				mustWriteFile(t, filepath.Join(clone.Path, "junk.txt"), "junk")
				mustWriteFile(t, filepath.Join(clone.Path, "README.md"), "modified")

				if err := manager.Release(context.Background(), clone.Path, "LAB-687"); err != nil {
					t.Fatalf("Release() error = %v", err)
				}

				if branch := gitCurrentBranch(t, clone.Path); branch != "main" {
					t.Fatalf("current branch = %q, want %q", branch, "main")
				}
				if _, err := os.Stat(filepath.Join(clone.Path, "junk.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("junk.txt stat error = %v, want not exist", err)
				}
				if _, err := os.Stat(filepath.Join(clone.Path, ".orca-pool")); err != nil {
					t.Fatalf(".orca-pool stat error = %v", err)
				}
				if gitBranchExists(t, clone.Path, "LAB-687") {
					t.Fatal("task branch still exists after Release()")
				}

				record := lookupClone(t, store, project, clone.Path)
				if record.Status != state.CloneStatusFree {
					t.Fatalf("record.Status = %q, want %q", record.Status, state.CloneStatusFree)
				}
				if record.CurrentBranch != "" {
					t.Fatalf("record.CurrentBranch = %q, want empty", record.CurrentBranch)
				}
				if record.AssignedTask != "" {
					t.Fatalf("record.AssignedTask = %q, want empty", record.AssignedTask)
				}
			},
		},
		{
			name:       "uses configured base branch throughout lifecycle",
			baseBranch: "trunk",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() setup error = %v", err)
				}

				if err := manager.Release(context.Background(), clone.Path, "LAB-687"); err != nil {
					t.Fatalf("Release() error = %v", err)
				}

				if branch := gitCurrentBranch(t, clone.Path); branch != "trunk" {
					t.Fatalf("current branch = %q, want %q", branch, "trunk")
				}
			},
		},
		{
			name: "marks clone free when task branch is already missing",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() setup error = %v", err)
				}

				mustRun(t, clone.Path, "git", "checkout", "main")
				mustRun(t, clone.Path, "git", "branch", "-D", "LAB-687")

				if err := manager.Release(context.Background(), clone.Path, "LAB-687"); err != nil {
					t.Fatalf("Release() error = %v", err)
				}

				record := lookupClone(t, store, project, clone.Path)
				if record.Status != state.CloneStatusFree {
					t.Fatalf("record.Status = %q, want %q", record.Status, state.CloneStatusFree)
				}
			},
		},
		{
			name: "leaves clone occupied when cleanup fails",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				path := filepath.Join(t.TempDir(), "orca-bad")
				mustMkdir(t, path)
				mustTouch(t, filepath.Join(path, ".orca-pool"))

				if _, err := store.EnsureClone(context.Background(), project, path); err != nil {
					t.Fatalf("EnsureClone() setup error = %v", err)
				}
				ok, err := store.TryOccupyClone(context.Background(), project, path, "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("TryOccupyClone() setup error = %v", err)
				}
				if !ok {
					t.Fatal("TryOccupyClone() setup = false, want true")
				}

				err = manager.Release(context.Background(), path, "LAB-687")
				if err == nil {
					t.Fatal("Release() error = nil, want non-nil")
				}

				record := lookupClone(t, store, project, path)
				if record.Status != state.CloneStatusOccupied {
					t.Fatalf("record.Status = %q, want %q", record.Status, state.CloneStatusOccupied)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			project := filepath.Join(root, "project")
			origin := newOrigin(t, defaultBaseBranch(tc.baseBranch))
			clones := []string{
				newClone(t, origin, filepath.Join(root, "orca01")),
			}
			store := newStore(t)
			manager := newManager(t, project, staticConfig{
				pattern:    filepath.Join(root, "orca*"),
				baseBranch: tc.baseBranch,
			}, store)

			tc.run(t, manager, store, project, clones)
		})
	}
}

func newStore(t *testing.T) *state.SQLiteStore {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "state.db")
	store, err := state.OpenSQLite(dbPath)
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

func newManager(t *testing.T, project string, cfg staticConfig, store *state.SQLiteStore, options ...pool.Option) *pool.Manager {
	t.Helper()

	manager, err := pool.New(project, cfg, store, options...)
	if err != nil {
		t.Fatalf("pool.New() error = %v", err)
	}

	return manager
}

func newOrigin(t *testing.T, baseBranch string) string {
	t.Helper()

	root := t.TempDir()
	source := filepath.Join(root, "source")
	mustRun(t, "", "git", "init", "-b", baseBranch, source)
	mustRun(t, source, "git", "config", "user.name", "Orca Tests")
	mustRun(t, source, "git", "config", "user.email", "orca-tests@example.com")
	mustWriteFile(t, filepath.Join(source, "README.md"), "hello")
	mustRun(t, source, "git", "add", "README.md")
	mustRun(t, source, "git", "commit", "-m", "initial commit")

	origin := filepath.Join(root, "origin.git")
	mustRun(t, "", "git", "clone", "--bare", source, origin)

	return origin
}

func defaultBaseBranch(baseBranch string) string {
	if strings.TrimSpace(baseBranch) == "" {
		return "main"
	}

	return baseBranch
}

func newClone(t *testing.T, origin, dest string) string {
	t.Helper()

	mustRun(t, "", "git", "clone", origin, dest)
	mustTouch(t, filepath.Join(dest, ".orca-pool"))

	return dest
}

func lookupClone(t *testing.T, store *state.SQLiteStore, project, path string) state.CloneRecord {
	t.Helper()

	records, err := store.ListClones(context.Background(), project)
	if err != nil {
		t.Fatalf("ListClones() error = %v", err)
	}

	for _, record := range records {
		if record.Path == path {
			return record
		}
	}

	t.Fatalf("clone %q not found in state", path)
	return state.CloneRecord{}
}

func gitCurrentBranch(t *testing.T, dir string) string {
	t.Helper()

	return strings.TrimSpace(mustOutput(t, dir, "git", "rev-parse", "--abbrev-ref", "HEAD"))
}

func gitBranchExists(t *testing.T, dir, branch string) bool {
	t.Helper()

	cmd := exec.Command("git", "show-ref", "--verify", "--quiet", "refs/heads/"+branch)
	cmd.Dir = dir
	err := cmd.Run()
	return err == nil
}

func gitStatusPorcelain(t *testing.T, dir string) string {
	t.Helper()

	return strings.TrimSpace(mustOutput(t, dir, "git", "status", "--porcelain"))
}

func gitStatusClean(status string) bool {
	for _, line := range strings.Split(strings.TrimSpace(status), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line == "?? .orca-pool" {
			continue
		}
		return false
	}

	return true
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()

	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", path, err)
	}
}

func mustTouch(t *testing.T, path string) {
	t.Helper()

	mustWriteFile(t, path, "")
}

func mustWriteFile(t *testing.T, path, contents string) {
	t.Helper()

	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func mustRun(t *testing.T, dir, name string, args ...string) {
	t.Helper()

	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}
}

func mustOutput(t *testing.T, dir, name string, args ...string) string {
	t.Helper()

	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, out)
	}

	return string(out)
}
