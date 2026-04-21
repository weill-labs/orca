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
	poolDir     string
	cloneOrigin string
	baseBranch  string
}

func (c staticConfig) PoolDir() string     { return c.poolDir }
func (c staticConfig) CloneOrigin() string  { return c.cloneOrigin }
func (c staticConfig) BaseBranch() string   { return c.baseBranch }

func TestManagerDiscover(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		setup        func(t *testing.T, poolDir string, store *state.SQLiteStore, project string)
		wantPaths    []string
		wantStatuses map[string]pool.Status
	}{
		{
			name: "returns only subdirectories of pool dir",
			setup: func(t *testing.T, poolDir string, store *state.SQLiteStore, project string) {
				t.Helper()

				mustMkdir(t, filepath.Join(poolDir, "clone-01"))
				mustMkdir(t, filepath.Join(poolDir, "clone-02"))
				// regular file should be ignored
				mustWriteFile(t, filepath.Join(poolDir, "notes.txt"), "not a clone")
			},
			wantPaths: []string{
				"clone-01",
				"clone-02",
			},
			wantStatuses: map[string]pool.Status{
				"clone-01": pool.StatusFree,
				"clone-02": pool.StatusFree,
			},
		},
		{
			name: "returns empty when pool dir does not exist",
			setup: func(t *testing.T, poolDir string, store *state.SQLiteStore, project string) {
				t.Helper()
				// remove the pool dir that was created by setup
				_ = os.RemoveAll(poolDir)
			},
			wantPaths: nil,
		},
		{
			name: "preserves occupied status from state",
			setup: func(t *testing.T, poolDir string, store *state.SQLiteStore, project string) {
				t.Helper()

				path := filepath.Join(poolDir, "clone-01")
				mustMkdir(t, path)

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
			},
			wantPaths: []string{
				"clone-01",
			},
			wantStatuses: map[string]pool.Status{
				"clone-01": pool.StatusOccupied,
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			project := filepath.Join(root, "project")
			poolDir := filepath.Join(root, "pool")
			mustMkdir(t, poolDir)
			store := newStore(t)

			tc.setup(t, poolDir, store, project)
			manager := newManager(t, project, staticConfig{poolDir: poolDir}, store)

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
				if tc.wantStatuses != nil {
					if clone.Status != tc.wantStatuses[gotBase] {
						t.Fatalf("clone[%d].Status = %q, want %q", i, clone.Status, tc.wantStatuses[gotBase])
					}
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
		options    func(clones []string) []pool.Option
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
			name: "auto-creates clone when pool is exhausted",
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

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() error = %v", err)
				}

				if clone.Status != pool.StatusOccupied {
					t.Fatalf("clone.Status = %q, want %q", clone.Status, pool.StatusOccupied)
				}
				// auto-created clone should be clone-03 (after clone-01, clone-02)
				if got := filepath.Base(clone.Path); got != "clone-03" {
					t.Fatalf("auto-created clone name = %q, want %q", got, "clone-03")
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
					t.Fatalf("git status --porcelain = %q, want clean worktree", got)
				}
				if _, err := os.Stat(filepath.Join(clone.Path, "junk.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("junk.txt stat error = %v, want not exist", err)
				}
			},
		},
		{
			name: "repairs diverged base branch before creating issue branch",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				expectedHead := divergeCloneBaseBranch(t, clones[0], "main")

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() error = %v", err)
				}

				if got, want := gitCurrentBranch(t, clone.Path), "LAB-687"; got != want {
					t.Fatalf("current branch = %q, want %q", got, want)
				}
				if got, want := gitHeadCommit(t, clone.Path), expectedHead; got != want {
					t.Fatalf("HEAD = %q, want %q", got, want)
				}
				if _, err := os.Stat(filepath.Join(clone.Path, "local-only.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("local-only.txt stat error = %v, want not exist", err)
				}
				if _, err := os.Stat(filepath.Join(clone.Path, "remote-only.txt")); err != nil {
					t.Fatalf("remote-only.txt stat error = %v", err)
				}
			},
		},
		{
			name: "skips unhealthy clone and allocates next healthy clone",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				mustRun(t, clones[0], "git", "remote", "set-url", "origin", filepath.Join(t.TempDir(), "missing-origin.git"))

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() error = %v", err)
				}

				if got, want := clone.Path, clones[1]; got != want {
					t.Fatalf("clone.Path = %q, want %q", got, want)
				}

				record := lookupClone(t, store, project, clones[0])
				if got, want := record.Status, state.CloneStatusFree; got != want {
					t.Fatalf("record.Status = %q, want %q", got, want)
				}
			},
		},
		{
			name: "skips free clone whose cwd is already used by another pane",
			options: func(clones []string) []pool.Option {
				return []pool.Option{
					pool.WithCWDUsageChecker(fakeCWDUsageChecker{
						paths: []string{clones[0]},
					}),
				}
			},
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() error = %v", err)
				}

				if got, want := clone.Path, clones[1]; got != want {
					t.Fatalf("clone.Path = %q, want %q", got, want)
				}

				record := lookupClone(t, store, project, clones[0])
				if got, want := record.Status, state.CloneStatusFree; got != want {
					t.Fatalf("record.Status = %q, want %q", got, want)
				}
				if got, want := gitCurrentBranch(t, clones[0]), "main"; got != want {
					t.Fatalf("current branch = %q, want %q", got, want)
				}
			},
		},
		{
			name: "returns checker error before allocation",
			options: func(clones []string) []pool.Option {
				return []pool.Option{
					pool.WithCWDUsageChecker(fakeCWDUsageChecker{
						err: errors.New("list panes failed"),
					}),
				}
			},
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				_, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err == nil || !strings.Contains(err.Error(), "check active pane cwd usage: list panes failed") {
					t.Fatalf("Allocate() error = %v, want checker failure", err)
				}

				for _, clonePath := range clones {
					record := lookupClone(t, store, project, clonePath)
					if got, want := record.Status, state.CloneStatusFree; got != want {
						t.Fatalf("record.Status = %q, want %q", got, want)
					}
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
			poolDir := filepath.Join(root, "pool")
			mustMkdir(t, poolDir)
			origin := newOrigin(t, defaultBaseBranch(tc.baseBranch))
			clones := []string{
				newClone(t, origin, filepath.Join(poolDir, "clone-01")),
				newClone(t, origin, filepath.Join(poolDir, "clone-02")),
			}
			store := newStore(t)
			options := []pool.Option(nil)
			if tc.options != nil {
				options = tc.options(clones)
			}
			manager := newManager(t, project, staticConfig{
				poolDir:     poolDir,
				cloneOrigin: origin,
				baseBranch:  tc.baseBranch,
			}, store, options...)

			tc.run(t, manager, store, project, clones)
		})
	}
}

func TestManagerCreateClone(t *testing.T) {
	t.Parallel()

	t.Run("creates clone-01 in empty pool", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		project := filepath.Join(root, "project")
		poolDir := filepath.Join(root, "pool")
		origin := newOrigin(t, "main")
		store := newStore(t)
		manager := newManager(t, project, staticConfig{
			poolDir:     poolDir,
			cloneOrigin: origin,
		}, store)

		path, err := manager.CreateClone(context.Background())
		if err != nil {
			t.Fatalf("CreateClone() error = %v", err)
		}

		if got := filepath.Base(path); got != "clone-01" {
			t.Fatalf("clone name = %q, want clone-01", got)
		}
		if _, err := os.Stat(filepath.Join(path, ".git")); err != nil {
			t.Fatalf(".git stat error = %v", err)
		}
	})

	t.Run("increments clone number", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		project := filepath.Join(root, "project")
		poolDir := filepath.Join(root, "pool")
		origin := newOrigin(t, "main")
		newClone(t, origin, filepath.Join(poolDir, "clone-01"))
		newClone(t, origin, filepath.Join(poolDir, "clone-02"))
		store := newStore(t)
		manager := newManager(t, project, staticConfig{
			poolDir:     poolDir,
			cloneOrigin: origin,
		}, store)

		path, err := manager.CreateClone(context.Background())
		if err != nil {
			t.Fatalf("CreateClone() error = %v", err)
		}

		if got := filepath.Base(path); got != "clone-03" {
			t.Fatalf("clone name = %q, want clone-03", got)
		}
	})

	t.Run("fails without clone origin", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		project := filepath.Join(root, "project")
		poolDir := filepath.Join(root, "pool")
		store := newStore(t)
		manager := newManager(t, project, staticConfig{
			poolDir: poolDir,
		}, store)

		_, err := manager.CreateClone(context.Background())
		if err == nil {
			t.Fatal("CreateClone() error = nil, want error about missing origin")
		}
		if !errors.Is(err, pool.ErrNoFreeClones) {
			t.Fatalf("CreateClone() error = %v, want ErrNoFreeClones", err)
		}
	})
}

func TestManagerHealthCheck(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		baseBranch string
		setup      func(t *testing.T, clonePath string)
		verify     func(t *testing.T, clonePath string)
		wantErr    string
		verifyErr  func(t *testing.T, err error)
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
					t.Fatalf("git status --porcelain = %q, want clean worktree", got)
				}
				if _, err := os.Stat(filepath.Join(clonePath, "junk.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("junk.txt stat error = %v, want not exist", err)
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
			verifyErr: func(t *testing.T, err error) {
				t.Helper()

				for _, want := range []string{
					"git ls-remote --exit-code origin HEAD",
					"git fetch origin main",
				} {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("HealthCheck() error = %v, want substring %q", err, want)
					}
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
			poolDir := filepath.Join(root, "pool")
			mustMkdir(t, poolDir)
			origin := newOrigin(t, defaultBaseBranch(tc.baseBranch))
			clonePath := newClone(t, origin, filepath.Join(poolDir, "clone-01"))
			store := newStore(t)
			manager := newManager(t, project, staticConfig{
				poolDir:    poolDir,
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
				if tc.verifyErr != nil {
					tc.verifyErr(t, err)
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

func TestManagerHealthCheckHealthyCloneSkipsCleanup(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	project := filepath.Join(root, "project")
	poolDir := filepath.Join(root, "pool")
	mustMkdir(t, poolDir)
	origin := newOrigin(t, "main")
	clonePath := newClone(t, origin, filepath.Join(poolDir, "clone-01"))
	store := newStore(t)
	runner := &recordingRunner{err: errors.New("unexpected cleanup call")}
	manager := newManager(t, project, staticConfig{
		poolDir: poolDir,
	}, store, pool.WithRunner(runner))

	if err := manager.HealthCheck(context.Background(), clonePath); err != nil {
		t.Fatalf("HealthCheck() error = %v", err)
	}
	if got, want := runner.calls, 0; got != want {
		t.Fatalf("cleanup calls = %d, want %d", got, want)
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
			name: "repairs diverged base branch before marking clone free",
			run: func(t *testing.T, manager *pool.Manager, store *state.SQLiteStore, project string, clones []string) {
				t.Helper()

				clone, err := manager.Allocate(context.Background(), "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("Allocate() setup error = %v", err)
				}

				expectedHead := divergeCloneBaseBranch(t, clone.Path, "main")
				mustRun(t, clone.Path, "git", "checkout", "LAB-687")

				if err := manager.Release(context.Background(), clone.Path, "LAB-687"); err != nil {
					t.Fatalf("Release() error = %v", err)
				}

				if got, want := gitCurrentBranch(t, clone.Path), "main"; got != want {
					t.Fatalf("current branch = %q, want %q", got, want)
				}
				if got, want := gitHeadCommit(t, clone.Path), expectedHead; got != want {
					t.Fatalf("HEAD = %q, want %q", got, want)
				}
				if _, err := os.Stat(filepath.Join(clone.Path, "local-only.txt")); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("local-only.txt stat error = %v, want not exist", err)
				}
				if _, err := os.Stat(filepath.Join(clone.Path, "remote-only.txt")); err != nil {
					t.Fatalf("remote-only.txt stat error = %v", err)
				}
				if gitBranchExists(t, clone.Path, "LAB-687") {
					t.Fatal("task branch still exists after Release()")
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

				path := filepath.Join(t.TempDir(), "clone-bad")
				mustMkdir(t, path)

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
			poolDir := filepath.Join(root, "pool")
			mustMkdir(t, poolDir)
			origin := newOrigin(t, defaultBaseBranch(tc.baseBranch))
			clones := []string{
				newClone(t, origin, filepath.Join(poolDir, "clone-01")),
			}
			store := newStore(t)
			manager := newManager(t, project, staticConfig{
				poolDir:     poolDir,
				cloneOrigin: origin,
				baseBranch:  tc.baseBranch,
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
	configureTestGitIdentity(t, source)
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

	return dest
}

func advanceOrigin(t *testing.T, clonePath, baseBranch, relativePath, contents string) string {
	t.Helper()

	origin := strings.TrimSpace(mustOutput(t, clonePath, "git", "remote", "get-url", "origin"))
	worktree := filepath.Join(t.TempDir(), "origin-worktree")
	mustRun(t, "", "git", "clone", origin, worktree)
	configureTestGitIdentity(t, worktree)
	mustRun(t, worktree, "git", "checkout", baseBranch)
	mustWriteFile(t, filepath.Join(worktree, relativePath), contents)
	mustRun(t, worktree, "git", "add", relativePath)
	mustRun(t, worktree, "git", "commit", "-m", "advance origin")
	mustRun(t, worktree, "git", "push", "origin", baseBranch)

	return gitHeadCommit(t, worktree)
}

func divergeCloneBaseBranch(t *testing.T, clonePath, baseBranch string) string {
	t.Helper()

	configureTestGitIdentity(t, clonePath)
	mustRun(t, clonePath, "git", "config", "pull.ff", "only")
	expectedHead := advanceOrigin(t, clonePath, baseBranch, "remote-only.txt", "remote update")
	mustRun(t, clonePath, "git", "checkout", baseBranch)
	mustWriteFile(t, filepath.Join(clonePath, "local-only.txt"), "local update")
	mustRun(t, clonePath, "git", "add", "local-only.txt")
	mustRun(t, clonePath, "git", "commit", "-m", "local divergent commit")

	return expectedHead
}

func configureTestGitIdentity(t *testing.T, dir string) {
	t.Helper()

	mustRun(t, dir, "git", "config", "user.name", "Orca Tests")
	mustRun(t, dir, "git", "config", "user.email", "orca-tests@example.com")
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

func gitHeadCommit(t *testing.T, dir string) string {
	t.Helper()

	return strings.TrimSpace(mustOutput(t, dir, "git", "rev-parse", "HEAD"))
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
		if strings.TrimSpace(line) == "" {
			continue
		}
		return false
	}

	return true
}

type recordingRunner struct {
	calls int
	err   error
}

func (r *recordingRunner) Run(_ context.Context, _, _ string, _ ...string) error {
	r.calls++
	return r.err
}

type fakeCWDUsageChecker struct {
	paths []string
	err   error
}

func (c fakeCWDUsageChecker) ActiveCWDs(context.Context) ([]string, error) {
	if c.err != nil {
		return nil, c.err
	}
	return append([]string(nil), c.paths...), nil
}

func mustMkdir(t *testing.T, path string) {
	t.Helper()

	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", path, err)
	}
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
