package pool_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/state"
)

func TestAdapterAcquireReturnsNamedClone(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	project := filepath.Join(root, "project")
	poolDir := filepath.Join(root, "pool")
	mustMkdir(t, poolDir)
	origin := newOrigin(t, "main")
	clonePath := newClone(t, origin, filepath.Join(poolDir, "clone-01"))
	store := newStore(t)
	manager := newManager(t, project, staticConfig{
		poolDir:     poolDir,
		cloneOrigin: origin,
	}, store)

	adapter := pool.NewAdapter(manager)
	clone, err := adapter.Acquire(context.Background(), project, "LAB-718")
	if err != nil {
		t.Fatalf("Acquire() error = %v", err)
	}

	if got, want := clone.Name, "clone-01"; got != want {
		t.Fatalf("clone.Name = %q, want %q", got, want)
	}
	if got, want := clone.Path, clonePath; got != want {
		t.Fatalf("clone.Path = %q, want %q", got, want)
	}
	if got, want := clone.CurrentBranch, "LAB-718"; got != want {
		t.Fatalf("clone.CurrentBranch = %q, want %q", got, want)
	}
}

func TestAdapterReleaseUsesCloneBranchInformation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		currentBranch string
		assignedTask  string
	}{
		{
			name:          "prefers current branch",
			currentBranch: "LAB-718",
			assignedTask:  "LAB-999",
		},
		{
			name:          "falls back to assigned task",
			currentBranch: "",
			assignedTask:  "LAB-718",
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
			origin := newOrigin(t, "main")
			clonePath := newClone(t, origin, filepath.Join(poolDir, "clone-01"))
			store := newStore(t)
			manager := newManager(t, project, staticConfig{
				poolDir:     poolDir,
				cloneOrigin: origin,
			}, store)
			adapter := pool.NewAdapter(manager)

			if _, err := store.EnsureClone(context.Background(), project, clonePath); err != nil {
				t.Fatalf("EnsureClone() error = %v", err)
			}
			ok, err := store.TryOccupyClone(context.Background(), project, clonePath, "LAB-718", "LAB-718")
			if err != nil {
				t.Fatalf("TryOccupyClone() error = %v", err)
			}
			if !ok {
				t.Fatal("TryOccupyClone() = false, want true")
			}

			err = adapter.Release(context.Background(), project, pool.Clone{
				Path:          clonePath,
				CurrentBranch: tc.currentBranch,
				AssignedTask:  tc.assignedTask,
			})
			if err != nil {
				t.Fatalf("Release() error = %v", err)
			}

			record := lookupClone(t, store, project, clonePath)
			if got, want := record.Status, state.CloneStatusFree; got != want {
				t.Fatalf("record.Status = %q, want %q", got, want)
			}
		})
	}
}

func TestAdapterRecordsCloneFailureAndSuccess(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	project := filepath.Join(root, "project")
	poolDir := filepath.Join(root, "pool")
	clonePath := filepath.Join(poolDir, "clone-01")
	mustMkdir(t, clonePath)
	markClone(t, clonePath)
	store := newStore(t)
	manager := newManager(t, project, staticConfig{
		poolDir: poolDir,
	}, store)
	adapter := pool.NewAdapter(manager)

	if _, err := store.EnsureClone(context.Background(), project, clonePath); err != nil {
		t.Fatalf("EnsureClone() error = %v", err)
	}
	if err := adapter.RecordCloneFailure(context.Background(), project, pool.Clone{Path: clonePath}); err != nil {
		t.Fatalf("RecordCloneFailure() error = %v", err)
	}
	record := lookupClone(t, store, project, clonePath)
	if got, want := record.FailureCount, 1; got != want {
		t.Fatalf("record.FailureCount after failure = %d, want %d", got, want)
	}

	if err := adapter.RecordCloneSuccess(context.Background(), project, pool.Clone{Path: clonePath}); err != nil {
		t.Fatalf("RecordCloneSuccess() error = %v", err)
	}
	record = lookupClone(t, store, project, clonePath)
	if got, want := record.FailureCount, 0; got != want {
		t.Fatalf("record.FailureCount after success = %d, want %d", got, want)
	}
}
