package state_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/weill-labs/orca/internal/state"
)

func TestSQLiteCloneStore(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		run  func(t *testing.T, store *state.SQLiteStore, project string)
	}{
		{
			name: "ensure clone inserts free record",
			run: func(t *testing.T, store *state.SQLiteStore, project string) {
				t.Helper()

				path := filepath.Join(t.TempDir(), "orca01")
				record, err := store.EnsureClone(context.Background(), project, path)
				if err != nil {
					t.Fatalf("EnsureClone() error = %v", err)
				}

				if record.Path != path {
					t.Fatalf("record.Path = %q, want %q", record.Path, path)
				}
				if record.Status != state.CloneStatusFree {
					t.Fatalf("record.Status = %q, want %q", record.Status, state.CloneStatusFree)
				}

				records, err := store.ListClones(context.Background(), project)
				if err != nil {
					t.Fatalf("ListClones() error = %v", err)
				}
				if len(records) != 1 {
					t.Fatalf("len(records) = %d, want 1", len(records))
				}
			},
		},
		{
			name: "ensure clone preserves occupied record",
			run: func(t *testing.T, store *state.SQLiteStore, project string) {
				t.Helper()

				path := filepath.Join(t.TempDir(), "orca01")
				if _, err := store.EnsureClone(context.Background(), project, path); err != nil {
					t.Fatalf("EnsureClone() setup error = %v", err)
				}
				ok, err := store.TryOccupyClone(context.Background(), project, path, "LAB-687", "LAB-687")
				if err != nil {
					t.Fatalf("TryOccupyClone() error = %v", err)
				}
				if !ok {
					t.Fatal("TryOccupyClone() = false, want true")
				}

				record, err := store.EnsureClone(context.Background(), project, path)
				if err != nil {
					t.Fatalf("EnsureClone() error = %v", err)
				}

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
			name: "mark clone free clears branch and task",
			run: func(t *testing.T, store *state.SQLiteStore, project string) {
				t.Helper()

				path := filepath.Join(t.TempDir(), "orca01")
				if _, err := store.EnsureClone(context.Background(), project, path); err != nil {
					t.Fatalf("EnsureClone() setup error = %v", err)
				}
				if _, err := store.TryOccupyClone(context.Background(), project, path, "LAB-687", "LAB-687"); err != nil {
					t.Fatalf("TryOccupyClone() setup error = %v", err)
				}
				if err := store.MarkCloneFree(context.Background(), project, path); err != nil {
					t.Fatalf("MarkCloneFree() error = %v", err)
				}

				record, err := store.EnsureClone(context.Background(), project, path)
				if err != nil {
					t.Fatalf("EnsureClone() error = %v", err)
				}

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
			name: "try occupy clone rejects already occupied clone",
			run: func(t *testing.T, store *state.SQLiteStore, project string) {
				t.Helper()

				path := filepath.Join(t.TempDir(), "orca01")
				if _, err := store.EnsureClone(context.Background(), project, path); err != nil {
					t.Fatalf("EnsureClone() setup error = %v", err)
				}
				if _, err := store.TryOccupyClone(context.Background(), project, path, "LAB-687", "LAB-687"); err != nil {
					t.Fatalf("TryOccupyClone() first call error = %v", err)
				}

				ok, err := store.TryOccupyClone(context.Background(), project, path, "LAB-688", "LAB-688")
				if err != nil {
					t.Fatalf("TryOccupyClone() second call error = %v", err)
				}
				if ok {
					t.Fatal("TryOccupyClone() = true, want false")
				}
			},
		},
		{
			name: "record clone failure quarantines at threshold",
			run: func(t *testing.T, store *state.SQLiteStore, project string) {
				t.Helper()

				path := filepath.Join(t.TempDir(), "orca01")
				if _, err := store.EnsureClone(context.Background(), project, path); err != nil {
					t.Fatalf("EnsureClone() setup error = %v", err)
				}

				record, err := store.RecordCloneFailure(context.Background(), project, path, 3)
				if err != nil {
					t.Fatalf("RecordCloneFailure() first error = %v", err)
				}
				if got, want := record.FailureCount, 1; got != want {
					t.Fatalf("record.FailureCount after first failure = %d, want %d", got, want)
				}
				if got, want := record.Status, state.CloneStatusFree; got != want {
					t.Fatalf("record.Status after first failure = %q, want %q", got, want)
				}

				records, err := store.ListClones(context.Background(), project)
				if err != nil {
					t.Fatalf("ListClones() error = %v", err)
				}
				if got, want := records[0].FailureCount, 1; got != want {
					t.Fatalf("ListClones()[0].FailureCount = %d, want %d", got, want)
				}

				if err := store.ResetCloneFailures(context.Background(), project, path); err != nil {
					t.Fatalf("ResetCloneFailures() error = %v", err)
				}
				record, err = store.RecordCloneFailure(context.Background(), project, path, 3)
				if err != nil {
					t.Fatalf("RecordCloneFailure() after reset error = %v", err)
				}
				if got, want := record.FailureCount, 1; got != want {
					t.Fatalf("record.FailureCount after reset failure = %d, want %d", got, want)
				}

				for i := 0; i < 2; i++ {
					record, err = store.RecordCloneFailure(context.Background(), project, path, 3)
					if err != nil {
						t.Fatalf("RecordCloneFailure() threshold call %d error = %v", i+1, err)
					}
				}
				if got, want := record.Status, state.CloneStatusQuarantined; got != want {
					t.Fatalf("record.Status after threshold = %q, want %q", got, want)
				}
				if got, want := record.FailureCount, 3; got != want {
					t.Fatalf("record.FailureCount after threshold = %d, want %d", got, want)
				}

				if err := store.UnquarantineClone(context.Background(), project, path); err != nil {
					t.Fatalf("UnquarantineClone() error = %v", err)
				}
				record, err = store.EnsureClone(context.Background(), project, path)
				if err != nil {
					t.Fatalf("EnsureClone() after unquarantine error = %v", err)
				}
				if got, want := record.Status, state.CloneStatusFree; got != want {
					t.Fatalf("record.Status after unquarantine = %q, want %q", got, want)
				}
				if got, want := record.FailureCount, 0; got != want {
					t.Fatalf("record.FailureCount after unquarantine = %d, want %d", got, want)
				}
			},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

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

			tc.run(t, store, filepath.Join(t.TempDir(), "project"))
		})
	}
}
