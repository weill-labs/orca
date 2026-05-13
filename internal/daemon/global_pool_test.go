package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	legacy "github.com/weill-labs/orca/internal/state"
)

func TestMultiProjectPoolAdopt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		clone           Clone
		record          legacy.CloneRecord
		ensureErr       error
		occupyOK        bool
		occupyErr       error
		wantErr         string
		wantOccupyCalls int
		wantBranch      string
	}{
		{
			name:            "occupies free clone",
			clone:           Clone{CurrentBranch: "feature", AssignedTask: "LAB-1816"},
			occupyOK:        true,
			wantOccupyCalls: 1,
			wantBranch:      "feature",
		},
		{
			name:            "uses assigned task as branch fallback",
			clone:           Clone{AssignedTask: "LAB-1816"},
			occupyOK:        true,
			wantOccupyCalls: 1,
			wantBranch:      "LAB-1816",
		},
		{
			name:  "idempotent when clone already assigned to task",
			clone: Clone{CurrentBranch: "feature", AssignedTask: "LAB-1816"},
			record: legacy.CloneRecord{
				Status:       legacy.CloneStatusOccupied,
				AssignedTask: "LAB-1816",
			},
			wantOccupyCalls: 0,
		},
		{
			name:      "wraps ensure failure",
			clone:     Clone{CurrentBranch: "feature", AssignedTask: "LAB-1816"},
			ensureErr: errors.New("ensure failed"),
			wantErr:   "ensure adopted clone",
		},
		{
			name:      "wraps occupy failure",
			clone:     Clone{CurrentBranch: "feature", AssignedTask: "LAB-1816"},
			occupyErr: errors.New("occupy failed"),
			wantErr:   "occupy adopted clone",
		},
		{
			name:     "rejects clone occupied by another task",
			clone:    Clone{CurrentBranch: "feature", AssignedTask: "LAB-1816"},
			occupyOK: false,
			wantErr:  "is not free",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			projectPath, clonePath := newGlobalPoolAdoptProject(t)
			tt.clone.Path = clonePath
			if tt.record.Path == "" {
				tt.record.Path = clonePath
			}
			if tt.record.Project == "" {
				tt.record.Project = projectPath
			}
			if tt.record.Status == "" {
				tt.record.Status = legacy.CloneStatusFree
			}
			store := &fakeAdoptStore{
				record:    tt.record,
				ensureErr: tt.ensureErr,
				occupyOK:  tt.occupyOK,
				occupyErr: tt.occupyErr,
			}
			pool := &multiProjectPool{store: store}

			err := pool.Adopt(context.Background(), projectPath, tt.clone)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Adopt() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Adopt() error = %v", err)
			}
			if got := store.occupyCalls; got != tt.wantOccupyCalls {
				t.Fatalf("occupy calls = %d, want %d", got, tt.wantOccupyCalls)
			}
			if tt.wantOccupyCalls > 0 {
				if got := store.occupiedBranch; got != tt.wantBranch {
					t.Fatalf("occupied branch = %q, want %q", got, tt.wantBranch)
				}
				if got, want := store.occupiedTask, tt.clone.AssignedTask; got != want {
					t.Fatalf("occupied task = %q, want %q", got, want)
				}
				if got, want := store.occupiedPath, clonePath; got != want {
					t.Fatalf("occupied path = %q, want %q", got, want)
				}
			}
		})
	}
}

func TestMultiProjectPoolAdoptRejectsInvalidClonePath(t *testing.T) {
	t.Parallel()

	projectPath, _ := newGlobalPoolAdoptProject(t)
	pool := &multiProjectPool{store: &fakeAdoptStore{occupyOK: true}}

	err := pool.Adopt(context.Background(), projectPath, Clone{
		Path:         filepath.Join(filepath.Dir(projectPath), "outside-clone"),
		AssignedTask: "LAB-1816",
	})
	if err == nil || !strings.Contains(err.Error(), "must stay inside pool root") {
		t.Fatalf("Adopt() error = %v, want invalid clone path", err)
	}
}

func TestMultiProjectPoolAdoptRejectsInvalidProjectPath(t *testing.T) {
	t.Parallel()

	projectPath := filepath.Join(t.TempDir(), "missing")
	pool := &multiProjectPool{store: &fakeAdoptStore{occupyOK: true}}

	err := pool.Adopt(context.Background(), projectPath, Clone{
		Path:         filepath.Join(projectPath, OrcaPoolSubdir, "clone-01"),
		AssignedTask: "LAB-1816",
	})
	if err == nil || !strings.Contains(err.Error(), "canonicalize project path") {
		t.Fatalf("Adopt() error = %v, want project canonicalization failure", err)
	}
}

func newGlobalPoolAdoptProject(t *testing.T) (string, string) {
	t.Helper()

	projectPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(projectPath, ".git"), 0o755); err != nil {
		t.Fatalf("create .git: %v", err)
	}
	clonePath := filepath.Join(projectPath, OrcaPoolSubdir, "clone-01")
	return projectPath, clonePath
}

type fakeAdoptStore struct {
	record         legacy.CloneRecord
	ensureErr      error
	occupyOK       bool
	occupyErr      error
	ensureCalls    int
	occupyCalls    int
	occupiedPath   string
	occupiedBranch string
	occupiedTask   string
}

func (s *fakeAdoptStore) EnsureClone(_ context.Context, project, path string) (legacy.CloneRecord, error) {
	s.ensureCalls++
	if s.ensureErr != nil {
		return legacy.CloneRecord{}, s.ensureErr
	}
	record := s.record
	record.Project = project
	record.Path = path
	return record, nil
}

func (s *fakeAdoptStore) TryOccupyClone(_ context.Context, _ string, path, branch, task string) (bool, error) {
	s.occupyCalls++
	s.occupiedPath = path
	s.occupiedBranch = branch
	s.occupiedTask = task
	if s.occupyErr != nil {
		return false, s.occupyErr
	}
	return s.occupyOK, nil
}

func (s *fakeAdoptStore) MarkCloneFree(context.Context, string, string) error {
	return nil
}
