package pool_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/state"
)

func TestAllocateReportsQuarantinedClonesWhenNoFreeCloneExists(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	project := root + "/project"
	poolDir := root + "/pool"
	mustMkdir(t, poolDir)
	origin := newOrigin(t, "main")
	clonePath := newClone(t, origin, poolDir+"/clone-01")
	store := newQuarantineStore(map[string]state.CloneRecord{
		clonePath: {Path: clonePath, Status: state.CloneStatus("quarantined")},
	})
	manager, err := pool.New(project, staticConfig{
		poolDir:     poolDir,
		cloneOrigin: origin,
	}, store)
	if err != nil {
		t.Fatalf("pool.New() error = %v", err)
	}

	_, err = manager.Allocate(context.Background(), "LAB-1697", "LAB-1697")
	if err == nil {
		t.Fatal("Allocate() succeeded, want quarantined clone error")
	}
	if got, want := err.Error(), "no available clones; 1 are quarantined: clone-01"; !strings.Contains(got, want) {
		t.Fatalf("Allocate() error = %v, want %q", err, want)
	}
}

func TestAllocateSkipsQuarantinedClone(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	project := root + "/project"
	poolDir := root + "/pool"
	mustMkdir(t, poolDir)
	origin := newOrigin(t, "main")
	quarantinedPath := newClone(t, origin, poolDir+"/clone-01")
	healthyPath := newClone(t, origin, poolDir+"/clone-02")
	store := newQuarantineStore(map[string]state.CloneRecord{
		quarantinedPath: {Path: quarantinedPath, Status: state.CloneStatus("quarantined")},
		healthyPath:     {Path: healthyPath, Status: state.CloneStatusFree},
	})
	manager, err := pool.New(project, staticConfig{
		poolDir:     poolDir,
		cloneOrigin: origin,
	}, store)
	if err != nil {
		t.Fatalf("pool.New() error = %v", err)
	}

	clone, err := manager.Allocate(context.Background(), "LAB-1697", "LAB-1697")
	if err != nil {
		t.Fatalf("Allocate() error = %v", err)
	}
	if clone.Path != healthyPath {
		t.Fatalf("allocated clone = %q, want %q", clone.Path, healthyPath)
	}
}

type quarantineStore struct {
	mu      sync.Mutex
	records map[string]state.CloneRecord
}

func newQuarantineStore(records map[string]state.CloneRecord) *quarantineStore {
	copied := make(map[string]state.CloneRecord, len(records))
	for path, record := range records {
		copied[path] = record
	}
	return &quarantineStore{records: copied}
}

func (s *quarantineStore) EnsureClone(_ context.Context, project, path string) (state.CloneRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[path]
	if !ok {
		record = state.CloneRecord{Project: project, Path: path, Status: state.CloneStatusFree}
		s.records[path] = record
	}
	record.Project = project
	record.Path = path
	return record, nil
}

func (s *quarantineStore) TryOccupyClone(_ context.Context, project, path, branch, task string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[path]
	if !ok || record.Status != state.CloneStatusFree {
		return false, nil
	}
	record.Project = project
	record.Status = state.CloneStatusOccupied
	record.CurrentBranch = branch
	record.AssignedTask = task
	s.records[path] = record
	return true, nil
}

func (s *quarantineStore) MarkCloneFree(_ context.Context, project, path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.records[path]
	record.Project = project
	record.Status = state.CloneStatusFree
	record.CurrentBranch = ""
	record.AssignedTask = ""
	s.records[path] = record
	return nil
}

var _ pool.Store = (*quarantineStore)(nil)
