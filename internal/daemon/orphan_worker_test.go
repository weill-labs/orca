package daemon

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/pool"
)

func TestWorkersForIssueWithEmptyIssueReturnsNil(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	// Seed a worker so we'd detect any accidental list traversal.
	seedOrphanWorker(t, deps, "LAB-1863", "worker-stub", "pane-stub", "")

	d := deps.newDaemon(t)

	got, err := d.workersForIssue(context.Background(), "/tmp/project", "")
	if err != nil {
		t.Fatalf("workersForIssue(empty) error = %v, want nil", err)
	}
	if got != nil {
		t.Fatalf("workersForIssue(empty) = %#v, want nil", got)
	}
}

func TestDaemonStartSweepsOrphanWorkersAndKeepsHealthyWorkers(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	orphanClonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-orphan")
	markClonePathForTest(t, orphanClonePath)
	seedOrphanWorker(t, deps, "LAB-1852", "worker-orphan", "pane-orphan", orphanClonePath)
	seedActiveAssignment(t, deps, "LAB-1853", "pane-healthy")
	deps.amux.paneExists = map[string]bool{"pane-healthy": true}

	healthyTask, ok := deps.state.task("LAB-1853")
	if !ok {
		t.Fatal("healthy task missing after seed")
	}
	healthyWorkerBefore, ok := deps.state.worker(healthyTask.WorkerID)
	if !ok {
		t.Fatal("healthy worker missing after seed")
	}

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "orphan worker startup cleanup", func() bool {
		_, orphanExists := deps.state.worker("worker-orphan")
		healthyWorker, healthyExists := deps.state.worker(healthyTask.WorkerID)
		return !orphanExists &&
			healthyExists &&
			healthyWorker.Issue == healthyWorkerBefore.Issue &&
			healthyWorker.PaneID == healthyWorkerBefore.PaneID
	})

	if _, ok := deps.state.worker("worker-orphan"); ok {
		t.Fatal("orphan worker still present after startup")
	}
	healthyWorker, ok := deps.state.worker(healthyTask.WorkerID)
	if !ok {
		t.Fatal("healthy worker missing after startup")
	}
	if got, want := healthyWorker.Issue, "LAB-1853"; got != want {
		t.Fatalf("healthy worker issue = %q, want %q", got, want)
	}
	if got, want := healthyWorker.PaneID, "pane-healthy"; got != want {
		t.Fatalf("healthy worker pane = %q, want %q", got, want)
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          "clone-orphan",
		Path:          orphanClonePath,
		CurrentBranch: "LAB-1852",
		AssignedTask:  "LAB-1852",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("startup kill calls = %#v, want none", got)
	}
	deps.events.requireTypes(t, EventReconcileFinding, EventDaemonStarted)
	if got := deps.events.lastMessage(EventReconcileFinding); !strings.Contains(got, ReconcileOrphanWorker) {
		t.Fatalf("reconcile event message = %q, want orphan worker kind", got)
	}
}

func TestDaemonStartSweepsEmptyProjectOrphanWorkerFromClonePath(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.pool.requireProjectRelease = true
	orphanClonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-orphan")
	markClonePathForTest(t, orphanClonePath)
	worker := seedOrphanWorker(t, deps, "LAB-1885", "worker-empty-project", "pane-empty-project", orphanClonePath)
	worker.Project = ""
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker(empty project) error = %v", err)
	}

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Project = ""
	})
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "empty-project orphan worker startup cleanup", func() bool {
		_, orphanExists := deps.state.worker("worker-empty-project")
		return !orphanExists
	})

	if _, ok := deps.state.worker("worker-empty-project"); ok {
		t.Fatal("empty-project orphan worker still present after startup")
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          "clone-orphan",
		Path:          orphanClonePath,
		CurrentBranch: "LAB-1885",
		AssignedTask:  "LAB-1885",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	event, ok := deps.events.lastEventOfType(EventReconcileFinding)
	if !ok {
		t.Fatal("reconcile finding event missing")
	}
	if event.Project != "/tmp/project" || event.WorkerID != "worker-empty-project" {
		t.Fatalf("reconcile event = %#v, want inferred project and worker ID", event)
	}
}

func TestDaemonStartDeletesEmptyProjectOrphanWorkerWithNoClone(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.pool.requireProjectRelease = true
	worker := seedOrphanWorker(t, deps, "LAB-1886", "worker-no-clone", "pane-no-clone", "")
	worker.Project = ""
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker(empty project) error = %v", err)
	}

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Project = ""
	})
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "empty-project no-clone orphan worker startup cleanup", func() bool {
		_, orphanExists := deps.state.worker("worker-no-clone")
		return !orphanExists
	})

	if _, ok := deps.state.worker("worker-no-clone"); ok {
		t.Fatal("empty-project no-clone orphan worker still present after startup")
	}
	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
}

func TestDaemonStartEmitsFindingWhenOrphanWorkerCleanupFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.pool.releaseErr = errors.New("release unavailable")
	orphanClonePath := filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-orphan")
	markClonePathForTest(t, orphanClonePath)
	seedOrphanWorker(t, deps, "LAB-1887", "worker-cleanup-fails", "pane-cleanup-fails", orphanClonePath)

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "orphan worker cleanup failure event", func() bool {
		event, ok := deps.events.lastEventOfType(EventReconcileFinding)
		return ok &&
			event.WorkerID == "worker-cleanup-fails" &&
			strings.Contains(event.Message, "release unavailable")
	})

	if _, ok := deps.state.worker("worker-cleanup-fails"); !ok {
		t.Fatal("orphan worker was deleted after cleanup failure")
	}
	event, _ := deps.events.lastEventOfType(EventReconcileFinding)
	if event.WorkerID != "worker-cleanup-fails" {
		t.Fatalf("reconcile event worker = %q, want worker-cleanup-fails", event.WorkerID)
	}
	for _, want := range []string{ReconcileOrphanWorker, "release unavailable"} {
		if !strings.Contains(event.Message, want) {
			t.Fatalf("reconcile event message = %q, want to contain %q", event.Message, want)
		}
	}
}

func TestDaemonStartDeletesOrphanWorkerWithMissingCloneMarker(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	orphanClonePath := filepath.Join(t.TempDir(), OrcaPoolSubdir, "clone-missing-marker")
	if err := os.MkdirAll(orphanClonePath, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", orphanClonePath, err)
	}
	deps.pool.releaseErr = errors.New("pool: clone path is missing .orca-pool marker")
	seedOrphanWorker(t, deps, "LAB-1889", "worker-missing-marker", "pane-missing-marker", orphanClonePath)

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "missing-marker orphan worker cleanup", func() bool {
		_, orphanExists := deps.state.worker("worker-missing-marker")
		return !orphanExists
	})

	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	event, ok := deps.events.lastEventOfType(EventReconcileFinding)
	if !ok {
		t.Fatal("reconcile finding event missing")
	}
	if event.WorkerID != "worker-missing-marker" || !strings.Contains(event.Message, ReconcileOrphanWorker) {
		t.Fatalf("reconcile event = %#v, want missing-marker orphan cleanup", event)
	}
}

func TestDaemonStartDeletesOrphanWorkerWithMissingCloneDirectory(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	orphanClonePath := filepath.Join(t.TempDir(), OrcaPoolSubdir, "clone-missing-dir")
	deps.pool.releaseErr = errors.New("pool: clone path does not exist")
	seedOrphanWorker(t, deps, "LAB-1889", "worker-missing-dir", "pane-missing-dir", orphanClonePath)

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "missing-directory orphan worker cleanup", func() bool {
		_, orphanExists := deps.state.worker("worker-missing-dir")
		return !orphanExists
	})

	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	event, ok := deps.events.lastEventOfType(EventReconcileFinding)
	if !ok {
		t.Fatal("reconcile finding event missing")
	}
	if event.WorkerID != "worker-missing-dir" || !strings.Contains(event.Message, ReconcileOrphanWorker) {
		t.Fatalf("reconcile event = %#v, want missing-directory orphan cleanup", event)
	}
}

func TestDaemonStartPrunesMissingPoolEntriesBeforeOrphanWorkerCleanup(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	orphanClonePath := filepath.Join(t.TempDir(), OrcaPoolSubdir, "clone-missing-record")
	deps.state.putCloneForTest(state.Clone{
		Path:      orphanClonePath,
		Status:    string(pool.StatusOccupied),
		Issue:     "LAB-1889",
		Branch:    "LAB-1889",
		UpdatedAt: deps.clock.Now(),
	})
	deps.pool.releaseErr = errors.New("pool release should be skipped after startup prune")
	seedOrphanWorker(t, deps, "LAB-1889", "worker-missing-record", "pane-missing-record", orphanClonePath)

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "missing pool entry prune and orphan worker cleanup", func() bool {
		_, orphanExists := deps.state.worker("worker-missing-record")
		_, cloneExists := deps.state.clone(orphanClonePath)
		return !orphanExists && !cloneExists
	})

	if got := deps.pool.releasedClones(); len(got) != 0 {
		t.Fatalf("released clones = %#v, want none", got)
	}
	pruneEvent, ok := deps.events.lastEventOfType(EventPoolEntryPruned)
	if !ok {
		t.Fatal("pool prune event missing")
	}
	if pruneEvent.ClonePath != orphanClonePath || pruneEvent.CloneName != "clone-missing-record" {
		t.Fatalf("pool prune event = %#v, want missing record clone", pruneEvent)
	}
}
