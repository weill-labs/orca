package state

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	legacy "github.com/weill-labs/orca/internal/state"
)

func TestPostgresStoreLifecycleAndQueries(t *testing.T) {
	store := newTestPostgresStore(t)
	project := "/repo"
	now := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	status, err := store.ProjectStatus(context.Background(), project)
	if err != nil {
		t.Fatalf("ProjectStatus() empty error = %v", err)
	}
	if status.Daemon != nil {
		t.Fatalf("empty status daemon = %#v, want nil", status.Daemon)
	}

	if err := store.UpsertDaemon(context.Background(), project, DaemonStatus{
		Session: "orca",
		PID:     42,
		Status:  "running",
	}); err != nil {
		t.Fatalf("UpsertDaemon() error = %v", err)
	}

	clonePath := filepath.Join(t.TempDir(), "orca01")
	record, err := store.EnsureClone(context.Background(), project, clonePath)
	if err != nil {
		t.Fatalf("EnsureClone() error = %v", err)
	}
	if got, want := record.Status, legacy.CloneStatusFree; got != want {
		t.Fatalf("record.Status = %q, want %q", got, want)
	}

	ok, err := store.TryOccupyClone(context.Background(), project, clonePath, "LAB-718", "LAB-718")
	if err != nil {
		t.Fatalf("TryOccupyClone() error = %v", err)
	}
	if !ok {
		t.Fatal("TryOccupyClone() = false, want true")
	}

	ok, err = store.TryOccupyClone(context.Background(), project, clonePath, "LAB-718", "LAB-718")
	if err != nil {
		t.Fatalf("TryOccupyClone() second call error = %v", err)
	}
	if ok {
		t.Fatal("TryOccupyClone() second call = true, want false")
	}

	prNumber := 17
	if err := store.UpsertTask(context.Background(), project, Task{
		Issue:     "LAB-718",
		Status:    "active",
		Agent:     "codex",
		Prompt:    "Implement socket IPC",
		WorkerID:  "worker-01",
		ClonePath: clonePath,
		PRNumber:  &prNumber,
	}); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}

	if err := store.UpsertWorker(context.Background(), project, Worker{
		WorkerID:      "worker-01",
		CurrentPaneID: "pane-1",
		Agent:         "codex",
		State:         "healthy",
		Issue:         "LAB-718",
		ClonePath:     clonePath,
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	firstEvent, err := store.AppendEvent(context.Background(), Event{
		Project:  project,
		Kind:     "task.assigned",
		Issue:    "LAB-718",
		WorkerID: "worker-01",
		Message:  "LAB-718 assigned",
		Payload:  []byte(`{"pane":"pane-1"}`),
	})
	if err != nil {
		t.Fatalf("AppendEvent() error = %v", err)
	}
	secondEvent, err := store.AppendEvent(context.Background(), Event{
		Project: project,
		Kind:    "task.updated",
		Issue:   "LAB-718",
		Message: "LAB-718 updated",
	})
	if err != nil {
		t.Fatalf("AppendEvent() second error = %v", err)
	}

	status, err = store.ProjectStatus(context.Background(), project)
	if err != nil {
		t.Fatalf("ProjectStatus() error = %v", err)
	}
	if status.Daemon == nil || status.Daemon.Status != "running" {
		t.Fatalf("status.Daemon = %#v, want running daemon", status.Daemon)
	}
	if got, want := status.Summary.Active, 1; got != want {
		t.Fatalf("status.Summary.Active = %d, want %d", got, want)
	}
	if got, want := status.Summary.Workers, 1; got != want {
		t.Fatalf("status.Summary.Workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.FreeClones, 0; got != want {
		t.Fatalf("status.Summary.FreeClones = %d, want %d", got, want)
	}

	taskStatus, err := store.TaskStatus(context.Background(), project, "LAB-718")
	if err != nil {
		t.Fatalf("TaskStatus() error = %v", err)
	}
	if got, want := taskStatus.Task.Issue, "LAB-718"; got != want {
		t.Fatalf("taskStatus.Task.Issue = %q, want %q", got, want)
	}
	if taskStatus.Task.PRNumber == nil || *taskStatus.Task.PRNumber != 17 {
		t.Fatalf("taskStatus.Task.PRNumber = %#v, want 17", taskStatus.Task.PRNumber)
	}
	if got, want := len(taskStatus.Events), 2; got != want {
		t.Fatalf("len(taskStatus.Events) = %d, want %d", got, want)
	}
	if string(taskStatus.Events[0].Payload) != `{"pane":"pane-1"}` {
		t.Fatalf("taskStatus.Events[0].Payload = %s", taskStatus.Events[0].Payload)
	}

	workers, err := store.ListWorkers(context.Background(), project)
	if err != nil {
		t.Fatalf("ListWorkers() error = %v", err)
	}
	if got, want := len(workers), 1; got != want {
		t.Fatalf("len(workers) = %d, want %d", got, want)
	}

	clones, err := store.ListClones(context.Background(), project)
	if err != nil {
		t.Fatalf("ListClones() error = %v", err)
	}
	if got, want := len(clones), 1; got != want {
		t.Fatalf("len(clones) = %d, want %d", got, want)
	}
	if got, want := clones[0].Status, "occupied"; got != want {
		t.Fatalf("clones[0].Status = %q, want %q", got, want)
	}

	eventsCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventsCh, errCh := store.Events(eventsCtx, project, firstEvent.ID)
	select {
	case event := <-eventsCh:
		if event.ID != secondEvent.ID {
			t.Fatalf("Events() event.ID = %d, want %d", event.ID, secondEvent.ID)
		}
	case err := <-errCh:
		t.Fatalf("Events() err = %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Events()")
	}
	cancel()
	select {
	case err, ok := <-errCh:
		if ok && err != nil {
			t.Fatalf("Events() unexpected err after cancel = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Events() cancel shutdown")
	}

	updatedTask, err := store.UpdateTaskStatus(context.Background(), project, "LAB-718", "cancelled", time.Time{})
	if err != nil {
		t.Fatalf("UpdateTaskStatus() error = %v", err)
	}
	if got, want := updatedTask.Status, "cancelled"; got != want {
		t.Fatalf("updatedTask.Status = %q, want %q", got, want)
	}

	if err := store.DeleteWorker(context.Background(), project, "worker-01"); err != nil {
		t.Fatalf("DeleteWorker() error = %v", err)
	}
	if err := store.DeleteWorker(context.Background(), project, "worker-01"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteWorker() second error = %v, want ErrNotFound", err)
	}

	if err := store.MarkCloneFree(context.Background(), project, clonePath); err != nil {
		t.Fatalf("MarkCloneFree() error = %v", err)
	}
	if err := store.MarkCloneFree(context.Background(), project, "missing"); !errors.Is(err, legacy.ErrCloneNotFound) {
		t.Fatalf("MarkCloneFree() missing error = %v, want ErrCloneNotFound", err)
	}

	if err := store.MarkDaemonStopped(context.Background(), project, time.Time{}); err != nil {
		t.Fatalf("MarkDaemonStopped() error = %v", err)
	}
	status, err = store.ProjectStatus(context.Background(), project)
	if err != nil {
		t.Fatalf("ProjectStatus() after stop error = %v", err)
	}
	if status.Daemon == nil || status.Daemon.Status != "stopped" || status.Daemon.PID != 0 {
		t.Fatalf("status.Daemon after stop = %#v, want stopped daemon", status.Daemon)
	}
}

func TestPostgresStoreGlobalQueriesAndMergeQueue(t *testing.T) {
	store := newTestPostgresStore(t)
	now := time.Date(2026, 4, 3, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	for _, tc := range []struct {
		project   string
		issue     string
		workerID  string
		clonePath string
		prNumber  int
	}{
		{project: "/repo-a", issue: "LAB-901", workerID: "worker-01", clonePath: "/clones/a", prNumber: 43},
		{project: "/repo-b", issue: "LAB-902", workerID: "worker-02", clonePath: "/clones/b", prNumber: 42},
	} {
		if _, err := store.EnsureClone(context.Background(), tc.project, tc.clonePath); err != nil {
			t.Fatalf("EnsureClone(%s) error = %v", tc.project, err)
		}
		if tc.project == "/repo-a" {
			ok, err := store.TryOccupyClone(context.Background(), tc.project, tc.clonePath, tc.issue, tc.issue)
			if err != nil {
				t.Fatalf("TryOccupyClone(%s) error = %v", tc.project, err)
			}
			if !ok {
				t.Fatalf("TryOccupyClone(%s) = false, want true", tc.project)
			}
		}
		if err := store.UpsertTask(context.Background(), tc.project, Task{
			Issue:     tc.issue,
			Status:    "active",
			Agent:     "codex",
			Prompt:    "Ship it",
			WorkerID:  tc.workerID,
			ClonePath: tc.clonePath,
			PRNumber:  &tc.prNumber,
		}); err != nil {
			t.Fatalf("UpsertTask(%s) error = %v", tc.issue, err)
		}
		if err := store.UpsertWorker(context.Background(), tc.project, Worker{
			WorkerID:      tc.workerID,
			CurrentPaneID: "pane-" + tc.workerID,
			Agent:         "codex",
			State:         "healthy",
			Issue:         tc.issue,
			ClonePath:     tc.clonePath,
		}); err != nil {
			t.Fatalf("UpsertWorker(%s) error = %v", tc.workerID, err)
		}
	}

	position, err := store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:   "/repo-a",
		Issue:     "LAB-901",
		PRNumber:  43,
		Status:    "queued",
		CreatedAt: now.Add(2 * time.Minute),
		UpdatedAt: now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatalf("EnqueueMergeEntry(43) error = %v", err)
	}
	if got, want := position, 1; got != want {
		t.Fatalf("position(43) = %d, want %d", got, want)
	}
	position, err = store.EnqueueMergeEntry(context.Background(), MergeQueueEntry{
		Project:   "/repo-a",
		Issue:     "LAB-900",
		PRNumber:  42,
		Status:    "queued",
		CreatedAt: now.Add(time.Minute),
		UpdatedAt: now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("EnqueueMergeEntry(42) error = %v", err)
	}
	if got, want := position, 2; got != want {
		t.Fatalf("position(42) = %d, want %d", got, want)
	}

	entries, err := store.MergeEntries(context.Background(), "/repo-a")
	if err != nil {
		t.Fatalf("MergeEntries() error = %v", err)
	}
	if got, want := len(entries), 2; got != want {
		t.Fatalf("len(MergeEntries()) = %d, want %d", got, want)
	}
	if got, want := entries[0].PRNumber, 42; got != want {
		t.Fatalf("MergeEntries()[0].PRNumber = %d, want %d", got, want)
	}

	entry, err := store.MergeEntry(context.Background(), "/repo-a", 42)
	if err != nil {
		t.Fatalf("MergeEntry(42) error = %v", err)
	}
	if entry == nil {
		t.Fatal("MergeEntry(42) = nil, want entry")
	}
	entry.Status = "checking_ci"
	entry.UpdatedAt = now.Add(3 * time.Minute)
	if err := store.UpdateMergeEntry(context.Background(), *entry); err != nil {
		t.Fatalf("UpdateMergeEntry() error = %v", err)
	}

	entry, err = store.MergeEntry(context.Background(), "/repo-a", 42)
	if err != nil {
		t.Fatalf("MergeEntry(42) updated error = %v", err)
	}
	if entry == nil || entry.Status != "checking_ci" {
		t.Fatalf("updated MergeEntry(42) = %#v, want checking_ci", entry)
	}

	if err := store.DeleteMergeEntry(context.Background(), "/repo-a", 43); err != nil {
		t.Fatalf("DeleteMergeEntry(43) error = %v", err)
	}
	if err := store.DeleteMergeEntry(context.Background(), "/repo-a", 43); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteMergeEntry(43) second error = %v, want ErrNotFound", err)
	}

	allTasks, err := store.AllNonTerminalTasks(context.Background())
	if err != nil {
		t.Fatalf("AllNonTerminalTasks() error = %v", err)
	}
	if got, want := len(allTasks), 2; got != want {
		t.Fatalf("len(AllNonTerminalTasks()) = %d, want %d", got, want)
	}

	assignments, err := store.AllActiveAssignments(context.Background())
	if err != nil {
		t.Fatalf("AllActiveAssignments() error = %v", err)
	}
	if got, want := len(assignments), 2; got != want {
		t.Fatalf("len(AllActiveAssignments()) = %d, want %d", got, want)
	}

	allEntries, err := store.AllMergeEntries(context.Background())
	if err != nil {
		t.Fatalf("AllMergeEntries() error = %v", err)
	}
	if got, want := len(allEntries), 1; got != want {
		t.Fatalf("len(AllMergeEntries()) = %d, want %d", got, want)
	}

	workers, err := store.ListWorkers(context.Background(), "")
	if err != nil {
		t.Fatalf("ListWorkers(global) error = %v", err)
	}
	if got, want := len(workers), 2; got != want {
		t.Fatalf("len(ListWorkers(global)) = %d, want %d", got, want)
	}

	clones, err := store.ListClones(context.Background(), "")
	if err != nil {
		t.Fatalf("ListClones(global) error = %v", err)
	}
	if got, want := len(clones), 2; got != want {
		t.Fatalf("len(ListClones(global)) = %d, want %d", got, want)
	}

	status, err := store.ProjectStatus(context.Background(), "")
	if err != nil {
		t.Fatalf("ProjectStatus(global) error = %v", err)
	}
	if got, want := status.Summary.Tasks, 2; got != want {
		t.Fatalf("status.Summary.Tasks = %d, want %d", got, want)
	}
	if got, want := status.Summary.Active, 2; got != want {
		t.Fatalf("status.Summary.Active = %d, want %d", got, want)
	}
	if got, want := status.Summary.Workers, 2; got != want {
		t.Fatalf("status.Summary.Workers = %d, want %d", got, want)
	}
	if got, want := status.Summary.Clones, 2; got != want {
		t.Fatalf("status.Summary.Clones = %d, want %d", got, want)
	}
	if got, want := status.Summary.FreeClones, 1; got != want {
		t.Fatalf("status.Summary.FreeClones = %d, want %d", got, want)
	}
}

func TestOpenPrefersPostgresWhenORCAStateDSNIsSet(t *testing.T) {
	_, dsn := newTestPostgresStoreWithDSN(t)

	t.Setenv("ORCA_STATE_DSN", dsn)
	opened, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer opened.Close()

	if _, ok := opened.(*PostgresStore); !ok {
		t.Fatalf("Open() returned %T, want *PostgresStore", opened)
	}
}

func newTestPostgresStore(t *testing.T) *PostgresStore {
	t.Helper()
	store, _ := newTestPostgresStoreWithDSN(t)
	return store
}

func newTestPostgresStoreWithDSN(t *testing.T) (*PostgresStore, string) {
	t.Helper()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("docker unavailable: %v", err)
	}
	pool.MaxWait = 45 * time.Second

	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "postgres",
		Tag:        "16-alpine",
		Env: []string{
			"POSTGRES_USER=orca",
			"POSTGRES_PASSWORD=secret",
			"POSTGRES_DB=orca",
		},
	}, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() {
		_ = pool.Purge(resource)
	})

	dsn := fmt.Sprintf("postgres://orca:secret@127.0.0.1:%s/orca?sslmode=disable", resource.GetPort("5432/tcp"))
	var store *PostgresStore
	if err := pool.Retry(func() error {
		var openErr error
		store, openErr = OpenPostgres(dsn)
		return openErr
	}); err != nil {
		t.Fatalf("OpenPostgres() error = %v", err)
	}

	t.Cleanup(func() {
		_ = store.Close()
	})
	return store, dsn
}
