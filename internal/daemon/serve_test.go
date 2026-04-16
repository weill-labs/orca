package daemon

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	legacy "github.com/weill-labs/orca/internal/state"
)

func TestStartDaemonLifecycleReportsRunningAfterStart(t *testing.T) {
	t.Parallel()

	calls := make([]string, 0, 2)
	instance := fakeDaemonLifecycle{
		start: func(context.Context) error {
			calls = append(calls, "start")
			return nil
		},
		stop: func(context.Context) error {
			calls = append(calls, "stop")
			return nil
		},
	}
	statusWriter := fakeOrderedDaemonStatusWriter{
		update: func(_ context.Context, status string, heartbeatAt time.Time) error {
			calls = append(calls, "status:"+status)
			if heartbeatAt.IsZero() {
				t.Fatal("heartbeatAt = zero, want startup timestamp")
			}
			return nil
		},
	}

	err := startDaemonLifecycle(context.Background(), instance, statusWriter, time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC), func(context.Context, time.Time) error {
		calls = append(calls, "mark-stopped")
		return nil
	})
	if err != nil {
		t.Fatalf("startDaemonLifecycle() error = %v", err)
	}

	if got, want := calls, []string{"start", "status:running"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("call order = %#v, want %#v", got, want)
	}
}

func TestStartDaemonLifecycleMarksStoppedWhenStartFails(t *testing.T) {
	t.Parallel()

	calls := make([]string, 0, 2)
	wantErr := errors.New("start failed")
	instance := fakeDaemonLifecycle{
		start: func(context.Context) error {
			calls = append(calls, "start")
			return wantErr
		},
		stop: func(context.Context) error {
			calls = append(calls, "stop")
			return nil
		},
	}
	statusWriter := fakeOrderedDaemonStatusWriter{
		update: func(context.Context, string, time.Time) error {
			calls = append(calls, "status")
			return nil
		},
	}

	err := startDaemonLifecycle(context.Background(), instance, statusWriter, time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC), func(context.Context, time.Time) error {
		calls = append(calls, "mark-stopped")
		return nil
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("startDaemonLifecycle() error = %v, want %v", err, wantErr)
	}

	if got, want := calls, []string{"start", "mark-stopped"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("call order = %#v, want %#v", got, want)
	}
}

func TestStartDaemonLifecycleStopsAndMarksStoppedWhenStatusUpdateFails(t *testing.T) {
	t.Parallel()

	calls := make([]string, 0, 4)
	wantErr := errors.New("write status")
	instance := fakeDaemonLifecycle{
		start: func(context.Context) error {
			calls = append(calls, "start")
			return nil
		},
		stop: func(context.Context) error {
			calls = append(calls, "stop")
			return nil
		},
	}
	statusWriter := fakeOrderedDaemonStatusWriter{
		update: func(context.Context, string, time.Time) error {
			calls = append(calls, "status")
			return wantErr
		},
	}

	err := startDaemonLifecycle(context.Background(), instance, statusWriter, time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC), func(context.Context, time.Time) error {
		calls = append(calls, "mark-stopped")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("startDaemonLifecycle() error = %v, want message containing %q", err, wantErr)
	}

	if got, want := calls, []string{"start", "status", "stop", "mark-stopped"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("call order = %#v, want %#v", got, want)
	}
}

func TestOpenDaemonStoreUsesSQLiteByDefault(t *testing.T) {
	t.Setenv("ORCA_STATE_DSN", "")

	originalSQLite := openSQLiteDaemonStore
	originalPostgres := openPostgresDaemonStore
	t.Cleanup(func() {
		openSQLiteDaemonStore = originalSQLite
		openPostgresDaemonStore = originalPostgres
	})

	var gotPath string
	openSQLiteDaemonStore = func(path string) (daemonStateStore, error) {
		gotPath = path
		return fakeDaemonStateStore{}, nil
	}
	openPostgresDaemonStore = func(string) (daemonStateStore, error) {
		t.Fatal("openPostgresDaemonStore should not be called when ORCA_STATE_DSN is unset")
		return nil, nil
	}

	store, err := openDaemonStore("/tmp/orca/state.db")
	if err != nil {
		t.Fatalf("openDaemonStore() error = %v", err)
	}
	if store == nil {
		t.Fatal("openDaemonStore() = nil, want store")
	}
	if got, want := gotPath, "/tmp/orca/state.db"; got != want {
		t.Fatalf("sqlite path = %q, want %q", got, want)
	}
}

func TestOpenDaemonStoreUsesPostgresWhenDSNIsSet(t *testing.T) {
	t.Setenv("ORCA_STATE_DSN", "postgres://orca:orca@localhost:5432/orca?sslmode=disable")

	originalSQLite := openSQLiteDaemonStore
	originalPostgres := openPostgresDaemonStore
	t.Cleanup(func() {
		openSQLiteDaemonStore = originalSQLite
		openPostgresDaemonStore = originalPostgres
	})

	var gotDSN string
	openSQLiteDaemonStore = func(string) (daemonStateStore, error) {
		t.Fatal("openSQLiteDaemonStore should not be called when ORCA_STATE_DSN is set")
		return nil, nil
	}
	openPostgresDaemonStore = func(dsn string) (daemonStateStore, error) {
		gotDSN = dsn
		return fakeDaemonStateStore{}, nil
	}

	store, err := openDaemonStore("/tmp/orca/state.db")
	if err != nil {
		t.Fatalf("openDaemonStore() error = %v", err)
	}
	if store == nil {
		t.Fatal("openDaemonStore() = nil, want store")
	}
	if got, want := gotDSN, "postgres://orca:orca@localhost:5432/orca?sslmode=disable"; got != want {
		t.Fatalf("postgres dsn = %q, want %q", got, want)
	}
}

type fakeDaemonLifecycle struct {
	start func(context.Context) error
	stop  func(context.Context) error
}

func (f fakeDaemonLifecycle) Start(ctx context.Context) error {
	if f.start == nil {
		return nil
	}
	return f.start(ctx)
}

func (f fakeDaemonLifecycle) Stop(ctx context.Context) error {
	if f.stop == nil {
		return nil
	}
	return f.stop(ctx)
}

type fakeOrderedDaemonStatusWriter struct {
	update func(context.Context, string, time.Time) error
}

func (f fakeOrderedDaemonStatusWriter) Update(ctx context.Context, status string, heartbeatAt time.Time) error {
	if f.update == nil {
		return nil
	}
	return f.update(ctx, status, heartbeatAt)
}

type fakeDaemonStateStore struct{}

func (fakeDaemonStateStore) ProjectStatus(context.Context, string) (state.ProjectStatus, error) {
	return state.ProjectStatus{}, nil
}

func (fakeDaemonStateStore) TaskStatus(context.Context, string, string) (state.TaskStatus, error) {
	return state.TaskStatus{}, nil
}

func (fakeDaemonStateStore) ListWorkers(context.Context, string) ([]state.Worker, error) {
	return nil, nil
}

func (fakeDaemonStateStore) ListClones(context.Context, string) ([]state.Clone, error) {
	return nil, nil
}

func (fakeDaemonStateStore) Events(context.Context, string, int64) (<-chan state.Event, <-chan error) {
	eventsCh := make(chan state.Event)
	errCh := make(chan error)
	close(eventsCh)
	close(errCh)
	return eventsCh, errCh
}

func (fakeDaemonStateStore) EnsureSchema(context.Context) error { return nil }
func (fakeDaemonStateStore) UpsertDaemon(context.Context, string, state.DaemonStatus) error {
	return nil
}
func (fakeDaemonStateStore) MarkDaemonStopped(context.Context, string, time.Time) error { return nil }
func (fakeDaemonStateStore) UpsertTask(context.Context, string, state.Task) error       { return nil }
func (fakeDaemonStateStore) UpdateTaskStatus(context.Context, string, string, string, time.Time) (state.Task, error) {
	return state.Task{}, nil
}
func (fakeDaemonStateStore) UpdateTaskBranch(context.Context, string, string, string, time.Time) (state.Task, error) {
	return state.Task{}, nil
}
func (fakeDaemonStateStore) AppendEvent(context.Context, state.Event) (state.Event, error) {
	return state.Event{}, nil
}
func (fakeDaemonStateStore) Close() error { return nil }
func (fakeDaemonStateStore) WorkerByID(context.Context, string, string) (state.Worker, error) {
	return state.Worker{}, nil
}
func (fakeDaemonStateStore) WorkerByPane(context.Context, string, string) (state.Worker, error) {
	return state.Worker{}, nil
}
func (fakeDaemonStateStore) NonTerminalTasks(context.Context, string) ([]state.Task, error) {
	return nil, nil
}
func (fakeDaemonStateStore) StaleCloneOccupancies(context.Context, string) ([]state.CloneOccupancy, error) {
	return nil, nil
}
func (fakeDaemonStateStore) UpsertWorker(context.Context, string, state.Worker) error { return nil }
func (fakeDaemonStateStore) ClaimWorker(context.Context, string, state.Worker) (state.Worker, error) {
	return state.Worker{}, nil
}
func (fakeDaemonStateStore) DeleteWorker(context.Context, string, string) error { return nil }
func (fakeDaemonStateStore) DeleteTask(context.Context, string, string) error   { return nil }
func (fakeDaemonStateStore) ClaimTask(context.Context, string, state.Task) (*state.Task, error) {
	return nil, nil
}
func (fakeDaemonStateStore) ActiveAssignments(context.Context, string) ([]state.Assignment, error) {
	return nil, nil
}
func (fakeDaemonStateStore) ActiveAssignmentByIssue(context.Context, string, string) (state.Assignment, error) {
	return state.Assignment{}, nil
}
func (fakeDaemonStateStore) ActiveAssignmentByBranch(context.Context, string, string) (state.Assignment, error) {
	return state.Assignment{}, nil
}
func (fakeDaemonStateStore) ActiveAssignmentByPRNumber(context.Context, string, int) (state.Assignment, error) {
	return state.Assignment{}, nil
}
func (fakeDaemonStateStore) EnqueueMergeEntry(context.Context, state.MergeQueueEntry) (int, error) {
	return 0, nil
}
func (fakeDaemonStateStore) MergeEntry(context.Context, string, int) (*state.MergeQueueEntry, error) {
	return nil, nil
}
func (fakeDaemonStateStore) MergeEntries(context.Context, string) ([]state.MergeQueueEntry, error) {
	return nil, nil
}
func (fakeDaemonStateStore) UpdateMergeEntry(context.Context, state.MergeQueueEntry) error {
	return nil
}
func (fakeDaemonStateStore) DeleteMergeEntry(context.Context, string, int) error { return nil }
func (fakeDaemonStateStore) EnsureClone(context.Context, string, string) (legacy.CloneRecord, error) {
	return legacy.CloneRecord{}, nil
}
func (fakeDaemonStateStore) TryOccupyClone(context.Context, string, string, string, string) (bool, error) {
	return false, nil
}
func (fakeDaemonStateStore) MarkCloneFree(context.Context, string, string) error { return nil }
func (fakeDaemonStateStore) TasksByPane(context.Context, string, string) ([]state.Task, error) {
	return nil, nil
}
