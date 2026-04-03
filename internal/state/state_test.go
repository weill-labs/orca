package state

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestOpen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		setup func(t *testing.T, dbPath string)
		check func(t *testing.T, store *Store)
	}{
		{
			name: "enables WAL and creates latest schema",
			check: func(t *testing.T, store *Store) {
				t.Helper()

				var journalMode string
				if err := store.db.QueryRow(`PRAGMA journal_mode;`).Scan(&journalMode); err != nil {
					t.Fatalf("journal_mode query: %v", err)
				}
				if journalMode != "wal" {
					t.Fatalf("journal_mode = %q, want %q", journalMode, "wal")
				}

				var version int
				if err := store.db.QueryRow(`PRAGMA user_version;`).Scan(&version); err != nil {
					t.Fatalf("user_version query: %v", err)
				}
				if version != schemaVersion {
					t.Fatalf("user_version = %d, want %d", version, schemaVersion)
				}
			},
		},
		{
			name: "migrates v1 databases on open",
			setup: func(t *testing.T, dbPath string) {
				t.Helper()

				db, err := sql.Open("sqlite", dbPath)
				if err != nil {
					t.Fatalf("sql.Open(): %v", err)
				}
				t.Cleanup(func() {
					_ = db.Close()
				})

				for _, stmt := range v1SchemaStatements() {
					if _, err := db.Exec(stmt); err != nil {
						t.Fatalf("seed v1 schema: %v", err)
					}
				}
				if _, err := db.Exec(`PRAGMA user_version = 1;`); err != nil {
					t.Fatalf("set user_version: %v", err)
				}
			},
			check: func(t *testing.T, store *Store) {
				t.Helper()

				var count int
				if err := store.db.QueryRow(`
SELECT COUNT(*)
FROM sqlite_master
WHERE type = 'index' AND name = 'idx_event_log_project_created_at';
`).Scan(&count); err != nil {
					t.Fatalf("sqlite_master query: %v", err)
				}
				if count != 1 {
					t.Fatalf("expected migration-created index, got count %d", count)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dbPath := filepath.Join(t.TempDir(), "state.db")
			if tt.setup != nil {
				tt.setup(t, dbPath)
			}

			store, err := Open(dbPath, filepath.Join(t.TempDir(), "repo"))
			if err != nil {
				t.Fatalf("Open() error = %v", err)
			}
			t.Cleanup(func() {
				_ = store.Close()
			})

			tt.check(t, store)
		})
	}
}

func TestTaskCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, primary *Store, secondary *Store)
	}{
		{
			name: "create get list update delete",
			run: func(t *testing.T, primary *Store, secondary *Store) {
				t.Helper()

				ctx := context.Background()
				createdAt := time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC)
				updatedAt := createdAt.Add(2 * time.Minute)
				completedAt := createdAt.Add(10 * time.Minute)
				prNumber := 590

				task := Task{
					IssueID:            "LAB-686",
					Status:             "queued",
					AssignedWorkerPane: "pane-3",
					AssignedClonePath:  "/tmp/amux03",
					PRNumber:           &prNumber,
					CreatedAt:          createdAt,
					UpdatedAt:          createdAt,
				}
				if err := primary.CreateTask(ctx, task); err != nil {
					t.Fatalf("CreateTask() error = %v", err)
				}

				got, err := primary.GetTask(ctx, task.IssueID)
				if err != nil {
					t.Fatalf("GetTask() error = %v", err)
				}
				if !reflect.DeepEqual(got, task) {
					t.Fatalf("GetTask() mismatch\nwant: %#v\ngot:  %#v", task, got)
				}

				if _, err := secondary.GetTask(ctx, task.IssueID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("secondary GetTask() error = %v, want ErrNotFound", err)
				}

				task.Status = "done"
				task.UpdatedAt = updatedAt
				task.CompletedAt = &completedAt
				if err := primary.UpdateTask(ctx, task); err != nil {
					t.Fatalf("UpdateTask() error = %v", err)
				}

				listed, err := primary.ListTasks(ctx)
				if err != nil {
					t.Fatalf("ListTasks() error = %v", err)
				}
				if !reflect.DeepEqual(listed, []Task{task}) {
					t.Fatalf("ListTasks() mismatch\nwant: %#v\ngot:  %#v", []Task{task}, listed)
				}

				if err := primary.DeleteTask(ctx, task.IssueID); err != nil {
					t.Fatalf("DeleteTask() error = %v", err)
				}
				if _, err := primary.GetTask(ctx, task.IssueID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("GetTask() after delete error = %v, want ErrNotFound", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			primary, secondary := openProjectStores(t)
			tt.run(t, primary, secondary)
		})
	}
}

func TestWorkerCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, primary *Store, secondary *Store)
	}{
		{
			name: "create get list update delete",
			run: func(t *testing.T, primary *Store, secondary *Store) {
				t.Helper()

				ctx := context.Background()
				createdAt := time.Date(2026, 4, 2, 11, 0, 0, 0, time.UTC)
				heartbeat := createdAt.Add(5 * time.Second)
				updatedHeartbeat := createdAt.Add(30 * time.Second)

				worker := Worker{
					PaneID:           "pane-5",
					PaneName:         "worker-5",
					AgentProfile:     "codex",
					CurrentTaskIssue: "LAB-686",
					HealthState:      "healthy",
					ClonePath:        "/tmp/amux05",
					CreatedAt:        createdAt,
					UpdatedAt:        createdAt,
					LastHeartbeatAt:  &heartbeat,
				}
				if err := primary.CreateWorker(ctx, worker); err != nil {
					t.Fatalf("CreateWorker() error = %v", err)
				}

				got, err := primary.GetWorker(ctx, worker.PaneID)
				if err != nil {
					t.Fatalf("GetWorker() error = %v", err)
				}
				if !reflect.DeepEqual(got, worker) {
					t.Fatalf("GetWorker() mismatch\nwant: %#v\ngot:  %#v", worker, got)
				}

				if _, err := secondary.GetWorker(ctx, worker.PaneID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("secondary GetWorker() error = %v, want ErrNotFound", err)
				}

				worker.HealthState = "stuck"
				worker.UpdatedAt = createdAt.Add(time.Minute)
				worker.LastHeartbeatAt = &updatedHeartbeat
				if err := primary.UpdateWorker(ctx, worker); err != nil {
					t.Fatalf("UpdateWorker() error = %v", err)
				}

				listed, err := primary.ListWorkers(ctx)
				if err != nil {
					t.Fatalf("ListWorkers() error = %v", err)
				}
				if !reflect.DeepEqual(listed, []Worker{worker}) {
					t.Fatalf("ListWorkers() mismatch\nwant: %#v\ngot:  %#v", []Worker{worker}, listed)
				}

				if err := primary.DeleteWorker(ctx, worker.PaneID); err != nil {
					t.Fatalf("DeleteWorker() error = %v", err)
				}
				if _, err := primary.GetWorker(ctx, worker.PaneID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("GetWorker() after delete error = %v, want ErrNotFound", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			primary, secondary := openProjectStores(t)
			tt.run(t, primary, secondary)
		})
	}
}

func TestCloneCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, primary *Store, secondary *Store)
	}{
		{
			name: "create get list update delete",
			run: func(t *testing.T, primary *Store, secondary *Store) {
				t.Helper()

				ctx := context.Background()
				createdAt := time.Date(2026, 4, 2, 12, 0, 0, 0, time.UTC)

				clone := Clone{
					Path:              "/tmp/amux08",
					Status:            "free",
					CurrentBranch:     "main",
					AssignedTaskIssue: "",
					CreatedAt:         createdAt,
					UpdatedAt:         createdAt,
				}
				if err := primary.CreateClone(ctx, clone); err != nil {
					t.Fatalf("CreateClone() error = %v", err)
				}

				got, err := primary.GetClone(ctx, clone.Path)
				if err != nil {
					t.Fatalf("GetClone() error = %v", err)
				}
				if !reflect.DeepEqual(got, clone) {
					t.Fatalf("GetClone() mismatch\nwant: %#v\ngot:  %#v", clone, got)
				}

				if _, err := secondary.GetClone(ctx, clone.Path); !errors.Is(err, ErrNotFound) {
					t.Fatalf("secondary GetClone() error = %v, want ErrNotFound", err)
				}

				clone.Status = "occupied"
				clone.CurrentBranch = "LAB-686"
				clone.AssignedTaskIssue = "LAB-686"
				clone.UpdatedAt = createdAt.Add(time.Minute)
				if err := primary.UpdateClone(ctx, clone); err != nil {
					t.Fatalf("UpdateClone() error = %v", err)
				}

				listed, err := primary.ListClones(ctx)
				if err != nil {
					t.Fatalf("ListClones() error = %v", err)
				}
				if !reflect.DeepEqual(listed, []Clone{clone}) {
					t.Fatalf("ListClones() mismatch\nwant: %#v\ngot:  %#v", []Clone{clone}, listed)
				}

				if err := primary.DeleteClone(ctx, clone.Path); err != nil {
					t.Fatalf("DeleteClone() error = %v", err)
				}
				if _, err := primary.GetClone(ctx, clone.Path); !errors.Is(err, ErrNotFound) {
					t.Fatalf("GetClone() after delete error = %v, want ErrNotFound", err)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			primary, secondary := openProjectStores(t)
			tt.run(t, primary, secondary)
		})
	}
}

func TestEventCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, primary *Store, secondary *Store)
	}{
		{
			name: "create get list update delete",
			run: func(t *testing.T, primary *Store, secondary *Store) {
				t.Helper()

				ctx := context.Background()
				createdAt := time.Date(2026, 4, 2, 13, 0, 0, 0, time.UTC)

				event := Event{
					TaskIssueID:  "LAB-686",
					WorkerPaneID: "pane-5",
					ClonePath:    "/tmp/amux08",
					EventType:    "task.assigned",
					Message:      "assigned LAB-686 to pane-5",
					CreatedAt:    createdAt,
				}
				id, err := primary.CreateEvent(ctx, event)
				if err != nil {
					t.Fatalf("CreateEvent() error = %v", err)
				}
				event.ID = id

				got, err := primary.GetEvent(ctx, id)
				if err != nil {
					t.Fatalf("GetEvent() error = %v", err)
				}
				if !reflect.DeepEqual(got, event) {
					t.Fatalf("GetEvent() mismatch\nwant: %#v\ngot:  %#v", event, got)
				}

				if _, err := secondary.GetEvent(ctx, id); !errors.Is(err, ErrNotFound) {
					t.Fatalf("secondary GetEvent() error = %v, want ErrNotFound", err)
				}

				event.Message = "assigned LAB-686 to pane-7"
				if err := primary.UpdateEvent(ctx, event); err != nil {
					t.Fatalf("UpdateEvent() error = %v", err)
				}

				listed, err := primary.ListEvents(ctx)
				if err != nil {
					t.Fatalf("ListEvents() error = %v", err)
				}
				if !reflect.DeepEqual(listed, []Event{event}) {
					t.Fatalf("ListEvents() mismatch\nwant: %#v\ngot:  %#v", []Event{event}, listed)
				}

				if err := primary.DeleteEvent(ctx, event.ID); err != nil {
					t.Fatalf("DeleteEvent() error = %v", err)
				}
				if _, err := primary.GetEvent(ctx, event.ID); !errors.Is(err, ErrNotFound) {
					t.Fatalf("GetEvent() after delete error = %v, want ErrNotFound", err)
				}
			},
		},
		{
			name: "update preserves original created at timestamp",
			run: func(t *testing.T, primary *Store, _ *Store) {
				t.Helper()

				ctx := context.Background()
				originalCreatedAt := time.Date(2026, 4, 2, 13, 30, 0, 0, time.UTC)

				event := Event{
					TaskIssueID:  "LAB-686",
					WorkerPaneID: "pane-5",
					ClonePath:    "/tmp/amux08",
					EventType:    "task.assigned",
					Message:      "assigned LAB-686 to pane-5",
					CreatedAt:    originalCreatedAt,
				}
				id, err := primary.CreateEvent(ctx, event)
				if err != nil {
					t.Fatalf("CreateEvent() error = %v", err)
				}

				event.ID = id
				event.Message = "assigned LAB-686 to pane-7"
				event.CreatedAt = time.Time{}

				if err := primary.UpdateEvent(ctx, event); err != nil {
					t.Fatalf("UpdateEvent() error = %v", err)
				}

				got, err := primary.GetEvent(ctx, id)
				if err != nil {
					t.Fatalf("GetEvent() error = %v", err)
				}

				if !got.CreatedAt.Equal(originalCreatedAt) {
					t.Fatalf("GetEvent().CreatedAt = %v, want %v", got.CreatedAt, originalCreatedAt)
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			primary, secondary := openProjectStores(t)
			tt.run(t, primary, secondary)
		})
	}
}

func openProjectStores(t *testing.T) (*Store, *Store) {
	t.Helper()

	dbDir := t.TempDir()
	dbPath := filepath.Join(dbDir, "state.db")

	primary, err := Open(dbPath, filepath.Join(dbDir, "project-a"))
	if err != nil {
		t.Fatalf("Open(primary) error = %v", err)
	}
	t.Cleanup(func() {
		_ = primary.Close()
	})

	secondary, err := Open(dbPath, filepath.Join(dbDir, "project-b"))
	if err != nil {
		t.Fatalf("Open(secondary) error = %v", err)
	}
	t.Cleanup(func() {
		_ = secondary.Close()
	})

	return primary, secondary
}

func v1SchemaStatements() []string {
	return []string{
		`CREATE TABLE tasks (
			project TEXT NOT NULL,
			issue_id TEXT NOT NULL,
			status TEXT NOT NULL,
			assigned_worker_pane TEXT,
			assigned_clone_path TEXT,
			pr_number INTEGER,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			started_at TEXT,
			completed_at TEXT,
			PRIMARY KEY (project, issue_id)
		);`,
		`CREATE TABLE workers (
			project TEXT NOT NULL,
			pane_id TEXT NOT NULL,
			pane_name TEXT NOT NULL,
			agent_profile TEXT NOT NULL,
			current_task_issue TEXT,
			health_state TEXT NOT NULL,
			clone_path TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			last_heartbeat_at TEXT,
			PRIMARY KEY (project, pane_id)
		);`,
		`CREATE TABLE clones (
			project TEXT NOT NULL,
			path TEXT NOT NULL,
			status TEXT NOT NULL,
			current_branch TEXT NOT NULL,
			assigned_task_issue TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			PRIMARY KEY (project, path)
		);`,
		`CREATE TABLE event_log (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			project TEXT NOT NULL,
			task_issue_id TEXT,
			worker_pane_id TEXT,
			clone_path TEXT,
			event_type TEXT NOT NULL,
			message TEXT NOT NULL,
			created_at TEXT NOT NULL
		);`,
	}
}
