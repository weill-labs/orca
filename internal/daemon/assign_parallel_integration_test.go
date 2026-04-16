package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/project"
)

func TestLocalControllerParallelAssignsSucceedDuringClientSchemaBackfill(t *testing.T) {
	const parallelAssigns = 6

	projectDir := filepath.Join(t.TempDir(), "project")
	if err := os.MkdirAll(filepath.Join(projectDir, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.git) error = %v", err)
	}

	projectPath, err := project.CanonicalPath(projectDir)
	if err != nil {
		t.Fatalf("CanonicalPath(%q) error = %v", projectDir, err)
	}

	tempDir := t.TempDir()
	paths := Paths{
		ConfigDir: tempDir,
		StateDB:   filepath.Join(tempDir, "state.db"),
		PIDDir:    filepath.Join(tempDir, "pids"),
	}

	seedStore, err := state.OpenSQLite(paths.StateDB)
	if err != nil {
		t.Fatalf("OpenSQLite() seed error = %v", err)
	}
	if err := seedPendingEventWorkerIDBackfill(context.Background(), seedStore, projectPath, 512); err != nil {
		t.Fatalf("seedPendingEventWorkerIDBackfill() error = %v", err)
	}
	if err := seedStore.Close(); err != nil {
		t.Fatalf("seedStore.Close() error = %v", err)
	}

	if err := os.MkdirAll(paths.PIDDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", paths.PIDDir, err)
	}
	if err := os.WriteFile(paths.pidFile(), []byte(fmt.Sprintf("%d", os.Getpid())), 0o644); err != nil {
		t.Fatalf("WriteFile(pidFile) error = %v", err)
	}

	socketPath := paths.socketFile()
	listener, err := listenUnixSocket(socketPath)
	if err != nil {
		t.Fatalf("listenUnixSocket(%q) error = %v", socketPath, err)
	}
	defer listener.Close()

	requests := make(chan rpcRequest, parallelAssigns)
	serverErrCh := make(chan error, 1)
	go func() {
		for i := 0; i < parallelAssigns; i++ {
			conn, err := listener.Accept()
			if err != nil {
				serverErrCh <- err
				return
			}

			var req rpcRequest
			if err := json.NewDecoder(conn).Decode(&req); err != nil {
				_ = conn.Close()
				serverErrCh <- err
				return
			}
			requests <- req

			result := TaskActionResult{
				Project:   projectPath,
				Issue:     fmt.Sprintf("LAB-14%02d", i),
				Status:    TaskStatusActive,
				Agent:     "claude",
				UpdatedAt: time.Now().UTC(),
			}
			if err := json.NewEncoder(conn).Encode(rpcSuccess(req.ID, result)); err != nil {
				_ = conn.Close()
				serverErrCh <- err
				return
			}
			if err := conn.Close(); err != nil {
				serverErrCh <- err
				return
			}
		}
		serverErrCh <- nil
	}()

	start := make(chan struct{})
	assignErrs := make(chan error, parallelAssigns)
	var wg sync.WaitGroup
	for i := 0; i < parallelAssigns; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			store, err := state.OpenSQLite(paths.StateDB)
			if err != nil {
				assignErrs <- fmt.Errorf("OpenSQLite(%d): %w", i, err)
				return
			}
			defer store.Close()

			controller, err := NewLocalController(ControllerOptions{
				Store: store,
				Paths: paths,
			})
			if err != nil {
				assignErrs <- fmt.Errorf("NewLocalController(%d): %w", i, err)
				return
			}

			_, err = controller.Assign(context.Background(), AssignRequest{
				Project: projectPath,
				Issue:   fmt.Sprintf("LAB-14%02d", i),
				Prompt:  "Handle parallel SQLite assign contention.",
				Agent:   "claude",
				Title:   fmt.Sprintf("Parallel assign %d", i+1),
			})
			assignErrs <- err
		}()
	}

	close(start)
	wg.Wait()
	close(assignErrs)

	for err := range assignErrs {
		if err != nil {
			t.Fatalf("parallel Assign() error = %v", err)
		}
	}

	if err := <-serverErrCh; err != nil {
		t.Fatalf("RPC server error = %v", err)
	}

	close(requests)
	if got, want := len(requests), parallelAssigns; got != want {
		t.Fatalf("assign request count = %d, want %d", got, want)
	}
	for req := range requests {
		if got, want := req.Method, "assign"; got != want {
			t.Fatalf("rpc method = %q, want %q", got, want)
		}
	}

	verifyStore, err := state.OpenSQLite(paths.StateDB)
	if err != nil {
		t.Fatalf("OpenSQLite() verify error = %v", err)
	}
	defer verifyStore.Close()

	rows, err := verifyStore.QueryContext(context.Background(), `
		SELECT COUNT(*)
		FROM events
		WHERE project = ? AND worker_id = ''
	`, projectPath)
	if err != nil {
		t.Fatalf("QueryContext() error = %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("rows.Next() = false, want true")
	}
	var remaining int
	if err := rows.Scan(&remaining); err != nil {
		t.Fatalf("rows.Scan() error = %v", err)
	}
	if got, want := remaining, 0; got != want {
		t.Fatalf("remaining backfill rows = %d, want %d", got, want)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows.Err() error = %v", err)
	}
}

func seedPendingEventWorkerIDBackfill(ctx context.Context, store *state.SQLiteStore, projectPath string, eventCount int) error {
	now := time.Date(2026, 4, 16, 23, 45, 0, 0, time.UTC)
	if err := store.UpsertWorker(ctx, projectPath, state.Worker{
		WorkerID:      "worker-legacy",
		CurrentPaneID: "pane-legacy",
		Agent:         "codex",
		State:         "healthy",
		CreatedAt:     now,
		LastSeenAt:    now,
	}); err != nil {
		return err
	}

	payload, err := json.Marshal(map[string]string{"pane_id": "pane-legacy"})
	if err != nil {
		return err
	}

	for i := 0; i < eventCount; i++ {
		if _, err := store.AppendEvent(ctx, state.Event{
			Project:   projectPath,
			Kind:      "task.assigned",
			Message:   fmt.Sprintf("legacy event %d", i),
			Payload:   payload,
			CreatedAt: now.Add(time.Duration(i) * time.Millisecond),
		}); err != nil {
			return err
		}
	}

	return nil
}
