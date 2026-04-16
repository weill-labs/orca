package state

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestSQLiteOpenHandlesParallelEventWorkerIDBackfill(t *testing.T) {
	const (
		project          = "/repo"
		parallelOpeners  = 6
		pendingBackfills = 512
	)

	dbPath := filepath.Join(t.TempDir(), "state.db")
	seedStore, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite() seed error = %v", err)
	}

	ctx := context.Background()
	now := time.Date(2026, 4, 16, 23, 30, 0, 0, time.UTC)
	if err := seedStore.UpsertWorker(ctx, project, Worker{
		WorkerID:      "worker-99",
		CurrentPaneID: "pane-legacy",
		Agent:         "codex",
		State:         "healthy",
		CreatedAt:     now,
		LastSeenAt:    now,
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	payload, err := json.Marshal(map[string]string{"pane_id": "pane-legacy"})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	for i := 0; i < pendingBackfills; i++ {
		if _, err := seedStore.AppendEvent(ctx, Event{
			Project:   project,
			Kind:      "task.assigned",
			Message:   fmt.Sprintf("legacy event %d", i),
			Payload:   payload,
			CreatedAt: now.Add(time.Duration(i) * time.Millisecond),
		}); err != nil {
			t.Fatalf("AppendEvent(%d) error = %v", i, err)
		}
	}

	if err := seedStore.Close(); err != nil {
		t.Fatalf("seedStore.Close() error = %v", err)
	}

	start := make(chan struct{})
	errs := make(chan error, parallelOpeners)
	var wg sync.WaitGroup
	for i := 0; i < parallelOpeners; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start

			store, err := OpenSQLite(dbPath)
			if err == nil {
				err = store.Close()
			}
			errs <- err
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("parallel OpenSQLite() error = %v", err)
		}
	}

	verifyStore, err := OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("OpenSQLite() verify error = %v", err)
	}
	t.Cleanup(func() {
		if err := verifyStore.Close(); err != nil {
			t.Fatalf("verifyStore.Close() error = %v", err)
		}
	})

	var remaining int
	if err := verifyStore.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM events
		WHERE project = ? AND worker_id = ''
	`, project).Scan(&remaining); err != nil {
		t.Fatalf("remaining backfill count query error = %v", err)
	}
	if got, want := remaining, 0; got != want {
		t.Fatalf("remaining backfill rows = %d, want %d", got, want)
	}
}
