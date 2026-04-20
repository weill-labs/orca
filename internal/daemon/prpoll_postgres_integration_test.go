package daemon

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

var (
	daemonPostgresTestContainerOnce sync.Once
	daemonPostgresTestContainer     *pgmodule.PostgresContainer
	daemonPostgresTestBaseDSN       string
	daemonPostgresTestContainerErr  error
	daemonPostgresTestSchemaSeq     atomic.Int64
)

func TestMain(m *testing.M) {
	code := m.Run()
	if daemonPostgresTestContainer != nil {
		_ = daemonPostgresTestContainer.Terminate(context.Background())
	}
	os.Exit(code)
}

func TestPRPollTickLeavesEvidenceWithPostgresState(t *testing.T) {
	t.Parallel()

	store := newDaemonPostgresStore(t)
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker)

	const (
		projectPath = "/tmp/project"
		issueID     = "LAB-1415"
		workerID    = "worker-01"
		paneID      = "pane-1"
	)

	now := deps.clock.Now()
	if err := store.UpsertTask(context.Background(), projectPath, state.Task{
		Issue:     issueID,
		Status:    TaskStatusActive,
		State:     TaskStateAssigned,
		Agent:     "codex",
		Prompt:    "Investigate silent PR polling",
		WorkerID:  workerID,
		ClonePath: deps.pool.clone.Path,
		Branch:    issueID,
		CreatedAt: now,
		UpdatedAt: now,
	}); err != nil {
		t.Fatalf("UpsertTask() error = %v", err)
	}
	if err := store.UpsertWorker(context.Background(), projectPath, state.Worker{
		WorkerID:      workerID,
		CurrentPaneID: paneID,
		Agent:         "codex",
		State:         WorkerHealthHealthy,
		Issue:         issueID,
		ClonePath:     deps.pool.clone.Path,
		LastCapture:   defaultCodexReadyOutput(),
		CreatedAt:     now,
		LastSeenAt:    now,
	}); err != nil {
		t.Fatalf("UpsertWorker() error = %v", err)
	}

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Project = ""
		opts.State = newSQLiteStateAdapter(store)
	})

	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	tickAndWaitForHeartbeat(t, d, deps, pollTicker, time.Second, "poll tick heartbeat")
	if got := deps.events.countType("pr.poll_trace"); got != 1 {
		t.Fatalf("pr.poll_trace event count = %d, want 1 after one poll tick; events = %#v", got, fakeEventTypes(deps.events))
	}
}

func newDaemonPostgresStore(t *testing.T) *state.PostgresStore {
	t.Helper()

	baseDSN := ensureDaemonPostgresTestContainer(t)
	schema := fmt.Sprintf("test_%d_%d", time.Now().UnixNano(), daemonPostgresTestSchemaSeq.Add(1))

	adminPool, err := openDaemonReadyPostgresPool(baseDSN)
	if err != nil {
		t.Fatalf("openDaemonReadyPostgresPool(admin) error = %v", err)
	}
	defer adminPool.Close()

	if _, err := adminPool.Exec(context.Background(), fmt.Sprintf(`CREATE SCHEMA "%s"`, schema)); err != nil {
		t.Fatalf("create schema %q error = %v", schema, err)
	}

	storeDSN, err := daemonPostgresDSNWithSearchPath(baseDSN, schema)
	if err != nil {
		t.Fatalf("daemonPostgresDSNWithSearchPath() error = %v", err)
	}

	store, err := state.OpenPostgres(storeDSN)
	if err != nil {
		t.Fatalf("OpenPostgres() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return store
}

func ensureDaemonPostgresTestContainer(t *testing.T) string {
	t.Helper()

	daemonPostgresTestContainerOnce.Do(func() {
		ctx := context.Background()

		container, err := pgmodule.Run(ctx,
			"postgres:16-alpine",
			pgmodule.WithDatabase("orca"),
			pgmodule.WithUsername("orca"),
			pgmodule.WithPassword("orca"),
			testcontainers.WithWaitStrategy(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second),
			),
		)
		if err != nil {
			daemonPostgresTestContainerErr = err
			return
		}

		dsn, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			_ = container.Terminate(ctx)
			daemonPostgresTestContainerErr = err
			return
		}

		daemonPostgresTestContainer = container
		daemonPostgresTestBaseDSN = dsn
	})

	if daemonPostgresTestContainerErr != nil {
		t.Skipf("postgres testcontainer unavailable: %v", daemonPostgresTestContainerErr)
	}

	return daemonPostgresTestBaseDSN
}

func daemonPostgresDSNWithSearchPath(baseDSN, schema string) (string, error) {
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func openDaemonReadyPostgresPool(dsn string) (*pgxpool.Pool, error) {
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			lastErr = err
		} else {
			pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = pool.Ping(pingCtx)
			cancel()
			if err == nil {
				return pool, nil
			}
			lastErr = err
			pool.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil, lastErr
}

func fakeEventTypes(events *fakeEvents) []string {
	events.mu.Lock()
	defer events.mu.Unlock()

	types := make([]string, 0, len(events.events))
	for _, event := range events.events {
		types = append(types, event.Type)
	}
	return types
}
