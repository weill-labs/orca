package daemon

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/weill-labs/orca/internal/amux"
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
	if got := deps.events.countType(EventPRPollTrace); got != 1 {
		t.Fatalf("pr.poll_trace event count = %d, want 1 after one poll tick; events = %#v", got, fakeEventTypes(deps.events))
	}
}

func TestTwoDaemonsOnSharedPostgresOnlyPollTheirOwnHost(t *testing.T) {
	t.Parallel()

	storeDSN := newSharedDaemonPostgresStoreDSN(t)
	storeA := openSharedDaemonPostgresStore(t, storeDSN)
	storeB := openSharedDaemonPostgresStore(t, storeDSN)

	depsA := newTestDeps(t)
	depsB := newTestDeps(t)
	captureTickerA := newFakeTicker()
	pollTickerA := newFakeTicker()
	captureTickerB := newFakeTicker()
	pollTickerB := newFakeTicker()
	depsA.tickers.enqueue(captureTickerA, pollTickerA)
	depsB.tickers.enqueue(captureTickerB, pollTickerB)

	const (
		projectPath = "/tmp/project"
		hostA       = "host-a"
		hostB       = "host-b"
	)

	now := depsA.clock.Now()
	if err := seedDaemonHostAssignment(t, storeDSN, projectPath, hostA, "LAB-2001", "worker-a", "pane-a", depsA.pool.clone.Path, now); err != nil {
		t.Fatalf("seed host A assignment error = %v", err)
	}
	if err := seedDaemonHostAssignment(t, storeDSN, projectPath, hostB, "LAB-2002", "worker-b", "pane-b", depsB.pool.clone.Path, now.Add(time.Minute)); err != nil {
		t.Fatalf("seed host B assignment error = %v", err)
	}

	depsA.amux.paneExists = map[string]bool{"pane-a": true, "pane-b": false}
	depsB.amux.paneExists = map[string]bool{"pane-a": false, "pane-b": true}

	daemonA := depsA.newDaemonWithOptions(t, func(opts *Options) {
		opts.Project = ""
		opts.Hostname = hostA
		opts.State = newSQLiteStateAdapter(storeA)
	})
	daemonB := depsB.newDaemonWithOptions(t, func(opts *Options) {
		opts.Project = ""
		opts.Hostname = hostB
		opts.State = newSQLiteStateAdapter(storeB)
	})

	ctx := context.Background()
	if err := daemonA.Start(ctx); err != nil {
		t.Fatalf("daemonA.Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = daemonA.Stop(context.Background())
	})
	if err := daemonB.Start(ctx); err != nil {
		t.Fatalf("daemonB.Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = daemonB.Stop(context.Background())
	})

	tickAndWaitForHeartbeat(t, daemonA, depsA, pollTickerA, time.Second, "daemon A poll tick heartbeat")
	tickAndWaitForHeartbeat(t, daemonB, depsB, pollTickerB, time.Second, "daemon B poll tick heartbeat")

	if calls := depsA.amux.paneExistsCalls; containsString(calls, "pane-b") {
		t.Fatalf("daemon A paneExists calls = %#v, want no remote pane checks", calls)
	}
	if calls := depsB.amux.paneExistsCalls; containsString(calls, "pane-a") {
		t.Fatalf("daemon B paneExists calls = %#v, want no remote pane checks", calls)
	}

	sharedEvents := collectProjectEvents(t, storeA, projectPath, 750*time.Millisecond)
	prPollTraces := 0
	for _, event := range sharedEvents {
		if event.Kind != EventPRPollTrace {
			continue
		}
		prPollTraces++
		if strings.Contains(event.Message, "pane_exists_error") {
			t.Fatalf("shared event log contains cross-host pane_exists_error: %#v", sharedEvents)
		}
	}
	if prPollTraces < 2 {
		t.Fatalf("shared pr.poll_trace count = %d, want at least 2 events from both daemons; events = %#v", prPollTraces, sharedEvents)
	}
}

func newDaemonPostgresStore(t *testing.T) *state.PostgresStore {
	t.Helper()

	storeDSN := newSharedDaemonPostgresStoreDSN(t)
	return openSharedDaemonPostgresStore(t, storeDSN)
}

func newSharedDaemonPostgresStoreDSN(t *testing.T) string {
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
	return storeDSN
}

func openSharedDaemonPostgresStore(t *testing.T, storeDSN string) *state.PostgresStore {
	t.Helper()

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

func seedDaemonHostAssignment(t *testing.T, storeDSN, projectPath, host, issue, workerID, paneID, clonePath string, now time.Time) error {
	t.Helper()

	pool, err := openDaemonReadyPostgresPool(storeDSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	if _, err := pool.Exec(context.Background(), `
		INSERT INTO tasks(project, issue, host, status, state, agent, prompt, caller_pane, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES($1, $2, $3, $4, $5, 'codex', 'Investigate host-scoped polling', '', $6, $7, $2, NULL, $8, $8)
	`, projectPath, issue, host, TaskStatusActive, TaskStateAssigned, workerID, clonePath, now.UTC()); err != nil {
		return err
	}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO workers(project, worker_id, host, agent_profile, current_pane_id, state, issue, clone_path, last_capture, created_at, last_seen_at)
		VALUES($1, $2, $3, 'codex', $4, $5, $6, $7, $8, $9, $9)
	`, projectPath, workerID, host, paneID, WorkerHealthHealthy, issue, clonePath, defaultCodexReadyOutput(), now.UTC()); err != nil {
		return err
	}
	return nil
}

func collectProjectEvents(t *testing.T, store *state.PostgresStore, projectPath string, duration time.Duration) []state.Event {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	eventsCh, errCh := store.Events(ctx, projectPath, 0)
	events := make([]state.Event, 0)
	for eventsCh != nil || errCh != nil {
		select {
		case event, ok := <-eventsCh:
			if !ok {
				eventsCh = nil
				continue
			}
			events = append(events, event)
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				t.Fatalf("Events() error = %v", err)
			}
		case <-ctx.Done():
			eventsCh = nil
			errCh = nil
		}
	}
	return events
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
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
		if err := amux.Wait(context.Background(), 100*time.Millisecond); err != nil {
			return nil, err
		}
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
