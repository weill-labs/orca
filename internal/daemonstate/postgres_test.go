package state

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	pgmodule "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/weill-labs/orca/internal/amux"
)

var (
	postgresTestContainerOnce sync.Once
	postgresTestContainer     *pgmodule.PostgresContainer
	postgresTestBaseDSN       string
	postgresTestContainerErr  error
)

func TestMain(m *testing.M) {
	code := m.Run()
	if postgresTestContainer != nil {
		_ = postgresTestContainer.Terminate(context.Background())
	}
	os.Exit(code)
}

func TestPostgresStoreLifecycleAndQueries(t *testing.T) {
	t.Parallel()
	testStoreLifecycleAndQueries(t, newPostgresContractHarness(t))
}

func TestPostgresStoreNotFoundAndHelpers(t *testing.T) {
	t.Parallel()

	testStoreNotFoundBehavior(t, newPostgresContractHarness(t))

	formatted := formatTime(time.Date(2026, 4, 2, 10, 0, 0, 0, time.UTC))
	if parsed := parseTime(formatted); parsed.IsZero() {
		t.Fatal("parseTime(formatTime(...)) returned zero time")
	}
	if parsed := parseTime("not-a-time"); !parsed.IsZero() {
		t.Fatalf("parseTime(invalid) = %v, want zero time", parsed)
	}
}

func TestPostgresStoreAllActiveQueriesAcrossProjects(t *testing.T) {
	t.Parallel()
	testStoreAllActiveQueriesAcrossProjects(t, newPostgresContractHarness(t))
}

func TestPostgresStoreGlobalStatusFansOutAcrossProjects(t *testing.T) {
	t.Parallel()
	testStoreGlobalStatusFansOutAcrossProjects(t, newPostgresContractHarness(t))
}

func TestPostgresStorePersistsWorkerMonitorStateAndMergeQueue(t *testing.T) {
	t.Parallel()
	testStorePersistsWorkerMonitorStateAndMergeQueue(t, newPostgresContractHarness(t))
}

func TestPostgresStoreWorkerByPaneAndNonTerminalTasks(t *testing.T) {
	t.Parallel()
	testStoreWorkerByPaneAndNonTerminalTasks(t, newPostgresContractHarness(t))
}

func TestPostgresStoreStaleCloneOccupancies(t *testing.T) {
	t.Parallel()
	testStoreStaleCloneOccupancies(t, newPostgresContractHarness)
}

func TestPostgresStoreMergeQueueOrderingAndNotFound(t *testing.T) {
	t.Parallel()
	testStoreMergeQueueOrderingAndNotFound(t, newPostgresContractHarness(t))
}

func TestPostgresStoreSchemaIncludesHostColumns(t *testing.T) {
	t.Parallel()
	testStoreSchemaIncludesHostColumns(t, newPostgresContractHarness(t))
}

func newPostgresContractHarness(t *testing.T) storeContractHarness {
	t.Helper()

	baseDSN := ensurePostgresTestContainer(t)
	schema := fmt.Sprintf("test_%d", time.Now().UnixNano())

	adminPool, err := openReadyPostgresPool(baseDSN)
	if err != nil {
		t.Fatalf("openReadyPostgresPool(admin) error = %v", err)
	}
	defer adminPool.Close()

	if _, err := adminPool.Exec(context.Background(), fmt.Sprintf(`CREATE SCHEMA "%s"`, schema)); err != nil {
		t.Fatalf("create schema %q error = %v", schema, err)
	}

	storeDSN, err := withSearchPath(baseDSN, schema)
	if err != nil {
		t.Fatalf("withSearchPath() error = %v", err)
	}

	store, err := OpenPostgres(storeDSN)
	if err != nil {
		t.Fatalf("OpenPostgres() error = %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})

	return storeContractHarness{
		store: store,
		setNow: func(now time.Time) {
			store.now = func() time.Time { return now }
		},
		assertHostColumns: func(t *testing.T) {
			t.Helper()

			for _, spec := range []struct {
				table  string
				column string
			}{
				{table: "tasks", column: "host"},
				{table: "workers", column: "host"},
			} {
				var exists bool
				if err := store.pool.QueryRow(context.Background(), `
					SELECT EXISTS (
						SELECT 1
						FROM information_schema.columns
						WHERE table_schema = current_schema()
							AND table_name = $1
							AND column_name = $2
					)
				`, spec.table, spec.column).Scan(&exists); err != nil {
					t.Fatalf("query column %s.%s error = %v", spec.table, spec.column, err)
				}
				if !exists {
					t.Fatalf("%s.%s column missing", spec.table, spec.column)
				}
			}
		},
	}
}

func ensurePostgresTestContainer(t *testing.T) string {
	t.Helper()

	postgresTestContainerOnce.Do(func() {
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
			postgresTestContainerErr = err
			return
		}

		dsn, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			_ = container.Terminate(ctx)
			postgresTestContainerErr = err
			return
		}

		postgresTestContainer = container
		postgresTestBaseDSN = dsn
	})

	if postgresTestContainerErr != nil {
		t.Skipf("postgres testcontainer unavailable: %v", postgresTestContainerErr)
	}

	return postgresTestBaseDSN
}

func withSearchPath(baseDSN, schema string) (string, error) {
	parsed, err := url.Parse(baseDSN)
	if err != nil {
		return "", err
	}

	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func openReadyPostgresPool(dsn string) (*pgxpool.Pool, error) {
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
