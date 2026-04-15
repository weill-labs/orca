package state

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"net/url"
	"os"
	"slices"
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

func TestPostgresStoreMaterializedViews(t *testing.T) {
	t.Parallel()

	store := mustPostgresStore(t)
	ctx := context.Background()
	seedProductivityMetricsFixture(t, store)

	if err := store.EnsureSchema(ctx); err != nil {
		t.Fatalf("EnsureSchema() second pass error = %v", err)
	}

	refreshMaterializedViewsConcurrently(t, store)

	type throughputRow struct {
		Day             string
		Project         string
		IssuesCompleted int64
		PRsMerged       int64
	}
	var throughput []throughputRow
	rows, err := store.pool.Query(ctx, `
		SELECT day::text, project, issues_completed, prs_merged
		FROM daily_throughput
		ORDER BY day, project
	`)
	if err != nil {
		t.Fatalf("query daily_throughput error = %v", err)
	}
	for rows.Next() {
		var row throughputRow
		if err := rows.Scan(&row.Day, &row.Project, &row.IssuesCompleted, &row.PRsMerged); err != nil {
			t.Fatalf("scan daily_throughput row error = %v", err)
		}
		throughput = append(throughput, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate daily_throughput rows error = %v", err)
	}
	rows.Close()

	if got, want := throughput, []throughputRow{
		{Day: "2026-04-10", Project: "/repo-alpha", IssuesCompleted: 2, PRsMerged: 2},
		{Day: "2026-04-11", Project: "/repo-alpha", IssuesCompleted: 1, PRsMerged: 0},
		{Day: "2026-04-11", Project: "/repo-beta", IssuesCompleted: 2, PRsMerged: 0},
		{Day: "2026-04-11", Project: "/repo-gamma", IssuesCompleted: 1, PRsMerged: 0},
		{Day: "2026-04-12", Project: "/repo-alpha", IssuesCompleted: 0, PRsMerged: 1},
	}; !slices.Equal(got, want) {
		t.Fatalf("daily_throughput rows = %#v, want %#v", got, want)
	}

	type cycleRow struct {
		Day                  string
		Project              string
		MedianCycleTimeHours float64
		P90CycleTimeHours    float64
		CodingIsNull         bool
		CIIsNull             bool
		ReviewIsNull         bool
		MergeQueueIsNull     bool
	}
	var cycleRows []cycleRow
	rows, err = store.pool.Query(ctx, `
		SELECT
			day::text,
			project,
			median_cycle_time_hours,
			p90_cycle_time_hours,
			median_coding_hours IS NULL,
			median_ci_hours IS NULL,
			median_review_hours IS NULL,
			median_merge_queue_hours IS NULL
		FROM daily_cycle_time
		ORDER BY day, project
	`)
	if err != nil {
		t.Fatalf("query daily_cycle_time error = %v", err)
	}
	for rows.Next() {
		var row cycleRow
		if err := rows.Scan(
			&row.Day,
			&row.Project,
			&row.MedianCycleTimeHours,
			&row.P90CycleTimeHours,
			&row.CodingIsNull,
			&row.CIIsNull,
			&row.ReviewIsNull,
			&row.MergeQueueIsNull,
		); err != nil {
			t.Fatalf("scan daily_cycle_time row error = %v", err)
		}
		cycleRows = append(cycleRows, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate daily_cycle_time rows error = %v", err)
	}
	rows.Close()

	if got, want := cycleRows, []cycleRow{
		{Day: "2026-04-10", Project: "/repo-alpha", MedianCycleTimeHours: 8, P90CycleTimeHours: 9.6, CodingIsNull: true, CIIsNull: true, ReviewIsNull: true, MergeQueueIsNull: true},
		{Day: "2026-04-11", Project: "/repo-alpha", MedianCycleTimeHours: 12, P90CycleTimeHours: 12, CodingIsNull: true, CIIsNull: true, ReviewIsNull: true, MergeQueueIsNull: true},
		{Day: "2026-04-11", Project: "/repo-beta", MedianCycleTimeHours: 7.5, P90CycleTimeHours: 8.7, CodingIsNull: true, CIIsNull: true, ReviewIsNull: true, MergeQueueIsNull: true},
		{Day: "2026-04-11", Project: "/repo-gamma", MedianCycleTimeHours: 6, P90CycleTimeHours: 6, CodingIsNull: true, CIIsNull: true, ReviewIsNull: true, MergeQueueIsNull: true},
	}; !slices.EqualFunc(got, want, func(a, b cycleRow) bool {
		return a.Day == b.Day &&
			a.Project == b.Project &&
			floatEqual(a.MedianCycleTimeHours, b.MedianCycleTimeHours) &&
			floatEqual(a.P90CycleTimeHours, b.P90CycleTimeHours) &&
			a.CodingIsNull == b.CodingIsNull &&
			a.CIIsNull == b.CIIsNull &&
			a.ReviewIsNull == b.ReviewIsNull &&
			a.MergeQueueIsNull == b.MergeQueueIsNull
	}) {
		t.Fatalf("daily_cycle_time rows = %#v, want %#v", got, want)
	}

	type qualityRow struct {
		Day                string
		Project            string
		TotalTasks         int64
		StuckTasks         int64
		StuckRate          sql.NullFloat64
		TotalNudges        int64
		NudgesPerCompleted sql.NullFloat64
		TotalRestarts      int64
		RestartRate        sql.NullFloat64
	}
	var qualityRows []qualityRow
	rows, err = store.pool.Query(ctx, `
		SELECT
			day::text,
			project,
			total_tasks,
			stuck_tasks,
			stuck_rate,
			total_nudges,
			nudges_per_completed,
			total_restarts,
			restart_rate
		FROM daily_quality
		ORDER BY day, project
	`)
	if err != nil {
		t.Fatalf("query daily_quality error = %v", err)
	}
	for rows.Next() {
		var row qualityRow
		if err := rows.Scan(
			&row.Day,
			&row.Project,
			&row.TotalTasks,
			&row.StuckTasks,
			&row.StuckRate,
			&row.TotalNudges,
			&row.NudgesPerCompleted,
			&row.TotalRestarts,
			&row.RestartRate,
		); err != nil {
			t.Fatalf("scan daily_quality row error = %v", err)
		}
		qualityRows = append(qualityRows, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate daily_quality rows error = %v", err)
	}
	rows.Close()

	if got, want := qualityRows, []qualityRow{
		{Day: "2026-04-10", Project: "/repo-alpha", TotalTasks: 2, StuckTasks: 1, StuckRate: validFloat(0.5), TotalNudges: 2, NudgesPerCompleted: validFloat(1), TotalRestarts: 0, RestartRate: validFloat(0)},
		{Day: "2026-04-11", Project: "/repo-alpha", TotalTasks: 1, StuckTasks: 0, StuckRate: validFloat(0), TotalNudges: 1, NudgesPerCompleted: validFloat(1), TotalRestarts: 1, RestartRate: validFloat(1)},
		{Day: "2026-04-11", Project: "/repo-beta", TotalTasks: 2, StuckTasks: 1, StuckRate: validFloat(0.5), TotalNudges: 1, NudgesPerCompleted: validFloat(0.5), TotalRestarts: 0, RestartRate: validFloat(0)},
		{Day: "2026-04-11", Project: "/repo-gamma", TotalTasks: 1, StuckTasks: 0, StuckRate: validFloat(0), TotalNudges: 0, NudgesPerCompleted: validFloat(0), TotalRestarts: 0, RestartRate: validFloat(0)},
		{Day: "2026-04-12", Project: "/repo-alpha", TotalTasks: 0, StuckTasks: 0, StuckRate: sql.NullFloat64{}, TotalNudges: 1, NudgesPerCompleted: sql.NullFloat64{}, TotalRestarts: 0, RestartRate: sql.NullFloat64{}},
	}; !slices.EqualFunc(got, want, func(a, b qualityRow) bool {
		return a.Day == b.Day &&
			a.Project == b.Project &&
			a.TotalTasks == b.TotalTasks &&
			a.StuckTasks == b.StuckTasks &&
			nullFloatEqual(a.StuckRate, b.StuckRate) &&
			a.TotalNudges == b.TotalNudges &&
			nullFloatEqual(a.NudgesPerCompleted, b.NudgesPerCompleted) &&
			a.TotalRestarts == b.TotalRestarts &&
			nullFloatEqual(a.RestartRate, b.RestartRate)
	}) {
		t.Fatalf("daily_quality rows = %#v, want %#v", got, want)
	}

	type workerRow struct {
		Day            string
		Project        string
		Agent          string
		ActiveWorkers  int64
		TasksCompleted int64
		TasksPerWorker float64
	}
	var workerRows []workerRow
	rows, err = store.pool.Query(ctx, `
		SELECT day::text, project, agent, active_workers, tasks_completed, tasks_per_worker
		FROM daily_workers
		ORDER BY day, project, agent
	`)
	if err != nil {
		t.Fatalf("query daily_workers error = %v", err)
	}
	for rows.Next() {
		var row workerRow
		if err := rows.Scan(&row.Day, &row.Project, &row.Agent, &row.ActiveWorkers, &row.TasksCompleted, &row.TasksPerWorker); err != nil {
			t.Fatalf("scan daily_workers row error = %v", err)
		}
		workerRows = append(workerRows, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate daily_workers rows error = %v", err)
	}
	rows.Close()

	if got, want := workerRows, []workerRow{
		{Day: "2026-04-10", Project: "/repo-alpha", Agent: "claude", ActiveWorkers: 1, TasksCompleted: 1, TasksPerWorker: 1},
		{Day: "2026-04-10", Project: "/repo-alpha", Agent: "codex", ActiveWorkers: 1, TasksCompleted: 1, TasksPerWorker: 1},
		{Day: "2026-04-11", Project: "/repo-alpha", Agent: "codex", ActiveWorkers: 1, TasksCompleted: 1, TasksPerWorker: 1},
		{Day: "2026-04-11", Project: "/repo-beta", Agent: "claude", ActiveWorkers: 1, TasksCompleted: 2, TasksPerWorker: 2},
		{Day: "2026-04-11", Project: "/repo-gamma", Agent: "codex", ActiveWorkers: 1, TasksCompleted: 1, TasksPerWorker: 1},
	}; !slices.EqualFunc(got, want, func(a, b workerRow) bool {
		return a.Day == b.Day &&
			a.Project == b.Project &&
			a.Agent == b.Agent &&
			a.ActiveWorkers == b.ActiveWorkers &&
			a.TasksCompleted == b.TasksCompleted &&
			floatEqual(a.TasksPerWorker, b.TasksPerWorker)
	}) {
		t.Fatalf("daily_workers rows = %#v, want %#v", got, want)
	}
}

func validFloat(value float64) sql.NullFloat64 {
	return sql.NullFloat64{Float64: value, Valid: true}
}

func nullFloatEqual(got, want sql.NullFloat64) bool {
	if got.Valid != want.Valid {
		return false
	}
	if !got.Valid {
		return true
	}
	return floatEqual(got.Float64, want.Float64)
}

func floatEqual(got, want float64) bool {
	return math.Abs(got-want) < 1e-9
}

func mustPostgresStore(t *testing.T) *PostgresStore {
	t.Helper()

	h := newPostgresContractHarness(t)
	store, ok := h.store.(*PostgresStore)
	if !ok {
		t.Fatalf("store type = %T, want *PostgresStore", h.store)
	}
	return store
}

func seedProductivityMetricsFixture(t *testing.T, store *PostgresStore) {
	t.Helper()

	ctx := context.Background()
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO tasks(project, issue, status, state, agent, worker_id, clone_path, branch, pr_number, created_at, updated_at)
		VALUES
			('/repo-alpha', 'LAB-1001', 'done', 'done', 'codex', 'worker-a1', '/tmp/alpha-1', 'lab-1001', 101, '2026-04-10T08:00:00Z', '2026-04-10T14:00:00Z'),
			('/repo-alpha', 'LAB-1002', 'done', 'done', 'claude', 'worker-a2', '/tmp/alpha-2', 'lab-1002', 102, '2026-04-10T09:00:00Z', '2026-04-10T19:00:00Z'),
			('/repo-alpha', 'LAB-1003', 'done', 'done', 'codex', 'worker-a1', '/tmp/alpha-1', 'lab-1003', 103, '2026-04-11T08:00:00Z', '2026-04-11T20:00:00Z'),
			('/repo-beta', 'LAB-2001', 'done', 'done', 'claude', 'worker-b1', '/tmp/beta-1', 'lab-2001', 201, '2026-04-11T10:00:00Z', '2026-04-11T19:00:00Z'),
			('/repo-beta', 'LAB-2002', 'done', 'done', 'claude', 'worker-b1', '/tmp/beta-1', 'lab-2002', 202, '2026-04-11T11:00:00Z', '2026-04-11T17:00:00Z'),
			('/repo-gamma', 'LAB-3001', 'done', 'done', 'codex', 'worker-g1', '/tmp/gamma-1', 'lab-3001', 301, '2026-04-11T07:00:00Z', '2026-04-11T13:00:00Z')
	`); err != nil {
		t.Fatalf("insert tasks error = %v", err)
	}

	if _, err := store.pool.Exec(ctx, `
		INSERT INTO events(project, kind, issue, worker_id, message, created_at)
		VALUES
			('/repo-alpha', 'pr.merged', 'LAB-1001', 'worker-a1', 'merged 1001', '2026-04-10T14:05:00Z'),
			('/repo-alpha', 'pr.merged', 'LAB-1002', 'worker-a2', 'merged 1002', '2026-04-10T19:05:00Z'),
			('/repo-alpha', 'worker.escalated', 'LAB-1001', 'worker-a1', 'escalated', '2026-04-10T12:00:00Z'),
			('/repo-alpha', 'worker.nudged', 'LAB-1001', 'worker-a1', 'nudge', '2026-04-10T09:30:00Z'),
			('/repo-alpha', 'worker.nudged_ci', 'LAB-1001', 'worker-a1', 'ci nudge', '2026-04-10T11:00:00Z'),
			('/repo-alpha', 'worker.nudged_review', 'LAB-1003', 'worker-a1', 'review nudge', '2026-04-11T18:00:00Z'),
			('/repo-alpha', 'worker.crash_report', 'LAB-1003', 'worker-a1', 'crash', '2026-04-11T12:30:00Z'),
			('/repo-alpha', 'pr.merged', 'LAB-1999', 'worker-a9', 'merged orphan', '2026-04-12T08:00:00Z'),
			('/repo-alpha', 'worker.nudged_conflict', 'LAB-1999', 'worker-a9', 'conflict nudge', '2026-04-12T08:30:00Z'),
			('/repo-beta', 'worker.escalated', 'LAB-2001', 'worker-b1', 'escalated', '2026-04-11T15:00:00Z'),
			('/repo-beta', 'worker.nudged', 'LAB-2001', 'worker-b1', 'nudge', '2026-04-11T16:00:00Z')
	`); err != nil {
		t.Fatalf("insert events error = %v", err)
	}
}

func refreshMaterializedViewsConcurrently(t *testing.T, store *PostgresStore) {
	t.Helper()

	for _, view := range []string{
		"daily_throughput",
		"daily_cycle_time",
		"daily_quality",
		"daily_workers",
	} {
		if _, err := store.pool.Exec(context.Background(), fmt.Sprintf(`REFRESH MATERIALIZED VIEW CONCURRENTLY %s`, view)); err != nil {
			t.Fatalf("REFRESH MATERIALIZED VIEW CONCURRENTLY %s error = %v", view, err)
		}
	}
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
