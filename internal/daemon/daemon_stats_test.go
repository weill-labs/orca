package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type daemonStatsTestMessage struct {
	Goroutines          int `json:"goroutines"`
	InFlightTasks       int `json:"in_flight_tasks"`
	PostgresConnections struct {
		OpenConnections int `json:"open_connections"`
		InUse           int `json:"in_use"`
		Idle            int `json:"idle"`
	} `json:"postgres_connections"`
	BreakerStates struct {
		Open   int `json:"open"`
		Closed int `json:"closed"`
	} `json:"breaker_states"`
	GitHubAPI struct {
		GraphQL struct {
			LastPollCalls              int `json:"last_poll_calls"`
			LastPollEstimatedPoints    int `json:"last_poll_estimated_points"`
			LastPollTerminalStateCalls int `json:"last_poll_terminal_state_calls"`
			LastPollReviewCalls        int `json:"last_poll_review_calls"`
			LastPollOtherCalls         int `json:"last_poll_other_calls"`
			TotalCalls                 int `json:"total_calls"`
			TotalEstimatedPoints       int `json:"total_estimated_points"`
		} `json:"graphql"`
		REST struct {
			LastPollCalls       int `json:"last_poll_calls"`
			LastPollReviewCalls int `json:"last_poll_review_calls"`
			LastPollOtherCalls  int `json:"last_poll_other_calls"`
			TotalCalls          int `json:"total_calls"`
		} `json:"rest"`
	} `json:"github_api"`
	ReconcileFindings   int               `json:"reconcile_findings"`
	WorkerRows          int               `json:"worker_rows"`
	PoolEntriesByStatus map[string]int    `json:"pool_entries_by_status"`
	CollectionErrors    map[string]string `json:"collection_errors,omitempty"`
}

type daemonStatsTestState struct {
	StateStore
	openConnections int
	inUse           int
	idle            int
	reconcileCount  int
	reconcileErr    error
}

func (s daemonStatsTestState) PostgresConnectionStats() (int, int, int) {
	return s.openConnections, s.inUse, s.idle
}

func (s daemonStatsTestState) ReconcileFindingCount(ctx context.Context, project string) (int, error) {
	if s.reconcileErr != nil {
		return 0, s.reconcileErr
	}
	return s.reconcileCount, nil
}

type daemonStatsTestPool struct {
	Pool
	entries []Clone
	err     error
}

func (p daemonStatsTestPool) PoolEntries(ctx context.Context, project string) ([]Clone, error) {
	if p.err != nil {
		return nil, p.err
	}
	entries := make([]Clone, len(p.entries))
	copy(entries, p.entries)
	return entries, nil
}

func TestDaemonStatsEmitsOnStatsTicker(t *testing.T) {
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	statsTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker, statsTicker)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.StatsInterval = 5 * time.Minute
	})
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := d.Stop(ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	if got := deps.events.countType("daemon.stats"); got != 0 {
		t.Fatalf("daemon.stats events before ticker = %d, want 0", got)
	}

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	waitFor(t, "first stats event", func() bool {
		return deps.events.countType("daemon.stats") == 1
	})

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	waitFor(t, "second stats event", func() bool {
		return deps.events.countType("daemon.stats") == 2
	})
}

func TestDaemonStatsMessageIncludesPopulatedFields(t *testing.T) {
	deps := newTestDeps(t)
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1872-A",
		Status:       TaskStatusActive,
		WorkerID:     "worker-01",
		PaneID:       "pane-01",
		PaneName:     "worker-01",
		ClonePath:    "/tmp/project/.orca/pool/clone-01",
		AgentProfile: "codex",
	})
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1872-B",
		Status:       TaskStatusStarting,
		WorkerID:     "worker-02",
		PaneID:       "pane-02",
		PaneName:     "worker-02",
		ClonePath:    "/tmp/project/.orca/pool/clone-02",
		AgentProfile: "codex",
	})
	deps.state.putTaskForTest(Task{
		Project: "/tmp/project",
		Issue:   "LAB-1872-C",
		Status:  TaskStatusDone,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-01",
		PaneID:       "pane-01",
		PaneName:     "worker-01",
		Issue:        "LAB-1872-A",
		ClonePath:    "/tmp/project/.orca/pool/clone-01",
		AgentProfile: "codex",
	}); err != nil {
		t.Fatalf("PutWorker(worker-01) error = %v", err)
	}
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-02",
		PaneID:       "pane-02",
		PaneName:     "worker-02",
		Issue:        "LAB-1872-B",
		ClonePath:    "/tmp/project/.orca/pool/clone-02",
		AgentProfile: "codex",
	}); err != nil {
		t.Fatalf("PutWorker(worker-02) error = %v", err)
	}
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:      "/tmp/project",
		WorkerID:     "worker-03",
		PaneID:       "pane-03",
		PaneName:     "worker-03",
		AgentProfile: "codex",
	}); err != nil {
		t.Fatalf("PutWorker(worker-03) error = %v", err)
	}

	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	statsTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker, statsTicker)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.StatsInterval = 5 * time.Minute
		opts.State = daemonStatsTestState{
			StateStore:      deps.state,
			openConnections: 7,
			inUse:           3,
			idle:            4,
			reconcileCount:  4,
		}
		opts.Pool = daemonStatsTestPool{
			Pool: deps.pool,
			entries: []Clone{
				{Name: "clone-01", Status: "free"},
				{Name: "clone-02", Status: "occupied"},
				{Name: "clone-03", Status: "occupied"},
				{Name: "clone-04", Status: "quarantined"},
			},
		}
	})
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := d.Stop(ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	stats := lastDaemonStatsMessage(t, deps)

	if stats.Goroutines <= 0 {
		t.Fatalf("goroutines = %d, want positive", stats.Goroutines)
	}
	if got, want := stats.InFlightTasks, 2; got != want {
		t.Fatalf("in_flight_tasks = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.OpenConnections, 7; got != want {
		t.Fatalf("postgres_connections.open_connections = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.InUse, 3; got != want {
		t.Fatalf("postgres_connections.in_use = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.Idle, 4; got != want {
		t.Fatalf("postgres_connections.idle = %d, want %d", got, want)
	}
	if got, want := stats.BreakerStates.Open, 0; got != want {
		t.Fatalf("breaker_states.open = %d, want %d", got, want)
	}
	if got, want := stats.BreakerStates.Closed, 1; got != want {
		t.Fatalf("breaker_states.closed = %d, want %d", got, want)
	}
	if got, want := stats.ReconcileFindings, 4; got != want {
		t.Fatalf("reconcile_findings = %d, want %d", got, want)
	}
	if got, want := stats.WorkerRows, 3; got != want {
		t.Fatalf("worker_rows = %d, want %d", got, want)
	}
	if got, want := stats.PoolEntriesByStatus["free"], 1; got != want {
		t.Fatalf("pool_entries_by_status.free = %d, want %d", got, want)
	}
	if got, want := stats.PoolEntriesByStatus["occupied"], 2; got != want {
		t.Fatalf("pool_entries_by_status.occupied = %d, want %d", got, want)
	}
	if got, want := stats.PoolEntriesByStatus["quarantined"], 1; got != want {
		t.Fatalf("pool_entries_by_status.quarantined = %d, want %d", got, want)
	}
}

func TestDaemonStatsIncludesGitHubAPIPollConsumption(t *testing.T) {
	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1918", "pane-1", 42)

	refs := []githubPRTerminalStateRef{
		{Key: prTerminalStateKey{Project: "/tmp/project", PRNumber: 42}, Owner: "weill-labs", Repo: "orca", PRNumber: 42},
	}
	deps.commands.queue("gh", prTerminalStateGraphQLArgs(refs), `{
		"data": {
			"pr0": {"pullRequest": {"number": 42, "merged": false, "mergedAt": null, "state": "OPEN"}}
		}
	}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pass"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}, ``, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	d.runPollTick(context.Background())
	stats := d.collectDaemonStats(context.Background())

	if got, want := stats.GitHubAPI.GraphQL.LastPollCalls, 4; got != want {
		t.Fatalf("github_api.graphql.last_poll_calls = %d, want %d", got, want)
	}
	if got, want := stats.GitHubAPI.GraphQL.LastPollEstimatedPoints, 4; got != want {
		t.Fatalf("github_api.graphql.last_poll_estimated_points = %d, want %d", got, want)
	}
	if got, want := stats.GitHubAPI.GraphQL.LastPollTerminalStateCalls, 1; got != want {
		t.Fatalf("github_api.graphql.last_poll_terminal_state_calls = %d, want %d", got, want)
	}
	if got, want := stats.GitHubAPI.GraphQL.LastPollReviewCalls, 1; got != want {
		t.Fatalf("github_api.graphql.last_poll_review_calls = %d, want %d", got, want)
	}
	if got, want := stats.GitHubAPI.GraphQL.LastPollOtherCalls, 2; got != want {
		t.Fatalf("github_api.graphql.last_poll_other_calls = %d, want %d", got, want)
	}
	if got, want := stats.GitHubAPI.REST.LastPollCalls, 0; got != want {
		t.Fatalf("github_api.rest.last_poll_calls = %d, want %d", got, want)
	}
}

func TestDaemonStatsMessageIncludesOptionalSourceErrors(t *testing.T) {
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	statsTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker, statsTicker)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.StatsInterval = 5 * time.Minute
		opts.State = daemonStatsTestState{
			StateStore:   deps.state,
			reconcileErr: errors.New("reconcile unavailable"),
		}
		opts.Pool = daemonStatsTestPool{
			Pool: deps.pool,
			err:  errors.New("pool unavailable"),
		}
	})
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := d.Stop(ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	stats := lastDaemonStatsMessage(t, deps)

	if got, want := stats.CollectionErrors["reconcile_findings"], "reconcile unavailable"; got != want {
		t.Fatalf("collection_errors.reconcile_findings = %q, want %q", got, want)
	}
	if got, want := stats.CollectionErrors["pool_entries_by_status"], "pool unavailable"; got != want {
		t.Fatalf("collection_errors.pool_entries_by_status = %q, want %q", got, want)
	}
}

func TestDaemonStatsHandlesMissingOptionalSources(t *testing.T) {
	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	pollTicker := newFakeTicker()
	statsTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, pollTicker, statsTicker)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.StatsInterval = 5 * time.Minute
	})
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer func() {
		if err := d.Stop(ctx); err != nil {
			t.Fatalf("Stop() error = %v", err)
		}
	}()

	deps.clock.Advance(5 * time.Minute)
	statsTicker.tick(deps.clock.Now())
	stats := lastDaemonStatsMessage(t, deps)

	if got, want := stats.PostgresConnections.OpenConnections, 0; got != want {
		t.Fatalf("postgres_connections.open_connections = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.InUse, 0; got != want {
		t.Fatalf("postgres_connections.in_use = %d, want %d", got, want)
	}
	if got, want := stats.PostgresConnections.Idle, 0; got != want {
		t.Fatalf("postgres_connections.idle = %d, want %d", got, want)
	}
	if stats.PoolEntriesByStatus == nil {
		t.Fatal("pool_entries_by_status = nil, want empty object")
	}
	if got := len(stats.PoolEntriesByStatus); got != 0 {
		t.Fatalf("pool_entries_by_status length = %d, want 0", got)
	}
}

func lastDaemonStatsMessage(t *testing.T, deps *testDeps) daemonStatsTestMessage {
	t.Helper()

	waitFor(t, "stats event", func() bool {
		return deps.events.countType("daemon.stats") > 0
	})
	event, ok := deps.events.lastEventOfType("daemon.stats")
	if !ok {
		t.Fatal("daemon.stats event missing")
	}
	var stats daemonStatsTestMessage
	if err := json.Unmarshal([]byte(event.Message), &stats); err != nil {
		t.Fatalf("daemon.stats message is not valid JSON: %v\nmessage: %s", err, event.Message)
	}
	return stats
}
