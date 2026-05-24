package daemon

import (
	"context"
	"encoding/json"
	"runtime"
	"strings"
)

const unknownPoolEntryStatus = "unknown"

type daemonStatsSnapshot struct {
	Goroutines          int                        `json:"goroutines"`
	InFlightTasks       int                        `json:"in_flight_tasks"`
	PostgresConnections postgresConnectionSnapshot `json:"postgres_connections"`
	BreakerStates       circuitBreakerStateCounts  `json:"breaker_states"`
	ReconcileFindings   int                        `json:"reconcile_findings"`
	WorkerRows          int                        `json:"worker_rows"`
	PoolEntriesByStatus map[string]int             `json:"pool_entries_by_status"`
	CollectionErrors    map[string]string          `json:"collection_errors,omitempty"`
}

type postgresConnectionSnapshot struct {
	OpenConnections int `json:"open_connections"`
	InUse           int `json:"in_use"`
	Idle            int `json:"idle"`
}

type circuitBreakerStateCounts struct {
	Open   int `json:"open"`
	Closed int `json:"closed"`
}

type postgresConnectionStatsSource interface {
	PostgresConnectionStats() (open, inUse, idle int)
}

type reconcileFindingCountSource interface {
	ReconcileFindingCount(ctx context.Context, project string) (int, error)
}

type poolEntriesSource interface {
	PoolEntries(ctx context.Context, project string) ([]Clone, error)
}

func (d *Daemon) emitDaemonStats(ctx context.Context) {
	snapshot := d.collectDaemonStats(ctx)
	message, err := json.Marshal(snapshot)
	if err != nil {
		message = []byte(`{"collection_errors":{"message":"marshal daemon stats"}}`)
	}
	d.emit(ctx, Event{
		Time:    d.now(),
		Type:    EventDaemonStats,
		Project: d.project,
		Message: string(message),
	})
}

func (d *Daemon) collectDaemonStats(ctx context.Context) daemonStatsSnapshot {
	snapshot := daemonStatsSnapshot{
		Goroutines:          runtime.NumGoroutine(),
		BreakerStates:       d.daemonCircuitBreakerStates(),
		PoolEntriesByStatus: make(map[string]int),
	}
	d.collectTaskStats(ctx, &snapshot)
	d.collectPostgresConnectionStats(&snapshot)
	d.collectReconcileFindingStats(ctx, &snapshot)
	d.collectPoolEntryStats(ctx, &snapshot)
	return snapshot
}

func (d *Daemon) collectTaskStats(ctx context.Context, snapshot *daemonStatsSnapshot) {
	tasks, err := d.state.NonTerminalTasks(ctx, d.project)
	if err != nil {
		snapshot.addCollectionError("in_flight_tasks", err)
	} else {
		for _, task := range tasks {
			if task.Status == TaskStatusActive || task.Status == TaskStatusStarting {
				snapshot.InFlightTasks++
			}
		}
	}

	workers, err := d.state.ListWorkers(ctx, d.project)
	if err != nil {
		snapshot.addCollectionError("worker_rows", err)
		return
	}
	snapshot.WorkerRows = len(workers)
}

func (d *Daemon) collectPostgresConnectionStats(snapshot *daemonStatsSnapshot) {
	source, ok := d.state.(postgresConnectionStatsSource)
	if !ok {
		return
	}
	open, inUse, idle := source.PostgresConnectionStats()
	snapshot.PostgresConnections = postgresConnectionSnapshot{
		OpenConnections: open,
		InUse:           inUse,
		Idle:            idle,
	}
}

func (d *Daemon) collectReconcileFindingStats(ctx context.Context, snapshot *daemonStatsSnapshot) {
	source, ok := d.state.(reconcileFindingCountSource)
	if !ok {
		return
	}
	count, err := source.ReconcileFindingCount(ctx, d.project)
	if err != nil {
		snapshot.addCollectionError("reconcile_findings", err)
		return
	}
	snapshot.ReconcileFindings = count
}

func (d *Daemon) collectPoolEntryStats(ctx context.Context, snapshot *daemonStatsSnapshot) {
	source, ok := d.pool.(poolEntriesSource)
	if !ok {
		return
	}
	entries, err := source.PoolEntries(ctx, d.project)
	if err != nil {
		snapshot.addCollectionError("pool_entries_by_status", err)
		return
	}
	for _, entry := range entries {
		status := strings.TrimSpace(string(entry.Status))
		if status == "" {
			status = unknownPoolEntryStatus
		}
		snapshot.PoolEntriesByStatus[status]++
	}
}

func (d *Daemon) daemonCircuitBreakerStates() circuitBreakerStateCounts {
	var counts circuitBreakerStateCounts
	for _, breaker := range d.daemonCircuitBreakers() {
		if breaker.IsOpen() {
			counts.Open++
			continue
		}
		counts.Closed++
	}
	return counts
}

func (d *Daemon) daemonCircuitBreakers() []*CircuitBreaker {
	if d.monitorGitHubCircuit == nil {
		return nil
	}
	return []*CircuitBreaker{d.monitorGitHubCircuit}
}

func (s *daemonStatsSnapshot) addCollectionError(field string, err error) {
	if err == nil {
		return
	}
	if s.CollectionErrors == nil {
		s.CollectionErrors = make(map[string]string)
	}
	s.CollectionErrors[field] = err.Error()
}
