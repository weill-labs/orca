package daemon

import (
	"context"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	legacy "github.com/weill-labs/orca/internal/state"
)

type daemonStateStore interface {
	state.Store
	WorkerByID(ctx context.Context, project, workerID string) (state.Worker, error)
	WorkerByPane(ctx context.Context, project, paneID string) (state.Worker, error)
	NonTerminalTasks(ctx context.Context, project string) ([]state.Task, error)
	StaleCloneOccupancies(ctx context.Context, project string) ([]state.CloneOccupancy, error)
	UpsertWorker(ctx context.Context, project string, worker state.Worker) error
	ClaimWorker(ctx context.Context, project string, worker state.Worker) (state.Worker, error)
	DeleteWorker(ctx context.Context, project, workerID string) error
	DeleteTask(ctx context.Context, project, issue string) error
	ClaimTask(ctx context.Context, project string, task state.Task) (*state.Task, error)
	ActiveAssignments(ctx context.Context, project string) ([]state.Assignment, error)
	ActiveAssignmentByIssue(ctx context.Context, project, issue string) (state.Assignment, error)
	ActiveAssignmentByBranch(ctx context.Context, project, branch string) (state.Assignment, error)
	ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (state.Assignment, error)
	EnqueueMergeEntry(ctx context.Context, entry state.MergeQueueEntry) (int, error)
	MergeEntry(ctx context.Context, project string, prNumber int) (*state.MergeQueueEntry, error)
	MergeEntries(ctx context.Context, project string) ([]state.MergeQueueEntry, error)
	UpdateMergeEntry(ctx context.Context, entry state.MergeQueueEntry) error
	DeleteMergeEntry(ctx context.Context, project string, prNumber int) error
	EnsureClone(ctx context.Context, project, path string) (legacy.CloneRecord, error)
	TryOccupyClone(ctx context.Context, project, path, branch, task string) (bool, error)
	MarkCloneFree(ctx context.Context, project, path string) error
	UpdateTaskStatus(ctx context.Context, project, issue, status string, updatedAt time.Time) (state.Task, error)
	TasksByPane(ctx context.Context, project, paneID string) ([]state.Task, error)
}
