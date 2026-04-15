package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	legacy "github.com/weill-labs/orca/internal/state"
)

var ErrNotFound = errors.New("state: not found")

type Reader interface {
	ProjectStatus(ctx context.Context, project string) (ProjectStatus, error)
	TaskStatus(ctx context.Context, project, issue string) (TaskStatus, error)
	ListWorkers(ctx context.Context, project string) ([]Worker, error)
	ListClones(ctx context.Context, project string) ([]Clone, error)
	Events(ctx context.Context, project string, afterID int64) (<-chan Event, <-chan error)
}

type Writer interface {
	EnsureSchema(ctx context.Context) error
	UpsertDaemon(ctx context.Context, project string, daemon DaemonStatus) error
	MarkDaemonStopped(ctx context.Context, project string, updatedAt time.Time) error
	UpsertTask(ctx context.Context, project string, task Task) error
	UpdateTaskStatus(ctx context.Context, project, issue, status string, updatedAt time.Time) (Task, error)
	AppendEvent(ctx context.Context, event Event) (Event, error)
}

type Store interface {
	Reader
	Writer
	Close() error
}

type Backend interface {
	Store
	WorkerByID(ctx context.Context, project, workerID string) (Worker, error)
	WorkerByPane(ctx context.Context, project, paneID string) (Worker, error)
	NonTerminalTasks(ctx context.Context, project string) ([]Task, error)
	StaleCloneOccupancies(ctx context.Context, project string) ([]CloneOccupancy, error)
	UpsertWorker(ctx context.Context, project string, worker Worker) error
	ClaimWorker(ctx context.Context, project string, worker Worker) (Worker, error)
	DeleteWorker(ctx context.Context, project, workerID string) error
	DeleteTask(ctx context.Context, project, issue string) error
	ClaimTask(ctx context.Context, project string, task Task) (*Task, error)
	ActiveAssignments(ctx context.Context, project string) ([]Assignment, error)
	ActiveAssignmentByIssue(ctx context.Context, project, issue string) (Assignment, error)
	ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (Assignment, error)
	EnqueueMergeEntry(ctx context.Context, entry MergeQueueEntry) (int, error)
	MergeEntry(ctx context.Context, project string, prNumber int) (*MergeQueueEntry, error)
	MergeEntries(ctx context.Context, project string) ([]MergeQueueEntry, error)
	UpdateMergeEntry(ctx context.Context, entry MergeQueueEntry) error
	DeleteMergeEntry(ctx context.Context, project string, prNumber int) error
	EnsureClone(ctx context.Context, project, path string) (legacy.CloneRecord, error)
	TryOccupyClone(ctx context.Context, project, path, branch, task string) (bool, error)
	MarkCloneFree(ctx context.Context, project, path string) error
	TasksByPane(ctx context.Context, project, paneID string) ([]Task, error)
	AllNonTerminalTasks(ctx context.Context) ([]Task, error)
	AllActiveAssignments(ctx context.Context) ([]Assignment, error)
	AllMergeEntries(ctx context.Context) ([]MergeQueueEntry, error)
}

type DaemonStatus struct {
	Session   string    `json:"session"`
	PID       int       `json:"pid"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Summary struct {
	Tasks          int `json:"tasks"`
	Queued         int `json:"queued"`
	Active         int `json:"active"`
	Done           int `json:"done"`
	Cancelled      int `json:"cancelled"`
	Workers        int `json:"workers"`
	HealthyWorkers int `json:"healthy_workers"`
	StuckWorkers   int `json:"stuck_workers"`
	Clones         int `json:"clones"`
	FreeClones     int `json:"free_clones"`
}

type ProjectStatus struct {
	Project string        `json:"project"`
	Daemon  *DaemonStatus `json:"daemon,omitempty"`
	Summary Summary       `json:"summary"`
	Tasks   []Task        `json:"tasks"`
}

type Task struct {
	Project       string    `json:"project,omitempty"`
	Issue         string    `json:"issue"`
	Status        string    `json:"status"`
	Agent         string    `json:"agent"`
	Prompt        string    `json:"prompt,omitempty"`
	CallerPane    string    `json:"caller_pane,omitempty"`
	WorkerID      string    `json:"worker_id,omitempty"`
	CurrentPaneID string    `json:"current_pane_id,omitempty"`
	ClonePath     string    `json:"clone_path,omitempty"`
	PRNumber      *int      `json:"pr_number,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type TaskStatus struct {
	Task   Task    `json:"task"`
	Events []Event `json:"events"`
}

type Assignment struct {
	Task   Task   `json:"task"`
	Worker Worker `json:"worker"`
}

type Worker struct {
	Project                      string    `json:"project,omitempty"`
	WorkerID                     string    `json:"worker_id"`
	CurrentPaneID                string    `json:"current_pane_id,omitempty"`
	Agent                        string    `json:"agent"`
	State                        string    `json:"state"`
	Issue                        string    `json:"issue,omitempty"`
	ClonePath                    string    `json:"clone_path,omitempty"`
	LastReviewCount              int       `json:"last_review_count,omitempty"`
	LastInlineReviewCommentCount int       `json:"last_inline_review_comment_count,omitempty"`
	LastIssueCommentCount        int       `json:"last_issue_comment_count,omitempty"`
	ReviewNudgeCount             int       `json:"review_nudge_count,omitempty"`
	LastCIState                  string    `json:"last_ci_state,omitempty"`
	CINudgeCount                 int       `json:"ci_nudge_count,omitempty"`
	CIFailurePollCount           int       `json:"ci_failure_poll_count,omitempty"`
	CIEscalated                  bool      `json:"ci_escalated,omitempty"`
	LastMergeableState           string    `json:"last_mergeable_state,omitempty"`
	NudgeCount                   int       `json:"nudge_count,omitempty"`
	RestartCount                 int       `json:"restart_count,omitempty"`
	LastCapture                  string    `json:"last_capture,omitempty"`
	LastActivityAt               time.Time `json:"last_activity_at,omitempty"`
	LastPRNumber                 int       `json:"last_pr_number,omitempty"`
	LastPushAt                   time.Time `json:"last_push_at,omitempty"`
	LastPRPollAt                 time.Time `json:"last_pr_poll_at,omitempty"`
	FirstCrashAt                 time.Time `json:"first_crash_at,omitempty"`
	CreatedAt                    time.Time `json:"created_at"`
	LastSeenAt                   time.Time `json:"last_seen_at"`
}

type MergeQueueEntry struct {
	Project   string    `json:"project"`
	Issue     string    `json:"issue"`
	PRNumber  int       `json:"pr_number"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Clone struct {
	Path      string    `json:"path"`
	Status    string    `json:"status"`
	Issue     string    `json:"issue,omitempty"`
	Branch    string    `json:"branch,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type CloneOccupancy struct {
	Project       string    `json:"project,omitempty"`
	Path          string    `json:"path"`
	CurrentBranch string    `json:"branch,omitempty"`
	AssignedTask  string    `json:"issue,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Event struct {
	ID        int64           `json:"id"`
	Project   string          `json:"project"`
	Kind      string          `json:"kind"`
	Issue     string          `json:"issue,omitempty"`
	WorkerID  string          `json:"worker_id,omitempty"`
	Message   string          `json:"message"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}
