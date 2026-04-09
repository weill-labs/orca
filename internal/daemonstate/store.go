package state

import (
	"context"
	"encoding/json"
	"errors"
	"time"
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
