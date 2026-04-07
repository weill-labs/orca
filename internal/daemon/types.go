package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	"github.com/weill-labs/orca/internal/pool"
)

const (
	TaskStatusStarting  = "starting"
	TaskStatusActive    = "active"
	TaskStatusCancelled = "cancelled"
	TaskStatusDone      = "done"
	TaskStatusFailed    = "failed"

	IssueStateInProgress = "In Progress"
	IssueStateDone       = "Done"

	WorkerHealthHealthy   = "healthy"
	WorkerHealthStuck     = "stuck"
	WorkerHealthEscalated = "escalated"

	MergeQueueStatusQueued         = "queued"
	MergeQueueStatusAwaitingChecks = "awaiting_checks"
	MergeQueueStatusCheckingCI     = "checking_ci"
	MergeQueueStatusRebasing       = "rebasing"
	MergeQueueStatusMerging        = "merging"

	EventDaemonStarted         = "daemon.started"
	EventDaemonStopped         = "daemon.stopped"
	EventIssueStatusSkipped    = "issue.status_skipped"
	EventTaskAssigned          = "task.assigned"
	EventTaskAssignFailed      = "task.assign_failed"
	EventTaskCancelled         = "task.cancelled"
	EventTaskCompleted         = "task.completed"
	EventTaskFailed            = "task.failed"
	EventTaskCompletionFailed  = "task.completion_failed"
	EventWorkerHandshake       = "worker.handshake"
	EventWorkerNudged          = "worker.nudged"
	EventWorkerNudgedCI        = "worker.nudged_ci"
	EventWorkerCIEscalated     = "worker.ci_escalated"
	EventWorkerNudgedConflict  = "worker.nudged_conflict"
	EventWorkerNudgedReview    = "worker.nudged_review"
	EventWorkerReviewEscalated = "worker.review_escalated"
	EventWorkerEscalated       = "worker.escalated"
	EventWorkerRecovered       = "worker.recovered"
	EventWorkerPostmortem      = "worker.postmortem"
	EventPRDetected            = "pr.detected"
	EventPREnqueued            = "pr.enqueued"
	EventPRLandingStarted      = "pr.landing_started"
	EventPRLandingFailed       = "pr.landing_failed"
	EventPRMerged              = "pr.merged"
)

var (
	ErrAlreadyStarted = errors.New("daemon already started")
	ErrNotStarted     = errors.New("daemon not started")
	ErrTaskNotFound   = errors.New("task not found")
	ErrWorkerNotFound = errors.New("worker not found")
)

type Options struct {
	Project          string
	Session          string
	LeadPane         string
	PIDPath          string
	Config           ConfigProvider
	State            StateStore
	Pool             Pool
	Amux             AmuxClient
	IssueTracker     IssueTracker
	Commands         CommandRunner
	Events           EventSink
	Now              func() time.Time
	NewTicker        func(time.Duration) Ticker
	Sleep            func(context.Context, time.Duration) error
	CaptureInterval  time.Duration
	PollInterval     time.Duration
	MergeGracePeriod time.Duration
}

type ConfigProvider interface {
	AgentProfile(ctx context.Context, name string) (AgentProfile, error)
}

type StateStore interface {
	ClaimTask(ctx context.Context, task Task) (*Task, error)
	RestoreTask(ctx context.Context, project, issue string, previous *Task) error
	PutTask(ctx context.Context, task Task) error
	DeleteTask(ctx context.Context, project, issue string) error
	TaskByIssue(ctx context.Context, project, issue string) (Task, error)
	TasksByPane(ctx context.Context, project, paneID string) ([]Task, error)
	NonTerminalTasks(ctx context.Context, project string) ([]Task, error)
	PutWorker(ctx context.Context, worker Worker) error
	WorkerByPane(ctx context.Context, project, paneID string) (Worker, error)
	DeleteWorker(ctx context.Context, project, paneID string) error
	ActiveAssignments(ctx context.Context, project string) ([]ActiveAssignment, error)
	ActiveAssignmentByIssue(ctx context.Context, project, issue string) (ActiveAssignment, error)
	ActiveAssignmentByPRNumber(ctx context.Context, project string, prNumber int) (ActiveAssignment, error)
	EnqueueMerge(ctx context.Context, entry MergeQueueEntry) (int, error)
	MergeEntry(ctx context.Context, project string, prNumber int) (*MergeQueueEntry, error)
	MergeEntries(ctx context.Context, project string) ([]MergeQueueEntry, error)
	UpdateMergeEntry(ctx context.Context, entry MergeQueueEntry) error
	DeleteMergeEntry(ctx context.Context, project string, prNumber int) error
	RecordEvent(ctx context.Context, event Event) error
}

type Pool interface {
	Acquire(ctx context.Context, project, issue string) (Clone, error)
	Release(ctx context.Context, project string, clone Clone) error
}

type AmuxClient interface {
	Spawn(ctx context.Context, req SpawnRequest) (Pane, error)
	PaneExists(ctx context.Context, paneID string) (bool, error)
	ListPanes(ctx context.Context) ([]Pane, error)
	Metadata(ctx context.Context, paneID string) (map[string]string, error)
	SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error
	RemoveMetadata(ctx context.Context, paneID string, keys ...string) error
	SendKeys(ctx context.Context, paneID string, keys ...string) error
	Capture(ctx context.Context, paneID string) (string, error)
	CapturePane(ctx context.Context, paneID string) (PaneCapture, error)
	CaptureHistory(ctx context.Context, paneID string) (PaneCapture, error)
	KillPane(ctx context.Context, paneID string) error
	WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error
	WaitIdleSettle(ctx context.Context, paneID string, timeout, settle time.Duration) error
	WaitContent(ctx context.Context, paneID, substring string, timeout time.Duration) error
}

type IssueTracker interface {
	SetIssueStatus(ctx context.Context, issue, state string) error
	IssueTitle(ctx context.Context, issue string) (string, error)
}

type CommandRunner interface {
	Run(ctx context.Context, dir, name string, args ...string) ([]byte, error)
}

type EventSink interface {
	Emit(ctx context.Context, event Event) error
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type AgentProfile struct {
	Name              string
	StartCommand      string
	ResumeSequence    []string
	PostmortemEnabled bool
	StuckTextPatterns []string
	StuckTimeout      time.Duration
	GoBased           bool
	NudgeCommand      string
	MaxNudgeRetries   int
}

type Clone = pool.Clone
type Pane = amux.Pane
type PaneCapture = amux.PaneCapture
type SpawnRequest = amux.SpawnRequest

type Task struct {
	Project      string    `json:"project,omitempty"`
	Issue        string    `json:"issue,omitempty"`
	Status       string    `json:"status,omitempty"`
	Prompt       string    `json:"prompt,omitempty"`
	PaneID       string    `json:"pane_id,omitempty"`
	PaneName     string    `json:"pane_name,omitempty"`
	CloneName    string    `json:"clone_name,omitempty"`
	ClonePath    string    `json:"clone_path,omitempty"`
	Branch       string    `json:"branch,omitempty"`
	AgentProfile string    `json:"agent_profile,omitempty"`
	PRNumber     int       `json:"pr_number,omitempty"`
	CreatedAt    time.Time `json:"created_at,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
}

type Worker struct {
	Project               string    `json:"project,omitempty"`
	PaneID                string    `json:"pane_id,omitempty"`
	PaneName              string    `json:"pane_name,omitempty"`
	Issue                 string    `json:"issue,omitempty"`
	ClonePath             string    `json:"clone_path,omitempty"`
	AgentProfile          string    `json:"agent_profile,omitempty"`
	Health                string    `json:"health,omitempty"`
	LastReviewCount       int       `json:"last_review_count,omitempty"`
	LastIssueCommentCount int       `json:"last_issue_comment_count,omitempty"`
	ReviewNudgeCount      int       `json:"review_nudge_count,omitempty"`
	LastCIState           string    `json:"last_ci_state,omitempty"`
	CINudgeCount          int       `json:"ci_nudge_count,omitempty"`
	CIFailurePollCount    int       `json:"ci_failure_poll_count,omitempty"`
	CIEscalated           bool      `json:"ci_escalated,omitempty"`
	LastMergeableState    string    `json:"last_mergeable_state,omitempty"`
	NudgeCount            int       `json:"nudge_count,omitempty"`
	LastCapture           string    `json:"last_capture,omitempty"`
	LastActivityAt        time.Time `json:"last_activity_at,omitempty"`
	UpdatedAt             time.Time `json:"updated_at,omitempty"`
}

type ActiveAssignment struct {
	Task   Task   `json:"task"`
	Worker Worker `json:"worker"`
}

type MergeQueueEntry struct {
	Project   string    `json:"project,omitempty"`
	Issue     string    `json:"issue,omitempty"`
	PRNumber  int       `json:"pr_number,omitempty"`
	Status    string    `json:"status,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

type ProcessQueue struct {
	Entries []MergeQueueEntry
	Ack     chan struct{}
}

type MergeQueueUpdate struct {
	Entry         MergeQueueEntry
	Delete        bool
	EventType     string
	EventMessage  string
	FailurePrompt string
}

type Event struct {
	Time         time.Time `json:"time"`
	Type         string    `json:"type"`
	Project      string    `json:"project,omitempty"`
	Issue        string    `json:"issue,omitempty"`
	PaneID       string    `json:"pane_id,omitempty"`
	PaneName     string    `json:"pane_name,omitempty"`
	CloneName    string    `json:"clone_name,omitempty"`
	ClonePath    string    `json:"clone_path,omitempty"`
	Branch       string    `json:"branch,omitempty"`
	AgentProfile string    `json:"agent_profile,omitempty"`
	PRNumber     int       `json:"pr_number,omitempty"`
	Retry        int       `json:"retry,omitempty"`
	Message      string    `json:"message,omitempty"`
}
