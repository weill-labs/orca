package daemon

import (
	"context"
	"errors"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	"github.com/weill-labs/orca/internal/pool"
)

const (
	TaskStatusActive    = "active"
	TaskStatusCancelled = "cancelled"
	TaskStatusDone      = "done"
	TaskStatusFailed    = "failed"

	IssueStateInProgress = "In Progress"
	IssueStateDone       = "Done"

	WorkerHealthHealthy = "healthy"
	WorkerHealthStuck   = "stuck"

	EventDaemonStarted      = "daemon.started"
	EventDaemonStopped      = "daemon.stopped"
	EventTaskAssigned       = "task.assigned"
	EventTaskAssignFailed   = "task.assign_failed"
	EventTaskCancelled      = "task.cancelled"
	EventTaskCompleted      = "task.completed"
	EventWorkerNudged       = "worker.nudged"
	EventWorkerNudgedReview = "worker.nudged_review"
	EventWorkerEscalated    = "worker.escalated"
	EventWorkerRecovered    = "worker.recovered"
	EventPRDetected         = "pr.detected"
	EventPREnqueued         = "pr.enqueued"
	EventPRLandingStarted   = "pr.landing_started"
	EventPRLandingFailed    = "pr.landing_failed"
	EventPRMerged           = "pr.merged"
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
	CaptureInterval  time.Duration
	PollInterval     time.Duration
	MergeGracePeriod time.Duration
}

type ConfigProvider interface {
	AgentProfile(ctx context.Context, name string) (AgentProfile, error)
}

type StateStore interface {
	PutTask(ctx context.Context, task Task) error
	TaskByIssue(ctx context.Context, project, issue string) (Task, error)
	PutWorker(ctx context.Context, worker Worker) error
	DeleteWorker(ctx context.Context, project, paneID string) error
	RecordEvent(ctx context.Context, event Event) error
}

type Pool interface {
	Acquire(ctx context.Context, project, issue string) (Clone, error)
	Release(ctx context.Context, project string, clone Clone) error
}

type AmuxClient interface {
	Spawn(ctx context.Context, req SpawnRequest) (Pane, error)
	SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error
	SendKeys(ctx context.Context, paneID string, keys ...string) error
	Capture(ctx context.Context, paneID string) (string, error)
	KillPane(ctx context.Context, paneID string) error
	WaitIdle(ctx context.Context, paneID string, timeout time.Duration) error
}

type IssueTracker interface {
	SetIssueStatus(ctx context.Context, issue, state string) error
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
	StuckTextPatterns []string
	StuckTimeout      time.Duration
	NudgeCommand      string
	MaxNudgeRetries   int
}

type Clone = pool.Clone
type Pane = amux.Pane
type SpawnRequest = amux.SpawnRequest

type Task struct {
	Project      string    `json:"project,omitempty"`
	Issue        string    `json:"issue,omitempty"`
	Status       string    `json:"status,omitempty"`
	PaneID       string    `json:"pane_id,omitempty"`
	PaneName     string    `json:"pane_name,omitempty"`
	CloneName    string    `json:"clone_name,omitempty"`
	ClonePath    string    `json:"clone_path,omitempty"`
	Branch       string    `json:"branch,omitempty"`
	AgentProfile string    `json:"agent_profile,omitempty"`
	PRNumber     int       `json:"pr_number,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
}

type Worker struct {
	Project      string    `json:"project,omitempty"`
	PaneID       string    `json:"pane_id,omitempty"`
	PaneName     string    `json:"pane_name,omitempty"`
	Issue        string    `json:"issue,omitempty"`
	ClonePath    string    `json:"clone_path,omitempty"`
	AgentProfile string    `json:"agent_profile,omitempty"`
	Health       string    `json:"health,omitempty"`
	NudgeCount   int       `json:"nudge_count,omitempty"`
	UpdatedAt    time.Time `json:"updated_at,omitempty"`
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
