package orca

import (
	"context"
	"errors"
	"time"
)

const (
	TaskStatusActive     = "active"
	TaskStatusDone       = "done"
	CloneStatusFree      = "free"
	CloneStatusOccupied  = "occupied"
	WorkerStateRunning   = "running"
	WorkerStateNudged    = "nudged"
	WorkerStateAttention = "needs_attention"
	WorkerStateExited    = "exited"
)

var (
	ErrNoEligibleClones = errors.New("orca: no eligible clones discovered")
	ErrNoFreeClones     = errors.New("orca: no free clones available")
	ErrTaskNotFound     = errors.New("orca: task not found")
	ErrWorkerNotFound   = errors.New("orca: worker not found")
	ErrCloneNotFound    = errors.New("orca: clone not found")
)

type Clock interface {
	Now() time.Time
}

type AmuxClient interface {
	SpawnPane(ctx context.Context, req SpawnRequest) (Pane, error)
	SendKeys(ctx context.Context, paneID, keys string) error
	SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error
	Capture(ctx context.Context, paneID string) (string, error)
	KillPane(ctx context.Context, paneID string) error
}

type RepositoryManager interface {
	Prepare(ctx context.Context, clonePath, branch string) error
	Cleanup(ctx context.Context, clonePath, branch string) error
}

type PRTracker interface {
	BranchStatus(ctx context.Context, branch string) (PRStatus, error)
}

type SpawnRequest struct {
	CWD     string
	Command string
}

type Pane struct {
	ID   string
	Name string
}

type PRStatus struct {
	Number int
	Merged bool
}

type AgentProfile struct {
	Name              string
	StartCommand      string
	StuckTextPatterns []string
	StuckTimeout      time.Duration
	NudgeCommand      string
	MaxNudgeRetries   int
	WrapUpMessage     string
}

type Config struct {
	ProjectPath string
	StatePath   string
	Session     string
	PoolPattern string
	Agent       AgentProfile
}

type Dependencies struct {
	AMUX  AmuxClient
	Repos RepositoryManager
	PRs   PRTracker
	Clock Clock
}

type AssignRequest struct {
	IssueID string
	Prompt  string
}

type Assignment struct {
	IssueID   string
	Branch    string
	ClonePath string
	PaneID    string
	PaneName  string
}

type Task struct {
	Project        string
	IssueID        string
	Status         string
	Branch         string
	ClonePath      string
	WorkerID       string
	Prompt         string
	PRNumber       int
	NudgeCount     int
	LastCapture    string
	LastActivityAt time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Worker struct {
	Project        string
	PaneID         string
	PaneName       string
	Agent          string
	TaskID         string
	ClonePath      string
	State          string
	LastCapture    string
	LastActivityAt time.Time
	NudgeCount     int
	UpdatedAt      time.Time
}

type Clone struct {
	Project   string
	Path      string
	Status    string
	Branch    string
	TaskID    string
	UpdatedAt time.Time
}

type Event struct {
	ID        int64
	Project   string
	Type      string
	TaskID    string
	WorkerID  string
	ClonePath string
	Message   string
	CreatedAt time.Time
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now().UTC()
}
