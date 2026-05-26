package worksource

import (
	"context"
	"errors"
)

// ErrNotFound reports that a source could not find the requested work item.
var ErrNotFound = errors.New("work item not found")

// ErrAlreadyClaimed reports that a work item was claimed by another worker.
var ErrAlreadyClaimed = errors.New("work item already claimed")

// WorkItem is a normalized unit of work from a source such as Linear or beads.
type WorkItem struct {
	ID        string
	Title     string
	Body      string
	Priority  int
	Score     float64
	Rationale string
	Labels    []string
	AgentHint string
	Meta      map[string]string
}

// Outcome records the final disposition of a completed work item.
type Outcome int

const (
	OutcomeUnknown Outcome = iota
	OutcomeMerged
	OutcomeAbandoned
	OutcomeFailed
)

// Source provides a common interface for discovering and updating work items.
type Source interface {
	Ready(ctx context.Context, limit int) ([]WorkItem, error)
	Get(ctx context.Context, id string) (WorkItem, error)
	Claim(ctx context.Context, id, workerID string) error
	Release(ctx context.Context, id, reason string) error
	Complete(ctx context.Context, id string, o Outcome) error
}

// ManualSource preserves the existing push-based assignment model.
type ManualSource struct{}

func (ManualSource) Ready(ctx context.Context, limit int) ([]WorkItem, error) {
	return nil, nil
}

func (ManualSource) Get(ctx context.Context, id string) (WorkItem, error) {
	return WorkItem{}, ErrNotFound
}

func (ManualSource) Claim(ctx context.Context, id, workerID string) error {
	return nil
}

func (ManualSource) Release(ctx context.Context, id, reason string) error {
	return nil
}

func (ManualSource) Complete(ctx context.Context, id string, o Outcome) error {
	return nil
}
