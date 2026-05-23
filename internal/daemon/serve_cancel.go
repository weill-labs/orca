package daemon

import (
	"context"
	"errors"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

func taskActionResultForCancelledIssue(ctx context.Context, store daemonStateStore, projectPath, issue string, updatedAt time.Time) (TaskActionResult, error) {
	result, err := taskActionResultForIssue(ctx, store, projectPath, issue)
	if err == nil {
		return result, nil
	}
	if !errors.Is(err, state.ErrNotFound) && !errors.Is(err, ErrTaskNotFound) {
		return TaskActionResult{}, err
	}
	return TaskActionResult{
		Project:   projectPath,
		Issue:     issue,
		Status:    TaskStatusCancelled,
		UpdatedAt: updatedAt,
	}, nil
}
