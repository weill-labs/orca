package daemon

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

func ValidateBatchEntries(entries []BatchEntry) error {
	if len(entries) == 0 {
		return errors.New("batch manifest requires at least one entry")
	}

	for i, entry := range entries {
		index := i + 1
		if strings.TrimSpace(entry.Issue) == "" {
			return fmt.Errorf("batch manifest entry %d requires issue", index)
		}
		if strings.TrimSpace(entry.Agent) == "" {
			return fmt.Errorf("batch manifest entry %d requires agent", index)
		}
		if strings.TrimSpace(entry.Prompt) == "" {
			return fmt.Errorf("batch manifest entry %d requires prompt", index)
		}
	}

	return nil
}

func normalizeBatchEntries(entries []BatchEntry) []BatchEntry {
	normalized := make([]BatchEntry, len(entries))
	for i, entry := range entries {
		normalized[i] = BatchEntry{
			Issue:  strings.TrimSpace(entry.Issue),
			Agent:  strings.TrimSpace(entry.Agent),
			Prompt: entry.Prompt,
			Title:  strings.TrimSpace(entry.Title),
		}
	}
	return normalized
}

func (d *Daemon) Batch(ctx context.Context, req BatchRequest) (BatchResult, error) {
	if err := d.requireStarted(); err != nil {
		return BatchResult{}, err
	}
	if err := ValidateBatchEntries(req.Entries); err != nil {
		return BatchResult{}, err
	}
	if req.Delay < 0 {
		return BatchResult{}, errors.New("batch delay must be non-negative")
	}

	projectPath := req.Project
	if projectPath == "" {
		projectPath = d.project
	}

	entries := normalizeBatchEntries(req.Entries)
	result := BatchResult{
		Project:  projectPath,
		Results:  make([]TaskActionResult, 0, len(entries)),
		Failures: make([]BatchFailure, 0),
	}

	for i, entry := range entries {
		if err := d.assign(ctx, projectPath, entry.Issue, entry.Prompt, entry.Agent, "", entry.Title); err != nil {
			result.Failures = append(result.Failures, BatchFailure{
				Issue: entry.Issue,
				Error: err.Error(),
			})
		} else {
			taskResult, err := d.taskActionResult(ctx, projectPath, entry.Issue)
			if err != nil {
				return BatchResult{}, fmt.Errorf("load task %s: %w", entry.Issue, err)
			}
			result.Results = append(result.Results, taskResult)
		}
		if i == len(entries)-1 || req.Delay == 0 {
			continue
		}
		if err := d.sleep(ctx, req.Delay); err != nil {
			return BatchResult{}, fmt.Errorf("wait between batch assigns: %w", err)
		}
	}

	return result, nil
}

func (d *Daemon) taskActionResult(ctx context.Context, projectPath, issue string) (TaskActionResult, error) {
	task, err := d.state.TaskByIssue(ctx, projectPath, issue)
	if err != nil {
		return TaskActionResult{}, err
	}

	return TaskActionResult{
		Project:   projectPath,
		Issue:     task.Issue,
		Status:    task.Status,
		Agent:     task.AgentProfile,
		UpdatedAt: task.UpdatedAt,
	}, nil
}
