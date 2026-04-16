package state

import (
	"database/sql"
	"time"
)

func scanPostgresTask(scanner rowScanner, includeProject bool) (Task, error) {
	var task Task
	var prNumber sql.NullInt64
	var currentPaneID sql.NullString

	fields := []any{
		&task.Issue,
		&task.Status,
		&task.State,
		&task.Agent,
		&task.Prompt,
		&task.CallerPane,
		&task.WorkerID,
		&currentPaneID,
		&task.ClonePath,
		&task.Branch,
		&prNumber,
		&task.CreatedAt,
		&task.UpdatedAt,
	}
	if includeProject {
		fields = append([]any{&task.Project}, fields...)
	}

	if err := scanner.Scan(fields...); err != nil {
		return Task{}, err
	}

	task.CreatedAt = normalizeTime(task.CreatedAt)
	task.UpdatedAt = normalizeTime(task.UpdatedAt)
	if prNumber.Valid {
		value := int(prNumber.Int64)
		task.PRNumber = &value
	}
	if currentPaneID.Valid {
		task.CurrentPaneID = currentPaneID.String
	}
	if task.State == "" {
		task.State = defaultPersistedTaskState(task.Status, task.PRNumber)
	}
	return task, nil
}

func scanPostgresWorker(scanner rowScanner, includeProject bool) (Worker, error) {
	var worker Worker
	var lastActivityAt sql.NullTime
	var lastPushAt sql.NullTime
	var lastPRPollAt sql.NullTime
	var firstCrashAt sql.NullTime

	fields := []any{
		&worker.WorkerID,
		&worker.CurrentPaneID,
		&worker.Agent,
		&worker.State,
		&worker.Issue,
		&worker.ClonePath,
		&worker.LastReviewCount,
		&worker.LastInlineReviewCommentCount,
		&worker.LastIssueCommentCount,
		&worker.LastIssueCommentWatermark,
		&worker.ReviewNudgeCount,
		&worker.ReviewApproved,
		&worker.LastCIState,
		&worker.CINudgeCount,
		&worker.CIFailurePollCount,
		&worker.CIEscalated,
		&worker.LastMergeableState,
		&worker.NudgeCount,
		&worker.LastCapture,
		&lastActivityAt,
		&worker.LastPRNumber,
		&lastPushAt,
		&lastPRPollAt,
		&worker.RestartCount,
		&firstCrashAt,
		&worker.CreatedAt,
		&worker.LastSeenAt,
	}
	if includeProject {
		fields = append([]any{&worker.Project}, fields...)
	}

	if err := scanner.Scan(fields...); err != nil {
		return Worker{}, err
	}

	worker.CreatedAt = normalizeTime(worker.CreatedAt)
	worker.LastSeenAt = normalizeTime(worker.LastSeenAt)
	if lastActivityAt.Valid {
		worker.LastActivityAt = normalizeTime(lastActivityAt.Time)
	}
	if lastPushAt.Valid {
		worker.LastPushAt = normalizeTime(lastPushAt.Time)
	}
	if lastPRPollAt.Valid {
		worker.LastPRPollAt = normalizeTime(lastPRPollAt.Time)
	}
	if firstCrashAt.Valid {
		worker.FirstCrashAt = normalizeTime(firstCrashAt.Time)
	}
	return worker, nil
}

func scanPostgresAssignment(scanner rowScanner, includeProject bool) (Assignment, error) {
	var task Task
	var worker Worker
	var prNumber sql.NullInt64
	var lastActivityAt sql.NullTime
	var lastPushAt sql.NullTime
	var lastPRPollAt sql.NullTime
	var firstCrashAt sql.NullTime

	fields := []any{
		&task.Issue,
		&task.Status,
		&task.State,
		&task.Agent,
		&task.Prompt,
		&task.CallerPane,
		&task.WorkerID,
		&task.CurrentPaneID,
		&task.ClonePath,
		&task.Branch,
		&prNumber,
		&task.CreatedAt,
		&task.UpdatedAt,
		&worker.WorkerID,
		&worker.CurrentPaneID,
		&worker.Agent,
		&worker.State,
		&worker.Issue,
		&worker.ClonePath,
		&worker.LastReviewCount,
		&worker.LastInlineReviewCommentCount,
		&worker.LastIssueCommentCount,
		&worker.LastIssueCommentWatermark,
		&worker.ReviewNudgeCount,
		&worker.ReviewApproved,
		&worker.LastCIState,
		&worker.CINudgeCount,
		&worker.CIFailurePollCount,
		&worker.CIEscalated,
		&worker.LastMergeableState,
		&worker.NudgeCount,
		&worker.LastCapture,
		&lastActivityAt,
		&worker.LastPRNumber,
		&lastPushAt,
		&lastPRPollAt,
		&worker.RestartCount,
		&firstCrashAt,
		&worker.CreatedAt,
		&worker.LastSeenAt,
	}
	if includeProject {
		fields = append([]any{&task.Project}, fields...)
	}

	if err := scanner.Scan(fields...); err != nil {
		return Assignment{}, err
	}

	task.CreatedAt = normalizeTime(task.CreatedAt)
	task.UpdatedAt = normalizeTime(task.UpdatedAt)
	if prNumber.Valid {
		value := int(prNumber.Int64)
		task.PRNumber = &value
	}
	if task.State == "" {
		task.State = defaultPersistedTaskState(task.Status, task.PRNumber)
	}

	worker.Project = task.Project
	worker.CreatedAt = normalizeTime(worker.CreatedAt)
	worker.LastSeenAt = normalizeTime(worker.LastSeenAt)
	if lastActivityAt.Valid {
		worker.LastActivityAt = normalizeTime(lastActivityAt.Time)
	}
	if lastPushAt.Valid {
		worker.LastPushAt = normalizeTime(lastPushAt.Time)
	}
	if lastPRPollAt.Valid {
		worker.LastPRPollAt = normalizeTime(lastPRPollAt.Time)
	}
	if firstCrashAt.Valid {
		worker.FirstCrashAt = normalizeTime(firstCrashAt.Time)
	}

	return Assignment{
		Task:   task,
		Worker: worker,
	}, nil
}

func scanPostgresMergeQueueEntry(scanner rowScanner) (MergeQueueEntry, error) {
	var entry MergeQueueEntry
	if err := scanner.Scan(&entry.Project, &entry.Issue, &entry.PRNumber, &entry.Status, &entry.CreatedAt, &entry.UpdatedAt); err != nil {
		return MergeQueueEntry{}, err
	}
	entry.CreatedAt = normalizeTime(entry.CreatedAt)
	entry.UpdatedAt = normalizeTime(entry.UpdatedAt)
	return entry, nil
}

func normalizeNullTime(value sql.NullTime) time.Time {
	if !value.Valid {
		return time.Time{}
	}
	return normalizeTime(value.Time)
}
