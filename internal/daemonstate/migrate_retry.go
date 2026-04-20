package state

import (
	"context"
	"errors"
	"io"
	"strings"
	"time"
)

const (
	postCopyRetryMaxAttempts = 3
	postCopyRetryDelay       = 2 * time.Second
)

func retrySyncPostgresEventSequence(
	ctx context.Context,
	queryer pgxQueryExecer,
	syncEventSequence func(context.Context, pgxQueryExecer) error,
	wait func(context.Context, time.Duration) error,
) error {
	return retryPostCopyStep(ctx, MigrationProgressSyncSequence, "", wait, func() error {
		return syncEventSequence(ctx, queryer)
	})
}

func retryPostgresTableCount(ctx context.Context, queryer pgxCountQueryer, table string, wait func(context.Context, time.Duration) error) (int64, error) {
	return retryPostgresTableCountWithFunc(ctx, queryer, table, postgresTableCount, wait)
}

func retryPostgresTableCountWithFunc(
	ctx context.Context,
	queryer pgxCountQueryer,
	table string,
	tableCount func(context.Context, pgxCountQueryer, string) (int64, error),
	wait func(context.Context, time.Duration) error,
) (int64, error) {
	var count int64
	err := retryPostCopyStep(ctx, MigrationProgressVerifyTable, table, wait, func() error {
		nextCount, err := tableCount(ctx, queryer, table)
		if err != nil {
			return err
		}
		count = nextCount
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

func retryPostCopyStep(
	ctx context.Context,
	phase MigrationProgressPhase,
	table string,
	wait func(context.Context, time.Duration) error,
	fn func() error,
) error {
	if wait == nil {
		wait = waitForPostCopyRetry
	}

	for attempt := 1; attempt <= postCopyRetryMaxAttempts; attempt++ {
		EmitMigrationProgress(ctx, MigrationProgress{
			Phase:       phase,
			Table:       table,
			Attempt:     attempt,
			MaxAttempts: postCopyRetryMaxAttempts,
		})

		err := fn()
		if err == nil {
			return nil
		}
		if !isRetryablePostCopyError(err) || attempt == postCopyRetryMaxAttempts {
			return err
		}
		if err := wait(ctx, postCopyRetryDelay); err != nil {
			return err
		}
	}

	return nil
}

func isRetryablePostCopyError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	return strings.Contains(strings.ToLower(err.Error()), "unexpected eof")
}

func waitForPostCopyRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
