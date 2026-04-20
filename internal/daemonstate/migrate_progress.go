package state

import "context"

type MigrationProgressPhase string

const (
	MigrationProgressDryRun       MigrationProgressPhase = "dry_run"
	MigrationProgressTruncate     MigrationProgressPhase = "truncate"
	MigrationProgressCopyTable    MigrationProgressPhase = "copy_table"
	MigrationProgressCommit       MigrationProgressPhase = "commit"
	MigrationProgressSyncSequence MigrationProgressPhase = "sync_sequence"
	MigrationProgressVerifyTable  MigrationProgressPhase = "verify_table"
)

type MigrationProgress struct {
	Phase       MigrationProgressPhase `json:"phase"`
	Table       string                 `json:"table,omitempty"`
	SourceRows  int64                  `json:"source_rows,omitempty"`
	Attempt     int                    `json:"attempt,omitempty"`
	MaxAttempts int                    `json:"max_attempts,omitempty"`
}

type migrationProgressContextKey struct{}

func WithMigrationProgress(ctx context.Context, reporter func(MigrationProgress)) context.Context {
	if reporter == nil {
		return ctx
	}
	return context.WithValue(ctx, migrationProgressContextKey{}, reporter)
}

func EmitMigrationProgress(ctx context.Context, progress MigrationProgress) {
	if ctx == nil {
		return
	}
	reporter, ok := ctx.Value(migrationProgressContextKey{}).(func(MigrationProgress))
	if !ok || reporter == nil {
		return
	}
	reporter(progress)
}
