package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

func TestRunMigrateStateCommandValidatesFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "missing from", args: []string{"--to", "postgres://localhost/orca"}, wantErr: "migrate-state requires --from"},
		{name: "missing to", args: []string{"--from", "sqlite:///tmp/state.db"}, wantErr: "migrate-state requires --to"},
		{name: "extra arg", args: []string{"--from", "sqlite:///tmp/state.db", "--to", "postgres://localhost/orca", "extra"}, wantErr: "migrate-state does not accept positional arguments"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := runMigrateStateCommand(context.Background(), &bytes.Buffer{}, tt.args, migrateStateCommandDeps{})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("runMigrateStateCommand(%q) error = %v, want substring %q", tt.args, err, tt.wantErr)
			}
		})
	}
}

func TestRunMigrateStateCommandWritesSummary(t *testing.T) {
	t.Parallel()

	sourceStore := &migrateStateStoreStub{}
	destinationStore := &migrateStateStoreStub{}
	var gotOptions state.MigrationOptions

	var stdout bytes.Buffer
	err := runMigrateStateCommand(context.Background(), &stdout, []string{
		"--from", "sqlite:///tmp/source.db",
		"--to", "postgres://orca:secret@localhost:5432/orca?sslmode=disable",
		"--dry-run",
		"--truncate",
	}, migrateStateCommandDeps{
		openSourceStore: func(uri string) (state.Store, error) {
			if got, want := uri, "sqlite:///tmp/source.db"; got != want {
				t.Fatalf("source uri = %q, want %q", got, want)
			}
			return sourceStore, nil
		},
		openDestinationStore: func(uri string) (state.Store, error) {
			if got, want := uri, "postgres://orca:secret@localhost:5432/orca?sslmode=disable"; got != want {
				t.Fatalf("destination uri = %q, want %q", got, want)
			}
			return destinationStore, nil
		},
		migrate: func(_ context.Context, from, to state.Store, options state.MigrationOptions) (state.MigrationSummary, error) {
			if from != sourceStore {
				t.Fatalf("source store = %T, want source stub", from)
			}
			if to != destinationStore {
				t.Fatalf("destination store = %T, want destination stub", to)
			}
			gotOptions = options
			return state.MigrationSummary{
				DryRun:   true,
				Truncate: true,
				Tables: []state.TableMigrationSummary{
					{Table: "tasks", SourceRows: 3, DestinationRowsBefore: 1, DestinationRowsAfter: 1},
					{Table: "events", SourceRows: 5, DestinationRowsBefore: 2, DestinationRowsAfter: 2},
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("runMigrateStateCommand() error = %v", err)
	}

	if got, want := gotOptions, (state.MigrationOptions{DryRun: true, Truncate: true}); got != want {
		t.Fatalf("migration options = %#v, want %#v", got, want)
	}
	if !sourceStore.closed {
		t.Fatal("expected source store to be closed")
	}
	if !destinationStore.closed {
		t.Fatal("expected destination store to be closed")
	}

	output := stdout.String()
	for _, want := range []string{
		"mode:",
		"dry-run",
		"from:",
		"sqlite:///tmp/source.db",
		"to:",
		"postgres://orca:xxxxx@localhost:5432/orca?sslmode=disable",
		"truncate:",
		"tasks",
		"events",
		"total",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("stdout = %q, want substring %q", output, want)
		}
	}
}

func TestRunMigrateStateCommandWritesProgressBeforeSummary(t *testing.T) {
	t.Parallel()

	sourceStore := &migrateStateStoreStub{}
	destinationStore := &migrateStateStoreStub{}

	var stdout bytes.Buffer
	err := runMigrateStateCommand(context.Background(), &stdout, []string{
		"--from", "sqlite:///tmp/source.db",
		"--to", "postgres://orca:secret@localhost:5432/orca?sslmode=disable",
	}, migrateStateCommandDeps{
		openSourceStore:      func(string) (state.Store, error) { return sourceStore, nil },
		openDestinationStore: func(string) (state.Store, error) { return destinationStore, nil },
		migrate: func(ctx context.Context, _, _ state.Store, _ state.MigrationOptions) (state.MigrationSummary, error) {
			state.EmitMigrationProgress(ctx, state.MigrationProgress{
				Phase:      state.MigrationProgressCopyTable,
				Table:      "events",
				SourceRows: 5,
			})
			state.EmitMigrationProgress(ctx, state.MigrationProgress{
				Phase:       state.MigrationProgressSyncSequence,
				Attempt:     1,
				MaxAttempts: 3,
			})

			return state.MigrationSummary{
				Tables: []state.TableMigrationSummary{
					{Table: "events", SourceRows: 5, DestinationRowsBefore: 0, DestinationRowsAfter: 5},
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("runMigrateStateCommand() error = %v", err)
	}

	output := stdout.String()
	progressLine := "progress: copied events (5 rows)"
	summaryLine := "mode:"
	progressIndex := strings.Index(output, progressLine)
	summaryIndex := strings.Index(output, summaryLine)
	if progressIndex == -1 {
		t.Fatalf("stdout = %q, want substring %q", output, progressLine)
	}
	if summaryIndex == -1 {
		t.Fatalf("stdout = %q, want substring %q", output, summaryLine)
	}
	if progressIndex > summaryIndex {
		t.Fatalf("stdout = %q, want progress before summary", output)
	}
	if !strings.Contains(output, "progress: syncing event id sequence (attempt 1/3)") {
		t.Fatalf("stdout = %q, want sync progress line", output)
	}
}

func TestFormatMigrationProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		progress state.MigrationProgress
		want     string
	}{
		{
			name:     "dry run",
			progress: state.MigrationProgress{Phase: state.MigrationProgressDryRun},
			want:     "dry run; skipping destination writes",
		},
		{
			name:     "truncate",
			progress: state.MigrationProgress{Phase: state.MigrationProgressTruncate},
			want:     "truncating destination tables",
		},
		{
			name: "copy table",
			progress: state.MigrationProgress{
				Phase:      state.MigrationProgressCopyTable,
				Table:      "events",
				SourceRows: 5,
			},
			want: "copied events (5 rows)",
		},
		{
			name:     "commit",
			progress: state.MigrationProgress{Phase: state.MigrationProgressCommit},
			want:     "committed copied rows",
		},
		{
			name: "sync attempt",
			progress: state.MigrationProgress{
				Phase:       state.MigrationProgressSyncSequence,
				Attempt:     2,
				MaxAttempts: 3,
			},
			want: "syncing event id sequence (attempt 2/3)",
		},
		{
			name: "verify table",
			progress: state.MigrationProgress{
				Phase:       state.MigrationProgressVerifyTable,
				Table:       "tasks",
				Attempt:     1,
				MaxAttempts: 3,
			},
			want: "verifying tasks row count (attempt 1/3)",
		},
		{
			name:     "unknown",
			progress: state.MigrationProgress{},
			want:     "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := formatMigrationProgress(tt.progress); got != tt.want {
				t.Fatalf("formatMigrationProgress(%+v) = %q, want %q", tt.progress, got, tt.want)
			}
		})
	}
}

func TestFormatMigrationAttempt(t *testing.T) {
	t.Parallel()

	if got, want := formatMigrationAttempt("syncing", 0, 0), "syncing"; got != want {
		t.Fatalf("formatMigrationAttempt() = %q, want %q", got, want)
	}
	if got, want := formatMigrationAttempt("syncing", 2, 3), "syncing (attempt 2/3)"; got != want {
		t.Fatalf("formatMigrationAttempt() = %q, want %q", got, want)
	}
}

type migrateStateStoreStub struct {
	closed bool
}

func (s *migrateStateStoreStub) Close() error {
	s.closed = true
	return nil
}

func (*migrateStateStoreStub) EnsureSchema(context.Context) error {
	return nil
}

func (*migrateStateStoreStub) UpsertDaemon(context.Context, string, state.DaemonStatus) error {
	return nil
}

func (*migrateStateStoreStub) MarkDaemonStopped(context.Context, string, time.Time) error {
	return nil
}

func (*migrateStateStoreStub) UpsertTask(context.Context, string, state.Task) error {
	return nil
}

func (*migrateStateStoreStub) UpdateTaskStatus(context.Context, string, string, string, time.Time) (state.Task, error) {
	return state.Task{}, nil
}

func (*migrateStateStoreStub) AppendEvent(context.Context, state.Event) (state.Event, error) {
	return state.Event{}, nil
}

func (*migrateStateStoreStub) ProjectStatus(context.Context, string) (state.ProjectStatus, error) {
	return state.ProjectStatus{}, nil
}

func (*migrateStateStoreStub) TaskStatus(context.Context, string, string) (state.TaskStatus, error) {
	return state.TaskStatus{}, nil
}

func (*migrateStateStoreStub) ListWorkers(context.Context, string) ([]state.Worker, error) {
	return nil, nil
}

func (*migrateStateStoreStub) ListClones(context.Context, string) ([]state.Clone, error) {
	return nil, nil
}

func (*migrateStateStoreStub) Events(context.Context, string, int64) (<-chan state.Event, <-chan error) {
	eventsCh := make(chan state.Event)
	errCh := make(chan error)
	close(eventsCh)
	close(errCh)
	return eventsCh, errCh
}
