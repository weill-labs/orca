package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

type migrateStateCommandDeps struct {
	openSourceStore      func(string) (state.Store, error)
	openDestinationStore func(string) (state.Store, error)
	migrate              func(context.Context, state.Store, state.Store, state.MigrationOptions) (state.MigrationSummary, error)
}

func defaultMigrateStateCommandDeps() migrateStateCommandDeps {
	return migrateStateCommandDeps{
		openSourceStore:      func(uri string) (state.Store, error) { return openMigrationStore(uri, true) },
		openDestinationStore: func(uri string) (state.Store, error) { return openMigrationStore(uri, false) },
		migrate:              state.Migrate,
	}
}

func RunMigrateStateCommand(ctx context.Context, stdout io.Writer, args []string) error {
	return runMigrateStateCommand(ctx, stdout, args, defaultMigrateStateCommandDeps())
}

func (a *App) runMigrateState(ctx context.Context, args []string) error {
	return RunMigrateStateCommand(ctx, a.stdout, args)
}

func runMigrateStateCommand(ctx context.Context, stdout io.Writer, args []string, deps migrateStateCommandDeps) error {
	if len(args) == 1 && isHelpFlag(args[0]) {
		_, err := fmt.Fprintln(stdout, helpRegistry["migrate-state"])
		return err
	}

	fs := newFlagSet("migrate-state")
	var fromURI string
	var toURI string
	var dryRun bool
	var truncate bool
	fs.StringVar(&fromURI, "from", "", "source state store URI")
	fs.StringVar(&toURI, "to", "", "destination state store URI")
	fs.BoolVar(&dryRun, "dry-run", false, "report what would be migrated without modifying the destination")
	fs.BoolVar(&truncate, "truncate", false, "wipe destination tables before copying rows")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("migrate-state does not accept positional arguments")
	}

	fromURI = strings.TrimSpace(fromURI)
	if fromURI == "" {
		return fmt.Errorf("migrate-state requires --from")
	}
	toURI = strings.TrimSpace(toURI)
	if toURI == "" {
		return fmt.Errorf("migrate-state requires --to")
	}

	sourceStore, err := deps.openSourceStore(fromURI)
	if err != nil {
		return err
	}
	defer sourceStore.Close()

	destinationStore, err := deps.openDestinationStore(toURI)
	if err != nil {
		return err
	}
	defer destinationStore.Close()

	ctx = state.WithMigrationProgress(ctx, func(progress state.MigrationProgress) {
		if line := formatMigrationProgress(progress); line != "" {
			_, _ = fmt.Fprintf(stdout, "progress: %s\n", line)
		}
	})

	summary, err := deps.migrate(ctx, sourceStore, destinationStore, state.MigrationOptions{
		DryRun:   dryRun,
		Truncate: truncate,
	})
	if err != nil {
		return err
	}

	return writeMigrationSummary(stdout, redactMigrationURI(fromURI), redactMigrationURI(toURI), summary)
}

func openMigrationStore(rawURI string, requireExistingSQLiteFile bool) (state.Store, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return nil, fmt.Errorf("parse state URI %q: %w", rawURI, err)
	}

	switch parsed.Scheme {
	case "sqlite":
		path, err := sqlitePathFromURI(parsed)
		if err != nil {
			return nil, err
		}
		if requireExistingSQLiteFile {
			if _, err := os.Stat(path); err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return nil, fmt.Errorf("source sqlite database %q does not exist", path)
				}
				return nil, fmt.Errorf("stat sqlite database %q: %w", path, err)
			}
		}
		store, err := state.OpenSQLite(path)
		if err != nil {
			return nil, err
		}
		return store, nil
	case "postgres", "postgresql":
		store, err := state.OpenPostgres(rawURI)
		if err != nil {
			return nil, err
		}
		return store, nil
	default:
		return nil, fmt.Errorf("unsupported state URI %q", rawURI)
	}
}

func sqlitePathFromURI(parsed *url.URL) (string, error) {
	if parsed == nil {
		return "", fmt.Errorf("sqlite URI is required")
	}
	if parsed.Host != "" {
		return "", fmt.Errorf("sqlite URI %q must use sqlite:///absolute/path syntax", parsed.String())
	}
	if strings.TrimSpace(parsed.Path) == "" {
		return "", fmt.Errorf("sqlite URI %q must include an absolute path", parsed.String())
	}
	return parsed.Path, nil
}

func redactMigrationURI(rawURI string) string {
	parsed, err := url.Parse(rawURI)
	if err != nil {
		return rawURI
	}

	switch parsed.Scheme {
	case "postgres", "postgresql":
		return parsed.Redacted()
	default:
		return rawURI
	}
}

func writeMigrationSummary(w io.Writer, fromURI, toURI string, summary state.MigrationSummary) error {
	tw := tabwriter.NewWriter(w, 0, 8, 2, ' ', 0)

	mode := "apply"
	if summary.DryRun {
		mode = "dry-run"
	}

	if _, err := fmt.Fprintf(tw, "mode:\t%s\n", mode); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(tw, "from:\t%s\n", fromURI); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(tw, "to:\t%s\n", toURI); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(tw, "truncate:\t%t\n", summary.Truncate); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(tw, "table\tsource\tdestination_before\tdestination_after"); err != nil {
		return err
	}
	for _, table := range summary.Tables {
		if _, err := fmt.Fprintf(tw, "%s\t%d\t%d\t%d\n", table.Table, table.SourceRows, table.DestinationRowsBefore, table.DestinationRowsAfter); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintf(tw, "total\t%d\t%d\t%d\n", summary.TotalSourceRows(), summary.TotalDestinationRowsBefore(), summary.TotalDestinationRowsAfter()); err != nil {
		return err
	}

	return tw.Flush()
}

func formatMigrationProgress(progress state.MigrationProgress) string {
	switch progress.Phase {
	case state.MigrationProgressDryRun:
		return "dry run; skipping destination writes"
	case state.MigrationProgressTruncate:
		return "truncating destination tables"
	case state.MigrationProgressCopyTable:
		return fmt.Sprintf("copied %s (%d rows)", progress.Table, progress.SourceRows)
	case state.MigrationProgressCommit:
		return "committed copied rows"
	case state.MigrationProgressSyncSequence:
		return formatMigrationAttempt("syncing event id sequence", progress.Attempt, progress.MaxAttempts)
	case state.MigrationProgressVerifyTable:
		return formatMigrationAttempt(fmt.Sprintf("verifying %s row count", progress.Table), progress.Attempt, progress.MaxAttempts)
	default:
		return ""
	}
}

func formatMigrationAttempt(label string, attempt, maxAttempts int) string {
	if attempt <= 0 || maxAttempts <= 0 {
		return label
	}
	return fmt.Sprintf("%s (attempt %d/%d)", label, attempt, maxAttempts)
}
