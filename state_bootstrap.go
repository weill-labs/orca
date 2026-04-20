package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/weill-labs/orca/internal/config"
	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

type stateBootstrapDeps struct {
	resolveStateBackend func(string) (config.StateBackend, error)
	stat                func(string) (os.FileInfo, error)
	openSQLiteStore     func(string) (stateStore, error)
	openPostgresStore   func(string) (stateStore, error)
	migrate             func(context.Context, state.Store, state.Store, state.MigrationOptions) (state.MigrationSummary, error)
}

var defaultStateBootstrapDeps = stateBootstrapDeps{
	resolveStateBackend: config.ResolveStateBackend,
	stat:                os.Stat,
	openSQLiteStore:     openSQLiteStateStore,
	openPostgresStore:   openPostgresStateStore,
	migrate:             state.Migrate,
}

func bootstrapLegacySQLiteState(ctx context.Context, command string, paths daemon.Paths, deps stateBootstrapDeps) error {
	if strings.TrimSpace(command) != "start" {
		return nil
	}

	resolveStateBackend := deps.resolveStateBackend
	if resolveStateBackend == nil {
		resolveStateBackend = config.ResolveStateBackend
	}
	backend, err := resolveStateBackend(paths.StateDB)
	if err != nil {
		return err
	}
	if backend.Kind != config.StateBackendPostgres {
		return nil
	}

	stat := deps.stat
	if stat == nil {
		stat = os.Stat
	}
	if _, err := stat(paths.StateDB); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat legacy sqlite state %s: %w", paths.StateDB, err)
	}

	openSQLiteStore := deps.openSQLiteStore
	if openSQLiteStore == nil {
		openSQLiteStore = openSQLiteStateStore
	}
	sourceStore, err := openSQLiteStore(paths.StateDB)
	if err != nil {
		return fmt.Errorf("open legacy sqlite state %s: %w", paths.StateDB, err)
	}
	defer sourceStore.Close()

	openPostgresStore := deps.openPostgresStore
	if openPostgresStore == nil {
		openPostgresStore = openPostgresStateStore
	}
	destinationStore, err := openPostgresStore(backend.DSN)
	if err != nil {
		return fmt.Errorf("open configured postgres state: %w", err)
	}
	defer destinationStore.Close()

	migrate := deps.migrate
	if migrate == nil {
		migrate = state.Migrate
	}

	summary, err := migrate(ctx, sourceStore, destinationStore, state.MigrationOptions{DryRun: true})
	if err != nil {
		return fmt.Errorf("check legacy sqlite migration: %w", err)
	}
	if summary.TotalDestinationRowsBefore() != 0 {
		return nil
	}

	if _, err := migrate(ctx, sourceStore, destinationStore, state.MigrationOptions{}); err != nil {
		return fmt.Errorf("auto-migrate legacy sqlite state: %w", err)
	}

	return nil
}
