package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/project"
)

type ServeRequest struct {
	Session string
	Project string
	StateDB string
	PIDFile string
}

func RunProcess(ctx context.Context, req ServeRequest) error {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return err
	}

	store, err := state.OpenSQLite(req.StateDB)
	if err != nil {
		return err
	}
	defer store.Close()

	now := time.Now().UTC()
	pid := os.Getpid()

	if err := os.MkdirAll(filepath.Dir(req.PIDFile), 0o755); err != nil {
		return fmt.Errorf("create daemon pid directory: %w", err)
	}
	if err := os.WriteFile(req.PIDFile, []byte(strconv.Itoa(pid)), 0o644); err != nil {
		return fmt.Errorf("write daemon pid file: %w", err)
	}
	defer os.Remove(req.PIDFile)

	if err := store.UpsertDaemon(ctx, projectPath, state.DaemonStatus{
		Session:   req.Session,
		PID:       pid,
		Status:    "running",
		StartedAt: now,
		UpdatedAt: now,
	}); err != nil {
		return err
	}

	if _, err := store.AppendEvent(ctx, state.Event{
		Project:   projectPath,
		Kind:      "daemon.started",
		Message:   fmt.Sprintf("daemon started for %s", projectPath),
		CreatedAt: now,
	}); err != nil {
		return err
	}

	<-ctx.Done()

	stoppedAt := time.Now().UTC()
	if err := store.MarkDaemonStopped(context.Background(), projectPath, stoppedAt); err != nil {
		return err
	}

	_, err = store.AppendEvent(context.Background(), state.Event{
		Project:   projectPath,
		Kind:      "daemon.stopped",
		Message:   fmt.Sprintf("daemon stopped for %s", projectPath),
		CreatedAt: stoppedAt,
	})
	return err
}
