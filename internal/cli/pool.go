package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/weill-labs/orca/internal/config"
	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/pool"
)

const poolStatusMissingMarker = "missing_marker"
const poolStatusInvalidMarker = "invalid_marker"
const poolStatusInvalidPath = "invalid_path"

func (a *App) runPool(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("pool", args); handled {
		return err
	}

	fs := newFlagSet("pool")
	var projectPath string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	positionals := fs.Args()
	if len(positionals) > 0 {
		switch positionals[0] {
		case "prune":
			return a.runPoolPrune(ctx, projectPath, jsonOutput, positionals[1:])
		case "reset":
			return a.runPoolReset(ctx, projectPath, jsonOutput, positionals[1:])
		case "unquarantine":
			return a.runPoolUnquarantine(ctx, projectPath, jsonOutput, positionals[1:])
		default:
			return fmt.Errorf("unknown pool subcommand %q", positionals[0])
		}
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	clones, err := a.state.ListClones(ctx, projectPath)
	if err != nil {
		return err
	}
	clones = annotatePoolCloneEligibility(projectPath, clones)
	if jsonOutput {
		return writeJSON(a.stdout, clones)
	}
	return writeClones(a.stdout, clones)
}

type cloneUnquarantineStore interface {
	UnquarantineClone(ctx context.Context, project, path string) error
}

type clonePruneStore interface {
	DeleteClone(ctx context.Context, project, path string) error
	AppendEvent(ctx context.Context, event state.Event) (state.Event, error)
}

type cloneResetStore interface {
	pool.Store
	pool.CloneResetStore
}

type cliPoolConfig struct {
	poolDir string
	origin  string
}

func (c cliPoolConfig) PoolDir() string     { return c.poolDir }
func (c cliPoolConfig) CloneOrigin() string { return c.origin }
func (c cliPoolConfig) BaseBranch() string  { return "" }

func (a *App) runPoolPrune(ctx context.Context, projectPath string, jsonOutput bool, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("usage: orca pool [--project PATH] prune")
	}
	store, ok := a.state.(clonePruneStore)
	if !ok {
		return fmt.Errorf("state store does not support clone pruning")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}
	clones, err := a.state.ListClones(ctx, projectPath)
	if err != nil {
		return err
	}

	pruned := make([]state.Clone, 0)
	for _, clone := range clones {
		if _, err := os.Stat(clone.Path); err == nil {
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect clone %q: %w", clone.Path, err)
		}

		if err := store.DeleteClone(ctx, projectPath, clone.Path); err != nil {
			return err
		}
		if err := appendPoolEntryPrunedEvent(ctx, store, projectPath, clone); err != nil {
			return err
		}
		pruned = append(pruned, clone)
	}

	if jsonOutput {
		return writeJSON(a.stdout, struct {
			Pruned []state.Clone `json:"pruned"`
		}{Pruned: pruned})
	}
	if len(pruned) == 0 {
		_, err = fmt.Fprintln(a.stdout, "pruned 0 pool entries")
		return err
	}
	for _, clone := range pruned {
		if _, err := fmt.Fprintf(a.stdout, "pruned %s\n", filepath.Base(clone.Path)); err != nil {
			return err
		}
	}
	return nil
}

func appendPoolEntryPrunedEvent(ctx context.Context, store clonePruneStore, projectPath string, clone state.Clone) error {
	payload, err := json.Marshal(struct {
		Path         string `json:"path"`
		Status       string `json:"status"`
		FailureCount int    `json:"failure_count,omitempty"`
	}{
		Path:         clone.Path,
		Status:       clone.Status,
		FailureCount: clone.FailureCount,
	})
	if err != nil {
		return err
	}
	_, err = store.AppendEvent(ctx, state.Event{
		Project: projectPath,
		Kind:    daemon.EventPoolEntryPruned,
		Message: fmt.Sprintf("pruned missing pool entry %s", filepath.Base(clone.Path)),
		Payload: payload,
	})
	return err
}

func (a *App) runPoolReset(ctx context.Context, projectPath string, jsonOutput bool, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: orca pool [--project PATH] reset CLONE")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}
	clones, err := a.state.ListClones(ctx, projectPath)
	if err != nil {
		return err
	}
	clone, err := resolvePoolClone(clones, args[0])
	if err != nil {
		return err
	}
	if clone.Status == string(pool.StatusOccupied) {
		return fmt.Errorf("clone %q is occupied by task %s; cancel the task first", filepath.Base(clone.Path), clone.Issue)
	}
	store, ok := a.state.(cloneResetStore)
	if !ok {
		return fmt.Errorf("state store does not support clone reset")
	}

	origin, err := config.DetectOrigin(projectPath)
	if err != nil {
		if _, statErr := os.Stat(clone.Path); errors.Is(statErr, os.ErrNotExist) {
			return err
		}
		origin = ""
	}
	manager, err := pool.New(projectPath, cliPoolConfig{
		poolDir: filepath.Join(projectPath, daemon.OrcaPoolSubdir),
		origin:  origin,
	}, store)
	if err != nil {
		return err
	}
	resetClone, err := manager.Reset(ctx, clone.Path)
	if err != nil {
		return err
	}

	output := state.Clone{
		Path:         resetClone.Path,
		Status:       string(resetClone.Status),
		Issue:        resetClone.AssignedTask,
		Branch:       resetClone.CurrentBranch,
		FailureCount: resetClone.FailureCount,
		UpdatedAt:    resetClone.UpdatedAt,
	}
	if jsonOutput {
		return writeJSON(a.stdout, output)
	}
	_, err = fmt.Fprintf(a.stdout, "reset %s\n", filepath.Base(resetClone.Path))
	return err
}

func (a *App) runPoolUnquarantine(ctx context.Context, projectPath string, jsonOutput bool, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: orca pool [--project PATH] unquarantine CLONE")
	}
	store, ok := a.state.(cloneUnquarantineStore)
	if !ok {
		return fmt.Errorf("state store does not support clone unquarantine")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}
	clones, err := a.state.ListClones(ctx, projectPath)
	if err != nil {
		return err
	}
	clone, err := resolvePoolClone(clones, args[0])
	if err != nil {
		return err
	}
	if clone.Status != "quarantined" {
		return fmt.Errorf("clone %q is not quarantined (status: %s)", filepath.Base(clone.Path), clone.Status)
	}
	if err := store.UnquarantineClone(ctx, projectPath, clone.Path); err != nil {
		return err
	}

	clone.Status = "free"
	clone.Issue = ""
	clone.Branch = ""
	clone.FailureCount = 0
	if jsonOutput {
		return writeJSON(a.stdout, clone)
	}
	_, err = fmt.Fprintf(a.stdout, "unquarantined %s\n", filepath.Base(clone.Path))
	return err
}

func resolvePoolClone(clones []state.Clone, ref string) (state.Clone, error) {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return state.Clone{}, fmt.Errorf("clone is required")
	}
	for _, clone := range clones {
		if clone.Path == ref || filepath.Base(clone.Path) == ref {
			return clone, nil
		}
	}
	return state.Clone{}, fmt.Errorf("clone %q not found in pool", ref)
}

func annotatePoolCloneEligibility(projectPath string, clones []state.Clone) []state.Clone {
	if len(clones) == 0 {
		return clones
	}

	poolDir := filepath.Join(projectPath, daemon.OrcaPoolSubdir)
	annotated := append([]state.Clone(nil), clones...)
	for i := range annotated {
		if annotated[i].Status != string(pool.StatusFree) {
			continue
		}

		clonePath, err := pool.ValidateClonePath(poolDir, annotated[i].Path)
		if err != nil {
			annotated[i].Status = poolStatusInvalidPath
			continue
		}
		hasMarker, err := pool.HasCloneMarker(clonePath)
		if err != nil {
			annotated[i].Status = poolStatusInvalidMarker
			continue
		}
		if !hasMarker {
			annotated[i].Status = poolStatusMissingMarker
		}
	}
	return annotated
}
