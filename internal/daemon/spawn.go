package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	"github.com/weill-labs/orca/internal/config"
	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/project"
)

func (c *LocalController) Spawn(ctx context.Context, req SpawnPaneRequest) (SpawnPaneResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return SpawnPaneResult{}, err
	}

	manager, amuxClient, err := c.spawnRuntime(projectPath, req.Session)
	if err != nil {
		return SpawnPaneResult{}, err
	}

	manualRef := manualSpawnRef(c.now())
	clone, err := manager.Allocate(ctx, manualRef, manualRef)
	if err != nil {
		return SpawnPaneResult{}, err
	}

	pane, err := amuxClient.Spawn(ctx, amux.SpawnRequest{
		Session: req.Session,
		AtPane:  req.LeadPane,
		Name:    req.Title,
		CWD:     clone.Path,
	})
	if err != nil {
		releaseErr := manager.Release(ctx, clone.Path, clone.CurrentBranch)
		if releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
		return SpawnPaneResult{}, fmt.Errorf("spawn pane: %w", err)
	}

	return SpawnPaneResult{
		Project:   projectPath,
		PaneID:    pane.ID,
		PaneName:  pane.Name,
		ClonePath: clone.Path,
	}, nil
}

func manualSpawnRef(at time.Time) string {
	return fmt.Sprintf("spawn-%d", at.UTC().UnixNano())
}

func (c *LocalController) spawnRuntime(projectPath, session string) (*pool.Manager, amux.Client, error) {
	poolStore, ok := c.store.(pool.Store)
	if !ok {
		return nil, nil, fmt.Errorf("spawn requires clone-capable state store")
	}

	detectOrigin := c.detectOrigin
	if detectOrigin == nil {
		detectOrigin = config.DetectOrigin
	}

	origin, err := detectOrigin(projectPath)
	if err != nil {
		return nil, nil, fmt.Errorf("detect origin: %w", err)
	}

	poolDir := filepath.Join(projectPath, orcaPoolSubdir)
	if err := os.MkdirAll(poolDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("create pool directory: %w", err)
	}

	amuxClient := c.amux
	if amuxClient == nil {
		amuxClient = amux.NewClient(amux.Config{Session: session})
	}

	managerOptions := []pool.Option{
		pool.WithCWDUsageChecker(amuxCWDUsageChecker{amux: amuxClient}),
	}
	if c.poolRunner != nil {
		managerOptions = append(managerOptions, pool.WithRunner(c.poolRunner))
	}

	manager, err := pool.New(projectPath, internalPoolConfig{poolDir: poolDir, origin: origin}, poolStore, managerOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("create pool manager: %w", err)
	}
	return manager, amuxClient, nil
}

func (d *Daemon) taskPaneTarget(task Task) string {
	if callerPane := strings.TrimSpace(task.CallerPane); callerPane != "" {
		return callerPane
	}
	return strings.TrimSpace(d.leadPane)
}

func (d *Daemon) spawnPaneTarget(ctx context.Context, task Task) string {
	target := d.taskPaneTarget(task)
	if strings.TrimSpace(task.CallerPane) == "" {
		return target
	}

	if fallback := d.sameWindowNonLeadPane(ctx, target); fallback != "" {
		return fallback
	}
	return target
}

func (d *Daemon) sameWindowNonLeadPane(ctx context.Context, callerPane string) string {
	panes, err := d.amux.ListPanes(ctx)
	if err != nil {
		return ""
	}

	caller, ok := paneByReference(panes, callerPane)
	if !ok || !caller.Lead {
		return ""
	}

	window := strings.TrimSpace(caller.Window)
	if window == "" {
		return ""
	}

	for _, pane := range panes {
		if strings.TrimSpace(pane.Window) != window || pane.Lead || paneMatchesReference(pane, callerPane) {
			continue
		}
		return pane.Ref()
	}

	return ""
}

func paneByReference(panes []Pane, ref string) (Pane, bool) {
	for _, pane := range panes {
		if paneMatchesReference(pane, ref) {
			return pane, true
		}
	}
	return Pane{}, false
}

func paneMatchesReference(pane Pane, ref string) bool {
	target := strings.TrimSpace(ref)
	if target == "" {
		return false
	}

	return target == strings.TrimSpace(pane.ID) || target == strings.TrimSpace(pane.Name)
}

func workerPaneSpawnName(task Task, stableRef string) string {
	if issue := strings.TrimSpace(task.Issue); issue != "" {
		return "worker-" + issue
	}
	return strings.TrimSpace(stableRef)
}

func (d *Daemon) spawnWorkerPane(ctx context.Context, task Task, stableRef, clonePath string, profile AgentProfile) (Pane, error) {
	return d.amux.Spawn(ctx, SpawnRequest{
		Session: d.session,
		AtPane:  d.spawnPaneTarget(ctx, task),
		Name:    workerPaneSpawnName(task, stableRef),
		CWD:     clonePath,
		Command: profile.StartCommand,
	})
}
