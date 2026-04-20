package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/weill-labs/orca/internal/config"
	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

const (
	stateBackendSQLite   = "sqlite"
	stateBackendPostgres = "postgres"
	stateBackendUnknown  = "unknown"
)

type projectStatusResult struct {
	status            state.ProjectStatus
	daemonBuildCommit string
	warning           string
}

func (a *App) projectStatus(ctx context.Context, projectPath string) (projectStatusResult, error) {
	rpcStatus, err := a.projectStatusRPC(ctx, projectPath)
	if err == nil {
		return projectStatusResult{
			status:            rpcStatus.ProjectStatus,
			daemonBuildCommit: strings.TrimSpace(rpcStatus.BuildCommit),
			warning:           a.backendMismatchWarning(),
		}, nil
	}
	if !errors.Is(err, daemon.ErrDaemonNotRunning) {
		return projectStatusResult{}, err
	}

	status, err := a.state.ProjectStatus(ctx, projectPath)
	if err != nil {
		return projectStatusResult{}, err
	}

	probe := a.fallbackDaemonProbe(status)
	status = normalizeFallbackDaemonStatus(status, probe)

	return projectStatusResult{
		status:  status,
		warning: probe.warning,
	}, nil
}

func (a *App) backendMismatchWarning() string {
	pid, _, warning := a.daemonPIDStatus()
	if pid <= 0 {
		return ""
	}
	return warning
}

func (a *App) daemonPIDStatus() (int, bool, string) {
	probe := a.pidFileDaemonProbe()
	return probe.pid, probe.alive, probe.warning
}

func backendMismatchWarningForPID(pid int, readProcessEnviron func(int) ([]string, error), resolvePaths func() (daemon.Paths, error)) string {
	env, err := readProcessEnviron(pid)
	if err != nil {
		return ""
	}

	daemonBackend := stateBackendFromEnv(env)
	shellBackend := currentStateBackend(resolvePaths)
	if daemonBackend == stateBackendUnknown || shellBackend == stateBackendUnknown || daemonBackend == shellBackend {
		return ""
	}

	return fmt.Sprintf("Warning: daemon is running on %s but this shell reads %s. Update ~/.config/orca/config.toml or ORCA_STATE_* overrides so they match.", daemonBackend, shellBackend)
}

func currentStateBackend(resolvePaths func() (daemon.Paths, error)) string {
	if backend := stateBackendFromEnv(os.Environ()); backend != stateBackendUnknown {
		return backend
	}
	if resolvePaths == nil {
		resolvePaths = daemon.ResolvePaths
	}
	paths, err := resolvePaths()
	if err != nil {
		return stateBackendUnknown
	}
	backend, err := config.ResolveStateBackend(paths.StateDB)
	if err != nil {
		return stateBackendUnknown
	}
	return backend.Kind
}

func stateBackendFromEnv(env []string) string {
	for _, entry := range env {
		if !strings.HasPrefix(entry, "ORCA_STATE_DB=") {
			continue
		}
		if strings.TrimSpace(strings.TrimPrefix(entry, "ORCA_STATE_DB=")) != "" {
			return stateBackendSQLite
		}
	}

	for _, entry := range env {
		if !strings.HasPrefix(entry, "ORCA_STATE_DSN=") {
			continue
		}
		dsn := strings.TrimSpace(strings.TrimPrefix(entry, "ORCA_STATE_DSN="))
		if dsn == "" {
			break
		}
		if strings.HasPrefix(strings.ToLower(dsn), "sqlite:") {
			return stateBackendSQLite
		}
		return stateBackendPostgres
	}
	return stateBackendUnknown
}

func pidFilePath(paths daemon.Paths) string {
	return filepath.Join(paths.PIDDir, "orca.pid")
}

func defaultReadPIDFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	pid, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("parse pid file %s: %w", path, err)
	}
	return pid, nil
}

func defaultProcessAlive(pid int) (bool, error) {
	if pid <= 0 {
		return false, nil
	}

	err := syscall.Kill(pid, syscall.Signal(0))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	if errors.Is(err, syscall.EPERM) {
		return true, nil
	}
	return false, fmt.Errorf("check process %d: %w", pid, err)
}

func defaultReadProcessEnviron(pid int) ([]string, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "environ"))
	if err != nil {
		return nil, err
	}

	parts := strings.Split(string(data), "\x00")
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	return parts, nil
}
