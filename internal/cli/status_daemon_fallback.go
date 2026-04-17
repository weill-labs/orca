package cli

import (
	"fmt"

	state "github.com/weill-labs/orca/internal/daemonstate"
)

type daemonFallbackProbe struct {
	pid     int
	alive   bool
	warning string
}

func (a *App) pidFileDaemonProbe() daemonFallbackProbe {
	paths, err := a.resolvePaths()
	if err != nil {
		return daemonFallbackProbe{}
	}

	pid, err := a.readPIDFile(pidFilePath(paths))
	if err != nil {
		return daemonFallbackProbe{}
	}

	alive, err := a.processAlive(pid)
	if err != nil {
		return daemonFallbackProbe{pid: pid}
	}
	if !alive {
		return daemonFallbackProbe{pid: pid}
	}

	return daemonFallbackProbe{
		pid:     pid,
		alive:   true,
		warning: backendMismatchWarningForPID(pid, a.readProcessEnviron),
	}
}

func (a *App) fallbackDaemonProbe(status state.ProjectStatus) daemonFallbackProbe {
	pidFileProbe := a.pidFileDaemonProbe()
	if pidFileProbe.alive {
		return pidFileProbe
	}

	storedPID := 0
	if status.Daemon != nil {
		storedPID = status.Daemon.PID
	}
	if storedPID > 0 && storedPID != pidFileProbe.pid {
		alive, err := a.processAlive(storedPID)
		if err == nil && alive {
			return daemonFallbackProbe{
				pid:     storedPID,
				alive:   true,
				warning: backendMismatchWarningForPID(storedPID, a.readProcessEnviron),
			}
		}
	}

	if pidFileProbe.pid > 0 {
		return pidFileProbe
	}
	if storedPID > 0 {
		return daemonFallbackProbe{pid: storedPID}
	}
	return daemonFallbackProbe{}
}

func normalizeFallbackDaemonStatus(status state.ProjectStatus, probe daemonFallbackProbe) state.ProjectStatus {
	daemonStatus := status.Daemon
	if daemonStatus == nil {
		if probe.pid <= 0 && !probe.alive {
			return status
		}
		daemonStatus = &state.DaemonStatus{}
	} else {
		copy := *daemonStatus
		daemonStatus = &copy
	}

	if probe.pid > 0 {
		daemonStatus.PID = probe.pid
	}
	daemonStatus.Reason = ""
	if probe.alive {
		daemonStatus.Status = "unhealthy"
		daemonStatus.Reason = fmt.Sprintf("pid %d exists but socket not responding", probe.pid)
	} else {
		daemonStatus.Status = "stopped"
	}

	status.Daemon = daemonStatus
	return status
}
