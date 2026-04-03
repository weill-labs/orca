package daemon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/project"
)

var (
	ErrDaemonAlreadyRunning = errors.New("orca daemon is already running")
	ErrDaemonNotRunning     = errors.New("orca daemon is not running")
)

type Controller interface {
	Start(ctx context.Context, req StartRequest) (StartResult, error)
	Stop(ctx context.Context, req StopRequest) (StopResult, error)
	Assign(ctx context.Context, req AssignRequest) (TaskActionResult, error)
	Cancel(ctx context.Context, req CancelRequest) (TaskActionResult, error)
}

type Paths struct {
	ConfigDir string
	StateDB   string
	PIDDir    string
}

type ControllerOptions struct {
	Store        state.Store
	Paths        Paths
	Executable   string
	Now          func() time.Time
	StartTimeout time.Duration
	StopTimeout  time.Duration
}

type StartRequest struct {
	Session string
	Project string
}

type StartResult struct {
	Project   string    `json:"project"`
	Session   string    `json:"session"`
	PID       int       `json:"pid"`
	StartedAt time.Time `json:"started_at"`
}

type StopRequest struct {
	Project string
}

type StopResult struct {
	Project   string    `json:"project"`
	PID       int       `json:"pid"`
	StoppedAt time.Time `json:"stopped_at"`
}

type AssignRequest struct {
	Project string
	Issue   string
	Prompt  string
	Agent   string
}

type CancelRequest struct {
	Project string
	Issue   string
}

type TaskActionResult struct {
	Project   string    `json:"project"`
	Issue     string    `json:"issue"`
	Status    string    `json:"status"`
	Agent     string    `json:"agent,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type LocalController struct {
	store        state.Store
	paths        Paths
	executable   string
	now          func() time.Time
	startTimeout time.Duration
	stopTimeout  time.Duration
}

func ResolvePaths() (Paths, error) {
	if stateDB := strings.TrimSpace(os.Getenv("ORCA_STATE_DB")); stateDB != "" {
		return Paths{
			ConfigDir: filepath.Dir(stateDB),
			StateDB:   stateDB,
			PIDDir:    filepath.Join(filepath.Dir(stateDB), "pids"),
		}, nil
	}

	configDir := strings.TrimSpace(os.Getenv("ORCA_CONFIG_DIR"))
	if configDir == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return Paths{}, fmt.Errorf("resolve home directory: %w", err)
		}
		configDir = filepath.Join(homeDir, ".config", "orca")
	}

	return Paths{
		ConfigDir: configDir,
		StateDB:   filepath.Join(configDir, "state.db"),
		PIDDir:    filepath.Join(configDir, "pids"),
	}, nil
}

func NewLocalController(options ControllerOptions) (*LocalController, error) {
	if options.Store == nil {
		return nil, fmt.Errorf("daemon controller requires a state store")
	}

	if options.Paths.StateDB == "" || options.Paths.PIDDir == "" {
		return nil, fmt.Errorf("daemon controller requires resolved paths")
	}

	now := options.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}

	startTimeout := options.StartTimeout
	if startTimeout == 0 {
		startTimeout = 3 * time.Second
	}

	stopTimeout := options.StopTimeout
	if stopTimeout == 0 {
		stopTimeout = 5 * time.Second
	}

	return &LocalController{
		store:        options.Store,
		paths:        options.Paths,
		executable:   options.Executable,
		now:          now,
		startTimeout: startTimeout,
		stopTimeout:  stopTimeout,
	}, nil
}

func (c *LocalController) Start(ctx context.Context, req StartRequest) (StartResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return StartResult{}, err
	}

	session := strings.TrimSpace(req.Session)
	if session == "" {
		session = filepath.Base(projectPath)
	}

	if err := c.preparePIDState(ctx, projectPath); err != nil {
		return StartResult{}, err
	}

	executable, err := c.resolveExecutable()
	if err != nil {
		return StartResult{}, err
	}

	pidFile := c.paths.pidFile(projectPath)
	if err := os.MkdirAll(filepath.Dir(pidFile), 0o755); err != nil {
		return StartResult{}, fmt.Errorf("create daemon pid directory: %w", err)
	}

	devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
	if err != nil {
		return StartResult{}, fmt.Errorf("open %s: %w", os.DevNull, err)
	}
	defer devNull.Close()

	cmd := exec.Command(
		executable,
		"__daemon-serve",
		"--session", session,
		"--project", projectPath,
		"--state-db", c.paths.StateDB,
		"--pid-file", pidFile,
	)
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull

	if err := cmd.Start(); err != nil {
		return StartResult{}, fmt.Errorf("start daemon process: %w", err)
	}

	process := cmd.Process
	pid := process.Pid

	deadline := time.Now().Add(c.startTimeout)
	for {
		status, err := c.store.ProjectStatus(ctx, projectPath)
		if err == nil && status.Daemon != nil && status.Daemon.Status == "running" && status.Daemon.PID == pid {
			_ = process.Release()
			return StartResult{
				Project:   projectPath,
				Session:   status.Daemon.Session,
				PID:       pid,
				StartedAt: status.Daemon.StartedAt,
			}, nil
		}

		waitErr := waitForPollingInterval(ctx, deadline, 50*time.Millisecond)
		if waitErr == nil {
			continue
		}
		if cleanupErr := c.cleanupFailedStart(projectPath, pidFile, process); cleanupErr != nil {
			return StartResult{}, cleanupErr
		}
		if errors.Is(waitErr, errPollingDeadlineExceeded) {
			return StartResult{}, fmt.Errorf("daemon failed to report running state")
		}
		return StartResult{}, waitErr
	}
}

func (c *LocalController) Stop(ctx context.Context, req StopRequest) (StopResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return StopResult{}, err
	}

	pidFile := c.paths.pidFile(projectPath)
	pid, err := readPIDFile(pidFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return StopResult{}, ErrDaemonNotRunning
		}
		return StopResult{}, err
	}

	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return StopResult{}, fmt.Errorf("stop daemon process: %w", err)
	}

	deadline := time.Now().Add(c.stopTimeout)
	for {
		alive, err := processAlive(pid)
		if err != nil {
			return StopResult{}, err
		}
		if !alive {
			stoppedAt := c.now()
			_ = os.Remove(pidFile)
			_ = c.store.MarkDaemonStopped(ctx, projectPath, stoppedAt)
			return StopResult{
				Project:   projectPath,
				PID:       pid,
				StoppedAt: stoppedAt,
			}, nil
		}

		waitErr := waitForPollingInterval(ctx, deadline, 50*time.Millisecond)
		if waitErr == nil {
			continue
		}
		if errors.Is(waitErr, errPollingDeadlineExceeded) {
			return StopResult{}, fmt.Errorf("daemon did not stop within %s", c.stopTimeout)
		}
		return StopResult{}, waitErr
	}
}

func (c *LocalController) Assign(ctx context.Context, req AssignRequest) (TaskActionResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return TaskActionResult{}, err
	}
	if err := c.requireRunning(ctx, projectPath); err != nil {
		return TaskActionResult{}, err
	}

	now := c.now()
	task := state.Task{
		Issue:     strings.TrimSpace(req.Issue),
		Status:    "queued",
		Agent:     strings.TrimSpace(req.Agent),
		Prompt:    req.Prompt,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := c.store.UpsertTask(ctx, projectPath, task); err != nil {
		return TaskActionResult{}, err
	}

	if _, err := c.store.AppendEvent(ctx, state.Event{
		Project:   projectPath,
		Kind:      "task.assigned",
		Issue:     task.Issue,
		Message:   fmt.Sprintf("%s queued", task.Issue),
		CreatedAt: now,
	}); err != nil {
		return TaskActionResult{}, err
	}

	return TaskActionResult{
		Project:   projectPath,
		Issue:     task.Issue,
		Status:    task.Status,
		Agent:     task.Agent,
		UpdatedAt: task.UpdatedAt,
	}, nil
}

func (c *LocalController) Cancel(ctx context.Context, req CancelRequest) (TaskActionResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return TaskActionResult{}, err
	}
	if err := c.requireRunning(ctx, projectPath); err != nil {
		return TaskActionResult{}, err
	}

	now := c.now()
	task, err := c.store.UpdateTaskStatus(ctx, projectPath, strings.TrimSpace(req.Issue), "cancelled", now)
	if err != nil {
		return TaskActionResult{}, err
	}

	if _, err := c.store.AppendEvent(ctx, state.Event{
		Project:   projectPath,
		Kind:      "task.cancelled",
		Issue:     task.Issue,
		Message:   fmt.Sprintf("%s cancelled", task.Issue),
		CreatedAt: now,
	}); err != nil {
		return TaskActionResult{}, err
	}

	return TaskActionResult{
		Project:   projectPath,
		Issue:     task.Issue,
		Status:    task.Status,
		Agent:     task.Agent,
		UpdatedAt: task.UpdatedAt,
	}, nil
}

func (c *LocalController) preparePIDState(ctx context.Context, projectPath string) error {
	pidFile := c.paths.pidFile(projectPath)
	pid, err := readPIDFile(pidFile)
	if err == nil {
		alive, aliveErr := processAlive(pid)
		if aliveErr != nil {
			return aliveErr
		}
		if alive {
			return ErrDaemonAlreadyRunning
		}

		_ = os.Remove(pidFile)
		_ = c.store.MarkDaemonStopped(ctx, projectPath, c.now())
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	status, err := c.store.ProjectStatus(ctx, projectPath)
	if err != nil {
		return err
	}
	if status.Daemon != nil && status.Daemon.Status == "running" {
		alive, aliveErr := processAlive(status.Daemon.PID)
		if aliveErr != nil {
			return aliveErr
		}
		if alive {
			return ErrDaemonAlreadyRunning
		}

		_ = c.store.MarkDaemonStopped(ctx, projectPath, c.now())
	}

	return nil
}

func (c *LocalController) requireRunning(ctx context.Context, projectPath string) error {
	pidFile := c.paths.pidFile(projectPath)
	pid, err := readPIDFile(pidFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrDaemonNotRunning
		}
		return err
	}

	alive, err := processAlive(pid)
	if err != nil {
		return err
	}
	if !alive {
		_ = os.Remove(pidFile)
		_ = c.store.MarkDaemonStopped(ctx, projectPath, c.now())
		return ErrDaemonNotRunning
	}

	status, err := c.store.ProjectStatus(ctx, projectPath)
	if err != nil {
		return err
	}
	if status.Daemon == nil || status.Daemon.Status != "running" {
		return ErrDaemonNotRunning
	}

	return nil
}

func (c *LocalController) resolveExecutable() (string, error) {
	if c.executable != "" {
		return c.executable, nil
	}

	executable, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve current executable: %w", err)
	}

	return executable, nil
}

func readPIDFile(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}

	var pid int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(data)), "%d", &pid); err != nil {
		return 0, fmt.Errorf("parse pid file %s: %w", path, err)
	}

	return pid, nil
}

func processAlive(pid int) (bool, error) {
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

var errPollingDeadlineExceeded = errors.New("polling deadline exceeded")

func waitForPollingInterval(ctx context.Context, deadline time.Time, interval time.Duration) error {
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return errPollingDeadlineExceeded
	}
	if remaining < interval {
		interval = remaining
	}

	timer := time.NewTimer(interval)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *LocalController) cleanupFailedStart(projectPath, pidFile string, process *os.Process) error {
	if process != nil {
		if err := process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("kill timed-out daemon process: %w", err)
		}
		if _, err := process.Wait(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			return fmt.Errorf("wait for timed-out daemon process: %w", err)
		}
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_ = os.Remove(pidFile)
	_ = c.store.MarkDaemonStopped(cleanupCtx, projectPath, c.now())
	return nil
}

func (p Paths) pidFile(projectPath string) string {
	hash := sha256.Sum256([]byte(projectPath))
	return filepath.Join(p.PIDDir, hex.EncodeToString(hash[:])+".pid")
}

var _ Controller = (*LocalController)(nil)
