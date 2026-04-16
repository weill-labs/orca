package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/weill-labs/orca/internal/amux"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/project"
)

var (
	ErrDaemonAlreadyRunning = errors.New("orca daemon is already running")
	ErrDaemonNotRunning     = errors.New("orca daemon is not running")
)

type Controller interface {
	Start(ctx context.Context, req StartRequest) (StartResult, error)
	Stop(ctx context.Context, req StopRequest) (StopResult, error)
	Reload(ctx context.Context, req ReloadRequest) (ReloadResult, error)
	Assign(ctx context.Context, req AssignRequest) (TaskActionResult, error)
	Batch(ctx context.Context, req BatchRequest) (BatchResult, error)
	Spawn(ctx context.Context, req SpawnPaneRequest) (SpawnPaneResult, error)
	Enqueue(ctx context.Context, req EnqueueRequest) (MergeQueueActionResult, error)
	Cancel(ctx context.Context, req CancelRequest) (TaskActionResult, error)
	Resume(ctx context.Context, req ResumeRequest) (TaskActionResult, error)
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
	DetectOrigin func(string) (string, error)
	Amux         amux.Client
	PoolRunner   pool.Runner
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
	Force   bool
}

type StopResult struct {
	Project   string    `json:"project"`
	PID       int       `json:"pid"`
	StoppedAt time.Time `json:"stopped_at"`
}

type ReloadRequest struct {
	Project string
}

type ReloadResult struct {
	Project string `json:"project"`
	PID     int    `json:"pid"`
}

type AssignRequest struct {
	Project    string
	Issue      string
	Prompt     string
	Agent      string
	CallerPane string
	Title      string
}

type BatchEntry struct {
	Issue  string `json:"issue"`
	Agent  string `json:"agent"`
	Prompt string `json:"prompt"`
	Title  string `json:"title,omitempty"`
}

type BatchRequest struct {
	Project    string
	Entries    []BatchEntry
	Delay      time.Duration
	CallerPane string
}

type CancelRequest struct {
	Project string
	Issue   string
}

type ResumeRequest struct {
	Project string
	Issue   string
	Prompt  string
}

type EnqueueRequest struct {
	Project  string
	PRNumber int
}

type TaskActionResult struct {
	Project   string    `json:"project"`
	Issue     string    `json:"issue"`
	Status    string    `json:"status"`
	Agent     string    `json:"agent,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

type BatchFailure struct {
	Issue string `json:"issue"`
	Error string `json:"error"`
}

type BatchResult struct {
	Project  string             `json:"project"`
	Results  []TaskActionResult `json:"results"`
	Failures []BatchFailure     `json:"failures,omitempty"`
}

type SpawnPaneRequest struct {
	Project  string
	Session  string
	LeadPane string
	Title    string
}

type SpawnPaneResult struct {
	Project   string `json:"project"`
	PaneID    string `json:"pane_id"`
	PaneName  string `json:"pane_name,omitempty"`
	ClonePath string `json:"clone_path"`
}

type MergeQueueActionResult struct {
	Project   string    `json:"project"`
	PRNumber  int       `json:"pr_number"`
	Status    string    `json:"status"`
	Position  int       `json:"position"`
	UpdatedAt time.Time `json:"updated_at"`
}

type LocalController struct {
	store        state.Store
	paths        Paths
	executable   string
	now          func() time.Time
	startTimeout time.Duration
	stopTimeout  time.Duration
	detectOrigin func(string) (string, error)
	amux         amux.Client
	poolRunner   pool.Runner
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
		detectOrigin: options.DetectOrigin,
		amux:         options.Amux,
		poolRunner:   options.PoolRunner,
	}, nil
}

func (c *LocalController) Start(ctx context.Context, req StartRequest) (StartResult, error) {
	projectPath, err := canonicalProject(req.Project)
	if err != nil {
		return StartResult{}, err
	}

	session := strings.TrimSpace(req.Session)
	if session == "" {
		if projectPath != "" {
			session = filepath.Base(projectPath)
		} else {
			session = "orca"
		}
	}

	if err := c.preparePIDState(ctx); err != nil {
		return StartResult{}, err
	}

	executable, err := c.resolveExecutable()
	if err != nil {
		return StartResult{}, err
	}

	pidFile := c.paths.pidFile()
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
		"--state-db", c.paths.StateDB,
		"--pid-file", pidFile,
	)
	cmd.Stdin = devNull
	cmd.Stdout = devNull
	cmd.Stderr = devNull
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	if err := cmd.Start(); err != nil {
		return StartResult{}, fmt.Errorf("start daemon process: %w", err)
	}

	process := cmd.Process
	pid := process.Pid
	socketPath := c.paths.socketFile()

	deadline := time.Now().Add(c.startTimeout)
	for {
		if alive, err := processAlive(pid); err == nil && alive {
			if _, err := os.Stat(socketPath); err == nil {
				_ = process.Release()
				startedAt := c.now()
				return StartResult{
					Project:   projectPath,
					Session:   session,
					PID:       pid,
					StartedAt: startedAt,
				}, nil
			}
		}

		waitErr := waitForPollingInterval(ctx, deadline, 50*time.Millisecond)
		if waitErr == nil {
			continue
		}
		if cleanupErr := c.cleanupFailedStart(pidFile, process); cleanupErr != nil {
			return StartResult{}, cleanupErr
		}
		if errors.Is(waitErr, amux.ErrWaitDeadlineExceeded) {
			return StartResult{}, fmt.Errorf("daemon failed to report running state")
		}
		return StartResult{}, waitErr
	}
}

func (c *LocalController) Stop(ctx context.Context, req StopRequest) (StopResult, error) {
	projectPath, err := canonicalProject(req.Project)
	if err != nil {
		return StopResult{}, err
	}

	pidFile := c.paths.pidFile()
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

	waitErr := waitForDaemonExit(ctx, pid, c.stopTimeout)
	if waitErr == nil {
		return c.finalizeStoppedDaemon(ctx, projectPath, pid, pidFile), nil
	}
	if !errors.Is(waitErr, amux.ErrWaitDeadlineExceeded) {
		return StopResult{}, waitErr
	}

	if !req.Force {
		return StopResult{}, fmt.Errorf("daemon did not stop within %s", c.stopTimeout)
	}

	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return StopResult{}, fmt.Errorf("force stop daemon process: %w", err)
	}

	waitErr = waitForDaemonExit(ctx, pid, c.stopTimeout)
	if waitErr == nil {
		return c.finalizeStoppedDaemon(ctx, projectPath, pid, pidFile), nil
	}
	if errors.Is(waitErr, amux.ErrWaitDeadlineExceeded) {
		return StopResult{}, fmt.Errorf("daemon did not stop after SIGKILL within %s", c.stopTimeout)
	}
	return StopResult{}, waitErr
}

func (c *LocalController) Reload(ctx context.Context, req ReloadRequest) (ReloadResult, error) {
	projectPath, err := canonicalProject(req.Project)
	if err != nil {
		return ReloadResult{}, err
	}
	if err := c.requireRunning(ctx); err != nil {
		return ReloadResult{}, err
	}

	callCtx, cancel := contextWithOptionalTimeout(ctx, 30*time.Second)
	defer cancel()

	var result ReloadResult
	err = callRPC(callCtx, c.paths.socketFile(), "reload", reloadRPCParams{
		Project: projectPath,
	}, &result)
	if err != nil {
		return ReloadResult{}, err
	}
	return result, nil
}

func (c *LocalController) Assign(ctx context.Context, req AssignRequest) (TaskActionResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return TaskActionResult{}, err
	}
	if err := c.requireRunning(ctx); err != nil {
		return TaskActionResult{}, err
	}

	callCtx, cancel := contextWithOptionalTimeout(ctx, 0)
	defer cancel()

	var result TaskActionResult
	err = callRPC(callCtx, c.paths.socketFile(), "assign", assignRPCParams{
		Project:    projectPath,
		Issue:      strings.TrimSpace(req.Issue),
		Prompt:     req.Prompt,
		Agent:      strings.TrimSpace(req.Agent),
		CallerPane: strings.TrimSpace(req.CallerPane),
		Title:      strings.TrimSpace(req.Title),
	}, &result)
	if err != nil {
		return TaskActionResult{}, err
	}
	return result, nil
}

func (c *LocalController) Batch(ctx context.Context, req BatchRequest) (BatchResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return BatchResult{}, err
	}
	if err := ValidateBatchEntries(req.Entries); err != nil {
		return BatchResult{}, err
	}
	if req.Delay < 0 {
		return BatchResult{}, errors.New("batch delay must be non-negative")
	}
	if err := c.requireRunning(ctx); err != nil {
		return BatchResult{}, err
	}

	callCtx, cancel := contextWithOptionalTimeout(ctx, 0)
	defer cancel()

	var result BatchResult
	err = callRPC(callCtx, c.paths.socketFile(), "batch", batchRPCParams{
		Project:    projectPath,
		Entries:    normalizeBatchEntries(req.Entries),
		Delay:      req.Delay.String(),
		CallerPane: strings.TrimSpace(req.CallerPane),
	}, &result)
	if err != nil {
		return BatchResult{}, err
	}
	return result, nil
}

func (c *LocalController) Enqueue(ctx context.Context, req EnqueueRequest) (MergeQueueActionResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return MergeQueueActionResult{}, err
	}
	if err := c.requireRunning(ctx); err != nil {
		return MergeQueueActionResult{}, err
	}

	callCtx, cancel := contextWithOptionalTimeout(ctx, 30*time.Second)
	defer cancel()

	var result MergeQueueActionResult
	err = callRPC(callCtx, c.paths.socketFile(), "enqueue", enqueueRPCParams{
		Project:  projectPath,
		PRNumber: req.PRNumber,
	}, &result)
	if err != nil {
		return MergeQueueActionResult{}, err
	}
	return result, nil
}

func (c *LocalController) Cancel(ctx context.Context, req CancelRequest) (TaskActionResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return TaskActionResult{}, err
	}
	if err := c.requireRunning(ctx); err != nil {
		return TaskActionResult{}, err
	}

	callCtx, cancel := contextWithOptionalTimeout(ctx, 30*time.Second)
	defer cancel()

	var result TaskActionResult
	err = callRPC(callCtx, c.paths.socketFile(), "cancel", cancelRPCParams{
		Project: projectPath,
		Issue:   strings.TrimSpace(req.Issue),
	}, &result)
	if err != nil {
		return TaskActionResult{}, err
	}
	return result, nil
}

func (c *LocalController) Resume(ctx context.Context, req ResumeRequest) (TaskActionResult, error) {
	projectPath, err := project.CanonicalPath(req.Project)
	if err != nil {
		return TaskActionResult{}, err
	}
	if err := c.requireRunning(ctx); err != nil {
		return TaskActionResult{}, err
	}

	callCtx, cancel := contextWithOptionalTimeout(ctx, 30*time.Second)
	defer cancel()

	var result TaskActionResult
	err = callRPC(callCtx, c.paths.socketFile(), "resume", resumeRPCParams{
		Project: projectPath,
		Issue:   strings.TrimSpace(req.Issue),
		Prompt:  strings.TrimSpace(req.Prompt),
	}, &result)
	if err != nil {
		return TaskActionResult{}, err
	}
	return result, nil
}

func (c *LocalController) preparePIDState(ctx context.Context) error {
	pidFile := c.paths.pidFile()
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
		_ = c.store.MarkDaemonStopped(ctx, "", c.now())
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	status, err := c.store.ProjectStatus(ctx, "")
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
		_ = c.store.MarkDaemonStopped(ctx, "", c.now())
	}

	return nil
}

func (c *LocalController) requireRunning(ctx context.Context) error {
	pidFile := c.paths.pidFile()
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
		_ = c.store.MarkDaemonStopped(ctx, "", c.now())
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

func waitForPollingInterval(ctx context.Context, deadline time.Time, interval time.Duration) error {
	return amux.WaitUntil(ctx, deadline, interval)
}

func waitForDaemonExit(ctx context.Context, pid int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		alive, err := processAlive(pid)
		if err != nil {
			return err
		}
		if !alive {
			return nil
		}

		waitErr := waitForPollingInterval(ctx, deadline, 50*time.Millisecond)
		if waitErr != nil {
			return waitErr
		}
	}
}

func (c *LocalController) finalizeStoppedDaemon(ctx context.Context, projectPath string, pid int, pidFile string) StopResult {
	stoppedAt := c.now()
	_ = os.Remove(pidFile)
	_ = c.store.MarkDaemonStopped(ctx, "", stoppedAt)
	return StopResult{
		Project:   projectPath,
		PID:       pid,
		StoppedAt: stoppedAt,
	}
}

func (c *LocalController) cleanupFailedStart(pidFile string, process *os.Process) error {
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
	_ = c.store.MarkDaemonStopped(cleanupCtx, "", c.now())
	return nil
}

func (p Paths) pidFile() string {
	return filepath.Join(p.PIDDir, "orca.pid")
}

func (p Paths) socketFile() string {
	return socketFile(p.ConfigDir)
}

func canonicalProject(projectPath string) (string, error) {
	if strings.TrimSpace(projectPath) == "" {
		return "", nil
	}
	return project.CanonicalPath(projectPath)
}

var _ Controller = (*LocalController)(nil)
