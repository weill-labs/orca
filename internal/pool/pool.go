package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/weill-labs/orca/internal/state"
)

var ErrNoFreeClones = errors.New("pool: no free clones available")

type Status string

const (
	StatusFree     Status = "free"
	StatusOccupied Status = "occupied"
)

type Clone struct {
	Name          string
	Path          string
	Status        Status
	CurrentBranch string
	AssignedTask  string
}

type Config interface {
	PoolDir() string
	CloneOrigin() string
	BaseBranch() string
}

type Store interface {
	EnsureClone(ctx context.Context, project, path string) (state.CloneRecord, error)
	TryOccupyClone(ctx context.Context, project, path, branch, task string) (bool, error)
	MarkCloneFree(ctx context.Context, project, path string) error
}

type Runner interface {
	Run(ctx context.Context, dir, name string, args ...string) error
}

type CWDUsageChecker interface {
	ActiveCWDs(ctx context.Context) ([]string, error)
}

type Option func(*Manager)

type Manager struct {
	project         string
	poolDir         string
	cloneOrigin     string
	baseBranch      string
	store           Store
	runner          Runner
	cwdUsageChecker CWDUsageChecker
}

func New(project string, cfg Config, store Store, opts ...Option) (*Manager, error) {
	if cfg == nil {
		return nil, errors.New("pool: config is required")
	}
	if store == nil {
		return nil, errors.New("pool: store is required")
	}
	if strings.TrimSpace(cfg.PoolDir()) == "" {
		return nil, errors.New("pool: pool directory is required")
	}

	absProject, err := filepath.Abs(project)
	if err != nil {
		return nil, fmt.Errorf("resolve project path: %w", err)
	}

	manager := &Manager{
		project:     filepath.Clean(absProject),
		poolDir:     cfg.PoolDir(),
		cloneOrigin: cfg.CloneOrigin(),
		baseBranch:  defaultBaseBranch(cfg.BaseBranch()),
		store:       store,
		runner:      execRunner{},
	}

	for _, opt := range opts {
		opt(manager)
	}

	if manager.runner == nil {
		manager.runner = execRunner{}
	}

	return manager, nil
}

func WithRunner(runner Runner) Option {
	return func(manager *Manager) {
		manager.runner = runner
	}
}

func WithCloneOrigin(origin string) Option {
	return func(manager *Manager) {
		manager.cloneOrigin = origin
	}
}

func WithCWDUsageChecker(checker CWDUsageChecker) Option {
	return func(manager *Manager) {
		manager.cwdUsageChecker = checker
	}
}

func (m *Manager) Discover(ctx context.Context) ([]Clone, error) {
	paths, err := m.eligibleClonePaths()
	if err != nil {
		return nil, err
	}

	clones := make([]Clone, 0, len(paths))
	for _, path := range paths {
		record, err := m.store.EnsureClone(ctx, m.project, path)
		if err != nil {
			return nil, fmt.Errorf("ensure clone %q: %w", path, err)
		}
		clones = append(clones, fromState(record))
	}

	return clones, nil
}

// HealthCheck ensures a clone is on the configured base branch, clean apart
// from the local pool marker, and can still reach its remote. Unhealthy clones
// are repaired before use.
func (m *Manager) HealthCheck(ctx context.Context, path string) error {
	clonePath, err := resolveClonePath(path)
	if err != nil {
		return err
	}

	health, err := inspectCloneHealth(ctx, clonePath)
	if err != nil {
		return err
	}
	if health.healthy(m.baseBranch) {
		return nil
	}

	if err := m.cleanupClone(ctx, clonePath, ""); err != nil {
		if health.remoteErr != nil {
			return fmt.Errorf("repair clone %q after remote check failed: %w", clonePath, errors.Join(health.remoteErr, err))
		}
		return fmt.Errorf("repair clone %q: %w", clonePath, err)
	}

	return nil
}

func (m *Manager) Allocate(ctx context.Context, taskID, issueBranch string) (Clone, error) {
	clones, err := m.Discover(ctx)
	if err != nil {
		return Clone{}, err
	}
	activeCWDs, err := m.activeCWDSet(ctx)
	if err != nil {
		return Clone{}, fmt.Errorf("check active pane cwd usage: %w", err)
	}

	clone, err := m.tryAllocateExisting(ctx, clones, activeCWDs, taskID, issueBranch)
	if err != nil {
		return Clone{}, err
	}
	if clone != nil {
		return *clone, nil
	}

	newPath, err := m.CreateClone(ctx)
	if err != nil {
		return Clone{}, fmt.Errorf("auto-create clone: %w", err)
	}

	record, err := m.store.EnsureClone(ctx, m.project, newPath)
	if err != nil {
		return Clone{}, fmt.Errorf("register new clone %q: %w", newPath, err)
	}

	ok, err := m.store.TryOccupyClone(ctx, m.project, record.Path, issueBranch, taskID)
	if err != nil {
		return Clone{}, fmt.Errorf("occupy new clone %q: %w", newPath, err)
	}
	if !ok {
		return Clone{}, fmt.Errorf("occupy new clone %q: unexpectedly contested", newPath)
	}

	if err := m.prepareClone(ctx, record.Path, issueBranch); err != nil {
		_ = m.store.MarkCloneFree(ctx, m.project, record.Path)
		return Clone{}, fmt.Errorf("prepare new clone %q: %w", record.Path, err)
	}

	return Clone{
		Name:          filepath.Base(record.Path),
		Path:          record.Path,
		Status:        StatusOccupied,
		CurrentBranch: issueBranch,
		AssignedTask:  taskID,
	}, nil
}

func (m *Manager) activeCWDSet(ctx context.Context) (map[string]struct{}, error) {
	if m.cwdUsageChecker == nil {
		return nil, nil
	}

	paths, err := m.cwdUsageChecker.ActiveCWDs(ctx)
	if err != nil {
		return nil, err
	}

	set := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if strings.TrimSpace(path) == "" {
			continue
		}

		resolvedPath, err := resolveClonePath(path)
		if err != nil {
			return nil, err
		}
		set[resolvedPath] = struct{}{}
	}

	return set, nil
}

func (m *Manager) Release(ctx context.Context, path, taskBranch string) error {
	clonePath, err := resolveClonePath(path)
	if err != nil {
		return err
	}

	if err := m.cleanupClone(ctx, clonePath, taskBranch); err != nil {
		return fmt.Errorf("cleanup clone %q: %w", clonePath, err)
	}
	if err := m.store.MarkCloneFree(ctx, m.project, clonePath); err != nil {
		return fmt.Errorf("mark clone %q free: %w", clonePath, err)
	}

	return nil
}

func (m *Manager) tryAllocateExisting(ctx context.Context, clones []Clone, activeCWDs map[string]struct{}, taskID, issueBranch string) (*Clone, error) {
	for _, clone := range clones {
		if clone.Status != StatusFree {
			continue
		}
		if _, ok := activeCWDs[clone.Path]; ok {
			continue
		}

		ok, err := m.store.TryOccupyClone(ctx, m.project, clone.Path, issueBranch, taskID)
		if err != nil {
			return nil, fmt.Errorf("occupy clone %q: %w", clone.Path, err)
		}
		if !ok {
			continue
		}

		if err := m.HealthCheck(ctx, clone.Path); err != nil {
			freeErr := m.store.MarkCloneFree(ctx, m.project, clone.Path)
			if freeErr != nil {
				err = errors.Join(err, freeErr)
				return nil, fmt.Errorf("health check clone %q: %w", clone.Path, err)
			}
			continue
		}

		if err := m.prepareClone(ctx, clone.Path, issueBranch); err != nil {
			freeErr := m.store.MarkCloneFree(ctx, m.project, clone.Path)
			if freeErr != nil {
				err = errors.Join(err, freeErr)
			}
			return nil, fmt.Errorf("prepare clone %q: %w", clone.Path, err)
		}

		clone.Status = StatusOccupied
		clone.CurrentBranch = issueBranch
		clone.AssignedTask = taskID
		return &clone, nil
	}

	return nil, nil
}

// CreateClone creates a new clone in the pool directory by cloning the origin.
func (m *Manager) CreateClone(ctx context.Context) (string, error) {
	if strings.TrimSpace(m.cloneOrigin) == "" {
		return "", fmt.Errorf("%w: set a git origin remote or ORCA_CLONE_ORIGIN", ErrNoFreeClones)
	}

	if err := os.MkdirAll(m.poolDir, 0o755); err != nil {
		return "", fmt.Errorf("create pool directory: %w", err)
	}

	next, err := m.nextCloneName()
	if err != nil {
		return "", err
	}

	clonePath := filepath.Join(m.poolDir, next)
	if err := m.runner.Run(ctx, m.poolDir, "git", "clone", m.cloneOrigin, clonePath); err != nil {
		return "", fmt.Errorf("git clone into %q: %w", clonePath, err)
	}

	absPath, err := filepath.Abs(clonePath)
	if err != nil {
		return "", fmt.Errorf("resolve clone path %q: %w", clonePath, err)
	}

	return filepath.Clean(absPath), nil
}

func (m *Manager) nextCloneName() (string, error) {
	entries, err := os.ReadDir(m.poolDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "clone-01", nil
		}
		return "", fmt.Errorf("read pool directory: %w", err)
	}

	maxNum := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "clone-") {
			continue
		}
		suffix := strings.TrimPrefix(name, "clone-")
		var num int
		if _, err := fmt.Sscanf(suffix, "%d", &num); err == nil && num > maxNum {
			maxNum = num
		}
	}

	return fmt.Sprintf("clone-%02d", maxNum+1), nil
}

func (m *Manager) eligibleClonePaths() ([]string, error) {
	entries, err := os.ReadDir(m.poolDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read pool directory: %w", err)
	}

	paths := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		absPath, err := filepath.Abs(filepath.Join(m.poolDir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("resolve clone path %q: %w", entry.Name(), err)
		}
		paths = append(paths, filepath.Clean(absPath))
	}

	sort.Strings(paths)
	return paths, nil
}

func resolveClonePath(path string) (string, error) {
	clonePath, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("resolve clone path: %w", err)
	}

	return filepath.Clean(clonePath), nil
}

func (m *Manager) prepareClone(ctx context.Context, path, issueBranch string) error {
	commands := [][]string{
		{"checkout", m.baseBranch},
		{"pull"},
		{"checkout", "-B", issueBranch},
	}

	for _, args := range commands {
		if err := m.runner.Run(ctx, path, "git", args...); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) cleanupClone(ctx context.Context, path, taskBranch string) error {
	commands := [][]string{
		{"reset", "--hard"},
		{"checkout", m.baseBranch},
		{"pull"},
		{"clean", "-fdx"},
	}

	for _, args := range commands {
		if err := m.runner.Run(ctx, path, "git", args...); err != nil {
			return err
		}
	}

	exists, err := branchExists(ctx, path, taskBranch)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	if err := m.runner.Run(ctx, path, "git", "branch", "-D", taskBranch); err != nil {
		return err
	}

	return nil
}

func defaultBaseBranch(baseBranch string) string {
	if strings.TrimSpace(baseBranch) == "" {
		return "main"
	}

	return baseBranch
}

func branchExists(ctx context.Context, path, branch string) (bool, error) {
	if strings.TrimSpace(branch) == "" {
		return false, nil
	}

	cmd := exec.CommandContext(ctx, "git", "show-ref", "--verify", "--quiet", "refs/heads/"+branch)
	cmd.Dir = path

	err := cmd.Run()
	if err == nil {
		return true, nil
	}

	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 1 {
		return false, nil
	}

	return false, fmt.Errorf("git show-ref --verify --quiet refs/heads/%s: %w", branch, err)
}

type cloneHealth struct {
	clean         bool
	currentBranch string
	remoteErr     error
}

func (h cloneHealth) healthy(baseBranch string) bool {
	return h.clean && h.currentBranch == baseBranch && h.remoteErr == nil
}

func inspectCloneHealth(ctx context.Context, path string) (cloneHealth, error) {
	status, err := gitOutput(ctx, path, "status", "--porcelain")
	if err != nil {
		return cloneHealth{}, err
	}

	branch, err := gitOutput(ctx, path, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return cloneHealth{}, err
	}

	return cloneHealth{
		clean:         statusClean(status),
		currentBranch: strings.TrimSpace(branch),
		remoteErr:     gitCommand(ctx, path, "ls-remote", "--exit-code", "origin", "HEAD"),
	}, nil
}

func statusClean(status string) bool {
	for _, line := range strings.Split(strings.TrimSpace(status), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		return false
	}

	return true
}

func gitOutput(ctx context.Context, path string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = path

	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}

	return string(out), nil
}

func gitCommand(ctx context.Context, path string, args ...string) error {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = path

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}

	return nil
}

func fromState(record state.CloneRecord) Clone {
	return Clone{
		Name:          filepath.Base(record.Path),
		Path:          record.Path,
		Status:        Status(record.Status),
		CurrentBranch: record.CurrentBranch,
		AssignedTask:  record.AssignedTask,
	}
}

type execRunner struct{}

func (execRunner) Run(ctx context.Context, dir, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir

	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}

	return nil
}
