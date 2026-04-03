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

const markerFile = ".orca-pool"

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
	PoolPattern() string
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

type Option func(*Manager)

type Manager struct {
	project    string
	pattern    string
	baseBranch string
	store      Store
	runner     Runner
	homeDir    string
}

func New(project string, cfg Config, store Store, opts ...Option) (*Manager, error) {
	if cfg == nil {
		return nil, errors.New("pool: config is required")
	}
	if store == nil {
		return nil, errors.New("pool: store is required")
	}
	if strings.TrimSpace(cfg.PoolPattern()) == "" {
		return nil, errors.New("pool: pool pattern is required")
	}

	absProject, err := filepath.Abs(project)
	if err != nil {
		return nil, fmt.Errorf("resolve project path: %w", err)
	}

	homeDir, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("resolve home directory: %w", err)
	}

	manager := &Manager{
		project:    filepath.Clean(absProject),
		pattern:    cfg.PoolPattern(),
		baseBranch: defaultBaseBranch(cfg.BaseBranch()),
		store:      store,
		runner:     execRunner{},
		homeDir:    homeDir,
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

func WithHomeDir(homeDir string) Option {
	return func(manager *Manager) {
		manager.homeDir = homeDir
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

func (m *Manager) HealthCheck(ctx context.Context, path string) error {
	clonePath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve clone path: %w", err)
	}
	clonePath = filepath.Clean(clonePath)

	health, err := inspectCloneHealth(ctx, clonePath)
	if err != nil {
		return err
	}
	if health.healthy(m.baseBranch) {
		return nil
	}

	if err := m.cleanupClone(ctx, clonePath, ""); err != nil {
		if health.remoteErr != nil {
			return fmt.Errorf("repair clone %q after remote check failed: %w", clonePath, err)
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

	for _, clone := range clones {
		if clone.Status != StatusFree {
			continue
		}

		ok, err := m.store.TryOccupyClone(ctx, m.project, clone.Path, issueBranch, taskID)
		if err != nil {
			return Clone{}, fmt.Errorf("occupy clone %q: %w", clone.Path, err)
		}
		if !ok {
			continue
		}

		if err := m.HealthCheck(ctx, clone.Path); err != nil {
			freeErr := m.store.MarkCloneFree(ctx, m.project, clone.Path)
			if freeErr != nil {
				err = errors.Join(err, freeErr)
			}
			return Clone{}, fmt.Errorf("health check clone %q: %w", clone.Path, err)
		}

		if err := m.prepareClone(ctx, clone.Path, issueBranch); err != nil {
			freeErr := m.store.MarkCloneFree(ctx, m.project, clone.Path)
			if freeErr != nil {
				err = errors.Join(err, freeErr)
			}
			return Clone{}, fmt.Errorf("prepare clone %q: %w", clone.Path, err)
		}

		clone.Status = StatusOccupied
		clone.CurrentBranch = issueBranch
		clone.AssignedTask = taskID
		return clone, nil
	}

	return Clone{}, ErrNoFreeClones
}

func (m *Manager) Release(ctx context.Context, path, taskBranch string) error {
	clonePath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve clone path: %w", err)
	}
	clonePath = filepath.Clean(clonePath)

	if err := m.cleanupClone(ctx, clonePath, taskBranch); err != nil {
		return fmt.Errorf("cleanup clone %q: %w", clonePath, err)
	}
	if err := m.store.MarkCloneFree(ctx, m.project, clonePath); err != nil {
		return fmt.Errorf("mark clone %q free: %w", clonePath, err)
	}

	return nil
}

func (m *Manager) eligibleClonePaths() ([]string, error) {
	pattern, err := m.expandPattern(m.pattern)
	if err != nil {
		return nil, err
	}

	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob clone pool pattern: %w", err)
	}

	sort.Strings(matches)

	paths := make([]string, 0, len(matches))
	for _, match := range matches {
		info, err := os.Stat(match)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("stat clone path %q: %w", match, err)
		}
		if !info.IsDir() {
			continue
		}

		markerPath := filepath.Join(match, markerFile)
		if _, err := os.Stat(markerPath); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return nil, fmt.Errorf("stat marker file %q: %w", markerPath, err)
		}

		absPath, err := filepath.Abs(match)
		if err != nil {
			return nil, fmt.Errorf("resolve clone path %q: %w", match, err)
		}
		paths = append(paths, filepath.Clean(absPath))
	}

	return paths, nil
}

func (m *Manager) expandPattern(pattern string) (string, error) {
	if pattern == "~" {
		return m.homeDir, nil
	}
	if strings.HasPrefix(pattern, "~/") {
		if strings.TrimSpace(m.homeDir) == "" {
			return "", errors.New("pool: home directory is required for ~ expansion")
		}
		return filepath.Join(m.homeDir, pattern[2:]), nil
	}

	return pattern, nil
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
		{"clean", "-fdx", "--exclude=.orca-pool"},
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
		line = strings.TrimSpace(line)
		if line == "" || line == "?? "+markerFile {
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
