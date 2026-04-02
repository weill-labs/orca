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
	Path          string
	Status        Status
	CurrentBranch string
	AssignedTask  string
}

type Config interface {
	PoolPattern() string
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
	project string
	pattern string
	store   Store
	runner  Runner
	homeDir string
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
		project: filepath.Clean(absProject),
		pattern: cfg.PoolPattern(),
		store:   store,
		runner:  execRunner{},
		homeDir: homeDir,
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
		{"checkout", "main"},
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
		{"checkout", "main"},
		{"pull"},
		{"clean", "-fdx", "--exclude=.orca-pool"},
		{"branch", "-D", taskBranch},
	}

	for _, args := range commands {
		if err := m.runner.Run(ctx, path, "git", args...); err != nil {
			return err
		}
	}

	return nil
}

func fromState(record state.CloneRecord) Clone {
	return Clone{
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
