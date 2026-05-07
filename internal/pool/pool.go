package pool

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	StatusFree        Status = "free"
	StatusOccupied    Status = "occupied"
	StatusQuarantined Status = "quarantined"

	CloneQuarantineFailureThreshold = 3

	ClonePoolMarker = ".orca-pool"
)

type Clone struct {
	Name          string
	Path          string
	Status        Status
	CurrentBranch string
	AssignedTask  string
	FailureCount  int
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

type CloneFailureStore interface {
	RecordCloneFailure(ctx context.Context, project, path string, threshold int) (state.CloneRecord, error)
	ResetCloneFailures(ctx context.Context, project, path string) error
	UnquarantineClone(ctx context.Context, project, path string) error
}

type NoAvailableClonesError struct {
	Quarantined []string
}

func (e NoAvailableClonesError) Error() string {
	return fmt.Sprintf("no available clones; %d are quarantined: %s", len(e.Quarantined), strings.Join(e.Quarantined, ", "))
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
	logf            func(string, ...any)
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
	absProject, err := filepath.Abs(project)
	if err != nil {
		return nil, fmt.Errorf("resolve project path: %w", err)
	}
	poolDir, err := normalizePoolRoot(cfg.PoolDir())
	if err != nil {
		return nil, err
	}

	manager := &Manager{
		project:     filepath.Clean(absProject),
		poolDir:     poolDir,
		cloneOrigin: cfg.CloneOrigin(),
		baseBranch:  defaultBaseBranch(cfg.BaseBranch()),
		logf:        log.Printf,
		store:       store,
		runner:      execRunner{},
	}

	for _, opt := range opts {
		opt(manager)
	}

	if manager.runner == nil {
		manager.runner = execRunner{}
	}
	if manager.logf == nil {
		manager.logf = log.Printf
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

func WithLogf(logf func(string, ...any)) Option {
	return func(manager *Manager) {
		manager.logf = logf
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
		clonePath, err := m.markedClonePath(record.Path)
		if err != nil {
			return nil, fmt.Errorf("ensure clone %q returned invalid path %q: %w", path, record.Path, err)
		}
		record.Path = clonePath
		clones = append(clones, fromState(record))
	}

	return clones, nil
}

// HealthCheck ensures a clone is on the configured base branch, clean apart
// from the local pool marker, and can still reach its remote. Unhealthy clones
// are repaired before use.
func (m *Manager) HealthCheck(ctx context.Context, path string) error {
	clonePath, err := m.markedClonePath(path)
	if err != nil {
		return err
	}

	health, err := inspectCloneHealth(ctx, clonePath, m.originBaseBranchRef())
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
	if err := noAvailableQuarantinedClonesError(clones, activeCWDs); err != nil {
		return Clone{}, err
	}

	newPath, err := m.CreateClone(ctx)
	if err != nil {
		return Clone{}, fmt.Errorf("auto-create clone: %w", err)
	}

	record, err := m.store.EnsureClone(ctx, m.project, newPath)
	if err != nil {
		return Clone{}, fmt.Errorf("register new clone %q: %w", newPath, err)
	}
	recordPath, err := m.markedClonePath(record.Path)
	if err != nil {
		return Clone{}, fmt.Errorf("register new clone %q returned invalid path %q: %w", newPath, record.Path, err)
	}
	record.Path = recordPath

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

func (m *Manager) RecordCloneFailure(ctx context.Context, path string) error {
	store, ok := m.store.(CloneFailureStore)
	if !ok {
		return nil
	}
	clonePath, err := m.markedClonePath(path)
	if err != nil {
		return err
	}
	_, err = store.RecordCloneFailure(ctx, m.project, clonePath, CloneQuarantineFailureThreshold)
	return err
}

func (m *Manager) RecordCloneSuccess(ctx context.Context, path string) error {
	store, ok := m.store.(CloneFailureStore)
	if !ok {
		return nil
	}
	clonePath, err := m.markedClonePath(path)
	if err != nil {
		return err
	}
	return store.ResetCloneFailures(ctx, m.project, clonePath)
}

func (m *Manager) Unquarantine(ctx context.Context, path string) error {
	store, ok := m.store.(CloneFailureStore)
	if !ok {
		return errors.New("pool store does not support clone quarantine")
	}
	clonePath, err := m.markedClonePath(path)
	if err != nil {
		return err
	}
	return store.UnquarantineClone(ctx, m.project, clonePath)
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

		resolvedPath, err := normalizeAbsolutePath("active pane cwd", path, true)
		if err != nil {
			continue
		}
		if err := validateCloneContained(m.poolDir, resolvedPath); err != nil {
			continue
		}
		set[resolvedPath] = struct{}{}
	}

	return set, nil
}

func (m *Manager) Release(ctx context.Context, path, taskBranch string) error {
	clonePath, err := m.markedClonePath(path)
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

func noAvailableQuarantinedClonesError(clones []Clone, activeCWDs map[string]struct{}) error {
	if selectableFreeCloneExists(clones, activeCWDs) {
		return nil
	}
	names := quarantinedCloneNames(clones)
	if len(names) == 0 {
		return nil
	}
	for _, clone := range clones {
		if clone.Status != StatusQuarantined {
			return nil
		}
	}
	return NoAvailableClonesError{Quarantined: names}
}

func selectableFreeCloneExists(clones []Clone, activeCWDs map[string]struct{}) bool {
	for _, clone := range clones {
		if clone.Status != StatusFree {
			continue
		}
		if _, ok := activeCWDs[clone.Path]; ok {
			continue
		}
		return true
	}
	return false
}

func quarantinedCloneNames(clones []Clone) []string {
	names := make([]string, 0)
	for _, clone := range clones {
		if clone.Status != StatusQuarantined {
			continue
		}
		name := strings.TrimSpace(clone.Name)
		if name == "" {
			name = filepath.Base(clone.Path)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	return names
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

	clonePath, err := m.clonePath(filepath.Join(m.poolDir, next))
	if err != nil {
		return "", err
	}
	if err := m.runner.Run(ctx, m.poolDir, "git", "clone", m.cloneOrigin, clonePath); err != nil {
		return "", fmt.Errorf("git clone into %q: %w", clonePath, err)
	}

	if err := ensureCloneMarker(clonePath); err != nil {
		return "", err
	}
	if err := m.runCloneSetupHook(ctx, clonePath); err != nil {
		return "", err
	}

	return clonePath, nil
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

		absPath, err := m.clonePath(filepath.Join(m.poolDir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("resolve clone path %q: %w", entry.Name(), err)
		}
		hasMarker, err := cloneHasMarker(absPath)
		if err != nil {
			return nil, err
		}
		if !hasMarker {
			continue
		}
		paths = append(paths, absPath)
	}

	sort.Strings(paths)
	return paths, nil
}

func normalizePoolRoot(path string) (string, error) {
	poolDir, err := normalizeAbsolutePath("pool directory", path, false)
	if err != nil {
		return "", err
	}
	if filepath.Dir(poolDir) == poolDir {
		return "", fmt.Errorf("pool: pool directory %q resolves to filesystem root", path)
	}
	return poolDir, nil
}

func (m *Manager) clonePath(path string) (string, error) {
	return ValidateClonePath(m.poolDir, path)
}

func (m *Manager) markedClonePath(path string) (string, error) {
	clonePath, err := m.clonePath(path)
	if err != nil {
		return "", err
	}
	if err := requireCloneMarker(clonePath); err != nil {
		return "", err
	}
	return clonePath, nil
}

func ValidateClonePath(poolRoot, path string) (string, error) {
	root, err := normalizePoolRoot(poolRoot)
	if err != nil {
		return "", err
	}
	clonePath, err := normalizeAbsolutePath("clone", path, true)
	if err != nil {
		return "", err
	}
	if err := validateCloneContained(root, clonePath); err != nil {
		return "", err
	}
	return clonePath, nil
}

func normalizeAbsolutePath(kind, path string, requireAlreadyAbsolute bool) (string, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", fmt.Errorf("pool: %s path is required", kind)
	}
	if strings.ContainsRune(trimmed, 0) {
		return "", fmt.Errorf("pool: %s path %q is invalid: contains NUL byte", kind, path)
	}
	if requireAlreadyAbsolute && !filepath.IsAbs(trimmed) {
		return "", fmt.Errorf("pool: %s path must be absolute: %q", kind, path)
	}
	if containsParentTraversal(trimmed) {
		return "", fmt.Errorf("pool: %s path %q contains parent traversal", kind, path)
	}

	absPath := trimmed
	if !filepath.IsAbs(absPath) {
		resolved, err := filepath.Abs(absPath)
		if err != nil {
			return "", fmt.Errorf("pool: resolve %s path %q: %w", kind, path, err)
		}
		absPath = resolved
	}
	return filepath.Clean(absPath), nil
}

func containsParentTraversal(path string) bool {
	for _, element := range strings.Split(filepath.ToSlash(path), "/") {
		if element == ".." {
			return true
		}
	}
	return false
}

func validateCloneContained(poolRoot, clonePath string) error {
	if clonePath == poolRoot {
		return fmt.Errorf("pool: clone path %q must be a child of pool root %q", clonePath, poolRoot)
	}
	rel, err := filepath.Rel(poolRoot, clonePath)
	if err != nil {
		return fmt.Errorf("pool: compare clone path %q with pool root %q: %w", clonePath, poolRoot, err)
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("pool: clone path %q must stay inside pool root %q", clonePath, poolRoot)
	}
	return nil
}

func cloneHasMarker(clonePath string) (bool, error) {
	markerPath := filepath.Join(clonePath, ClonePoolMarker)
	info, err := os.Lstat(markerPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("pool: inspect clone marker %q: %w", markerPath, err)
	}
	if !info.Mode().IsRegular() {
		return false, fmt.Errorf("pool: clone marker %q must be a regular file", markerPath)
	}
	return true, nil
}

func requireCloneMarker(clonePath string) error {
	hasMarker, err := cloneHasMarker(clonePath)
	if err != nil {
		return err
	}
	if !hasMarker {
		return fmt.Errorf("pool: clone path %q is missing %s marker", clonePath, ClonePoolMarker)
	}
	return nil
}

func ensureCloneMarker(clonePath string) error {
	markerPath := filepath.Join(clonePath, ClonePoolMarker)
	if err := os.WriteFile(markerPath, []byte("orca clone pool\n"), 0o644); err != nil {
		return fmt.Errorf("pool: write clone marker %q: %w", markerPath, err)
	}
	return nil
}

func (m *Manager) prepareClone(ctx context.Context, path, issueBranch string) error {
	clonePath, err := m.markedClonePath(path)
	if err != nil {
		return err
	}
	commands := append(m.resetToOriginBaseCommands(), []string{"checkout", "-B", issueBranch})

	for _, args := range commands {
		if err := m.runner.Run(ctx, clonePath, "git", args...); err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) cleanupClone(ctx context.Context, path, taskBranch string) error {
	clonePath, err := m.markedClonePath(path)
	if err != nil {
		return err
	}
	commands := append([][]string{{"reset", "--hard"}}, m.resetToOriginBaseCommands()...)
	commands = append(commands, []string{"clean", "-fdx", "-e", ClonePoolMarker})

	for _, args := range commands {
		if err := m.runner.Run(ctx, clonePath, "git", args...); err != nil {
			return err
		}
	}

	taskBranch = strings.TrimSpace(taskBranch)
	if taskBranch == "" || taskBranch == m.baseBranch {
		return nil
	}

	exists, err := branchExists(ctx, clonePath, taskBranch)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	if err := m.runner.Run(ctx, clonePath, "git", "branch", "-D", taskBranch); err != nil {
		return err
	}

	return nil
}

func (m *Manager) resetToOriginBaseCommands() [][]string {
	return [][]string{
		{"fetch", "origin", m.baseBranch},
		{"checkout", "--detach", m.originBaseBranchRef()},
		{"reset", "--hard", m.originBaseBranchRef()},
	}
}

func defaultBaseBranch(baseBranch string) string {
	if strings.TrimSpace(baseBranch) == "" {
		return "main"
	}

	return baseBranch
}

func (m *Manager) originBaseBranchRef() string {
	return "origin/" + m.baseBranch
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
	clean            bool
	currentBranch    string
	headCommit       string
	originBaseCommit string
	remoteErr        error
}

func (h cloneHealth) healthy(baseBranch string) bool {
	onBaseBranch := h.currentBranch == baseBranch
	detachedAtBase := h.currentBranch == "HEAD" && h.headCommit != "" && h.headCommit == h.originBaseCommit
	return h.clean && (onBaseBranch || detachedAtBase) && h.remoteErr == nil
}

func inspectCloneHealth(ctx context.Context, path, originBaseRef string) (cloneHealth, error) {
	status, err := gitOutput(ctx, path, "status", "--porcelain")
	if err != nil {
		return cloneHealth{}, err
	}

	branch, err := gitOutput(ctx, path, "rev-parse", "--abbrev-ref", "HEAD")
	if err != nil {
		return cloneHealth{}, err
	}

	headCommit, err := gitOutput(ctx, path, "rev-parse", "HEAD")
	if err != nil {
		return cloneHealth{}, err
	}
	originBaseCommit := ""
	if commit, err := gitOutput(ctx, path, "rev-parse", originBaseRef); err == nil {
		originBaseCommit = strings.TrimSpace(commit)
	}

	return cloneHealth{
		clean:            statusClean(status),
		currentBranch:    strings.TrimSpace(branch),
		headCommit:       strings.TrimSpace(headCommit),
		originBaseCommit: originBaseCommit,
		remoteErr:        gitCommand(ctx, path, "ls-remote", "--exit-code", "origin", "HEAD"),
	}, nil
}

func statusClean(status string) bool {
	for _, line := range strings.Split(strings.TrimSpace(status), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if statusLinePath(line) == ClonePoolMarker {
			continue
		}
		return false
	}

	return true
}

func statusLinePath(line string) string {
	if len(line) < 4 {
		return strings.TrimSpace(line)
	}
	return strings.TrimSpace(line[3:])
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
		FailureCount:  record.FailureCount,
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
