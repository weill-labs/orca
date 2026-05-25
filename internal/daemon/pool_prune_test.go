package daemon

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	state "github.com/weill-labs/orca/internal/daemonstate"
	legacy "github.com/weill-labs/orca/internal/state"
)

type stateWithoutClonePrune struct {
	StateStore
}

func TestDaemonStartLogsWhenStateDoesNotSupportPoolPrune(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	logs := make([]string, 0)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.State = stateWithoutClonePrune{StateStore: deps.state}
		opts.Logf = func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		}
	})
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if !strings.Contains(strings.Join(logs, "\n"), "state store does not support clone pruning") {
		t.Fatalf("logs = %#v, want unsupported prune store message", logs)
	}
}

func TestDaemonStartLogsAndContinuesWhenPoolPruneListFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.state.listClonesErr = errors.New("list unavailable")
	logs := make([]string, 0)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		}
	})
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if !strings.Contains(strings.Join(logs, "\n"), "list unavailable") {
		t.Fatalf("logs = %#v, want list error", logs)
	}
}

func TestDaemonStartSkipsEmptyPoolClonePath(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.state.putCloneForTest(state.Clone{
		Path:      "",
		Status:    "occupied",
		UpdatedAt: deps.clock.Now(),
	})

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if got := deps.events.countType(EventPoolEntryPruned); got != 0 {
		t.Fatalf("pool prune events = %d, want 0", got)
	}
}

func TestDaemonStartKeepsExistingPoolEntryDuringStartupPrune(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join(t.TempDir(), OrcaPoolSubdir, "clone-existing")
	markClonePathForTest(t, clonePath)
	deps.state.putCloneForTest(state.Clone{
		Path:      clonePath,
		Status:    "free",
		UpdatedAt: deps.clock.Now(),
	})

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if _, ok := deps.state.clone(clonePath); !ok {
		t.Fatal("existing clone entry was pruned")
	}
	if got := deps.events.countType(EventPoolEntryPruned); got != 0 {
		t.Fatalf("pool prune events = %d, want 0", got)
	}
}

func TestDaemonStartLogsAndContinuesWhenPoolPruneInspectFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join(t.TempDir(), strings.Repeat("x", 5000))
	deps.state.putCloneForTest(state.Clone{
		Path:      clonePath,
		Status:    "occupied",
		UpdatedAt: deps.clock.Now(),
	})
	logs := make([]string, 0)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		}
	})
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if _, ok := deps.state.clone(clonePath); !ok {
		t.Fatal("clone entry with inspect failure was pruned")
	}
	if !strings.Contains(strings.Join(logs, "\n"), "pool prune failed to inspect clone") {
		t.Fatalf("logs = %#v, want inspect error", logs)
	}
}

func TestDaemonStartLogsAndContinuesWhenPoolPruneDeleteFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join(t.TempDir(), OrcaPoolSubdir, "clone-delete-fails")
	deps.state.putCloneForTest(state.Clone{
		Path:      clonePath,
		Status:    "occupied",
		UpdatedAt: deps.clock.Now(),
	})
	deps.state.deleteCloneErr = errors.New("delete unavailable")
	logs := make([]string, 0)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		}
	})
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if _, ok := deps.state.clone(clonePath); !ok {
		t.Fatal("clone entry was deleted after delete failure")
	}
	if !strings.Contains(strings.Join(logs, "\n"), "delete unavailable") {
		t.Fatalf("logs = %#v, want delete error", logs)
	}
	if got := deps.events.countType(EventPoolEntryPruned); got != 0 {
		t.Fatalf("pool prune events = %d, want 0", got)
	}
}

func TestDaemonStartEmitsPoolPruneEventWhenEntryAlreadyDeleted(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	clonePath := filepath.Join(t.TempDir(), OrcaPoolSubdir, "clone-already-deleted")
	deps.state.putCloneForTest(state.Clone{
		Path:      clonePath,
		Status:    "occupied",
		UpdatedAt: deps.clock.Now(),
	})
	deps.state.deleteCloneErr = state.ErrNotFound

	d := deps.newDaemon(t)
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if got := deps.events.countType(EventPoolEntryPruned); got != 1 {
		t.Fatalf("pool prune events = %d, want 1", got)
	}
}

func TestClonePathMissing(t *testing.T) {
	t.Parallel()

	existing := t.TempDir()
	missing := filepath.Join(t.TempDir(), "missing")

	got, err := clonePathMissing(existing)
	if err != nil {
		t.Fatalf("clonePathMissing(existing) error = %v", err)
	}
	if got {
		t.Fatal("clonePathMissing(existing) = true, want false")
	}

	got, err = clonePathMissing(missing)
	if err != nil {
		t.Fatalf("clonePathMissing(missing) error = %v", err)
	}
	if !got {
		t.Fatal("clonePathMissing(missing) = false, want true")
	}
}

func TestIsCloneAlreadyDeletedError(t *testing.T) {
	t.Parallel()

	if !isCloneAlreadyDeletedError(state.ErrNotFound) {
		t.Fatal("isCloneAlreadyDeletedError(state.ErrNotFound) = false, want true")
	}
	if !isCloneAlreadyDeletedError(legacy.ErrCloneNotFound) {
		t.Fatal("isCloneAlreadyDeletedError(legacy.ErrCloneNotFound) = false, want true")
	}
	if isCloneAlreadyDeletedError(errors.New("other")) {
		t.Fatal("isCloneAlreadyDeletedError(other) = true, want false")
	}
}
