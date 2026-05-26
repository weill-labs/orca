package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/pool"
	"github.com/weill-labs/orca/internal/worksource"
)

func TestRunPullTick(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		enabled      bool
		freeClones   int
		readyItems   []worksource.WorkItem
		claimErrs    map[string]error
		startDaemon  bool
		wantReady    []int
		wantClaims   []string
		wantAssigned []string
	}{
		{
			name:       "disabled flag results in no source calls",
			enabled:    false,
			freeClones: 1,
			readyItems: []worksource.WorkItem{
				{ID: "LAB-1933", Body: "Implement idle pull"},
			},
			wantReady:  []int{},
			wantClaims: []string{},
		},
		{
			name:       "claims and dispatches up to free clone count",
			enabled:    true,
			freeClones: 2,
			readyItems: []worksource.WorkItem{
				{ID: "LAB-201", Title: "First item", Body: "Implement first item"},
				{ID: "LAB-202", Title: "Second item", Body: "Implement second item"},
				{ID: "LAB-203", Title: "Third item", Body: "Implement third item"},
			},
			startDaemon:  true,
			wantReady:    []int{2},
			wantClaims:   []string{"LAB-201", "LAB-202"},
			wantAssigned: []string{"LAB-201", "LAB-202"},
		},
		{
			name:       "already claimed items are skipped without dispatch",
			enabled:    true,
			freeClones: 2,
			readyItems: []worksource.WorkItem{
				{ID: "LAB-301", Body: "Implement claimed item"},
				{ID: "LAB-302", Body: "Implement available item"},
			},
			claimErrs: map[string]error{
				"LAB-301": worksource.ErrAlreadyClaimed,
			},
			startDaemon:  true,
			wantReady:    []int{2},
			wantClaims:   []string{"LAB-301", "LAB-302"},
			wantAssigned: []string{"LAB-302"},
		},
		{
			name:       "zero free clones means no ready call",
			enabled:    true,
			freeClones: 0,
			readyItems: []worksource.WorkItem{
				{ID: "LAB-401", Body: "Implement blocked item"},
			},
			wantReady:  []int{},
			wantClaims: []string{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			configurePullTestClones(t, deps, tt.freeClones)
			source := &fakePullWorkSource{
				readyItems: tt.readyItems,
				claimErrs:  tt.claimErrs,
			}
			if tt.freeClones > 0 {
				deps.amux.spawnPanes = pullTestPanes(tt.freeClones)
			}
			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.WorkSourceEnabled = tt.enabled
				opts.WorkSource = source
				opts.WorkSourceAgent = "codex"
				opts.Hostname = "host-a"
			})
			ctx := context.Background()
			if tt.startDaemon {
				if err := d.Start(ctx); err != nil {
					t.Fatalf("Start() error = %v", err)
				}
				t.Cleanup(func() {
					_ = d.Stop(context.Background())
				})
			}

			d.runPullTick(ctx)

			if got := source.readyLimits(); !reflect.DeepEqual(got, tt.wantReady) {
				t.Fatalf("Ready() limits = %#v, want %#v", got, tt.wantReady)
			}
			if got := source.claimedIDs(); !reflect.DeepEqual(got, tt.wantClaims) {
				t.Fatalf("Claim() ids = %#v, want %#v", got, tt.wantClaims)
			}
			for _, workerID := range source.claimedWorkerIDs() {
				if got, want := workerID, "orca:host-a"; got != want {
					t.Fatalf("Claim() workerID = %q, want %q", got, want)
				}
			}
			for _, issue := range tt.wantAssigned {
				task, ok := deps.state.task(issue)
				if !ok {
					t.Fatalf("task %s was not stored", issue)
				}
				if got, want := task.Status, TaskStatusActive; got != want {
					t.Fatalf("task %s status = %q, want %q", issue, got, want)
				}
				if got, want := task.AgentProfile, "codex"; got != want {
					t.Fatalf("task %s agent = %q, want %q", issue, got, want)
				}
			}
			if got, want := len(deps.amux.spawnRequests), len(tt.wantAssigned); got != want {
				t.Fatalf("spawn request count = %d, want %d", got, want)
			}
		})
	}
}

func TestStartVerifiesWorkSource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		enabled   bool
		verifyErr error
		wantErr   string
	}{
		{
			name:      "verify failure prevents start",
			enabled:   true,
			verifyErr: errors.New("bd version failed"),
			wantErr:   "verify work source",
		},
		{
			name:    "disabled source is not verified",
			enabled: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			source := &fakePullWorkSource{verifyErr: tt.verifyErr}
			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.WorkSourceEnabled = tt.enabled
				opts.WorkSource = source
			})

			err := d.Start(context.Background())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Start() error = %v, want substring %q", err, tt.wantErr)
				}
				if d.started.Load() {
					t.Fatal("daemon started after work source verification failed")
				}
				if _, statErr := os.Stat(deps.pidPath); !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("pid file stat error = %v, want not exists", statErr)
				}
			} else {
				if err != nil {
					t.Fatalf("Start() error = %v, want nil", err)
				}
				t.Cleanup(func() {
					_ = d.Stop(context.Background())
				})
			}

			if got, want := source.verifyCallCount(), boolToInt(tt.enabled); got != want {
				t.Fatalf("Verify() calls = %d, want %d", got, want)
			}
		})
	}
}

func TestMonitorPullTickRunsWhenEnabled(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	configurePullTestClones(t, deps, 1)
	captureTick := newFakeTicker()
	pollTick := newFakeTicker()
	pullTick := newFakeTicker()
	deps.tickers.enqueue(captureTick, pollTick, pullTick)
	source := &fakePullWorkSource{}
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.WorkSourceEnabled = true
		opts.WorkSource = source
		opts.Hostname = "host-a"
	})

	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	deps.clock.Advance(time.Second)
	pullTick.tick(deps.clock.Now())
	waitFor(t, "pull ready call", func() bool {
		return len(source.readyLimits()) == 1
	})

	if got, want := source.readyLimits(), []int{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Ready() limits = %#v, want %#v", got, want)
	}
}

func TestLoadWorkSourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		content   string
		want      workSourceConfig
		wantError string
	}{
		{
			name: "missing project uses defaults",
			want: defaultWorkSourceConfig(),
		},
		{
			name: "repo config without worksource uses defaults",
			content: strings.Join([]string{
				"[agents.codex",
				`start_command = "codex --yolo"`,
				"",
			}, "\n"),
			want: defaultWorkSourceConfig(),
		},
		{
			name: "beads source config",
			content: strings.Join([]string{
				"[worksource]",
				"enabled = true",
				`source = "beads"`,
				`beads_bin = "bd-test"`,
				`agent = "claude"`,
				"",
			}, "\n"),
			want: workSourceConfig{
				Enabled:  true,
				Source:   workSourceBeads,
				BeadsBin: "bd-test",
				Agent:    "claude",
			},
		},
		{
			name: "unknown source errors",
			content: strings.Join([]string{
				"[worksource]",
				`source = "linear"`,
				"",
			}, "\n"),
			wantError: "worksource.source",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			projectPath := ""
			if tt.content != "" {
				projectPath = t.TempDir()
				configDir := filepath.Join(projectPath, ".orca")
				if err := os.MkdirAll(configDir, 0o755); err != nil {
					t.Fatalf("MkdirAll(%q) error = %v", configDir, err)
				}
				configPath := filepath.Join(configDir, "config.toml")
				if err := os.WriteFile(configPath, []byte(tt.content), 0o644); err != nil {
					t.Fatalf("WriteFile(%q) error = %v", configPath, err)
				}
			}

			got, err := loadWorkSourceConfig(projectPath)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("loadWorkSourceConfig() error = %v, want substring %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadWorkSourceConfig() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("loadWorkSourceConfig() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

type fakePullWorkSource struct {
	mu         sync.Mutex
	readyItems []worksource.WorkItem
	readyErr   error
	claimErrs  map[string]error
	verifyErr  error
	readyCalls []int
	claims     []fakePullClaim
	verify     int
}

type fakePullClaim struct {
	id       string
	workerID string
}

func (f *fakePullWorkSource) Ready(_ context.Context, limit int) ([]worksource.WorkItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.readyCalls = append(f.readyCalls, limit)
	if f.readyErr != nil {
		return nil, f.readyErr
	}
	items := make([]worksource.WorkItem, len(f.readyItems))
	copy(items, f.readyItems)
	return items, nil
}

func (f *fakePullWorkSource) Get(context.Context, string) (worksource.WorkItem, error) {
	return worksource.WorkItem{}, worksource.ErrNotFound
}

func (f *fakePullWorkSource) Claim(_ context.Context, id, workerID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, fakePullClaim{id: id, workerID: workerID})
	return f.claimErrs[id]
}

func (f *fakePullWorkSource) Release(context.Context, string, string) error {
	return nil
}

func (f *fakePullWorkSource) Complete(context.Context, string, worksource.Outcome) error {
	return nil
}

func (f *fakePullWorkSource) Verify(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verify++
	return f.verifyErr
}

func (f *fakePullWorkSource) readyLimits() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int, len(f.readyCalls))
	copy(out, f.readyCalls)
	return out
}

func (f *fakePullWorkSource) claimedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.claims))
	for _, claim := range f.claims {
		out = append(out, claim.id)
	}
	return out
}

func (f *fakePullWorkSource) claimedWorkerIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, 0, len(f.claims))
	for _, claim := range f.claims {
		out = append(out, claim.workerID)
	}
	return out
}

func (f *fakePullWorkSource) verifyCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.verify
}

func configurePullTestClones(t *testing.T, deps *testDeps, freeCount int) {
	t.Helper()

	deps.pool.clone = Clone{}
	deps.pool.clones = make([]Clone, 0, freeCount)
	for i := 1; i <= freeCount; i++ {
		name := fmt.Sprintf("clone-%02d", i)
		path := filepath.Join("/tmp/project", OrcaPoolSubdir, name)
		markClonePathForTest(t, path)
		deps.pool.clones = append(deps.pool.clones, Clone{Name: name, Path: path})
		deps.state.putCloneForTest(state.Clone{
			Path:   path,
			Status: string(pool.StatusFree),
		})
	}
	if freeCount == 0 {
		deps.state.putCloneForTest(state.Clone{
			Path:   filepath.Join("/tmp/project", OrcaPoolSubdir, "clone-occupied"),
			Status: string(pool.StatusOccupied),
		})
	}
}

func pullTestPanes(count int) []Pane {
	panes := make([]Pane, 0, count)
	for i := 1; i <= count; i++ {
		panes = append(panes, Pane{
			ID:   fmt.Sprintf("pane-%d", i),
			Name: fmt.Sprintf("worker-%d", i),
		})
	}
	return panes
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

var _ worksource.Source = (*fakePullWorkSource)(nil)
var _ workSourceVerifier = (*fakePullWorkSource)(nil)
