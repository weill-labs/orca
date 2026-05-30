package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnqueueDirectFromPoolCloneProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		target         string
		seedAssignment bool
		wantErr        []string
	}{
		{
			name:           "resolves parent project",
			target:         "LAB-1995",
			seedAssignment: true,
		},
		{
			name:   "unknown issue includes clone project hint",
			target: "LAB-404",
			wantErr: []string{
				"LAB-404",
				"resolved from clone-pool path",
				"orca enqueue LAB-404 --project",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			projectPath := filepath.Join(t.TempDir(), "project")
			clonePath := filepath.Join(projectPath, OrcaPoolSubdir, "clone-01")
			markClonePathForTest(t, clonePath)

			deps := newTestDeps(t)
			deps.pool.clone = Clone{Name: "clone-01", Path: clonePath}
			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.Project = projectPath
				opts.LandingConfig = LandingConfig{Mode: LandingModeDirect, BaseBranch: "main"}
			})
			ctx := context.Background()
			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			if tt.seedAssignment {
				seedEnqueueActiveAssignment(t, deps, projectPath, clonePath, tt.target)
			}

			result, err := d.enqueueRequest(ctx, clonePath, EnqueueRequest{
				Project: clonePath,
				Target:  tt.target,
			})
			if len(tt.wantErr) > 0 {
				if err == nil {
					t.Fatal("enqueueRequest() succeeded, want error")
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Fatalf("enqueueRequest() error = %v, want substring %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("enqueueRequest() error = %v", err)
			}
			if got := result.Project; got != projectPath {
				t.Fatalf("result.Project = %q, want %q", got, projectPath)
			}
			entries, err := deps.state.MergeEntries(ctx, projectPath)
			if err != nil {
				t.Fatalf("MergeEntries() error = %v", err)
			}
			if got := len(entries); got != 1 {
				t.Fatalf("merge entry count = %d, want 1", got)
			}
			if got := entries[0].Project; got != projectPath {
				t.Fatalf("merge entry project = %q, want %q", got, projectPath)
			}
			if got := entries[0].ClonePath; got != clonePath {
				t.Fatalf("merge entry clone path = %q, want %q", got, clonePath)
			}
		})
	}
}

func seedEnqueueActiveAssignment(t *testing.T, deps *testDeps, projectPath, clonePath, issue string) {
	t.Helper()

	now := deps.clock.Now()
	workerID := "worker-01"
	deps.state.putTaskForTest(Task{
		Project:      projectPath,
		Issue:        issue,
		Status:       TaskStatusActive,
		WorkerID:     workerID,
		PaneID:       "pane-1",
		PaneName:     workerID,
		CloneName:    filepath.Base(clonePath),
		ClonePath:    clonePath,
		Branch:       issue,
		AgentProfile: "codex",
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        projectPath,
		WorkerID:       workerID,
		PaneID:         "pane-1",
		PaneName:       workerID,
		Issue:          issue,
		ClonePath:      clonePath,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastActivityAt: now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
}
