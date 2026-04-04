package daemon

import (
	"context"
	"reflect"
	"testing"
)

func TestDaemonStartReconcilesNonTerminalAssignments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		taskStatus        string
		paneID            string
		paneExists        map[string]bool
		startupSnapshot   []PaneCapture
		wantTaskStatus    string
		wantWorker        bool
		wantWorkerHealth  string
		wantRelease       bool
		wantFailedEvents  int
		wantEscalateEvent int
		wantCaptureCount  int
	}{
		{
			name:             "active worker stays active when pane is live",
			taskStatus:       TaskStatusActive,
			paneID:           "pane-1",
			startupSnapshot:  []PaneCapture{{Content: []string{"still running"}}},
			wantTaskStatus:   TaskStatusActive,
			wantWorker:       true,
			wantWorkerHealth: WorkerHealthHealthy,
			wantCaptureCount: 1,
		},
		{
			name:             "starting worker resumes as active when pane is live",
			taskStatus:       TaskStatusStarting,
			paneID:           "pane-1",
			startupSnapshot:  []PaneCapture{{Content: []string{"ready to continue"}}},
			wantTaskStatus:   TaskStatusActive,
			wantWorker:       true,
			wantWorkerHealth: WorkerHealthHealthy,
			wantCaptureCount: 1,
		},
		{
			name:             "starting worker fails when pane is missing",
			taskStatus:       TaskStatusStarting,
			paneID:           "pane-1",
			paneExists:       map[string]bool{"pane-1": false},
			wantTaskStatus:   TaskStatusFailed,
			wantWorker:       false,
			wantRelease:      true,
			wantFailedEvents: 1,
		},
		{
			name:              "active worker escalates when pane already exited",
			taskStatus:        TaskStatusActive,
			paneID:            "pane-1",
			startupSnapshot:   []PaneCapture{{Content: []string{"shell prompt"}, CurrentCommand: "bash", Exited: true}},
			wantTaskStatus:    TaskStatusActive,
			wantWorker:        true,
			wantWorkerHealth:  WorkerHealthEscalated,
			wantEscalateEvent: 1,
			wantCaptureCount:  1,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			seedRecoverableAssignment(t, deps, tt.taskStatus, "LAB-740", tt.paneID)
			deps.amux.paneExists = tt.paneExists
			if len(tt.startupSnapshot) > 0 {
				deps.amux.capturePaneSequence(tt.paneID, tt.startupSnapshot)
			}

			d := deps.newDaemon(t)
			ctx := context.Background()
			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			waitFor(t, "startup reconciliation", func() bool {
				task, ok := deps.state.task("LAB-740")
				return ok && task.Status == tt.wantTaskStatus
			})

			task, ok := deps.state.task("LAB-740")
			if !ok {
				t.Fatal("task missing after startup reconciliation")
			}
			if got, want := task.Status, tt.wantTaskStatus; got != want {
				t.Fatalf("task.Status = %q, want %q", got, want)
			}

			worker, workerOK := deps.state.worker(tt.paneID)
			if workerOK != tt.wantWorker {
				t.Fatalf("worker presence = %v, want %v", workerOK, tt.wantWorker)
			}
			if tt.wantWorker {
				if got, want := worker.Health, tt.wantWorkerHealth; got != want {
					t.Fatalf("worker.Health = %q, want %q", got, want)
				}
			}

			wantReleased := []Clone{}
			if tt.wantRelease {
				wantReleased = []Clone{{
					Name:          deps.pool.clone.Name,
					Path:          deps.pool.clone.Path,
					CurrentBranch: "LAB-740",
					AssignedTask:  "LAB-740",
				}}
			}
			if got := deps.pool.releasedClones(); !reflect.DeepEqual(got, wantReleased) {
				t.Fatalf("released clones = %#v, want %#v", got, wantReleased)
			}
			if got, want := deps.events.countType(EventTaskFailed), tt.wantFailedEvents; got != want {
				t.Fatalf("task failed events = %d, want %d", got, want)
			}
			if got, want := deps.events.countType(EventWorkerEscalated), tt.wantEscalateEvent; got != want {
				t.Fatalf("worker escalated events = %d, want %d", got, want)
			}
			if got, want := deps.amux.captureCount(tt.paneID), tt.wantCaptureCount; got != want {
				t.Fatalf("capture count = %d, want %d", got, want)
			}
			deps.amux.requireSentKeys(t, tt.paneID, nil)
		})
	}
}

func seedRecoverableAssignment(t *testing.T, deps *testDeps, status, issue, paneID string) {
	t.Helper()

	now := deps.clock.Now()
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        issue,
		Status:       status,
		Prompt:       "Recover daemon state",
		PaneID:       paneID,
		PaneName:     paneID,
		CloneName:    deps.pool.clone.Name,
		ClonePath:    deps.pool.clone.Path,
		Branch:       issue,
		AgentProfile: "codex",
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if paneID != "" {
		if err := deps.state.PutWorker(context.Background(), Worker{
			Project:        "/tmp/project",
			PaneID:         paneID,
			PaneName:       paneID,
			Issue:          issue,
			ClonePath:      deps.pool.clone.Path,
			AgentProfile:   "codex",
			Health:         WorkerHealthHealthy,
			LastActivityAt: now,
			UpdatedAt:      now,
		}); err != nil {
			t.Fatalf("PutWorker() error = %v", err)
		}
	}

	deps.pool.mu.Lock()
	defer deps.pool.mu.Unlock()
	if deps.pool.acquired == nil {
		deps.pool.acquired = make(map[string]bool)
	}
	deps.pool.acquired[deps.pool.clone.Path] = true
}
