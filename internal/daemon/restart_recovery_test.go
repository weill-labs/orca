package daemon

import (
	"context"
	"errors"
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
		paneExistsErr     error
		capturePaneErr    error
		skipWorker        bool
		wantTaskStatus    string
		wantWorker        bool
		wantWorkerHealth  string
		wantRelease       bool
		wantFailedEvents  int
		wantEscalateEvent int
		wantCaptureCount  int
		wantEventType     string
		wantEventMessage  string
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
			name:             "starting worker fails when pane id is missing",
			taskStatus:       TaskStatusStarting,
			paneID:           "",
			wantTaskStatus:   TaskStatusFailed,
			wantWorker:       false,
			wantRelease:      true,
			wantFailedEvents: 1,
		},
		{
			name:             "starting worker fails when persisted worker is missing",
			taskStatus:       TaskStatusStarting,
			paneID:           "pane-1",
			skipWorker:       true,
			wantTaskStatus:   TaskStatusFailed,
			wantWorker:       false,
			wantRelease:      true,
			wantFailedEvents: 1,
		},
		{
			name:             "starting worker fails when pane liveness check errors",
			taskStatus:       TaskStatusStarting,
			paneID:           "pane-1",
			paneExistsErr:    errors.New("amux unavailable"),
			wantTaskStatus:   TaskStatusFailed,
			wantWorker:       false,
			wantRelease:      true,
			wantFailedEvents: 1,
			wantEventType:    EventTaskFailed,
			wantEventMessage: "worker pane liveness check failed on daemon startup: amux unavailable",
		},
		{
			name:             "starting worker fails when pane capture errors",
			taskStatus:       TaskStatusStarting,
			paneID:           "pane-1",
			capturePaneErr:   errors.New("capture failed"),
			wantTaskStatus:   TaskStatusFailed,
			wantWorker:       false,
			wantRelease:      true,
			wantFailedEvents: 1,
			wantCaptureCount: 1,
			wantEventType:    EventTaskFailed,
			wantEventMessage: "worker pane capture failed on daemon startup: capture failed",
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
		{
			name:              "active worker escalates when pane liveness check errors",
			taskStatus:        TaskStatusActive,
			paneID:            "pane-1",
			paneExistsErr:     errors.New("amux unavailable"),
			wantTaskStatus:    TaskStatusActive,
			wantWorker:        true,
			wantWorkerHealth:  WorkerHealthEscalated,
			wantEscalateEvent: 1,
			wantEventType:     EventWorkerEscalated,
			wantEventMessage:  "worker pane liveness check failed on daemon startup: amux unavailable",
		},
		{
			name:              "active worker escalates when pane capture errors",
			taskStatus:        TaskStatusActive,
			paneID:            "pane-1",
			capturePaneErr:    errors.New("capture failed"),
			wantTaskStatus:    TaskStatusActive,
			wantWorker:        true,
			wantWorkerHealth:  WorkerHealthEscalated,
			wantEscalateEvent: 1,
			wantCaptureCount:  1,
			wantEventType:     EventWorkerEscalated,
			wantEventMessage:  "worker pane capture failed on daemon startup: capture failed",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			seedRecoverableAssignment(t, deps, tt.taskStatus, "LAB-740", tt.paneID)
			if tt.skipWorker && tt.paneID != "" {
				delete(deps.state.workers, tt.paneID)
			}
			deps.amux.paneExists = tt.paneExists
			deps.amux.paneExistsErr = tt.paneExistsErr
			deps.amux.capturePaneErr = tt.capturePaneErr
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
			if tt.wantEventType != "" {
				event, ok := deps.events.lastEventOfType(tt.wantEventType)
				if !ok {
					t.Fatalf("lastEventOfType(%q) = false, want true", tt.wantEventType)
				}
				if got, want := event.Message, tt.wantEventMessage; got != want {
					t.Fatalf("event.Message = %q, want %q", got, want)
				}
			}
			if got, want := deps.amux.captureCount(tt.paneID), tt.wantCaptureCount; got != want {
				t.Fatalf("capture count = %d, want %d", got, want)
			}
			deps.amux.requireSentKeys(t, tt.paneID, nil)
		})
	}
}

func TestDaemonStartReconcilesEscalatedWorkerHealthFromPaneMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedRecoverableAssignment(t, deps, TaskStatusActive, "LAB-850", "pane-1")
	deps.amux.metadata = map[string]map[string]string{
		"pane-1": {
			"agent_profile": "codex",
			"status":        "escalated",
		},
	}
	deps.amux.capturePaneSequence("pane-1", []PaneCapture{{Content: []string{"still running"}}})

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	waitFor(t, "metadata reconciliation", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.Health == WorkerHealthEscalated
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker missing after startup reconciliation")
	}
	if got, want := worker.Health, WorkerHealthEscalated; got != want {
		t.Fatalf("worker.Health = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventWorkerEscalated), 0; got != want {
		t.Fatalf("worker escalated events = %d, want %d", got, want)
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
