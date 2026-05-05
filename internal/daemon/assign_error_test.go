package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type assignStateStub struct {
	*fakeState

	taskByIssueErr  error
	claimTaskErr    error
	claimWorkerErr  error
	tasksByPaneErr  error
	putTaskErrs     []error
	putWorkerErr    error
	deleteWorkerErr error
}

func (s *assignStateStub) TaskByIssue(ctx context.Context, project, issue string) (Task, error) {
	if s.taskByIssueErr != nil {
		return Task{}, s.taskByIssueErr
	}
	return s.fakeState.TaskByIssue(ctx, project, issue)
}

func (s *assignStateStub) ClaimTask(ctx context.Context, task Task) (*Task, error) {
	if s.claimTaskErr != nil {
		return nil, s.claimTaskErr
	}
	return s.fakeState.ClaimTask(ctx, task)
}

func (s *assignStateStub) ClaimWorker(ctx context.Context, worker Worker) (Worker, error) {
	if s.claimWorkerErr != nil {
		return Worker{}, s.claimWorkerErr
	}
	return s.fakeState.ClaimWorker(ctx, worker)
}

func (s *assignStateStub) TasksByPane(ctx context.Context, project, paneID string) ([]Task, error) {
	if s.tasksByPaneErr != nil {
		return nil, s.tasksByPaneErr
	}
	return s.fakeState.TasksByPane(ctx, project, paneID)
}

func (s *assignStateStub) PutTask(ctx context.Context, task Task) error {
	if len(s.putTaskErrs) > 0 {
		err := s.putTaskErrs[0]
		s.putTaskErrs = s.putTaskErrs[1:]
		if err != nil {
			return err
		}
	}
	return s.fakeState.PutTask(ctx, task)
}

func (s *assignStateStub) PutWorker(ctx context.Context, worker Worker) error {
	if s.putWorkerErr != nil {
		return s.putWorkerErr
	}
	return s.fakeState.PutWorker(ctx, worker)
}

func (s *assignStateStub) DeleteWorker(ctx context.Context, project, paneID string) error {
	if s.deleteWorkerErr != nil {
		return s.deleteWorkerErr
	}
	return s.fakeState.DeleteWorker(ctx, project, paneID)
}

type assignAmuxStub struct {
	*fakeAmux

	spawnErr       error
	setMetadataErr error
}

func (a *assignAmuxStub) Spawn(ctx context.Context, req SpawnRequest) (Pane, error) {
	if a.spawnErr != nil {
		return Pane{}, a.spawnErr
	}
	return a.fakeAmux.Spawn(ctx, req)
}

func (a *assignAmuxStub) SetMetadata(ctx context.Context, paneID string, metadata map[string]string) error {
	if a.setMetadataErr != nil {
		return a.setMetadataErr
	}
	return a.fakeAmux.SetMetadata(ctx, paneID, metadata)
}

func requireAssignFailureEvent(t *testing.T, deps *testDeps, wantMessage string) Event {
	t.Helper()

	if got, want := deps.events.countType(EventTaskAssignFailed), 1; got != want {
		t.Fatalf("assign failure events = %d, want %d", got, want)
	}

	event, ok := deps.events.lastEventOfType(EventTaskAssignFailed)
	if !ok {
		t.Fatal("missing task.assign_failed event")
	}
	if !strings.Contains(event.Message, wantMessage) {
		t.Fatalf("assign failure message = %q, want substring %q", event.Message, wantMessage)
	}
	return event
}

func TestAssignUsesAgentProfileArgumentWhenConfigNameIsBlank(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.config.profiles["codex"] = AgentProfile{
		StartCommand:      "codex",
		PostmortemEnabled: true,
		StuckTimeout:      5,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   3,
	}

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-892", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	waitFor(t, "task registration", func() bool {
		task, ok := deps.state.task("LAB-892")
		return ok && task.Status == TaskStatusActive
	})

	task, ok := deps.state.task("LAB-892")
	if !ok {
		t.Fatal("task not stored in state")
	}
	if got, want := task.AgentProfile, "codex"; got != want {
		t.Fatalf("task.AgentProfile = %q, want %q", got, want)
	}
}

func TestAssignAdditionalErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*testDeps, *assignStateStub, *assignAmuxStub)
		wantErr string
		assert  func(*testing.T, *testDeps)
	}{
		{
			name: "task lookup error",
			setup: func(deps *testDeps, state *assignStateStub, _ *assignAmuxStub) {
				state.taskByIssueErr = errors.New("db unavailable")
			},
			wantErr: "load task LAB-892",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				if got := deps.pool.acquireCallCount(); got != 0 {
					t.Fatalf("pool acquire calls = %d, want 0", got)
				}
				requireAssignFailureEvent(t, deps, "load task LAB-892: db unavailable")
			},
		},
		{
			name: "open pr lookup error",
			setup: func(deps *testDeps, _ *assignStateStub, _ *assignAmuxStub) {
				deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-892", "--state", "open", "--json", "number"}, ``, errors.New("gh failed"))
			},
			wantErr: "check open PRs for LAB-892",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				if got := deps.pool.acquireCallCount(); got != 0 {
					t.Fatalf("pool acquire calls = %d, want 0", got)
				}
				requireAssignFailureEvent(t, deps, "check open PRs for LAB-892: gh failed")
			},
		},
		{
			name: "claim task error",
			setup: func(_ *testDeps, state *assignStateStub, _ *assignAmuxStub) {
				state.claimTaskErr = errors.New("claim failed")
			},
			wantErr: "claim failed",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				if got := deps.pool.acquireCallCount(); got != 0 {
					t.Fatalf("pool acquire calls = %d, want 0", got)
				}
				requireAssignFailureEvent(t, deps, "claim failed")
			},
		},
		{
			name: "claim worker error",
			setup: func(_ *testDeps, state *assignStateStub, _ *assignAmuxStub) {
				state.claimWorkerErr = errors.New("worker claim failed")
			},
			wantErr: "claim worker",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				if got := deps.pool.acquireCallCount(); got != 0 {
					t.Fatalf("pool acquire calls = %d, want 0", got)
				}
				requireAssignFailureEvent(t, deps, "claim worker: worker claim failed")
			},
		},
		{
			name: "acquire clone error",
			setup: func(deps *testDeps, _ *assignStateStub, _ *assignAmuxStub) {
				deps.pool.acquired = map[string]bool{deps.pool.clone.Path: true}
			},
			wantErr: "acquire clone",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				if _, ok := deps.state.task("LAB-892"); ok {
					t.Fatal("task stored despite acquire rollback")
				}
			},
		},
		{
			name: "prepare clone error",
			setup: func(deps *testDeps, _ *assignStateStub, _ *assignAmuxStub) {
				deps.commands.queue("git", []string{"checkout", "--detach", "origin/main"}, ``, errors.New("checkout failed"))
			},
			wantErr: "prepare clone",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				if got := deps.pool.releasedClones(); len(got) != 1 {
					t.Fatalf("released clones = %#v, want 1 released clone", got)
				}
				event := requireAssignFailureEvent(t, deps, "prepare clone: checkout failed")
				if got, want := event.WorkerID, "worker-01"; got != want {
					t.Fatalf("assign failure worker = %q, want %q", got, want)
				}
				if got, want := event.ClonePath, deps.pool.clone.Path; got != want {
					t.Fatalf("assign failure clone path = %q, want %q", got, want)
				}
			},
		},
		{
			name: "spawn error",
			setup: func(_ *testDeps, _ *assignStateStub, amux *assignAmuxStub) {
				amux.spawnErr = errors.New("spawn failed")
			},
			wantErr: "spawn pane",
		},
		{
			name: "assignment metadata error",
			setup: func(_ *testDeps, state *assignStateStub, _ *assignAmuxStub) {
				state.tasksByPaneErr = errors.New("history failed")
			},
			wantErr: "build pane metadata",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				event := requireAssignFailureEvent(t, deps, "build pane metadata: load pane task history: history failed")
				if got, want := event.PaneID, "pane-1"; got != want {
					t.Fatalf("assign failure pane id = %q, want %q", got, want)
				}
			},
		},
		{
			name: "set metadata error",
			setup: func(_ *testDeps, _ *assignStateStub, amux *assignAmuxStub) {
				amux.setMetadataErr = errors.New("metadata failed")
			},
			wantErr: "set pane metadata",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				event := requireAssignFailureEvent(t, deps, "set pane metadata: metadata failed")
				if got, want := event.PaneID, "pane-1"; got != want {
					t.Fatalf("assign failure pane id = %q, want %q", got, want)
				}
			},
		},
		{
			name: "store pending task error",
			setup: func(_ *testDeps, state *assignStateStub, _ *assignAmuxStub) {
				state.putTaskErrs = []error{errors.New("put task failed")}
			},
			wantErr: "store pending task",
		},
		{
			name: "store pending worker error",
			setup: func(_ *testDeps, state *assignStateStub, _ *assignAmuxStub) {
				state.putWorkerErr = errors.New("put worker failed")
			},
			wantErr: "store pending worker",
		},
		{
			name: "enter send error",
			setup: func(_ *testDeps, _ *assignStateStub, amux *assignAmuxStub) {
				amux.sendKeysResults = []error{nil, errors.New("enter failed")}
			},
			wantErr: "send prompt: enter failed",
		},
		{
			name: "final store task error preserves adopted pr number",
			setup: func(deps *testDeps, state *assignStateStub, _ *assignAmuxStub) {
				deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-892", "--state", "open", "--json", "number"}, `[{"number":42}]`, nil)
				state.putTaskErrs = []error{nil, errors.New("store task failed")}
			},
			wantErr: "store task",
			assert: func(t *testing.T, deps *testDeps) {
				t.Helper()
				event := requireAssignFailureEvent(t, deps, "store task failed")
				if got, want := event.PRNumber, 42; got != want {
					t.Fatalf("assign failure PRNumber = %d, want %d", got, want)
				}
			},
		},
		{
			name: "ignore delete worker rollback error",
			setup: func(_ *testDeps, state *assignStateStub, amux *assignAmuxStub) {
				state.putTaskErrs = []error{errors.New("put task failed")}
				state.deleteWorkerErr = errors.New("delete worker failed")
				amux.setMetadataErr = nil
			},
			wantErr: "store pending task",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			deps.tickers.enqueue(newFakeTicker(), newFakeTicker())

			state := &assignStateStub{fakeState: deps.state}
			amux := &assignAmuxStub{fakeAmux: deps.amux}
			if tt.setup != nil {
				tt.setup(deps, state, amux)
			}

			d := deps.newDaemon(t)
			d.state = state
			d.amux = amux

			ctx := context.Background()
			if err := d.Start(ctx); err != nil {
				t.Fatalf("Start() error = %v", err)
			}
			t.Cleanup(func() {
				_ = d.Stop(context.Background())
			})

			err := d.Assign(ctx, "LAB-892", "Implement daemon core", "codex")
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Assign() error = %v, want substring %q", err, tt.wantErr)
			}

			if tt.assert != nil {
				tt.assert(t, deps)
			}
		})
	}
}
