package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestMergeQueueActorProcessAwaitingChecks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		checksOutput string
		checksErr    error
		mergeErr     error
		want         []MergeQueueUpdate
	}{
		{
			name:      "lookup error returns entry to awaiting checks",
			checksErr: errors.New("gh pr checks failed"),
			want: []MergeQueueUpdate{
				{
					Entry: MergeQueueEntry{
						Project:  "/tmp/project",
						Issue:    "LAB-689",
						PRNumber: 42,
						Status:   MergeQueueStatusAwaitingChecks,
					},
				},
			},
		},
		{
			name:         "pending checks keep entry queued for later",
			checksOutput: `[{"bucket":"pending"}]`,
			want: []MergeQueueUpdate{
				{
					Entry: MergeQueueEntry{
						Project:  "/tmp/project",
						Issue:    "LAB-689",
						PRNumber: 42,
						Status:   MergeQueueStatusAwaitingChecks,
					},
				},
			},
		},
		{
			name:         "failing checks delete entry and notify worker",
			checksOutput: `[{"bucket":"fail"}]`,
			want: []MergeQueueUpdate{
				{
					Entry: MergeQueueEntry{
						Project:  "/tmp/project",
						Issue:    "LAB-689",
						PRNumber: 42,
						Status:   MergeQueueStatusCheckingCI,
					},
					Delete:        true,
					EventType:     EventPRLandingFailed,
					EventMessage:  "required checks state is fail",
					FailurePrompt: mergeQueueChecksFailedPrompt(42),
				},
			},
		},
		{
			name:         "passing checks with merge error deletes entry and notifies worker",
			checksOutput: `[{"bucket":"pass"}]`,
			mergeErr:     errors.New("merge failed"),
			want: []MergeQueueUpdate{
				{
					Entry: MergeQueueEntry{
						Project:  "/tmp/project",
						Issue:    "LAB-689",
						PRNumber: 42,
						Status:   MergeQueueStatusMerging,
					},
				},
				{
					Entry: MergeQueueEntry{
						Project:  "/tmp/project",
						Issue:    "LAB-689",
						PRNumber: 42,
						Status:   MergeQueueStatusMerging,
					},
					Delete:        true,
					EventType:     EventPRLandingFailed,
					EventMessage:  "merge failed",
					FailurePrompt: mergeQueueMergeFailedPrompt(42),
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			commands := newFakeCommands()
			checkArgs := []string{"pr", "checks", "42", "--json", "bucket"}
			commands.queue("gh", checkArgs, tt.checksOutput, tt.checksErr)
			if tt.checksErr == nil && strings.Contains(tt.checksOutput, `"pass"`) {
				commands.queue("gh", []string{"pr", "merge", "42", "--squash"}, ``, tt.mergeErr)
			}

			updates := make(chan MergeQueueUpdate, 4)
			actor := newMergeQueueActor("/tmp/project", commands, updates)
			entry := MergeQueueEntry{
				Project:  "/tmp/project",
				Issue:    "LAB-689",
				PRNumber: 42,
				Status:   MergeQueueStatusCheckingCI,
			}

			actor.processAwaitingChecks(context.Background(), entry)

			if got, want := drainMergeQueueUpdates(updates), tt.want; len(got) != len(want) {
				t.Fatalf("len(updates) = %d, want %d (%#v)", len(got), len(want), got)
			} else {
				for i := range want {
					if got[i] != want[i] {
						t.Fatalf("update[%d] = %#v, want %#v", i, got[i], want[i])
					}
				}
			}
		})
	}
}

func TestMergeQueueActorProcessQueueNormalizesUnknownStatus(t *testing.T) {
	t.Parallel()

	updates := make(chan MergeQueueUpdate, 1)
	actor := newMergeQueueActor("/tmp/project", newFakeCommands(), updates)

	actor.processQueue(context.Background(), ProcessQueue{
		Entries: []MergeQueueEntry{{
			Project:  "/tmp/project",
			Issue:    "LAB-689",
			PRNumber: 42,
			Status:   "stalled",
		}},
	})

	got := drainMergeQueueUpdates(updates)
	want := []MergeQueueUpdate{{
		Entry: MergeQueueEntry{
			Project:  "/tmp/project",
			Issue:    "LAB-689",
			PRNumber: 42,
			Status:   MergeQueueStatusQueued,
		},
	}}
	if len(got) != len(want) {
		t.Fatalf("len(updates) = %d, want %d (%#v)", len(got), len(want), got)
	}
	if got[0] != want[0] {
		t.Fatalf("update = %#v, want %#v", got[0], want[0])
	}
}

func TestDispatchMergeQueueSkipsInFlightAndCleansInvalidEntries(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	now := deps.clock.Now()

	deps.state.mergeQueue = []MergeQueueEntry{
		{
			Project:   "/tmp/project",
			Issue:     "LAB-700",
			PRNumber:  41,
			Status:    MergeQueueStatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			Project:   "/tmp/project",
			Issue:     "LAB-701",
			PRNumber:  42,
			Status:    MergeQueueStatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			Project:   "/tmp/project",
			Issue:     "LAB-702",
			PRNumber:  43,
			Status:    MergeQueueStatusCheckingCI,
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			Project:   "/tmp/project",
			Issue:     "LAB-703",
			PRNumber:  44,
			Status:    MergeQueueStatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	for _, task := range []Task{
		{
			Project:      "/tmp/project",
			Issue:        "LAB-702",
			Status:       TaskStatusActive,
			Prompt:       "Check CI",
			PaneID:       "pane-2",
			PaneName:     "worker-2",
			CloneName:    "clone-02",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-702",
			AgentProfile: "codex",
			PRNumber:     43,
			CreatedAt:    now,
			UpdatedAt:    now,
		},
		{
			Project:      "/tmp/project",
			Issue:        "LAB-703",
			Status:       TaskStatusActive,
			Prompt:       "Queue merge",
			PaneID:       "pane-3",
			PaneName:     "worker-3",
			CloneName:    "clone-03",
			ClonePath:    deps.pool.clone.Path,
			Branch:       "LAB-703",
			AgentProfile: "codex",
			PRNumber:     44,
			CreatedAt:    now,
			UpdatedAt:    now,
		},
	} {
		deps.state.putTaskForTest(task)
		if err := deps.state.PutWorker(context.Background(), Worker{
			Project:        "/tmp/project",
			PaneID:         task.PaneID,
			PaneName:       task.PaneName,
			Issue:          task.Issue,
			ClonePath:      task.ClonePath,
			AgentProfile:   task.AgentProfile,
			Health:         WorkerHealthHealthy,
			LastActivityAt: now,
			UpdatedAt:      now,
		}); err != nil {
			t.Fatalf("PutWorker(%s) error = %v", task.Issue, err)
		}
	}

	d := deps.newDaemon(t)
	d.github = staticGitHubClient{mergedPRs: map[int]bool{41: true}}
	d.mergeQueueInbox = make(chan ProcessQueue)

	received := make(chan ProcessQueue, 1)
	go func() {
		msg := <-d.mergeQueueInbox
		received <- msg
		close(msg.Ack)
	}()

	d.dispatchMergeQueue(context.Background())

	msg := <-received
	if got, want := len(msg.Entries), 1; got != want {
		t.Fatalf("len(msg.Entries) = %d, want %d", got, want)
	}
	if got, want := msg.Entries[0].PRNumber, 44; got != want {
		t.Fatalf("msg.Entries[0].PRNumber = %d, want %d", got, want)
	}

	if entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 41); err != nil || entry != nil {
		t.Fatalf("merged entry after dispatch = %#v, %v, want nil, nil", entry, err)
	}
	if entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42); err != nil || entry != nil {
		t.Fatalf("orphaned entry after dispatch = %#v, %v, want nil, nil", entry, err)
	}
	if entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 43); err != nil || entry == nil || entry.Status != MergeQueueStatusCheckingCI {
		t.Fatalf("checking_ci entry after dispatch = %#v, %v, want checking_ci entry", entry, err)
	}

	if got, want := deps.events.countType(EventPRLandingFailed), 1; got != want {
		t.Fatalf("landing failed event count = %d, want %d", got, want)
	}
	if message := deps.events.lastMessage(EventPRLandingFailed); !strings.Contains(message, "no longer tracked by an active assignment") {
		t.Fatalf("landing failed message = %q, want missing assignment notice", message)
	}
}

func TestDispatchMergeQueueMergedEntryCompletesActiveAssignment(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	now := deps.clock.Now()
	deps.state.mergeQueue = []MergeQueueEntry{
		{
			Project:   "/tmp/project",
			Issue:     "LAB-1317",
			PRNumber:  42,
			Status:    MergeQueueStatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1317",
		Status:       TaskStatusActive,
		Prompt:       "Finish the merge queue path",
		WorkerID:     "worker-01",
		PaneID:       "pane-1",
		PaneName:     "worker-1",
		CloneName:    "clone-01",
		ClonePath:    deps.pool.clone.Path,
		Branch:       "LAB-1317",
		AgentProfile: "codex",
		PRNumber:     42,
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        "/tmp/project",
		WorkerID:       "worker-01",
		PaneID:         "pane-1",
		PaneName:       "worker-1",
		Issue:          "LAB-1317",
		ClonePath:      deps.pool.clone.Path,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastActivityAt: now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d := deps.newDaemon(t)
	d.github = staticGitHubClient{mergedPRs: map[int]bool{42: true}}
	d.mergeQueueInbox = make(chan ProcessQueue, 1)

	d.dispatchMergeQueue(context.Background())

	select {
	case msg := <-d.mergeQueueInbox:
		t.Fatalf("dispatchMergeQueue() sent %#v, want no queued work for merged PR", msg)
	default:
	}

	task, ok := deps.state.task("LAB-1317")
	if !ok {
		t.Fatal("LAB-1317 task missing after dispatch")
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := task.State, TaskStateDone; got != want {
		t.Fatalf("task.State = %q, want %q", got, want)
	}
	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker still present after merged merge queue dispatch")
	}
	entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
	if err != nil {
		t.Fatalf("MergeEntry() error = %v", err)
	}
	if entry != nil {
		t.Fatalf("MergeEntry() = %#v, want nil", entry)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{{Issue: "LAB-1317", State: IssueStateDone}}; len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if got, want := deps.events.countType(EventPRMerged), 1; got != want {
		t.Fatalf("pr merged event count = %d, want %d", got, want)
	}
	deps.events.requireTypes(t, EventPRMerged, EventWorkerPostmortem, EventTaskCompleted)
	if promptCount := deps.amux.countKey("pane-1", mergedWrapUpPrompt) + deps.amux.countKey("pane-1", mergedWrapUpPrompt+"\n"); promptCount != 1 {
		t.Fatalf("merged wrap-up prompt count = %d, want 1", promptCount)
	}
	if got, want := deps.amux.countKey("pane-1", postmortemCommand+"\n"), 1; got != want {
		t.Fatalf("postmortem prompt count = %d, want %d", got, want)
	}
}

func TestApplyMergeQueueUpdateStatusOnlyPersistenceErrorSkipsEvents(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	now := deps.clock.Now()
	deps.state.mergeQueue = []MergeQueueEntry{
		{
			Project:   "/tmp/project",
			Issue:     "LAB-689",
			PRNumber:  42,
			Status:    MergeQueueStatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}
	deps.state.updateMergeErr = errors.New("update merge entry: sqlite busy")

	d.applyMergeQueueUpdate(context.Background(), MergeQueueUpdate{
		Entry: MergeQueueEntry{
			Project:  "/tmp/project",
			Issue:    "LAB-689",
			PRNumber: 42,
			Status:   MergeQueueStatusRebasing,
		},
	})

	if got := deps.events.countType(EventPRLandingStarted); got != 0 {
		t.Fatalf("started event count = %d, want 0", got)
	}
	if got := deps.amux.countKey("pane-1", mergeQueueRebaseConflictPrompt(42)+"\n"); got != 0 {
		t.Fatalf("worker prompt count = %d, want 0", got)
	}
	entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
	if err != nil {
		t.Fatalf("MergeEntry() error = %v", err)
	}
	if entry == nil || entry.Status != MergeQueueStatusQueued {
		t.Fatalf("entry after failed status-only update = %#v, want queued entry", entry)
	}
}

func TestApplyMergeQueueUpdateEmitsEventWhenAssignmentMissing(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	d.applyMergeQueueUpdate(context.Background(), MergeQueueUpdate{
		Entry: MergeQueueEntry{
			Project:  "/tmp/project",
			Issue:    "LAB-689",
			PRNumber: 42,
			Status:   MergeQueueStatusQueued,
		},
		EventType:    EventPRLandingStarted,
		EventMessage: "processing queued PR landing",
	})

	if got, want := deps.events.countType(EventPRLandingStarted), 1; got != want {
		t.Fatalf("started event count = %d, want %d", got, want)
	}
	if message := deps.events.lastMessage(EventPRLandingStarted); message != "processing queued PR landing" {
		t.Fatalf("started event message = %q, want %q", message, "processing queued PR landing")
	}
}

func TestApplyMergeQueueUpdatesDrainsBufferedUpdates(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	now := deps.clock.Now()
	deps.state.mergeQueue = []MergeQueueEntry{
		{
			Project:   "/tmp/project",
			Issue:     "LAB-689",
			PRNumber:  42,
			Status:    MergeQueueStatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		},
		{
			Project:   "/tmp/project",
			Issue:     "LAB-690",
			PRNumber:  43,
			Status:    MergeQueueStatusQueued,
			CreatedAt: now,
			UpdatedAt: now,
		},
	}

	task := Task{
		Project:      "/tmp/project",
		Issue:        "LAB-690",
		Status:       TaskStatusActive,
		Prompt:       "Implement merge queue",
		PaneID:       "pane-1",
		PaneName:     "worker-1",
		CloneName:    "clone-01",
		ClonePath:    deps.pool.clone.Path,
		Branch:       "LAB-690",
		AgentProfile: "codex",
		PRNumber:     43,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	deps.state.putTaskForTest(task)
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        "/tmp/project",
		PaneID:         "pane-1",
		PaneName:       "worker-1",
		Issue:          "LAB-690",
		ClonePath:      deps.pool.clone.Path,
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastActivityAt: now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d.mergeQueueUpdates = make(chan MergeQueueUpdate, 2)
	d.mergeQueueUpdates <- MergeQueueUpdate{
		Entry: MergeQueueEntry{
			Project:  "/tmp/project",
			Issue:    "LAB-689",
			PRNumber: 42,
			Status:   MergeQueueStatusRebasing,
		},
	}
	d.mergeQueueUpdates <- MergeQueueUpdate{
		Entry: MergeQueueEntry{
			Project:  "/tmp/project",
			Issue:    "LAB-690",
			PRNumber: 43,
			Status:   MergeQueueStatusQueued,
		},
		EventType:    EventPRLandingStarted,
		EventMessage: "processing queued PR landing",
	}

	d.applyMergeQueueUpdates(context.Background())

	entry, err := deps.state.MergeEntry(context.Background(), "/tmp/project", 42)
	if err != nil {
		t.Fatalf("MergeEntry(42) error = %v", err)
	}
	if entry == nil || entry.Status != MergeQueueStatusRebasing {
		t.Fatalf("entry 42 after drain = %#v, want rebasing entry", entry)
	}
	if got, want := deps.events.countType(EventPRLandingStarted), 1; got != want {
		t.Fatalf("started event count = %d, want %d", got, want)
	}
	select {
	case update := <-d.mergeQueueUpdates:
		t.Fatalf("mergeQueueUpdates not drained, found %#v", update)
	default:
	}
}

func drainMergeQueueUpdates(updates <-chan MergeQueueUpdate) []MergeQueueUpdate {
	var out []MergeQueueUpdate
	for {
		select {
		case update := <-updates:
			out = append(out, update)
		default:
			return out
		}
	}
}

type staticGitHubClient struct {
	mergedPRs map[int]bool
}

func (c staticGitHubClient) lookupPRNumber(context.Context, string) (int, error) {
	return 0, nil
}

func (c staticGitHubClient) findPRByIssueID(context.Context, string) (int, string, error) {
	return 0, "", nil
}

func (c staticGitHubClient) lookupIssue(context.Context, int) (gitHubIssue, error) {
	return gitHubIssue{}, nil
}

func (c staticGitHubClient) lookupOpenPRNumber(context.Context, string) (int, error) {
	return 0, nil
}

func (c staticGitHubClient) lookupOpenOrMergedPRNumber(context.Context, string) (int, bool, error) {
	return 0, false, nil
}

func (c staticGitHubClient) lookupPRTerminalState(context.Context, int) (prTerminalState, error) {
	return prTerminalState{}, nil
}

func (c staticGitHubClient) isPRMerged(_ context.Context, prNumber int) (bool, error) {
	return c.mergedPRs[prNumber], nil
}

func (c staticGitHubClient) lookupPRReviews(context.Context, int) (prReviewPayload, bool, error) {
	return prReviewPayload{}, false, nil
}
