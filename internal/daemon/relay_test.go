package daemon

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDaemonRelayEventPullRequestReviewTriggersImmediateReviewPoll(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found")
	}
	worker.LastActivityAt = deps.clock.Now().Add(-time.Minute)
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "reviews,reviewDecision,comments"}, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("reviewer", "CHANGES_REQUESTED", "Please add tests."),
	}, nil), nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:       "evt-1",
		Type:     "pull_request_review",
		Repo:     "weill-labs/orca",
		PRNumber: 42,
	})

	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after relay review poll")
	}
	if got, want := worker.LastReviewCount, 1; got != want {
		t.Fatalf("worker.LastReviewCount = %d, want %d", got, want)
	}
	if got, want := worker.ReviewNudgeCount, 1; got != want {
		t.Fatalf("worker.ReviewNudgeCount = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedReview), 1; got != want {
		t.Fatalf("review nudge event count = %d, want %d", got, want)
	}
}

func TestDaemonRelayEventCheckRunTriggersImmediateCIPoll(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "git@github.com:weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:       "evt-2",
		Type:     "check_run",
		Repo:     "github.com/weill-labs/orca",
		PRNumber: 42,
	})

	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after relay ci poll")
	}
	if got, want := worker.LastCIState, ciStatePending; got != want {
		t.Fatalf("worker.LastCIState = %q, want %q", got, want)
	}
}

func TestDaemonRelayEventPullRequestOpenedSetsPRNumber(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	now := deps.clock.Now()
	deps.state.putTaskForTest(Task{
		Project:      "/tmp/project",
		Issue:        "LAB-1258",
		Status:       TaskStatusActive,
		Prompt:       "Handle relay opened event",
		PaneID:       "pane-1",
		PaneName:     "pane-1",
		CloneName:    "clone-LAB-1258",
		ClonePath:    "/tmp/LAB-1258",
		Branch:       "feature/relay-opened",
		AgentProfile: "codex",
		CreatedAt:    now,
		UpdatedAt:    now,
	})
	if err := deps.state.PutWorker(context.Background(), Worker{
		Project:        "/tmp/project",
		PaneID:         "pane-1",
		PaneName:       "pane-1",
		Issue:          "LAB-1258",
		ClonePath:      "/tmp/LAB-1258",
		AgentProfile:   "codex",
		Health:         WorkerHealthHealthy,
		LastCapture:    defaultCodexReadyOutput(),
		LastActivityAt: now,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:        "evt-open-1",
		EventType: "pull_request",
		Repo:      "weill-labs/orca",
		PRNumber:  42,
		PayloadSummary: map[string]any{
			"action":   "opened",
			"head_ref": "feature/relay-opened",
		},
	})

	task, ok := deps.state.task("LAB-1258")
	if !ok {
		t.Fatal("task not found after relay opened event")
	}
	if got, want := task.PRNumber, 42; got != want {
		t.Fatalf("task.PRNumber = %d, want %d", got, want)
	}
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after relay opened event")
	}
	if got, want := worker.LastPRNumber, 42; got != want {
		t.Fatalf("worker.LastPRNumber = %d, want %d", got, want)
	}
	if got, want := worker.LastPushAt, now; !got.Equal(want) {
		t.Fatalf("worker.LastPushAt = %v, want %v", got, want)
	}
	if got, want := worker.LastPRPollAt, now; !got.Equal(want) {
		t.Fatalf("worker.LastPRPollAt = %v, want %v", got, want)
	}
	if got, want := deps.events.countType(EventPRDetected), 1; got != want {
		t.Fatalf("detected event count = %d, want %d", got, want)
	}
}

func TestDaemonRelayEventPullRequestSynchronizeResetsFastPollWindow(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found")
	}
	worker.LastPushAt = deps.clock.Now().Add(-40 * time.Minute)
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:        "evt-sync-1",
		EventType: "pull_request",
		Repo:      "weill-labs/orca",
		PRNumber:  42,
		PayloadSummary: map[string]any{
			"action":   "synchronize",
			"head_ref": "LAB-1166",
		},
	})

	worker, ok = deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found after relay synchronize event")
	}
	if got, want := worker.LastPushAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("worker.LastPushAt = %v, want %v", got, want)
	}
	if got, want := worker.LastPRPollAt, deps.clock.Now(); !got.Equal(want) {
		t.Fatalf("worker.LastPRPollAt = %v, want %v", got, want)
	}
	if got, want := worker.LastCIState, ciStatePending; got != want {
		t.Fatalf("worker.LastCIState = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventPRDetected), 0; got != want {
		t.Fatalf("detected event count = %d, want %d", got, want)
	}
}

func TestDaemonRelayEventPullRequestClosedMergedCompletesTask(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-13T12:00:00Z"}`, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:        "evt-3",
		EventType: "pull_request",
		Repo:      "weill-labs/orca",
		PRNumber:  42,
		PayloadSummary: map[string]any{
			"action": "closed",
			"merged": true,
		},
	})

	task, ok := deps.state.task("LAB-1166")
	if !ok {
		t.Fatal("task not found after relay merge poll")
	}
	if got, want := task.Status, TaskStatusDone; got != want {
		t.Fatalf("task.Status = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventPRMerged), 1; got != want {
		t.Fatalf("merged event count = %d, want %d", got, want)
	}
}

func TestDaemonRelayEventPullRequestMergeRefreshesSiblingPRConflicts(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 41)
	seedTaskMonitorAssignment(t, deps, "LAB-1255", "pane-2", 42)

	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", "mergedAt"}, `{"mergedAt":"2026-04-13T12:00:00Z"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"UNKNOWN","mergeStateStatus":"CLEAN"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"UNKNOWN","mergeStateStatus":"CLEAN"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, `{"mergeable":"CONFLICTING","mergeStateStatus":"DIRTY"}`, nil)

	var (
		sleepMu sync.Mutex
		sleeps  []time.Duration
	)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Sleep = func(ctx context.Context, delay time.Duration) error {
			sleepMu.Lock()
			sleeps = append(sleeps, delay)
			sleepMu.Unlock()
			return nil
		}
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:         "evt-merge-1",
		Type:       "pull_request",
		Action:     "closed",
		Repo:       "weill-labs/orca",
		PRNumber:   41,
		Merged:     true,
		BaseBranch: "main",
	})

	waitFor(t, "relay merge conflict refresh", func() bool {
		task, ok := deps.state.task("LAB-1166")
		if !ok || task.Status != TaskStatusDone {
			return false
		}
		worker, ok := deps.state.worker("pane-2")
		return ok && worker.LastMergeableState == "CONFLICTING" && deps.events.countType(EventWorkerNudgedConflict) == 1
	})

	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 3; got != want {
		t.Fatalf("mergeable refresh call count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "41", "--json", prMergeableJSONFields}), 0; got != want {
		t.Fatalf("merged PR mergeable refresh calls = %d, want %d", got, want)
	}

	sleepMu.Lock()
	gotSleeps := append([]time.Duration(nil), sleeps...)
	sleepMu.Unlock()
	if want := []time.Duration{5 * time.Second, 5 * time.Second, 5 * time.Second}; !reflect.DeepEqual(gotSleeps, want) {
		t.Fatalf("sleep delays = %#v, want %#v", gotSleeps, want)
	}
}

func TestDaemonRelayEventPullRequestMergeSkipsSiblingConflictRefreshOnBaseBranchMismatch(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 41)
	seedTaskMonitorAssignment(t, deps, "LAB-1255", "pane-2", 42)

	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", "mergedAt"}, `{"mergedAt":"2026-04-13T12:00:00Z"}`, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})

	d.handleRelayEvent(context.Background(), relayEventMessage{
		ID:         "evt-merge-2",
		Type:       "pull_request",
		Action:     "closed",
		Repo:       "weill-labs/orca",
		PRNumber:   41,
		Merged:     true,
		BaseBranch: "release/2026-04",
	})

	waitFor(t, "relay merge detection without sibling refresh", func() bool {
		task, ok := deps.state.task("LAB-1166")
		return ok && task.Status == TaskStatusDone
	})

	if got, want := deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}), 0; got != want {
		t.Fatalf("mergeable refresh call count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventWorkerNudgedConflict), 0; got != want {
		t.Fatalf("conflict nudge event count = %d, want %d", got, want)
	}
}

func TestDaemonRelayConnectsAndReconnectsWithLastEventID(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1166", "pane-1", 42)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)

	server := newRelayTestServer(t)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.RelayURL = server.wsURL()
		opts.RelayToken = "relay-token"
		opts.Hostname = "devbox-01"
		opts.DetectOrigin = func(projectDir string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})
	if err := d.Start(context.Background()); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	first := server.nextConnection(t)
	if got, want := first.auth, "Bearer relay-token"; got != want {
		t.Fatalf("Authorization header = %q, want %q", got, want)
	}
	if got, want := first.identify.Type, "identify"; got != want {
		t.Fatalf("identify.Type = %q, want %q", got, want)
	}
	if got, want := first.identify.Hostname, "devbox-01"; got != want {
		t.Fatalf("identify.Hostname = %q, want %q", got, want)
	}
	if got := first.identify.LastEventID; got != "" {
		t.Fatalf("identify.LastEventID = %q, want empty", got)
	}
	if got, want := len(first.identify.Projects), 1; got != want {
		t.Fatalf("len(identify.Projects) = %d, want %d", got, want)
	}
	if got, want := first.identify.Projects[0].Path, "/tmp/project"; got != want {
		t.Fatalf("identify.Projects[0].Path = %q, want %q", got, want)
	}
	if got, want := first.identify.Projects[0].Repo, "weill-labs/orca"; got != want {
		t.Fatalf("identify.Projects[0].Repo = %q, want %q", got, want)
	}

	if err := first.conn.WriteJSON(relayEventMessage{
		ID:        "evt-9",
		EventType: "check_run",
		Repo:      "weill-labs/orca",
		PRNumber:  42,
	}); err != nil {
		t.Fatalf("WriteJSON(first event) error = %v", err)
	}

	waitFor(t, "relay event ci poll", func() bool {
		worker, ok := deps.state.worker("pane-1")
		return ok && worker.LastCIState == ciStatePending
	})

	_ = first.conn.Close()

	second := server.nextConnection(t)
	if got, want := second.identify.LastEventID, "evt-9"; got != want {
		t.Fatalf("identify.LastEventID after reconnect = %q, want %q", got, want)
	}
}

func TestDaemonRelayHealthEnqueuesPollIntervalUpdates(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	d.pollIntervalCh = make(chan time.Duration, 1)

	d.setRelayHealthy(true)
	select {
	case got := <-d.pollIntervalCh:
		if got != relayHealthyPollInterval {
			t.Fatalf("healthy poll interval = %v, want %v", got, relayHealthyPollInterval)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for healthy poll interval update")
	}

	d.setRelayHealthy(false)
	select {
	case got := <-d.pollIntervalCh:
		if got != d.pollInterval {
			t.Fatalf("fallback poll interval = %v, want %v", got, d.pollInterval)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for fallback poll interval update")
	}
}

func TestCurrentPRPollIntervalCapsRelayHealthyPollingForTrackedPRs(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-1318", "pane-1", 42)
	worker, ok := deps.state.worker("pane-1")
	if !ok {
		t.Fatal("worker not found")
	}
	worker.LastActivityAt = deps.clock.Now().Add(-45 * time.Minute)
	if err := deps.state.PutWorker(context.Background(), worker); err != nil {
		t.Fatalf("PutWorker() error = %v", err)
	}

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.PollInterval = 10 * time.Minute
	})
	d.relayHealthy.Store(true)

	if got, want := d.currentPRPollInterval(), openPRPollIntervalCap; got != want {
		t.Fatalf("currentPRPollInterval() = %v, want %v", got, want)
	}
}

func TestRelayRepoAliases(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "https remote",
			input: "https://github.com/weill-labs/orca.git",
			want:  []string{"https://github.com/weill-labs/orca", "weill-labs/orca", "github.com/weill-labs/orca"},
		},
		{
			name:  "ssh remote",
			input: "git@github.com:weill-labs/orca.git",
			want:  []string{"git@github.com:weill-labs/orca", "weill-labs/orca", "github.com/weill-labs/orca"},
		},
		{
			name:  "owner repo",
			input: "weill-labs/orca",
			want:  []string{"weill-labs/orca"},
		},
		{
			name:  "github prefix",
			input: "github.com/weill-labs/orca",
			want:  []string{"github.com/weill-labs/orca", "weill-labs/orca"},
		},
		{
			name:  "absolute path",
			input: "/tmp/project",
			want:  []string{"tmp/project"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := relayRepoAliases(tt.input); !equalStringSlices(got, tt.want) {
				t.Fatalf("relayRepoAliases(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

func TestRelayEventCheckKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		msg  relayEventMessage
		want taskMonitorCheckKind
		ok   bool
	}{
		{
			name: "pull request review triggers review poll",
			msg:  relayEventMessage{Type: "pull_request_review"},
			want: taskMonitorCheckReviewPoll,
			ok:   true,
		},
		{
			name: "issue comment triggers review poll",
			msg:  relayEventMessage{Type: "issue_comment"},
			want: taskMonitorCheckReviewPoll,
			ok:   true,
		},
		{
			name: "check suite triggers ci poll",
			msg:  relayEventMessage{Type: "check_suite"},
			want: taskMonitorCheckCIPoll,
			ok:   true,
		},
		{
			name: "check run triggers ci poll",
			msg:  relayEventMessage{Type: "check_run"},
			want: taskMonitorCheckCIPoll,
			ok:   true,
		},
		{
			name: "pull request merge event triggers merge poll",
			msg:  relayEventMessage{Type: "pull_request_merge"},
			want: taskMonitorCheckMergePoll,
			ok:   true,
		},
		{
			name: "merged pull request triggers merge poll",
			msg:  relayEventMessage{Type: "pull_request", Merged: true},
			want: taskMonitorCheckMergePoll,
			ok:   true,
		},
		{
			name: "sonar closed merged pull request triggers merge poll",
			msg: relayEventMessage{
				EventType: "pull_request",
				PayloadSummary: map[string]any{
					"action": "closed",
					"merged": true,
				},
			},
			want: taskMonitorCheckMergePoll,
			ok:   true,
		},
		{
			name: "unmerged pull request is ignored",
			msg:  relayEventMessage{Type: "pull_request"},
		},
		{
			name: "unknown event is ignored",
			msg:  relayEventMessage{Type: "push"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := relayEventCheckKind(tt.msg)
			if ok != tt.ok {
				t.Fatalf("relayEventCheckKind(%+v) ok = %t, want %t", tt.msg, ok, tt.ok)
			}
			if got != tt.want {
				t.Fatalf("relayEventCheckKind(%+v) = %v, want %v", tt.msg, got, tt.want)
			}
		})
	}
}

func TestCanonicalRelayRepo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "https remote",
			input: "https://github.com/weill-labs/orca.git",
			want:  "weill-labs/orca",
		},
		{
			name:  "ssh remote",
			input: "git@github.com:weill-labs/orca.git",
			want:  "weill-labs/orca",
		},
		{
			name:  "absolute path falls back to first alias",
			input: "/tmp/project",
			want:  "tmp/project",
		},
		{
			name:  "empty input returns empty",
			input: "   ",
			want:  "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := canonicalRelayRepo(tt.input); got != tt.want {
				t.Fatalf("canonicalRelayRepo(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestDaemonBuildRelayIdentifyMessage(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Project = "/tmp/project-a"
		opts.Hostname = "relay-host"
		opts.DetectOrigin = func(projectDir string) (string, error) {
			if projectDir == "/tmp/project-a" {
				return "https://github.com/weill-labs/project-a.git", nil
			}
			return "", errors.New("unknown project")
		}
	})

	now := deps.clock.Now()
	for _, task := range []Task{
		{Issue: "LAB-1", Project: "", Status: TaskStatusActive, CreatedAt: now, UpdatedAt: now},
		{Issue: "LAB-2", Project: "/tmp/project-a", Status: TaskStatusStarting, CreatedAt: now, UpdatedAt: now},
		{Issue: "LAB-3", Project: "/tmp/project-a", Status: TaskStatusDone, CreatedAt: now, UpdatedAt: now},
	} {
		if err := deps.state.PutTask(context.Background(), task); err != nil {
			t.Fatalf("PutTask(%s) error = %v", task.Issue, err)
		}
	}

	got := d.buildRelayIdentifyMessage(context.Background(), "  evt-9  ")
	want := relayIdentifyMessage{
		Type:        "identify",
		Hostname:    "relay-host",
		LastEventID: "evt-9",
		Projects: []relayMonitoredProject{
			{Path: "/tmp/project-a", Repo: "weill-labs/project-a"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("buildRelayIdentifyMessage() = %#v, want %#v", got, want)
	}
}

func TestRelayProjectMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		projectPath  string
		repo         string
		detectOrigin func(string) (string, error)
		want         bool
	}{
		{
			name:        "project path alias matches directly",
			projectPath: "/tmp/project",
			repo:        "tmp/project",
			want:        true,
		},
		{
			name:        "origin alias matches relay repo",
			projectPath: "/tmp/project",
			repo:        "weill-labs/orca",
			detectOrigin: func(string) (string, error) {
				return "https://github.com/weill-labs/orca.git", nil
			},
			want: true,
		},
		{
			name:        "missing detect origin cannot match remote repo",
			projectPath: "/tmp/project",
			repo:        "weill-labs/orca",
			want:        false,
		},
		{
			name:        "detect origin error does not match",
			projectPath: "/tmp/project",
			repo:        "weill-labs/orca",
			detectOrigin: func(string) (string, error) {
				return "", errors.New("origin missing")
			},
			want: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			deps := newTestDeps(t)
			d := deps.newDaemonWithOptions(t, func(opts *Options) {
				opts.DetectOrigin = tt.detectOrigin
			})

			if got := d.relayProjectMatches(tt.projectPath, tt.repo); got != tt.want {
				t.Fatalf("relayProjectMatches(%q, %q) = %t, want %t", tt.projectPath, tt.repo, got, tt.want)
			}
		})
	}
}

func TestRequestRelayReconnectClosesLiveConnection(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.RelayURL = "wss://relay.example.com/ws"
	})

	conn := &stubRelayConnection{}
	d.setRelayConn(conn)
	d.requestRelayReconnect()

	if !d.relayReconnect.Load() {
		t.Fatal("relayReconnect = false, want true")
	}
	if got, want := conn.closeCalls, 1; got != want {
		t.Fatalf("closeCalls = %d, want %d", got, want)
	}
	d.relayConnMu.Lock()
	currentConn := d.relayConn
	d.relayConnMu.Unlock()
	if currentConn != nil {
		t.Fatal("relayConn != nil after requestRelayReconnect")
	}
}

func TestRequestRelayReconnectNoopsWithoutLiveConnection(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.RelayURL = "wss://relay.example.com/ws"
	})

	d.requestRelayReconnect()
	if d.relayReconnect.Load() {
		t.Fatal("relayReconnect = true, want false when no relay connection is active")
	}
}

func TestConsumeRelayConnectionSetsReadDeadline(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)

	deadlineErr := errors.New("read deadline exceeded")
	conn := &stubRelayConnection{
		currentTime: time.Date(2500, 1, 1, 0, 0, 0, 0, time.UTC),
		readErr:     deadlineErr,
		readBlock:   make(chan struct{}),
	}

	type result struct {
		lastEventID string
		err         error
	}
	resultCh := make(chan result, 1)
	go func() {
		lastEventID, err := d.consumeRelayConnection(context.Background(), conn, "evt-1")
		resultCh <- result{lastEventID: lastEventID, err: err}
	}()

	select {
	case got := <-resultCh:
		if !errors.Is(got.err, deadlineErr) {
			t.Fatalf("consumeRelayConnection() error = %v, want %v", got.err, deadlineErr)
		}
		if got.lastEventID != "evt-1" {
			t.Fatalf("consumeRelayConnection() lastEventID = %q, want %q", got.lastEventID, "evt-1")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for consumeRelayConnection to return after read deadline")
	}

	if got, want := conn.setReadDeadlineCalls, 1; got != want {
		t.Fatalf("SetReadDeadline call count = %d, want %d", got, want)
	}
	if got := conn.lastReadDeadline; got.IsZero() {
		t.Fatal("SetReadDeadline received zero time")
	}
}

func TestEnqueuePollIntervalUpdateReplacesQueuedValue(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	d := deps.newDaemon(t)
	d.pollIntervalCh = make(chan time.Duration, 1)

	d.enqueuePollIntervalUpdate(30 * time.Second)
	d.enqueuePollIntervalUpdate(5 * time.Minute)

	select {
	case got := <-d.pollIntervalCh:
		if got != 5*time.Minute {
			t.Fatalf("queued poll interval = %v, want %v", got, 5*time.Minute)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for queued poll interval")
	}
}

type relayTestServer struct {
	server      *httptest.Server
	connections chan relayServerConnection
}

type relayServerConnection struct {
	auth     string
	identify relayIdentifyMessage
	conn     *websocket.Conn
}

func newRelayTestServer(t *testing.T) *relayTestServer {
	t.Helper()

	server := &relayTestServer{
		connections: make(chan relayServerConnection, 8),
	}
	upgrader := websocket.Upgrader{}
	server.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade() error = %v", err)
			return
		}

		var identify relayIdentifyMessage
		if err := conn.ReadJSON(&identify); err != nil {
			_ = conn.Close()
			t.Errorf("ReadJSON(identify) error = %v", err)
			return
		}

		select {
		case server.connections <- relayServerConnection{
			auth:     r.Header.Get("Authorization"),
			identify: identify,
			conn:     conn,
		}:
		case <-time.After(time.Second):
			_ = conn.Close()
			t.Errorf("timed out recording relay connection")
		}
	}))
	t.Cleanup(server.server.Close)

	return server
}

func (s *relayTestServer) wsURL() string {
	return "ws" + strings.TrimPrefix(s.server.URL, "http")
}

func (s *relayTestServer) nextConnection(t *testing.T) relayServerConnection {
	t.Helper()

	select {
	case conn := <-s.connections:
		return conn
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for relay connection")
		return relayServerConnection{}
	}
}

type stubRelayConnection struct {
	closeCalls           int
	setReadDeadlineCalls int
	lastReadDeadline     time.Time
	currentTime          time.Time
	readErr              error
	readBlock            chan struct{}
}

func (c *stubRelayConnection) ReadJSON(any) error {
	if c.readBlock != nil {
		<-c.readBlock
	}
	return c.readErr
}
func (c *stubRelayConnection) SetReadDeadline(deadline time.Time) error {
	c.setReadDeadlineCalls++
	c.lastReadDeadline = deadline
	if c.readBlock != nil && !deadline.After(c.currentTime) {
		select {
		case <-c.readBlock:
		default:
			close(c.readBlock)
		}
	}
	return nil
}
func (c *stubRelayConnection) WriteJSON(any) error { return nil }
func (c *stubRelayConnection) Close() error {
	c.closeCalls++
	return nil
}

func equalStringSlices(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
