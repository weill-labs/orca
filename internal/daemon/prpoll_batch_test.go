package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestPRPollBatchesTerminalStateGraphQLAcrossActivePRs(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-904-A", "pane-1", 41)
	seedTaskMonitorAssignment(t, deps, "LAB-904-B", "pane-2", 42)
	seedTaskMonitorAssignment(t, deps, "LAB-904-C", "pane-3", 43)

	refs := []githubPRTerminalStateRef{
		{Key: prTerminalStateKey{Project: "/tmp/project", PRNumber: 41}, Owner: "weill-labs", Repo: "orca", PRNumber: 41},
		{Key: prTerminalStateKey{Project: "/tmp/project", PRNumber: 42}, Owner: "weill-labs", Repo: "orca", PRNumber: 42},
		{Key: prTerminalStateKey{Project: "/tmp/project", PRNumber: 43}, Owner: "weill-labs", Repo: "orca", PRNumber: 43},
	}
	deps.commands.queue("gh", prTerminalStateGraphQLArgs(refs), `{
		"data": {
			"pr0": {"pullRequest": {"number": 41, "merged": true, "mergedAt": "2026-04-02T12:00:00Z", "state": "MERGED"}},
			"pr1": {"pullRequest": {"number": 42, "merged": false, "mergedAt": null, "state": "OPEN"}},
			"pr2": {"pullRequest": {"number": 43, "merged": false, "mergedAt": null, "state": "CLOSED"}}
		}
	}`, nil)
	deps.commands.queue("gh", []string{"pr", "checks", "42", "--json", "bucket"}, `[{"bucket":"pending"}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prMergeableJSONFields}, ``, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prReviewJSONFields}, ``, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			if projectDir != "/tmp/project" {
				t.Fatalf("DetectOrigin(%q), want /tmp/project", projectDir)
			}
			return "https://github.com/weill-labs/orca.git", nil
		}
	})
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	d.runPollTick(context.Background())

	taskA, ok := deps.state.task("LAB-904-A")
	if !ok {
		t.Fatal("LAB-904-A task missing")
	}
	if got, want := taskA.Status, TaskStatusDone; got != want {
		t.Fatalf("LAB-904-A status = %q, want %q", got, want)
	}
	taskB, ok := deps.state.task("LAB-904-B")
	if !ok {
		t.Fatal("LAB-904-B task missing")
	}
	if got, want := taskB.Status, TaskStatusActive; got != want {
		t.Fatalf("LAB-904-B status = %q, want %q", got, want)
	}
	workerB, ok := deps.state.worker("pane-2")
	if !ok {
		t.Fatal("LAB-904-B worker missing")
	}
	if got, want := workerB.LastCIState, ciStatePending; got != want {
		t.Fatalf("LAB-904-B LastCIState = %q, want %q", got, want)
	}
	taskC, ok := deps.state.task("LAB-904-C")
	if !ok {
		t.Fatal("LAB-904-C task missing")
	}
	if got, want := taskC.Status, TaskStatusFailed; got != want {
		t.Fatalf("LAB-904-C status = %q, want %q", got, want)
	}
	if got, want := deps.events.countType(EventPRMerged), 1; got != want {
		t.Fatalf("merged event count = %d, want %d", got, want)
	}
	if got, want := deps.events.countType(EventPRClosed), 1; got != want {
		t.Fatalf("closed event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", prTerminalStateGraphQLArgs(refs)), 1; got != want {
		t.Fatalf("GraphQL terminal state call count = %d, want %d", got, want)
	}
	for _, prNumber := range []int{41, 42, 43} {
		args := []string{"pr", "view", fmt.Sprintf("%d", prNumber), "--json", prSnapshotJSONFields}
		if got := deps.commands.countCalls("gh", args); got != 0 {
			t.Fatalf("per-PR terminal state call count for #%d = %d, want 0", prNumber, got)
		}
	}
}

func TestPRPollFallsBackToPerPRTerminalStateWhenGraphQLFails(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-904-A", "pane-1", 41)
	seedTaskMonitorAssignment(t, deps, "LAB-904-B", "pane-2", 42)

	refs := []githubPRTerminalStateRef{
		{Key: prTerminalStateKey{Project: "/tmp/project", PRNumber: 41}, Owner: "weill-labs", Repo: "orca", PRNumber: 41},
		{Key: prTerminalStateKey{Project: "/tmp/project", PRNumber: 42}, Owner: "weill-labs", Repo: "orca", PRNumber: 42},
	}
	deps.commands.queue("gh", prTerminalStateGraphQLArgs(refs), `{"errors":[{"message":"GraphQL unavailable"}]}`, errors.New("gh graphql failed"))
	deps.commands.queue("gh", []string{"pr", "view", "41", "--json", prSnapshotJSONFields}, `{"mergedAt":"2026-04-02T12:00:00Z","state":"MERGED"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":"2026-04-02T12:01:00Z","state":"MERGED"}`, nil)

	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(string) (string, error) {
			return "https://github.com/weill-labs/orca.git", nil
		}
	})
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	d.runPollTick(context.Background())

	if got, want := deps.events.countType(EventPRMerged), 2; got != want {
		t.Fatalf("merged event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", prTerminalStateGraphQLArgs(refs)), 1; got != want {
		t.Fatalf("GraphQL terminal state call count = %d, want %d", got, want)
	}
	gotPerPR := []int{
		deps.commands.countCalls("gh", []string{"pr", "view", "41", "--json", prSnapshotJSONFields}),
		deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}),
	}
	if want := []int{1, 1}; !reflect.DeepEqual(gotPerPR, want) {
		t.Fatalf("per-PR terminal state calls = %#v, want %#v", gotPerPR, want)
	}
}

func TestPRPollSkipsUnbatchableOriginsWithoutAbortingBatch(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	seedTaskMonitorAssignment(t, deps, "LAB-904-A", "pane-1", 41)
	seedTaskMonitorAssignment(t, deps, "LAB-904-B", "pane-2", 42)
	seedTaskMonitorAssignment(t, deps, "LAB-904-C", "pane-3", 43)
	setTaskPRRepoForTest(t, deps, "LAB-904-B", "/tmp/gitlab")
	setTaskPRRepoForTest(t, deps, "LAB-904-C", "/tmp/broken-origin")

	refs := []githubPRTerminalStateRef{
		{Key: prTerminalStateKey{Project: "/tmp/project", PRNumber: 41}, Owner: "weill-labs", Repo: "orca", PRNumber: 41},
	}
	deps.commands.queue("gh", prTerminalStateGraphQLArgs(refs), `{
		"data": {
			"pr0": {"pullRequest": {"number": 41, "merged": true, "mergedAt": "2026-04-02T12:00:00Z", "state": "MERGED"}}
		}
	}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}, `{"mergedAt":"2026-04-02T12:01:00Z","state":"MERGED"}`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "43", "--json", prSnapshotJSONFields}, `{"mergedAt":"2026-04-02T12:02:00Z","state":"MERGED"}`, nil)

	var logMu sync.Mutex
	var logs []string
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.DetectOrigin = func(projectDir string) (string, error) {
			switch projectDir {
			case "/tmp/project":
				return "https://github.com/weill-labs/orca.git", nil
			case "/tmp/gitlab":
				return "https://gitlab.com/weill-labs/orca.git", nil
			case "/tmp/broken-origin":
				return "", errors.New("origin not found")
			default:
				t.Fatalf("DetectOrigin(%q) called for unexpected project", projectDir)
				return "", nil
			}
		}
		opts.Logf = func(format string, args ...any) {
			logMu.Lock()
			defer logMu.Unlock()
			logs = append(logs, fmt.Sprintf(format, args...))
		}
	})
	t.Cleanup(func() {
		d.stopAllTaskMonitors(true)
	})

	d.runPollTick(context.Background())

	if got, want := deps.events.countType(EventPRMerged), 3; got != want {
		t.Fatalf("merged event count = %d, want %d", got, want)
	}
	if got, want := deps.commands.countCalls("gh", prTerminalStateGraphQLArgs(refs)), 1; got != want {
		t.Fatalf("GraphQL terminal state call count = %d, want %d", got, want)
	}
	gotPerPR := []int{
		deps.commands.countCalls("gh", []string{"pr", "view", "41", "--json", prSnapshotJSONFields}),
		deps.commands.countCalls("gh", []string{"pr", "view", "42", "--json", prSnapshotJSONFields}),
		deps.commands.countCalls("gh", []string{"pr", "view", "43", "--json", prSnapshotJSONFields}),
	}
	if want := []int{0, 1, 1}; !reflect.DeepEqual(gotPerPR, want) {
		t.Fatalf("per-PR terminal state calls = %#v, want %#v", gotPerPR, want)
	}

	logMu.Lock()
	gotLogs := strings.Join(logs, "\n")
	logMu.Unlock()
	for _, want := range []string{
		"origin for /tmp/gitlab is not a GitHub repository",
		"detect origin for /tmp/broken-origin: origin not found",
	} {
		if !strings.Contains(gotLogs, want) {
			t.Fatalf("batch skip logs = %q, want substring %q", gotLogs, want)
		}
	}
}

func TestLookupBatchedPRTerminalStatesReturnsNilForNoRefs(t *testing.T) {
	t.Parallel()

	d := &Daemon{}
	got, err := d.lookupBatchedPRTerminalStates(context.Background(), []ActiveAssignment{
		{Task: Task{Project: "/tmp/project", State: TaskStatePRDetected, PRNumber: 41}},
	})
	if err != nil {
		t.Fatalf("lookupBatchedPRTerminalStates() error = %v", err)
	}
	if got != nil {
		t.Fatalf("lookupBatchedPRTerminalStates() = %#v, want nil", got)
	}
}

func TestLookupBatchedPRTerminalStatesNoBatchClient(t *testing.T) {
	t.Parallel()

	d := &Daemon{
		project: "/tmp/project",
		github:  staticGitHubClient{},
		detectOrigin: func(projectDir string) (string, error) {
			if projectDir != "/tmp/project" {
				t.Fatalf("DetectOrigin(%q), want /tmp/project", projectDir)
			}
			return "https://github.com/weill-labs/orca.git", nil
		},
	}
	got, err := d.lookupBatchedPRTerminalStates(context.Background(), []ActiveAssignment{
		{Task: Task{Project: "/tmp/project", State: TaskStatePRDetected, PRNumber: 41}},
	})
	if err != nil {
		t.Fatalf("lookupBatchedPRTerminalStates() error = %v", err)
	}
	if got != nil {
		t.Fatalf("lookupBatchedPRTerminalStates() = %#v, want nil", got)
	}
}

func TestPRTerminalStateRefsReturnsNilWhenNoTasksHavePRs(t *testing.T) {
	t.Parallel()

	d := &Daemon{
		detectOrigin: func(projectDir string) (string, error) {
			t.Fatalf("DetectOrigin(%q) should not be called", projectDir)
			return "", nil
		},
	}
	got, err := d.prTerminalStateRefs([]ActiveAssignment{
		{Task: Task{Project: "/tmp/project", State: TaskStatePRDetected}},
	})
	if err != nil {
		t.Fatalf("prTerminalStateRefs() error = %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("prTerminalStateRefs() = %#v, want empty", got)
	}
}

func TestGitHubOwnerRepoFromOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		origin    string
		wantOwner string
		wantRepo  string
		wantOK    bool
	}{
		{
			name:      "https github",
			origin:    "https://github.com/weill-labs/orca.git",
			wantOwner: "weill-labs",
			wantRepo:  "orca",
			wantOK:    true,
		},
		{
			name:      "ssh scp syntax",
			origin:    "git@github.com:weill-labs/orca.git",
			wantOwner: "weill-labs",
			wantRepo:  "orca",
			wantOK:    true,
		},
		{
			name:      "plain github path",
			origin:    "github.com/weill-labs/orca",
			wantOwner: "weill-labs",
			wantRepo:  "orca",
			wantOK:    true,
		},
		{
			name:   "non github host",
			origin: "https://gitlab.com/weill-labs/orca.git",
		},
		{
			name:   "malformed github path",
			origin: "https://github.com/weill-labs",
		},
		{
			name:   "nested repo path",
			origin: "https://github.com/weill-labs/orca/extra.git",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotOwner, gotRepo, gotOK := githubOwnerRepoFromOrigin(tt.origin)
			if gotOwner != tt.wantOwner || gotRepo != tt.wantRepo || gotOK != tt.wantOK {
				t.Fatalf("githubOwnerRepoFromOrigin(%q) = %q, %q, %t; want %q, %q, %t", tt.origin, gotOwner, gotRepo, gotOK, tt.wantOwner, tt.wantRepo, tt.wantOK)
			}
		})
	}
}

func setTaskPRRepoForTest(t *testing.T, deps *testDeps, issue, prRepo string) {
	t.Helper()

	task, ok := deps.state.task(issue)
	if !ok {
		t.Fatalf("task %q not found", issue)
	}
	task.PRRepo = prRepo
	deps.state.putTaskForTest(task)
}
