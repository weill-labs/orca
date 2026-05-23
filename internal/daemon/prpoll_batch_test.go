package daemon

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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

