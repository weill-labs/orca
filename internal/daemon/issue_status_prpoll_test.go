package daemon

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/weill-labs/orca/internal/linear"
)

func TestPRMergeCleanupSkipsEntityNotFoundDoneUpdate(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.config.profiles["codex"] = AgentProfile{
		Name:              "codex",
		StartCommand:      "codex --yolo",
		ResumeSequence:    []string{"codex --yolo resume", "Enter", "."},
		PostmortemEnabled: false,
		StuckTimeout:      5 * time.Minute,
		NudgeCommand:      "Enter",
		MaxNudgeRetries:   3,
	}
	deps.amux.rejectCanceledContext = true
	deps.pool.rejectCanceledContext = true
	deps.state.rejectCanceledContext = true
	deps.issueTracker.errors = map[string]error{
		IssueStateDone: fmt.Errorf("lookup issue %s: %w", "fix-resume-sequence", linear.ErrEntityNotFound),
	}
	deps.commands.queue("gh", []string{"pr", "list", "--head", "fix-resume-sequence", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "fix-resume-sequence", "--json", "number"}, `[{"number":42}]`, nil)
	deps.commands.queue("gh", []string{"pr", "view", "42", "--json", "mergedAt"}, `{"mergedAt":"2026-04-02T12:00:00Z"}`, nil)

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "fix-resume-sequence", "Implement daemon core", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}

	prTicker.tick(deps.clock.Now())
	waitFor(t, "task completion after merge despite missing Linear issue", func() bool {
		task, ok := deps.state.task("fix-resume-sequence")
		return ok && task.Status == TaskStatusDone
	})

	if _, ok := deps.state.worker("pane-1"); ok {
		t.Fatal("worker still present after merge cleanup")
	}
	if got, want := deps.pool.releasedClones(), []Clone{{
		Name:          deps.pool.clone.Name,
		Path:          deps.pool.clone.Path,
		CurrentBranch: "fix-resume-sequence",
		AssignedTask:  "fix-resume-sequence",
	}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("released clones = %#v, want %#v", got, want)
	}
	if got, want := deps.issueTracker.statuses(), []issueStatusUpdate{
		{Issue: "fix-resume-sequence", State: IssueStateInProgress},
		{Issue: "fix-resume-sequence", State: IssueStateDone},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("issue tracker statuses = %#v, want %#v", got, want)
	}
	if event, ok := deps.events.lastEventOfType(EventPRMerged); !ok {
		t.Fatal("missing PR merged event")
	} else if got, want := event.Message, "pull request merged"; got != want {
		t.Fatalf("PR merged event message = %q, want %q", got, want)
	}
	if event, ok := deps.events.lastEventOfType(EventIssueStatusSkipped); !ok {
		t.Fatal("missing issue status skipped event")
	} else {
		if got, want := event.Issue, "fix-resume-sequence"; got != want {
			t.Fatalf("issue status skipped event issue = %q, want %q", got, want)
		}
		if !strings.Contains(event.Message, IssueStateDone) {
			t.Fatalf("issue status skipped event message = %q, want done state context", event.Message)
		}
	}
	if got := deps.amux.killCalls; len(got) != 0 {
		t.Fatalf("kill calls = %#v, want none", got)
	}
}
