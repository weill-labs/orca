package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPRReviewPollingPromotesLargeReviewNudgeToFile(t *testing.T) {
	t.Parallel()

	deps := newReviewPromptFileTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1902", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1902", "--json", "number"}, `[{"number":1902}]`, nil)

	body := strings.Repeat("Greptile found a blocking issue in this path and included enough detail to overflow Codex paste handling. ", 12)
	queuePRReviewPayload(deps, 1902, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("greptile-apps", "CHANGES_REQUESTED", body),
	}, nil))

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1902", "Implement review prompt file delivery", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	expectedFullPrompt := formatBlockingReviewFeedback(1902, []prFeedback{
		{Author: "greptile-apps", Body: body},
	})
	promptFile := filepath.Join(deps.pool.clone.Path, assignmentPromptFileDir, "review-1902.md")
	expectedReference := "Read " + assignmentPromptFileDir + "/review-1902.md and address the review feedback on PR #1902, then push."

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "large review poll cycle completion")
	waitFor(t, "large review nudge reference prompt", func() bool {
		return deps.amux.countKey("pane-1", expectedReference+"\n") == 1
	})

	if strings.Contains(expectedReference, deps.pool.clone.Path) {
		t.Fatalf("reference prompt = %q, want clone-relative path", expectedReference)
	}
	if got := deps.amux.countKey("pane-1", expectedFullPrompt+"\n"); got != 0 {
		t.Fatalf("large inline review nudge count = %d, want 0", got)
	}
	content, err := os.ReadFile(promptFile)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", promptFile, err)
	}
	if got, want := string(content), expectedFullPrompt+"\n"; got != want {
		t.Fatalf("review prompt file content = %q, want %q", got, want)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{
		wrappedCodexPrompt("LAB-1902", "Implement review prompt file delivery") + "\n",
		expectedReference + "\n",
	})
}

func TestPRReviewPollingKeepsSmallReviewNudgeInline(t *testing.T) {
	t.Parallel()

	deps := newReviewPromptFileTestDeps(t)
	captureTicker := newFakeTicker()
	prTicker := newFakeTicker()
	deps.tickers.enqueue(captureTicker, prTicker)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1903", "--state", "open", "--json", "number"}, `[]`, nil)
	deps.commands.queue("gh", []string{"pr", "list", "--head", "LAB-1903", "--json", "number"}, `[{"number":1903}]`, nil)
	queuePRReviewPayload(deps, 1903, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("alice", "CHANGES_REQUESTED", "Please add tests."),
	}, nil))

	d := deps.newDaemon(t)
	ctx := context.Background()
	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	if err := d.Assign(ctx, "LAB-1903", "Implement small review nudge", "codex"); err != nil {
		t.Fatalf("Assign() error = %v", err)
	}
	makeWorkerIdleForReviewNudge(deps)

	expectedPrompt := formatBlockingReviewFeedback(1903, []prFeedback{
		{Author: "alice", Body: "Please add tests."},
	})

	tickAndWaitForHeartbeat(t, d, deps, prTicker, adaptivePRFastPollInterval, "small review poll cycle completion")
	waitFor(t, "small inline review nudge", func() bool {
		return deps.amux.countKey("pane-1", expectedPrompt+"\n") == 1
	})

	promptFile := filepath.Join(deps.pool.clone.Path, assignmentPromptFileDir, "review-1903.md")
	if _, err := os.Stat(promptFile); !os.IsNotExist(err) {
		t.Fatalf("review prompt file stat error = %v, want not exist", err)
	}
	deps.amux.requireSentKeys(t, "pane-1", []string{
		wrappedCodexPrompt("LAB-1903", "Implement small review nudge") + "\n",
		expectedPrompt + "\n",
	})
}

func TestPrepareReviewPromptForDeliveryRequiresClonePath(t *testing.T) {
	t.Parallel()

	d := &Daemon{}
	_, err := d.prepareReviewPromptForDelivery(Task{PRNumber: 42}, AgentProfile{Name: "codex"}, strings.Repeat("x", 600))
	if err == nil || !strings.Contains(err.Error(), "review prompt file requires clone path") {
		t.Fatalf("prepareReviewPromptForDelivery() error = %v, want clone path error", err)
	}
}

func TestReviewNudgePrepareFailureLeavesNudgeUnpersisted(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	body := strings.Repeat("Greptile posted a long blocking review comment. ", 20)
	queuePRReviewPayload(deps, 42, marshalReviewPayload(t, "CHANGES_REQUESTED", []prReview{
		testReview("greptile-apps", "CHANGES_REQUESTED", body),
	}, nil))

	var logs []string
	d := deps.newDaemonWithOptions(t, func(opts *Options) {
		opts.Logf = func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		}
	})
	now := deps.clock.Now()
	active := ActiveAssignment{
		Task: Task{
			Project:      "/tmp/project",
			Issue:        "LAB-1904",
			PaneID:       "pane-1",
			ClonePath:    "",
			AgentProfile: "codex",
			PRNumber:     42,
		},
		Worker: Worker{
			Project:        "/tmp/project",
			WorkerID:       "worker-01",
			PaneID:         "pane-1",
			Issue:          "LAB-1904",
			AgentProfile:   "codex",
			LastCapture:    defaultCodexReadyOutput(),
			LastActivityAt: now.Add(-11 * time.Second),
		},
	}

	update := d.checkTaskReviewPoll(context.Background(), active, AgentProfile{Name: "codex"})
	if !update.hasNudges() {
		t.Fatal("expected review nudge to be queued")
	}
	update.runNudges(context.Background(), d)

	if got := update.Active.Worker.ReviewNudgeCount; got != 0 {
		t.Fatalf("ReviewNudgeCount = %d, want 0", got)
	}
	if update.WorkerChanged {
		t.Fatal("WorkerChanged = true, want false after prepare failure")
	}
	deps.amux.requireSentKeys(t, "pane-1", nil)
	if !containsLogLine(logs, "prepare review prompt delivery failed") {
		t.Fatalf("logs = %#v, want prepare failure log", logs)
	}
}

func TestCodexReviewPromptFileReferenceWithoutPRNumber(t *testing.T) {
	t.Parallel()

	if got, want := codexReviewPromptFileReference(".orca/prompts/review.md", 0), "Read .orca/prompts/review.md and address the PR review feedback, then push."; got != want {
		t.Fatalf("codexReviewPromptFileReference() = %q, want %q", got, want)
	}
}

func newReviewPromptFileTestDeps(t *testing.T) *testDeps {
	t.Helper()

	deps := newTestDeps(t)
	clonePath := filepath.Join(t.TempDir(), "project", OrcaPoolSubdir, "clone-01")
	markClonePathForTest(t, clonePath)
	deps.pool.clone.Path = clonePath
	return deps
}

func containsLogLine(logs []string, needle string) bool {
	for _, line := range logs {
		if strings.Contains(line, needle) {
			return true
		}
	}
	return false
}
