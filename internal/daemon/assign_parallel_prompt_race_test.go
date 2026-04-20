package daemon

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	amuxapi "github.com/weill-labs/orca/internal/amux"
)

func TestAssignParallelCodexStartupStressN3(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	deps.pool.clones = testParallelAssignClones(t, deps.pool.clone, 3)
	deps.amux.spawnPanes = testParallelAssignPanes(9)

	race := newParallelCodexStartupRace(deps.amux, 100*time.Millisecond)
	deps.amux.sendKeysHook = race.onSendKeys
	deps.amux.waitContentFunc = race.waitContent

	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	assignments := []struct {
		issue  string
		prompt string
	}{
		{issue: "LAB-1416", prompt: "Implement parallel assign stress test one"},
		{issue: "LAB-1417", prompt: "Implement parallel assign stress test two"},
		{issue: "LAB-1418", prompt: "Implement parallel assign stress test three"},
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(assignments))
	for _, assignment := range assignments {
		assignment := assignment
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- d.Assign(ctx, assignment.issue, assignment.prompt, "codex")
		}()
	}
	wg.Wait()
	close(errs)

	var gotErrs []error
	for err := range errs {
		if err != nil {
			gotErrs = append(gotErrs, err)
		}
	}
	if len(gotErrs) > 0 {
		t.Fatalf("parallel Assign() errors = %v", gotErrs)
	}

	for _, assignment := range assignments {
		task, ok := deps.state.task(assignment.issue)
		if !ok {
			t.Fatalf("task %s missing after parallel assign", assignment.issue)
		}
		if got, want := task.Status, TaskStatusActive; got != want {
			t.Fatalf("task %s status = %q, want %q", assignment.issue, got, want)
		}
	}

	if got, want := race.maxConcurrentPromptTexts(), 1; got != want {
		t.Fatalf("max concurrent startup prompt sends = %d, want %d", got, want)
	}
	if got := deps.events.countType(EventWorkerPromptDeliveryRetry); got != 0 {
		t.Fatalf("prompt delivery retries = %d, want 0", got)
	}
}

type parallelCodexStartupRace struct {
	amux       *fakeAmux
	promptHold time.Duration

	mu                   sync.Mutex
	activePromptTexts    int
	maxPromptTexts       int
	paneIssues           map[string]string
	doomedIssues         map[string]bool
}

func newParallelCodexStartupRace(amux *fakeAmux, promptHold time.Duration) *parallelCodexStartupRace {
	return &parallelCodexStartupRace{
		amux:         amux,
		promptHold:   promptHold,
		paneIssues:   make(map[string]string),
		doomedIssues: make(map[string]bool),
	}
}

func (r *parallelCodexStartupRace) onSendKeys(paneID string, keys []string) {
	if !isParallelAssignStartupPrompt(keys) {
		return
	}

	issue := parallelAssignPromptIssue(keys[0])
	if issue == "" {
		return
	}

	r.mu.Lock()
	r.paneIssues[paneID] = issue
	r.activePromptTexts++
	if r.activePromptTexts > r.maxPromptTexts {
		r.maxPromptTexts = r.activePromptTexts
	}
	if r.activePromptTexts > 1 {
		r.doomedIssues[issue] = true
	}
	r.mu.Unlock()

	time.Sleep(r.promptHold)

	r.mu.Lock()
	r.activePromptTexts--
	r.mu.Unlock()
}

func (r *parallelCodexStartupRace) waitContent(paneID, substring string, _ time.Duration) (bool, error) {
	if substring != codexWorkingText {
		return false, nil
	}

	r.mu.Lock()
	issue := r.paneIssues[paneID]
	doomed := r.doomedIssues[issue]
	r.mu.Unlock()
	if !doomed {
		return false, nil
	}

	r.amux.capturePaneSequence(paneID, []PaneCapture{{
		Content: []string{
			"bash-5.2$",
			fmt.Sprintf("%s prompt echoed at shell", issue),
		},
		CurrentCommand: "bash",
	}})
	return true, amuxapi.ErrWaitContentTimeout
}

func (r *parallelCodexStartupRace) maxConcurrentPromptTexts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxPromptTexts
}

func isParallelAssignStartupPrompt(keys []string) bool {
	return len(keys) == 1 && keys[0] != "Enter" && strings.Contains(keys[0], codexAssignmentPromptSuffix)
}

func parallelAssignPromptIssue(prompt string) string {
	for _, match := range explicitIssueIDPattern.FindAllString(prompt, -1) {
		issue := normalizeIssueIdentifier(match)
		if strings.HasPrefix(issue, "LAB-") {
			return issue
		}
	}
	return ""
}

func testParallelAssignClones(t *testing.T, first Clone, total int) []Clone {
	t.Helper()

	clones := []Clone{first}
	for i := 2; i <= total; i++ {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("clone-%02d", i))
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("MkdirAll(%q) error = %v", path, err)
		}
		clones = append(clones, Clone{
			Name: fmt.Sprintf("clone-%02d", i),
			Path: path,
		})
	}
	return clones
}

func testParallelAssignPanes(total int) []Pane {
	panes := make([]Pane, 0, total)
	for i := 1; i <= total; i++ {
		panes = append(panes, Pane{
			ID:   fmt.Sprintf("pane-%d", i),
			Name: fmt.Sprintf("worker-%d", i),
		})
	}
	return panes
}
