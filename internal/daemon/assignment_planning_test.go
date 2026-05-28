package daemon

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestPlanParallelAssignmentsGroupsByOwnedPathsAndFlagsUnknown(t *testing.T) {
	t.Parallel()

	result, err := PlanParallelAssignments(AssignmentPlanRequest{
		Project:  "/repo",
		Parallel: true,
		Issues:   []string{"LAB-101", "LAB-102", "LAB-103", "LAB-104"},
	}, []AssignmentPlanCandidate{
		{
			Issue: "LAB-101",
			Body:  "Files: `internal/cli/app.go`, `internal/daemon/controller.go`",
		},
		{
			Issue: "LAB-102",
			Body:  "Owned paths: internal/worksource/beads_source.go",
		},
		{
			Issue: "LAB-103",
			Body:  "Files: internal/cli/app.go",
		},
		{
			Issue: "LAB-104",
			Body:  "Investigate assignment behavior without a file list.",
		},
	})
	if err != nil {
		t.Fatalf("PlanParallelAssignments() error = %v", err)
	}

	if got, want := planBatchIssues(result.Batches), [][]string{
		{"LAB-101", "LAB-102"},
		{"LAB-103"},
		{"LAB-104"},
	}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batches = %#v, want %#v", got, want)
	}

	lab103 := requirePlannedIssue(t, result, "LAB-103")
	if got, want := lab103.OwnedPaths, []string{"internal/cli/app.go"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LAB-103 owned paths = %#v, want %#v", got, want)
	}

	if got, want := len(result.Conflicts), 1; got != want {
		t.Fatalf("conflict count = %d, want %d (%#v)", got, want, result.Conflicts)
	}
	conflict := result.Conflicts[0]
	if conflict.Issue != "LAB-103" || conflict.ConflictsWith != "LAB-101" {
		t.Fatalf("conflict pair = %s/%s, want LAB-103/LAB-101", conflict.Issue, conflict.ConflictsWith)
	}
	if got, want := conflict.Paths, []string{"internal/cli/app.go"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("conflict paths = %#v, want %#v", got, want)
	}

	if got, want := len(result.Warnings), 1; got != want {
		t.Fatalf("warning count = %d, want %d (%#v)", got, want, result.Warnings)
	}
	warning := result.Warnings[0]
	if warning.Issue != "LAB-104" || warning.Kind != "unknown_ownership" {
		t.Fatalf("warning = %#v, want LAB-104 unknown_ownership", warning)
	}
	if !strings.Contains(warning.Message, "--path LAB-104=") {
		t.Fatalf("warning message = %q, want override guidance", warning.Message)
	}
}

func TestPlanParallelAssignmentsUsesExplicitPathOverrides(t *testing.T) {
	t.Parallel()

	result, err := PlanParallelAssignments(AssignmentPlanRequest{
		Project:  "/repo",
		Parallel: true,
		Issues:   []string{"LAB-201", "LAB-202"},
		PathOverrides: map[string][]string{
			"LAB-202": {"internal/cli/app.go", "internal/cli/plan.go"},
		},
	}, []AssignmentPlanCandidate{
		{
			Issue: "LAB-201",
			Body:  "Files: internal/daemon/assign.go",
		},
		{
			Issue: "LAB-202",
			Body:  "No parseable file ownership yet.",
		},
	})
	if err != nil {
		t.Fatalf("PlanParallelAssignments() error = %v", err)
	}

	if got, want := planBatchIssues(result.Batches), [][]string{{"LAB-201", "LAB-202"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batches = %#v, want %#v", got, want)
	}
	lab202 := requirePlannedIssue(t, result, "LAB-202")
	if got, want := lab202.OwnershipSource, "override"; got != want {
		t.Fatalf("LAB-202 ownership source = %q, want %q", got, want)
	}
	if len(result.Warnings) != 0 {
		t.Fatalf("warnings = %#v, want none", result.Warnings)
	}
}

func TestPlanParallelAssignmentsUsesCandidateOrderWhenIssuesOmitted(t *testing.T) {
	t.Parallel()

	result, err := PlanParallelAssignments(AssignmentPlanRequest{
		Project:  "/repo",
		Parallel: true,
	}, []AssignmentPlanCandidate{
		{Issue: "LAB-301", Body: "Files: internal/daemon"},
		{Issue: "LAB-302", Body: "Files:\n- internal/daemon/assign.go\n# Notes\n- internal/daemon/ignored.go"},
		{Issue: "LAB-303", Body: "Files: internal/cli/plan.go"},
	})
	if err != nil {
		t.Fatalf("PlanParallelAssignments() error = %v", err)
	}

	if got, want := planBatchIssues(result.Batches), [][]string{{"LAB-301", "LAB-303"}, {"LAB-302"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batches = %#v, want %#v", got, want)
	}
	if got, want := len(result.Conflicts), 1; got != want {
		t.Fatalf("conflict count = %d, want %d (%#v)", got, want, result.Conflicts)
	}
	if got, want := result.Conflicts[0].Paths, []string{"internal/daemon"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("conflict paths = %#v, want %#v", got, want)
	}
	lab302 := requirePlannedIssue(t, result, "LAB-302")
	if got, want := lab302.OwnedPaths, []string{"internal/daemon/assign.go"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("LAB-302 owned paths = %#v, want %#v", got, want)
	}
}

func TestPlanParallelAssignmentsRejectsInvalidPathOverrides(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		overrides map[string][]string
		wantErr   string
	}{
		{
			name:      "missing issue",
			overrides: map[string][]string{"": {"internal/daemon/assign.go"}},
			wantErr:   "path override requires an issue id",
		},
		{
			name:      "absolute path",
			overrides: map[string][]string{"LAB-401": {"/tmp/assign.go"}},
			wantErr:   "invalid path override",
		},
		{
			name:      "empty path",
			overrides: map[string][]string{"LAB-402": {""}},
			wantErr:   "invalid path override",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := PlanParallelAssignments(AssignmentPlanRequest{
				Project:       "/repo",
				Parallel:      true,
				Issues:        []string{"LAB-401"},
				PathOverrides: tt.overrides,
			}, []AssignmentPlanCandidate{{Issue: "LAB-401", Body: "Files: internal/daemon/assign.go"}})
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("PlanParallelAssignments() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestAssignRecordsPlanningDecisionInPaneMetadata(t *testing.T) {
	t.Parallel()

	deps := newTestDeps(t)
	deps.tickers.enqueue(newFakeTicker(), newFakeTicker())
	d := deps.newDaemon(t)
	ctx := context.Background()

	if err := d.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	t.Cleanup(func() {
		_ = d.Stop(context.Background())
	})

	const planningDecision = "parallel batch 1: no owned path overlap with LAB-1970"
	if err := d.assignWithPlanningDecision(ctx, "/tmp/project", "LAB-1968", "Implement assignment planning", "codex", "", "", planningDecision); err != nil {
		t.Fatalf("assignWithPlanningDecision() error = %v", err)
	}

	metadata, err := deps.amux.Metadata(ctx, "pane-1")
	if err != nil {
		t.Fatalf("Metadata() error = %v", err)
	}
	if got := metadata["planning_decision"]; got != planningDecision {
		t.Fatalf("planning_decision metadata = %q, want %q", got, planningDecision)
	}

	event, ok := deps.events.lastEventOfType(EventTaskAssigned)
	if !ok {
		t.Fatal("missing task.assigned event")
	}
	if got := event.PlanningDecision; got != planningDecision {
		t.Fatalf("event planning decision = %q, want %q", got, planningDecision)
	}
}

func TestLocalControllerPlanLoadsBeadsIssueDescriptions(t *testing.T) {
	t.Parallel()

	projectPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(projectPath, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	beadsDir := filepath.Join(projectPath, ".beads")
	if err := os.Mkdir(beadsDir, 0o755); err != nil {
		t.Fatalf("Mkdir(.beads) error = %v", err)
	}
	issues := strings.Join([]string{
		`{"_type":"issue","id":"orca-a","title":"Plan CLI","description":"Files: internal/cli/app.go","status":"open","issue_type":"task","external_ref":"LAB-301","dependency_count":0}`,
		`{"_type":"issue","id":"orca-b","title":"Plan daemon","description":"Files: internal/cli/app.go","status":"open","issue_type":"task","dependency_count":0}`,
		`{"_type":"issue","id":"orca-epic","title":"Epic","description":"Files: docs/specs/orca-design.md","status":"open","issue_type":"epic","dependency_count":0}`,
	}, "\n")
	if err := os.WriteFile(filepath.Join(beadsDir, "issues.jsonl"), []byte(issues+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(issues.jsonl) error = %v", err)
	}

	controller := &LocalController{}
	result, err := controller.Plan(context.Background(), AssignmentPlanRequest{
		Project:  projectPath,
		Parallel: true,
		Issues:   []string{"LAB-301", "orca-b"},
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	if got, want := planBatchIssues(result.Batches), [][]string{{"LAB-301"}, {"orca-b"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batches = %#v, want %#v", got, want)
	}
	if got, want := len(result.Conflicts), 1; got != want {
		t.Fatalf("conflict count = %d, want %d (%#v)", got, want, result.Conflicts)
	}
	if got, want := result.Conflicts[0].Paths, []string{"internal/cli/app.go"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("conflict paths = %#v, want %#v", got, want)
	}
}

func TestLocalControllerPlanUsesReadyBeadsWhenIssuesOmitted(t *testing.T) {
	t.Parallel()

	projectPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(projectPath, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}
	beadsDir := filepath.Join(projectPath, ".beads")
	if err := os.Mkdir(beadsDir, 0o755); err != nil {
		t.Fatalf("Mkdir(.beads) error = %v", err)
	}
	issues := strings.Join([]string{
		`{"_type":"issue","id":"orca-ready","title":"Ready","description":"Files: internal/cli/plan.go","status":"open","issue_type":"task","dependency_count":0}`,
		`{"_type":"issue","id":"orca-blocked","title":"Blocked","description":"Files: internal/daemon/assign.go","status":"open","issue_type":"task","dependency_count":1}`,
		`{"_type":"issue","id":"orca-done","title":"Done","description":"Files: internal/daemon/helpers.go","status":"closed","issue_type":"task","dependency_count":0}`,
		`{"_type":"issue","id":"orca-epic","title":"Epic","description":"Files: docs/specs/orca-design.md","status":"open","issue_type":"epic","dependency_count":0}`,
	}, "\n")
	if err := os.WriteFile(filepath.Join(beadsDir, "issues.jsonl"), []byte(issues+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile(issues.jsonl) error = %v", err)
	}

	controller := &LocalController{}
	result, err := controller.Plan(context.Background(), AssignmentPlanRequest{
		Project:  projectPath,
		Parallel: true,
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if got, want := planBatchIssues(result.Batches), [][]string{{"orca-ready"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batches = %#v, want %#v", got, want)
	}
}

func TestLocalControllerPlanTreatsMissingBeadsIssueAsUnknown(t *testing.T) {
	t.Parallel()

	projectPath := t.TempDir()
	if err := os.Mkdir(filepath.Join(projectPath, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(.git) error = %v", err)
	}

	controller := &LocalController{}
	result, err := controller.Plan(context.Background(), AssignmentPlanRequest{
		Project:  projectPath,
		Parallel: true,
		Issues:   []string{"LAB-501"},
	})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if got, want := planBatchIssues(result.Batches), [][]string{{"LAB-501"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("batches = %#v, want %#v", got, want)
	}
	if got, want := len(result.Warnings), 1; got != want {
		t.Fatalf("warning count = %d, want %d (%#v)", got, want, result.Warnings)
	}
	if got, want := result.Warnings[0].Kind, "unknown_ownership"; got != want {
		t.Fatalf("warning kind = %q, want %q", got, want)
	}
}

func planBatchIssues(batches []AssignmentPlanBatch) [][]string {
	out := make([][]string, 0, len(batches))
	for _, batch := range batches {
		issues := make([]string, 0, len(batch.Issues))
		for _, issue := range batch.Issues {
			issues = append(issues, issue.Issue)
		}
		out = append(out, issues)
	}
	return out
}

func requirePlannedIssue(t *testing.T, result AssignmentPlanResult, issue string) AssignmentPlanIssue {
	t.Helper()

	for _, batch := range result.Batches {
		for _, planned := range batch.Issues {
			if planned.Issue == issue {
				return planned
			}
		}
	}
	t.Fatalf("planned issue %s not found in %#v", issue, result.Batches)
	return AssignmentPlanIssue{}
}
