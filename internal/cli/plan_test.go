package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/daemon"
)

func TestRunPlanParallelFormatsBatchesAndOverrides(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	d := &fakeDaemon{
		planResult: daemon.AssignmentPlanResult{
			Project:  repoRoot,
			Parallel: true,
			Batches: []daemon.AssignmentPlanBatch{
				{
					Number: 1,
					Issues: []daemon.AssignmentPlanIssue{
						{Issue: "LAB-101", OwnedPaths: []string{"internal/cli/app.go"}, OwnershipSource: "parsed"},
						{Issue: "LAB-102", OwnedPaths: []string{"internal/daemon/assign.go"}, OwnershipSource: "override"},
					},
				},
				{
					Number: 2,
					Issues: []daemon.AssignmentPlanIssue{
						{Issue: "LAB-103", OwnedPaths: []string{"internal/cli/app.go"}, OwnershipSource: "parsed"},
					},
				},
			},
			Conflicts: []daemon.AssignmentPlanConflict{
				{Issue: "LAB-103", ConflictsWith: "LAB-101", Paths: []string{"internal/cli/app.go"}},
			},
			Warnings: []daemon.AssignmentPlanWarning{
				{Issue: "LAB-104", Kind: "unknown_ownership", Message: "no owned paths found; use --path LAB-104=path/to/file"},
			},
		},
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	app := New(Options{
		Daemon: d,
		State:  &fakeState{},
		Stdout: &stdout,
		Stderr: &stderr,
		Cwd: func() (string, error) {
			return repoRoot, nil
		},
	})

	err := app.Run(context.Background(), []string{
		"plan",
		"--parallel",
		"--path", "LAB-102=internal/daemon/assign.go",
		"LAB-101",
		"LAB-102",
		"LAB-103",
	})
	if err != nil {
		t.Fatalf("Run(plan) error = %v", err)
	}
	if d.planRequest == nil {
		t.Fatal("Plan() was not called")
	}
	if got, want := d.planRequest.Project, repoRoot; got != want {
		t.Fatalf("plan project = %q, want %q", got, want)
	}
	if got, want := d.planRequest.PathOverrides["LAB-102"], []string{"internal/daemon/assign.go"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("plan override = %#v, want %#v", got, want)
	}

	output := stdout.String()
	for _, want := range []string{
		"batch 1",
		"LAB-101",
		"override",
		"conflicts",
		"LAB-103 overlaps LAB-101",
		"unknown_ownership",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("stdout = %q, want substring %q", output, want)
		}
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunAssignPassesPlanningDecision(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	d := &fakeDaemon{}
	var stdout bytes.Buffer
	app := New(Options{
		Daemon: d,
		State:  &fakeState{},
		Stdout: &stdout,
		Stderr: &bytes.Buffer{},
		Cwd: func() (string, error) {
			return repoRoot, nil
		},
	})

	err := app.Run(context.Background(), []string{
		"assign",
		"LAB-1968",
		"--prompt", "Implement planning",
		"--planning-decision", "parallel batch 1: no overlap",
	})
	if err != nil {
		t.Fatalf("Run(assign) error = %v", err)
	}
	if d.assignRequest == nil {
		t.Fatal("Assign() was not called")
	}
	if got, want := d.assignRequest.PlanningDecision, "parallel batch 1: no overlap"; got != want {
		t.Fatalf("assign planning decision = %q, want %q", got, want)
	}
}
