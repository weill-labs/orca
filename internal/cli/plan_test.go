package cli

import (
	"bytes"
	"context"
	"flag"
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
	if !strings.HasSuffix(output, "\n\n") {
		t.Fatalf("stdout = %q, want final blank line", output)
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

func TestRunPlanParallelWritesJSON(t *testing.T) {
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
						{Issue: "LAB-201", OwnedPaths: []string{"internal/cli/plan.go"}, OwnershipSource: "parsed"},
					},
				},
			},
		},
	}

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

	err := app.Run(context.Background(), []string{"plan", "--json", "--parallel", "--path=LAB-201=internal/cli/plan.go", "LAB-201"})
	if err != nil {
		t.Fatalf("Run(plan --json) error = %v", err)
	}
	if !strings.Contains(stdout.String(), `"owned_paths":["internal/cli/plan.go"]`) {
		t.Fatalf("stdout = %q, want JSON owned paths", stdout.String())
	}
}

func TestRunPlanParallelValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "requires parallel",
			args:    []string{"plan", "LAB-301"},
			wantErr: "plan requires --parallel",
		},
		{
			name:    "rejects malformed path override",
			args:    []string{"plan", "--parallel", "--path", "LAB-301", "LAB-301"},
			wantErr: "--path must use ISSUE=path",
		},
		{
			name:    "rejects empty path override",
			args:    []string{"plan", "--parallel", "--path", "LAB-301=", "LAB-301"},
			wantErr: "--path for LAB-301 requires at least one path",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repoRoot := newRepoRoot(t)
			app := New(Options{
				Daemon: &fakeDaemon{},
				State:  &fakeState{},
				Stdout: &bytes.Buffer{},
				Stderr: &bytes.Buffer{},
				Cwd: func() (string, error) {
					return repoRoot, nil
				},
			})
			err := app.Run(context.Background(), tt.args)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Run() error = %v, want substring %q", err, tt.wantErr)
			}
		})
	}
}

func TestParsePlanArgsInterspersedFlags(t *testing.T) {
	t.Parallel()

	fs := newFlagSet("plan")
	var parallel bool
	var projectPath string
	var agent string
	fs.BoolVar(&parallel, "parallel", false, "")
	fs.StringVar(&projectPath, "project", "", "")
	fs.StringVar(&agent, "agent", "", "")

	issues, err := parsePlanArgs(fs, []string{"LAB-401", "--parallel", "--agent", "codex", "--project", "/repo", "--", "LAB-402"})
	if err != nil {
		t.Fatalf("parsePlanArgs() error = %v", err)
	}
	if got, want := strings.Join(issues, ","), "LAB-401,LAB-402"; got != want {
		t.Fatalf("issues = %q, want %q", got, want)
	}
	if !parallel {
		t.Fatal("parallel flag = false, want true")
	}
	if got, want := projectPath, "/repo"; got != want {
		t.Fatalf("projectPath = %q, want %q", got, want)
	}
	if got, want := agent, "codex"; got != want {
		t.Fatalf("agent = %q, want %q", got, want)
	}

	fs = flag.NewFlagSet("plan", flag.ContinueOnError)
	fs.String("project", "", "")
	_, err = parsePlanArgs(fs, []string{"--project"})
	if err == nil || !strings.Contains(err.Error(), "flag needs an argument") {
		t.Fatalf("parsePlanArgs() missing value error = %v, want flag needs argument", err)
	}
}
