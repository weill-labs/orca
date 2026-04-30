package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/daemon"
)

func TestAppRunReconcileJSON(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	var stdout bytes.Buffer
	app := New(Options{
		Daemon: &fakeDaemon{
			reconcileResult: daemon.ReconcileResult{
				Project: repoRoot,
				Findings: []daemon.ReconcileFinding{{
					Kind:   daemon.ReconcileOrphanPane,
					Issue:  "LAB-1491",
					Action: "reported",
				}},
			},
		},
		State:   &fakeState{},
		Stdout:  &stdout,
		Stderr:  &bytes.Buffer{},
		Version: "build-123",
		Cwd: func() (string, error) {
			return repoRoot, nil
		},
	})

	if err := app.Run(context.Background(), []string{"reconcile", "--json"}); err != nil {
		t.Fatalf("Run(reconcile --json) error = %v", err)
	}
	if got := stdout.String(); !strings.Contains(got, `"kind":"orphan_pane"`) {
		t.Fatalf("json output = %q, want orphan pane finding", got)
	}
}

func TestAppRunReconcileRejectsPositionalArguments(t *testing.T) {
	t.Parallel()

	repoRoot := newRepoRoot(t)
	app := New(Options{
		Daemon:  &fakeDaemon{},
		State:   &fakeState{},
		Stdout:  &bytes.Buffer{},
		Stderr:  &bytes.Buffer{},
		Version: "build-123",
		Cwd: func() (string, error) {
			return repoRoot, nil
		},
	})

	err := app.Run(context.Background(), []string{"reconcile", "LAB-1487"})
	if err == nil || !strings.Contains(err.Error(), "does not accept positional") {
		t.Fatalf("Run(reconcile LAB-1487) error = %v, want positional rejection", err)
	}
}

func TestWriteReconcileResultVariants(t *testing.T) {
	t.Parallel()

	t.Run("no drift", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		err := writeReconcileResult(&out, daemon.ReconcileResult{Project: "/repo"})
		if err != nil {
			t.Fatalf("writeReconcileResult() error = %v", err)
		}
		if got := out.String(); !strings.Contains(got, "drift: none") {
			t.Fatalf("output = %q, want no drift", got)
		}
	})

	t.Run("pane and pr fallbacks", func(t *testing.T) {
		t.Parallel()

		var out bytes.Buffer
		err := writeReconcileResult(&out, daemon.ReconcileResult{
			Project: "/repo",
			Findings: []daemon.ReconcileFinding{
				{
					Kind:    daemon.ReconcileAbandoned,
					Issue:   "LAB-1198",
					PaneID:  "906",
					PRState: "none",
					Action:  "reported",
					Message: "pane missing",
				},
				{
					Kind:     daemon.ReconcileRecoverableGhost,
					Issue:    "LAB-1487",
					PaneName: "w-LAB-1487",
					PRNumber: 470,
					PRState:  "merged",
					Action:   "fixed",
					Message:  "merged",
				},
			},
		})
		if err != nil {
			t.Fatalf("writeReconcileResult() error = %v", err)
		}
		got := out.String()
		for _, want := range []string{"906", "none", "w-LAB-1487", "#470 merged"} {
			if !strings.Contains(got, want) {
				t.Fatalf("output = %q, want %q", got, want)
			}
		}
	})
}

func TestWriteReconcileResultReturnsWriterErrors(t *testing.T) {
	t.Parallel()

	err := writeReconcileResult(errWriter{}, daemon.ReconcileResult{Project: "/repo"})
	if err == nil || !strings.Contains(err.Error(), "write failed") {
		t.Fatalf("writeReconcileResult() error = %v, want writer failure", err)
	}
}
