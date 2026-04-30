package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/weill-labs/orca/internal/daemon"
)

func (a *App) runReconcile(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("reconcile", args); handled {
		return err
	}

	fs := newFlagSet("reconcile")
	var projectPath string
	var fix bool
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&fix, "fix", false, "complete safe automated recovery for merged PR cleanup only")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("reconcile does not accept positional arguments")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	result, err := a.daemon.Reconcile(ctx, daemon.ReconcileRequest{
		Project: projectPath,
		Fix:     fix,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}
	return writeReconcileResult(a.stdout, result)
}

func writeReconcileResult(w io.Writer, result daemon.ReconcileResult) error {
	if _, err := fmt.Fprintf(w, "project: %s\n", result.Project); err != nil {
		return err
	}
	if len(result.Findings) == 0 {
		_, err := fmt.Fprintln(w, "drift: none")
		return err
	}

	if _, err := fmt.Fprintf(w, "drift: %d finding(s)\n", len(result.Findings)); err != nil {
		return err
	}
	if result.Fix {
		if _, err := fmt.Fprintf(w, "fixed: %d\n", result.Fixed); err != nil {
			return err
		}
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "\nKIND\tISSUE\tPANE\tPR\tACTION\tMESSAGE"); err != nil {
		return err
	}
	for _, finding := range result.Findings {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			finding.Kind,
			fallback(finding.Issue),
			reconcilePaneText(finding),
			reconcilePRText(finding),
			finding.Action,
			finding.Message,
		); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func reconcilePaneText(finding daemon.ReconcileFinding) string {
	if strings.TrimSpace(finding.PaneName) != "" {
		return finding.PaneName
	}
	if strings.TrimSpace(finding.PaneID) != "" {
		return finding.PaneID
	}
	return "-"
}

func reconcilePRText(finding daemon.ReconcileFinding) string {
	state := strings.TrimSpace(finding.PRState)
	if state == "" {
		state = "none"
	}
	if finding.PRNumber <= 0 {
		return state
	}
	return fmt.Sprintf("#%d %s", finding.PRNumber, state)
}
