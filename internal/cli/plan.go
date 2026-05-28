package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/weill-labs/orca/internal/daemon"
)

type repeatedStringFlag []string

func (f *repeatedStringFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *repeatedStringFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func (a *App) runPlan(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("plan", args); handled {
		return err
	}

	fs := newFlagSet("plan")
	var parallel bool
	var projectPath string
	var jsonOutput bool
	var pathFlags repeatedStringFlag
	fs.BoolVar(&parallel, "parallel", false, "group candidate issues into non-conflicting parallel batches")
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")
	fs.Var(&pathFlags, "path", "override owned paths for an issue as ISSUE=path[,path]")

	issues, err := parsePlanArgs(fs, args)
	if err != nil {
		return err
	}
	if !parallel {
		return fmt.Errorf("plan requires --parallel")
	}

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}
	pathOverrides, err := parsePlanPathOverrides(pathFlags)
	if err != nil {
		return err
	}

	result, err := a.daemon.Plan(ctx, daemon.AssignmentPlanRequest{
		Project:       projectPath,
		Parallel:      parallel,
		Issues:        issues,
		PathOverrides: pathOverrides,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}
	return writeAssignmentPlan(a.stdout, result)
}

func parsePlanArgs(fs interface {
	Parse([]string) error
}, args []string) ([]string, error) {
	flagArgs := make([]string, 0, len(args))
	issues := make([]string, 0, len(args))

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			issues = append(issues, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			issues = append(issues, arg)
			continue
		}

		flagArgs = append(flagArgs, arg)
		if planFlagConsumesValue(arg) && !strings.Contains(arg, "=") {
			if i+1 >= len(args) {
				return nil, fmt.Errorf("flag needs an argument: %s", arg)
			}
			i++
			flagArgs = append(flagArgs, args[i])
		}
	}

	if err := fs.Parse(flagArgs); err != nil {
		return nil, err
	}
	return issues, nil
}

func planFlagConsumesValue(arg string) bool {
	name := strings.TrimLeft(arg, "-")
	if before, _, ok := strings.Cut(name, "="); ok {
		name = before
	}
	switch name {
	case "project", "path":
		return true
	default:
		return false
	}
}

func parsePlanPathOverrides(values []string) (map[string][]string, error) {
	if len(values) == 0 {
		return nil, nil
	}

	overrides := make(map[string][]string, len(values))
	for _, value := range values {
		issue, paths, ok := strings.Cut(value, "=")
		issue = daemon.NormalizeIssueIdentifier(issue)
		if !ok || strings.TrimSpace(issue) == "" {
			return nil, fmt.Errorf("--path must use ISSUE=path[,path]")
		}
		for _, ownedPath := range strings.Split(paths, ",") {
			ownedPath = strings.TrimSpace(ownedPath)
			if ownedPath == "" {
				continue
			}
			overrides[issue] = append(overrides[issue], ownedPath)
		}
		if len(overrides[issue]) == 0 {
			return nil, fmt.Errorf("--path for %s requires at least one path", issue)
		}
	}
	return overrides, nil
}

func writeAssignmentPlan(w io.Writer, result daemon.AssignmentPlanResult) error {
	var builder strings.Builder
	fmt.Fprintf(&builder, "parallel assignment plan for %s\n\n", result.Project)

	for _, batch := range result.Batches {
		fmt.Fprintf(&builder, "batch %d\n", batch.Number)
		tw := tabwriter.NewWriter(&builder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ISSUE\tOWNERSHIP\tOWNED PATHS")
		for _, issue := range batch.Issues {
			paths := "unknown"
			if len(issue.OwnedPaths) > 0 {
				paths = strings.Join(issue.OwnedPaths, ", ")
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\n", issue.Issue, issue.OwnershipSource, paths)
		}
		_ = tw.Flush()
		fmt.Fprintln(&builder)
	}

	if len(result.Conflicts) > 0 {
		fmt.Fprintln(&builder, "conflicts")
		for _, conflict := range result.Conflicts {
			fmt.Fprintf(&builder, "%s overlaps %s: %s\n", conflict.Issue, conflict.ConflictsWith, strings.Join(conflict.Paths, ", "))
		}
		fmt.Fprintln(&builder)
	}

	if len(result.Warnings) > 0 {
		fmt.Fprintln(&builder, "warnings")
		tw := tabwriter.NewWriter(&builder, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "ISSUE\tKIND\tMESSAGE")
		for _, warning := range result.Warnings {
			fmt.Fprintf(tw, "%s\t%s\t%s\n", warning.Issue, warning.Kind, warning.Message)
		}
		_ = tw.Flush()
	}

	_, err := io.WriteString(w, builder.String())
	return err
}
