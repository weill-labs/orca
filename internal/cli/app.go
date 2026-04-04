package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/project"
)

const defaultAgent = "claude"

const usageText = `orca: agent orchestration daemon
usage: orca <command>

commands:
  start    Start the orca daemon
  stop     Stop the orca daemon
  status   Show daemon and task status
  assign   Assign an issue to a worker
  enqueue  Queue a PR for serialized landing
  cancel   Cancel a task
  workers  List workers and their state
  pool     List clone pool status
  events   Stream orchestration events as NDJSON
  version  Print version`

var errInvalidOptions = errors.New("cli: invalid options")

type Options struct {
	Daemon  daemon.Controller
	State   state.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	Version string
	Cwd     func() (string, error)
}

type App struct {
	daemon  daemon.Controller
	state   state.Reader
	stdout  io.Writer
	version string
	cwd     func() (string, error)
}

func UsageText() string {
	return usageText
}

func New(options Options) *App {
	if options.Daemon == nil || options.State == nil || options.Stdout == nil || options.Stderr == nil || options.Cwd == nil {
		panic(errInvalidOptions)
	}

	version := options.Version
	if version == "" {
		version = "dev"
	}

	return &App{
		daemon:  options.Daemon,
		state:   options.State,
		stdout:  options.Stdout,
		version: version,
		cwd:     options.Cwd,
	}
}

func (a *App) Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New(usageText)
	}

	switch args[0] {
	case "start":
		return a.runStart(ctx, args[1:])
	case "stop":
		return a.runStop(ctx, args[1:])
	case "status":
		return a.runStatus(ctx, args[1:])
	case "assign":
		return a.runAssign(ctx, args[1:])
	case "enqueue":
		return a.runEnqueue(ctx, args[1:])
	case "cancel":
		return a.runCancel(ctx, args[1:])
	case "workers":
		return a.runWorkers(ctx, args[1:])
	case "pool":
		return a.runPool(ctx, args[1:])
	case "events":
		return a.runEvents(ctx, args[1:])
	case "version":
		_, err := fmt.Fprintf(a.stdout, "orca %s\n", a.version)
		return err
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usageText)
	}
}

func (a *App) runStart(ctx context.Context, args []string) error {
	fs := newFlagSet("start")
	var session string
	var projectPath string
	var leadPane string
	var jsonOutput bool
	fs.StringVar(&session, "session", "", "amux session name")
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.StringVar(&leadPane, "lead-pane", "", "pane to split workers from")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("start does not accept positional arguments")
	}

	resolvedProject, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	result, err := a.daemon.Start(ctx, daemon.StartRequest{
		Session:  session,
		Project:  resolvedProject,
		LeadPane: leadPane,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	_, err = fmt.Fprintf(a.stdout, "started daemon for %s (session %s, pid %d)\n", result.Project, result.Session, result.PID)
	return err
}

func (a *App) runStop(ctx context.Context, args []string) error {
	fs := newFlagSet("stop")
	var projectPath string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("stop does not accept positional arguments")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	result, err := a.daemon.Stop(ctx, daemon.StopRequest{Project: projectPath})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	_, err = fmt.Fprintf(a.stdout, "stopped daemon for %s (pid %d)\n", result.Project, result.PID)
	return err
}

func (a *App) runStatus(ctx context.Context, args []string) error {
	fs := newFlagSet("status")
	var projectPath string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	issue, err := parseOptionalSinglePositional(fs, args)
	if err != nil {
		return err
	}

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	if issue == "" {
		status, err := a.state.ProjectStatus(ctx, projectPath)
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(a.stdout, status)
		}
		return writeProjectStatus(a.stdout, status)
	}

	taskStatus, err := a.state.TaskStatus(ctx, projectPath, issue)
	if err != nil {
		if errors.Is(err, state.ErrNotFound) {
			return fmt.Errorf("task %s not found", issue)
		}
		return err
	}
	if jsonOutput {
		return writeJSON(a.stdout, taskStatus)
	}
	return writeTaskStatus(a.stdout, taskStatus)
}

func (a *App) runAssign(ctx context.Context, args []string) error {
	fs := newFlagSet("assign")
	var prompt string
	var agent string
	var projectPath string
	var jsonOutput bool
	fs.StringVar(&prompt, "prompt", "", "task prompt")
	fs.StringVar(&agent, "agent", defaultAgent, "agent profile")
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	issue, err := parseRequiredSinglePositional(fs, args, "assign requires ISSUE")
	if err != nil {
		return err
	}
	if strings.TrimSpace(prompt) == "" {
		return fmt.Errorf("assign requires --prompt")
	}

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	result, err := a.daemon.Assign(ctx, daemon.AssignRequest{
		Project: projectPath,
		Issue:   issue,
		Prompt:  prompt,
		Agent:   agent,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	_, err = fmt.Fprintf(a.stdout, "%s assigned to %s\n", result.Issue, result.Agent)
	return err
}

func (a *App) runEnqueue(ctx context.Context, args []string) error {
	fs := newFlagSet("enqueue")
	var projectPath string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	rawPR, err := parseRequiredSinglePositional(fs, args, "enqueue requires PR_NUMBER")
	if err != nil {
		return err
	}

	prNumber, err := strconv.Atoi(strings.TrimSpace(rawPR))
	if err != nil || prNumber <= 0 {
		return fmt.Errorf("enqueue requires numeric PR_NUMBER")
	}

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	result, err := a.daemon.Enqueue(ctx, daemon.EnqueueRequest{
		Project:  projectPath,
		PRNumber: prNumber,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	_, err = fmt.Fprintf(a.stdout, "queued PR #%d for landing at position %d\n", result.PRNumber, result.Position)
	return err
}

func (a *App) runCancel(ctx context.Context, args []string) error {
	fs := newFlagSet("cancel")
	var projectPath string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	issue, err := parseRequiredSinglePositional(fs, args, "cancel requires ISSUE")
	if err != nil {
		return err
	}

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	result, err := a.daemon.Cancel(ctx, daemon.CancelRequest{
		Project: projectPath,
		Issue:   issue,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	_, err = fmt.Fprintf(a.stdout, "%s cancelled\n", result.Issue)
	return err
}

func (a *App) runWorkers(ctx context.Context, args []string) error {
	fs := newFlagSet("workers")
	var projectPath string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("workers does not accept positional arguments")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	workers, err := a.state.ListWorkers(ctx, projectPath)
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(a.stdout, workers)
	}
	return writeWorkers(a.stdout, workers)
}

func (a *App) runPool(ctx context.Context, args []string) error {
	fs := newFlagSet("pool")
	var projectPath string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("pool does not accept positional arguments")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	clones, err := a.state.ListClones(ctx, projectPath)
	if err != nil {
		return err
	}
	if jsonOutput {
		return writeJSON(a.stdout, clones)
	}
	return writeClones(a.stdout, clones)
}

func (a *App) runEvents(ctx context.Context, args []string) error {
	fs := newFlagSet("events")
	var projectPath string
	fs.StringVar(&projectPath, "project", "", "project path")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("events does not accept positional arguments")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	eventsCh, errCh := a.state.Events(ctx, projectPath, 0)
	encoder := json.NewEncoder(a.stdout)

	for eventsCh != nil || errCh != nil {
		select {
		case event, ok := <-eventsCh:
			if !ok {
				eventsCh = nil
				continue
			}
			if err := encoder.Encode(event); err != nil {
				return err
			}
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (a *App) resolveProject(projectPath string) (string, error) {
	if strings.TrimSpace(projectPath) == "" {
		currentDir, err := a.cwd()
		if err != nil {
			return "", fmt.Errorf("resolve current directory: %w", err)
		}
		projectPath = currentDir
	}

	return project.CanonicalPath(projectPath)
}

func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return errors.New(usageText)
		}
		return err
	}
	return nil
}

func parseOptionalSinglePositional(fs *flag.FlagSet, args []string) (string, error) {
	leading, remaining := stripLeadingPositional(args)
	if err := parseFlags(fs, remaining); err != nil {
		return "", err
	}

	rest := fs.Args()
	if leading == "" && len(rest) > 0 {
		leading = rest[0]
		rest = rest[1:]
	}
	if len(rest) > 0 {
		return "", fmt.Errorf("%s accepts at most one issue", fs.Name())
	}

	return leading, nil
}

func parseRequiredSinglePositional(fs *flag.FlagSet, args []string, missingErr string) (string, error) {
	leading, remaining := stripLeadingPositional(args)
	if err := parseFlags(fs, remaining); err != nil {
		return "", err
	}

	rest := fs.Args()
	if leading == "" && len(rest) > 0 {
		leading = rest[0]
		rest = rest[1:]
	}
	if len(rest) > 0 {
		return "", fmt.Errorf("%s accepts only one issue", fs.Name())
	}
	if leading == "" {
		return "", errors.New(missingErr)
	}

	return leading, nil
}

func stripLeadingPositional(args []string) (string, []string) {
	if len(args) == 0 || strings.HasPrefix(args[0], "-") {
		return "", args
	}

	return args[0], args[1:]
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(value)
}

func writeProjectStatus(w io.Writer, status state.ProjectStatus) error {
	daemonState := "stopped"
	if status.Daemon != nil && status.Daemon.Status != "" {
		daemonState = status.Daemon.Status
	}

	if _, err := fmt.Fprintf(w, "project: %s\n", status.Project); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "daemon: %s\n", daemonState); err != nil {
		return err
	}
	if status.Daemon != nil {
		if _, err := fmt.Fprintf(w, "session: %s\n", status.Daemon.Session); err != nil {
			return err
		}
		if status.Daemon.PID > 0 {
			if _, err := fmt.Fprintf(w, "pid: %d\n", status.Daemon.PID); err != nil {
				return err
			}
		}
	}
	if _, err := fmt.Fprintf(w, "tasks: %d total, %d queued, %d active, %d done, %d cancelled\n", status.Summary.Tasks, status.Summary.Queued, status.Summary.Active, status.Summary.Done, status.Summary.Cancelled); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "workers: %d total, %d healthy, %d stuck\n", status.Summary.Workers, status.Summary.HealthyWorkers, status.Summary.StuckWorkers); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "pool: %d total, %d free\n", status.Summary.Clones, status.Summary.FreeClones); err != nil {
		return err
	}

	if len(status.Tasks) == 0 {
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "\nISSUE\tSTATUS\tAGENT\tWORKER\tCLONE\tUPDATED"); err != nil {
		return err
	}
	for _, task := range status.Tasks {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", task.Issue, task.Status, fallback(task.Agent), fallback(task.WorkerID), fallback(task.ClonePath), formatTimestamp(task.UpdatedAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeTaskStatus(w io.Writer, taskStatus state.TaskStatus) error {
	task := taskStatus.Task
	if _, err := fmt.Fprintf(w, "issue: %s\n", task.Issue); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "status: %s\n", task.Status); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "agent: %s\n", fallback(task.Agent)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "worker: %s\n", fallback(task.WorkerID)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "clone: %s\n", fallback(task.ClonePath)); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "updated: %s\n", formatTimestamp(task.UpdatedAt)); err != nil {
		return err
	}
	if len(taskStatus.Events) == 0 {
		return nil
	}

	if _, err := fmt.Fprintln(w, "events:"); err != nil {
		return err
	}
	for _, event := range taskStatus.Events {
		if _, err := fmt.Fprintf(w, "  %s  %s  %s\n", formatTimestamp(event.CreatedAt), event.Kind, event.Message); err != nil {
			return err
		}
	}
	return nil
}

func writeWorkers(w io.Writer, workers []state.Worker) error {
	if len(workers) == 0 {
		_, err := fmt.Fprintln(w, "no workers")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PANE\tAGENT\tSTATE\tISSUE\tCLONE\tUPDATED"); err != nil {
		return err
	}
	for _, worker := range workers {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", worker.PaneID, fallback(worker.Agent), fallback(worker.State), fallback(worker.Issue), fallback(worker.ClonePath), formatTimestamp(worker.UpdatedAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func writeClones(w io.Writer, clones []state.Clone) error {
	if len(clones) == 0 {
		_, err := fmt.Fprintln(w, "no clones")
		return err
	}

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, "PATH\tSTATUS\tISSUE\tBRANCH\tUPDATED"); err != nil {
		return err
	}
	for _, clone := range clones {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", clone.Path, fallback(clone.Status), fallback(clone.Issue), fallback(clone.Branch), formatTimestamp(clone.UpdatedAt)); err != nil {
			return err
		}
	}
	return tw.Flush()
}

func fallback(value string) string {
	if strings.TrimSpace(value) == "" {
		return "-"
	}
	return value
}

func formatTimestamp(timestamp time.Time) string {
	if timestamp.IsZero() {
		return "-"
	}
	return timestamp.UTC().Format(time.RFC3339)
}
