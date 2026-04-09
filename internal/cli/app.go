package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/project"
)

const defaultAgent = "claude"
const amuxSessionEnvVar = "AMUX_SESSION"
const amuxPaneEnvVar = "AMUX_PANE"

const usageText = `orca: agent orchestration daemon
usage: orca <command>

commands:
  start    Start the orca daemon
  stop     Stop the orca daemon
  status   Show daemon and task status
  assign   Assign an issue to a worker
  batch    Assign multiple issues from a manifest
  spawn    Open a clone in a new amux pane
  enqueue  Queue a PR for serialized landing
  cancel   Cancel a task
  resume   Resume a task, recreating its pane if needed
  workers  List workers and their state
  pool     List clone pool status
  events   Stream orchestration events as NDJSON
  help     Show help for a command
  version  Print version`

// Keep these summaries aligned with the per-command FlagSet definitions below.
var commandUsage = map[string]string{
	"start": `usage: orca start [--session SESSION] [--project PATH] [--global] [--json]

Start the orca daemon.`,
	"stop": `usage: orca stop [--project PATH] [--force] [--global] [--json]

Stop the orca daemon.`,
	"status": `usage: orca status [ISSUE] [--project PATH] [--global] [--json]

Show daemon and task status.`,
	"assign": `usage: orca assign ISSUE --prompt TEXT [--agent NAME] [--project PATH] [--json]

Assign an issue to a worker.

Flags:
  --prompt  Task prompt
  --agent   Agent profile
  --project Project path
  --json    Emit JSON output`,
	"batch": `usage: orca batch MANIFEST [--project PATH] [--delay DURATION]

Assign multiple issues from a manifest.`,
	"enqueue": `usage: orca enqueue PR_NUMBER [--project PATH] [--json]

Queue a PR for serialized landing.`,
	"cancel": `usage: orca cancel ISSUE [--project PATH] [--json]

Cancel a task.`,
	"resume": `usage: orca resume ISSUE [--project PATH] [--json]

Resume a task in its existing pane.`,
	"workers": `usage: orca workers [--project PATH] [--json]

List workers and their state.`,
	"pool": `usage: orca pool [--project PATH] [--json]

List clone pool status.`,
	"events": `usage: orca events [--project PATH]

Stream orchestration events as NDJSON.`,
	"version": `usage: orca version

Print version.`,
}

var errInvalidOptions = errors.New("cli: invalid options")

type Options struct {
	Daemon           daemon.Controller
	State            state.Reader
	Stdout           io.Writer
	Stderr           io.Writer
	Version          string
	Cwd              func() (string, error)
	ProjectStatusRPC func(context.Context, string) (daemon.ProjectStatusRPCResult, error)
}

type App struct {
	daemon           daemon.Controller
	state            state.Reader
	stdout           io.Writer
	stderr           io.Writer
	version          string
	cwd              func() (string, error)
	projectStatusRPC func(context.Context, string) (daemon.ProjectStatusRPCResult, error)
}

func UsageText() string {
	return usageText
}

func WriteHelp(w io.Writer, args []string) (bool, error) {
	usage, ok := HelpText(args)
	if !ok {
		return false, nil
	}

	_, err := fmt.Fprintln(w, usage)
	return true, err
}

func HelpText(args []string) (string, bool) {
	if len(args) == 0 {
		return "", false
	}

	if args[0] == "help" {
		if len(args) == 1 {
			return usageText, true
		}
		if usage, ok := commandUsage[args[1]]; ok {
			return usage, true
		}
		return "", false
	}

	if isHelpToken(args[0]) {
		return usageText, true
	}

	if len(args) >= 2 && isHelpToken(args[1]) {
		if usage, ok := commandUsage[args[0]]; ok {
			return usage, true
		}
	}

	return "", false
}

func New(options Options) *App {
	if options.Daemon == nil || options.State == nil || options.Stdout == nil || options.Stderr == nil || options.Cwd == nil {
		panic(errInvalidOptions)
	}

	version := options.Version
	if version == "" {
		version = "dev"
	}

	projectStatusRPC := options.ProjectStatusRPC
	if projectStatusRPC == nil {
		projectStatusRPC = defaultProjectStatusRPC
	}

	return &App{
		daemon:           options.Daemon,
		state:            options.State,
		stdout:           options.Stdout,
		stderr:           options.Stderr,
		version:          version,
		cwd:              options.Cwd,
		projectStatusRPC: projectStatusRPC,
	}
}

func (a *App) Run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New(usageText)
	}
	if handled, err := WriteHelp(a.stdout, args); handled {
		return err
	}

	switch args[0] {
	case "help":
		return fmt.Errorf("unknown help topic %q", args[1])
	case "start":
		return a.runStart(ctx, args[1:])
	case "stop":
		return a.runStop(ctx, args[1:])
	case "status":
		return a.runStatus(ctx, args[1:])
	case "assign":
		return a.runAssign(ctx, args[1:])
	case "batch":
		return a.runBatch(ctx, args[1:])
	case "spawn":
		return a.runSpawn(ctx, args[1:])
	case "enqueue":
		return a.runEnqueue(ctx, args[1:])
	case "cancel":
		return a.runCancel(ctx, args[1:])
	case "resume":
		return a.runResume(ctx, args[1:])
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
	var global bool
	var jsonOutput bool
	fs.StringVar(&session, "session", "", "amux session name (defaults to AMUX_SESSION)")
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.StringVar(&leadPane, "lead-pane", "", "deprecated: fallback pane to split workers from")
	fs.BoolVar(&global, "global", false, "operate on the machine-wide daemon")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("start does not accept positional arguments")
	}

	resolvedProject := ""
	if !global {
		var err error
		resolvedProject, err = a.resolveProject(projectPath)
		if err != nil {
			return err
		}
	}

	if strings.TrimSpace(session) == "" {
		session = strings.TrimSpace(os.Getenv(amuxSessionEnvVar))
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

	if result.Project == "" {
		_, err = fmt.Fprintf(a.stdout, "started global daemon (session %s, pid %d)\n", result.Session, result.PID)
		return err
	}
	_, err = fmt.Fprintf(a.stdout, "started daemon for %s (session %s, pid %d)\n", result.Project, result.Session, result.PID)
	return err
}

func (a *App) runStop(ctx context.Context, args []string) error {
	fs := newFlagSet("stop")
	var projectPath string
	var force bool
	var global bool
	var jsonOutput bool
	var err error
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&force, "force", false, "escalate to SIGKILL after the grace period")
	fs.BoolVar(&global, "global", false, "operate on the machine-wide daemon")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("stop does not accept positional arguments")
	}

	if !global {
		projectPath, err = a.resolveProject(projectPath)
		if err != nil {
			return err
		}
	}

	result, err := a.daemon.Stop(ctx, daemon.StopRequest{
		Project: projectPath,
		Force:   force,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	if result.Project == "" {
		_, err = fmt.Fprintf(a.stdout, "stopped global daemon (pid %d)\n", result.PID)
		return err
	}
	_, err = fmt.Fprintf(a.stdout, "stopped daemon for %s (pid %d)\n", result.Project, result.PID)
	return err
}

func (a *App) runStatus(ctx context.Context, args []string) error {
	fs := newFlagSet("status")
	var projectPath string
	var global bool
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&global, "global", false, "show machine-wide daemon status")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	issue, err := parseOptionalSinglePositional(fs, args)
	if err != nil {
		return err
	}

	if global {
		if issue != "" {
			return fmt.Errorf("status ISSUE cannot be combined with --global")
		}
		projectPath = ""
	} else {
		projectPath, err = a.resolveProject(projectPath)
		if err != nil {
			return err
		}
	}

	if issue == "" {
		status, daemonBuildCommit, err := a.projectStatus(ctx, projectPath)
		if err != nil {
			return err
		}
		if jsonOutput {
			return writeJSON(a.stdout, status)
		}
		return writeProjectStatus(a.stdout, status, daemonBuildCommit, a.version)
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
	var callerPane string
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
	callerPane = strings.TrimSpace(os.Getenv(amuxPaneEnvVar))

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	result, err := a.daemon.Assign(ctx, daemon.AssignRequest{
		Project:    projectPath,
		Issue:      issue,
		Prompt:     prompt,
		Agent:      agent,
		CallerPane: callerPane,
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

func (a *App) runBatch(ctx context.Context, args []string) error {
	fs := newFlagSet("batch")
	var projectPath string
	var delay time.Duration
	var callerPane string
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.DurationVar(&delay, "delay", 5*time.Second, "delay between assigns")

	manifestPath, err := parseRequiredSinglePositional(fs, args, "batch requires MANIFEST")
	if err != nil {
		return err
	}
	if delay < 0 {
		return fmt.Errorf("batch delay must be non-negative")
	}

	entries, err := readBatchManifest(manifestPath)
	if err != nil {
		return err
	}
	if err := daemon.ValidateBatchEntries(entries); err != nil {
		return err
	}

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}
	callerPane = strings.TrimSpace(os.Getenv(amuxPaneEnvVar))

	result, err := a.daemon.Batch(ctx, daemon.BatchRequest{
		Project:    projectPath,
		Entries:    entries,
		Delay:      delay,
		CallerPane: callerPane,
	})
	if err != nil {
		return err
	}

	return a.writeBatchResult(result)
}

func (a *App) runSpawn(ctx context.Context, args []string) error {
	fs := newFlagSet("spawn")
	var projectPath string
	var session string
	var leadPane string
	var title string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.StringVar(&session, "session", "", "amux session name (defaults to AMUX_SESSION)")
	fs.StringVar(&leadPane, "lead-pane", "", "pane to split from")
	fs.StringVar(&title, "title", "", "pane title")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("spawn does not accept positional arguments")
	}

	projectPath, err := a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	if strings.TrimSpace(session) == "" {
		session = strings.TrimSpace(os.Getenv(amuxSessionEnvVar))
	}

	result, err := a.daemon.Spawn(ctx, daemon.SpawnPaneRequest{
		Project:  projectPath,
		Session:  session,
		LeadPane: leadPane,
		Title:    title,
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	_, err = fmt.Fprintf(a.stdout, "%s\t%s\n", result.PaneID, result.ClonePath)
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

func readBatchManifest(path string) ([]daemon.BatchEntry, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read batch manifest: %w", err)
	}

	var entries []daemon.BatchEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("decode batch manifest: %w", err)
	}
	return entries, nil
}

func (a *App) writeBatchResult(result daemon.BatchResult) error {
	for _, task := range result.Results {
		if _, err := fmt.Fprintf(a.stdout, "%s assigned to %s\n", task.Issue, task.Agent); err != nil {
			return err
		}
	}
	for _, failure := range result.Failures {
		if _, err := fmt.Fprintf(a.stderr, "%s failed: %s\n", failure.Issue, failure.Error); err != nil {
			return err
		}
	}
	if failures := len(result.Failures); failures > 0 {
		return fmt.Errorf("batch failed for %d %s", failures, pluralize("assignment", failures))
	}
	return nil
}

func pluralize(word string, count int) string {
	if count == 1 {
		return word
	}
	return word + "s"
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

func (a *App) runResume(ctx context.Context, args []string) error {
	fs := newFlagSet("resume")
	var projectPath string
	var prompt string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.StringVar(&prompt, "prompt", "", "instructions to send after resuming")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	issue, err := parseRequiredSinglePositional(fs, args, "resume requires ISSUE")
	if err != nil {
		return err
	}

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	result, err := a.daemon.Resume(ctx, daemon.ResumeRequest{
		Project: projectPath,
		Issue:   issue,
		Prompt:  strings.TrimSpace(prompt),
	})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	_, err = fmt.Fprintf(a.stdout, "%s resumed\n", result.Issue)
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

func isHelpToken(arg string) bool {
	// Support `orca help <cmd>` and the shorthand `orca <cmd> help`.
	return arg == "--help" || arg == "-h" || arg == "help"
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(value)
}

func (a *App) projectStatus(ctx context.Context, projectPath string) (state.ProjectStatus, string, error) {
	status, err := a.state.ProjectStatus(ctx, projectPath)
	if err != nil {
		return state.ProjectStatus{}, "", err
	}
	if status.Daemon == nil || status.Daemon.Status != "running" {
		return status, "", nil
	}

	rpcStatus, err := a.projectStatusRPC(ctx, projectPath)
	if err != nil {
		return status, "", nil
	}
	return rpcStatus.ProjectStatus, strings.TrimSpace(rpcStatus.BuildCommit), nil
}

func writeProjectStatus(w io.Writer, status state.ProjectStatus, daemonBuildCommit, installedBuildCommit string) error {
	return writeProjectStatusAt(w, status, daemonBuildCommit, installedBuildCommit, time.Now().UTC())
}

func writeProjectStatusAt(w io.Writer, status state.ProjectStatus, daemonBuildCommit, installedBuildCommit string, now time.Time) error {
	if _, err := fmt.Fprintf(w, "project: %s\n", status.Project); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "daemon: %s\n", daemonStatusText(status, daemonBuildCommit, installedBuildCommit)); err != nil {
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
		if shouldPrintHeartbeatAge(status.Daemon) {
			if _, err := fmt.Fprintf(w, "heartbeat age: %s\n", heartbeatAge(now, status.Daemon.UpdatedAt)); err != nil {
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

func shouldPrintHeartbeatAge(daemon *state.DaemonStatus) bool {
	if daemon == nil || daemon.UpdatedAt.IsZero() {
		return false
	}
	switch daemon.Status {
	case "running", "unhealthy":
		return true
	default:
		return false
	}
}

func heartbeatAge(now, heartbeatAt time.Time) string {
	if heartbeatAt.IsZero() || now.Before(heartbeatAt) {
		return "0s"
	}
	age := now.Sub(heartbeatAt)
	if age < time.Second {
		return "0s"
	}
	return age.Round(time.Second).String()
}

func defaultProjectStatusRPC(ctx context.Context, projectPath string) (daemon.ProjectStatusRPCResult, error) {
	paths, err := daemon.ResolvePaths()
	if err != nil {
		return daemon.ProjectStatusRPCResult{}, err
	}
	return daemon.ProjectStatusRPC(ctx, paths, projectPath)
}

func daemonStatusText(status state.ProjectStatus, daemonBuildCommit, installedBuildCommit string) string {
	daemonState := "stopped"
	if status.Daemon != nil && status.Daemon.Status != "" {
		daemonState = status.Daemon.Status
	}

	daemonBuildCommit = strings.TrimSpace(daemonBuildCommit)
	installedBuildCommit = strings.TrimSpace(installedBuildCommit)
	if daemonState != "running" || daemonBuildCommit == "" || installedBuildCommit == "" || daemonBuildCommit == installedBuildCommit {
		return daemonState
	}

	return fmt.Sprintf("%s (version %s, installed %s — restart recommended)", daemonState, daemonBuildCommit, installedBuildCommit)
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
	if warning := taskGitHubRateLimitWarning(taskStatus, time.Now().UTC()); warning != "" {
		if _, err := fmt.Fprintf(w, "%s\n", warning); err != nil {
			return err
		}
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
	if _, err := fmt.Fprintln(tw, "WORKER\tPANE\tAGENT\tSTATE\tISSUE\tCLONE\tLAST SEEN"); err != nil {
		return err
	}
	for _, worker := range workers {
		if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", fallback(worker.WorkerID), fallback(worker.CurrentPaneID), fallback(worker.Agent), fallback(worker.State), fallback(worker.Issue), fallback(worker.ClonePath), formatTimestamp(worker.LastSeenAt)); err != nil {
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
