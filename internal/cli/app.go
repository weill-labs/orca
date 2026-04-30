package cli

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
	"github.com/weill-labs/orca/internal/project"
)

const defaultAgent = "codex"
const amuxSessionEnvVar = "AMUX_SESSION"
const amuxPaneEnvVar = "AMUX_PANE"
const cancelClientTimeout = 10 * time.Second

const usageText = `orca: agent orchestration daemon
usage: orca <command>

commands:
  start    Start the orca daemon
  stop     Stop the orca daemon
  reload   Hot-reload the orca daemon
  status   Show daemon and task status
  migrate-state  Copy state from SQLite to Postgres
  metrics  Show latency metrics from orchestration events
  reconcile  Detect task and pane drift
  assign   Assign an issue to a worker
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
var helpRegistry = map[string]string{
	"start": `usage: orca start [--session SESSION] [--project PATH] [--global] [--json]

Start the orca daemon.`,
	"stop": `usage: orca stop [--project PATH] [--force] [--global] [--json]

Stop the orca daemon.`,
	"reload": `usage: orca reload [--project PATH] [--global] [--json]

Hot-reload the orca daemon.`,
	"status": `usage: orca status [ISSUE] [--project PATH] [--global] [--all-hosts] [--json]

Show daemon and task status.`,
	"migrate-state": `usage: orca migrate-state --from sqlite:///path/to/state.db --to postgres://... [--dry-run] [--truncate]

Copy SQLite state into Postgres.

Flags:
  --from     Source state store URI
  --to       Destination state store URI
  --dry-run  Report what would be migrated without writing
  --truncate Wipe destination tables before copying`,
	"metrics": `usage: orca metrics [--project PATH] [--since DURATION] [--json]

Show latency metrics from orchestration events.

Flags:
  --project Project path
  --since   Lookback window (default: 7d)
  --json    Emit JSON output`,
	"reconcile": `usage: orca reconcile [--project PATH] [--fix] [--json]

Detect drift between orca tasks and amux panes.

Flags:
  --project Project path
  --fix     Complete safe automated recovery for merged PR cleanup only
  --json    Emit JSON output`,
	"assign": `usage: orca assign ISSUE --prompt TEXT [--agent NAME] [--project PATH] [--json]

Assign an issue to a worker.

Flags:
  --prompt  Task prompt
  --agent   Agent profile
  --project Project path
  --json    Emit JSON output`,
	"spawn": `usage: orca spawn [--project PATH] [--session SESSION] [--lead-pane PANE] [--title TITLE] [--agent NAME] [--prompt TEXT] [--json]

Open a clone in a new amux pane.`,
	"enqueue": `usage: orca enqueue PR_NUMBER [--project PATH] [--json]

Queue a PR for serialized landing.`,
	"cancel": `usage: orca cancel ISSUE [--project PATH] [--force] [--json]

Cancel a task.`,
	"resume": `usage: orca resume ISSUE [--project PATH] [--json]

Resume a task in its existing pane.`,
	"workers": `usage: orca workers [--project PATH] [--json]

List workers and their state.`,
	"pool": `usage: orca pool [--project PATH] [--json]

List clone pool status.`,
	"events": `usage: orca events [--project PATH]

Stream orchestration events as NDJSON.`,
	"help": `usage: orca help [COMMAND]

Show help for a command.`,
	"version": `usage: orca version

Print version.`,
}

var errInvalidOptions = errors.New("cli: invalid options")

type Options struct {
	Daemon             daemon.Controller
	State              state.Reader
	Stdout             io.Writer
	Stderr             io.Writer
	Version            string
	Cwd                func() (string, error)
	ProjectStatusRPC   func(context.Context, string) (daemon.ProjectStatusRPCResult, error)
	ResolvePaths       func() (daemon.Paths, error)
	ReadPIDFile        func(string) (int, error)
	ProcessAlive       func(int) (bool, error)
	ReadProcessEnviron func(int) ([]string, error)
}

type App struct {
	daemon             daemon.Controller
	state              state.Reader
	stdout             io.Writer
	stderr             io.Writer
	version            string
	cwd                func() (string, error)
	projectStatusRPC   func(context.Context, string) (daemon.ProjectStatusRPCResult, error)
	resolvePaths       func() (daemon.Paths, error)
	readPIDFile        func(string) (int, error)
	processAlive       func(int) (bool, error)
	readProcessEnviron func(int) ([]string, error)
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

	if len(args) == 2 && args[1] == "help" {
		return lookupHelpUsage(args[0])
	}

	if args[0] == "help" {
		if len(args) == 1 {
			return usageText, true
		}
		if isHelpFlag(args[1]) {
			return lookupHelpUsage("help")
		}
		return lookupHelpUsage(args[1])
	}

	if isHelpFlag(args[0]) {
		return usageText, true
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
	resolvePaths := options.ResolvePaths
	if resolvePaths == nil {
		resolvePaths = daemon.ResolvePaths
	}
	readPIDFile := options.ReadPIDFile
	if readPIDFile == nil {
		readPIDFile = defaultReadPIDFile
	}
	processAlive := options.ProcessAlive
	if processAlive == nil {
		processAlive = defaultProcessAlive
	}
	readProcessEnviron := options.ReadProcessEnviron
	if readProcessEnviron == nil {
		readProcessEnviron = defaultReadProcessEnviron
	}

	return &App{
		daemon:             options.Daemon,
		state:              options.State,
		stdout:             options.Stdout,
		stderr:             options.Stderr,
		version:            version,
		cwd:                options.Cwd,
		projectStatusRPC:   projectStatusRPC,
		resolvePaths:       resolvePaths,
		readPIDFile:        readPIDFile,
		processAlive:       processAlive,
		readProcessEnviron: readProcessEnviron,
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
		return a.runHelp(ctx, args[1:])
	case "start":
		return a.runStart(ctx, args[1:])
	case "stop":
		return a.runStop(ctx, args[1:])
	case "reload":
		return a.runReload(ctx, args[1:])
	case "status":
		return a.runStatus(ctx, args[1:])
	case "migrate-state":
		return a.runMigrateState(ctx, args[1:])
	case "metrics":
		return a.runMetrics(ctx, args[1:])
	case "reconcile":
		return a.runReconcile(ctx, args[1:])
	case "assign":
		return a.runAssign(ctx, args[1:])
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
		return a.runVersion(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usageText)
	}
}

func (a *App) runStart(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("start", args); handled {
		return err
	}

	fs := newFlagSet("start")
	var session string
	var projectPath string
	var global bool
	var jsonOutput bool
	fs.StringVar(&session, "session", "", "amux session name (defaults to AMUX_SESSION)")
	fs.StringVar(&projectPath, "project", "", "project path")
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
		resolvedProject, err = a.resolveStartProject(projectPath)
		if err != nil {
			return err
		}
	}

	if strings.TrimSpace(session) == "" {
		session = strings.TrimSpace(os.Getenv(amuxSessionEnvVar))
	}

	result, err := a.daemon.Start(ctx, daemon.StartRequest{
		Session: session,
		Project: resolvedProject,
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
	if handled, err := a.writeCommandHelp("stop", args); handled {
		return err
	}

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

func (a *App) runReload(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("reload", args); handled {
		return err
	}

	fs := newFlagSet("reload")
	var projectPath string
	var global bool
	var jsonOutput bool
	var err error
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&global, "global", false, "operate on the machine-wide daemon")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("reload does not accept positional arguments")
	}

	if !global {
		projectPath, err = a.resolveProject(projectPath)
		if err != nil {
			return err
		}
	}

	result, err := a.daemon.Reload(ctx, daemon.ReloadRequest{Project: projectPath})
	if err != nil {
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	if result.Project == "" {
		_, err = fmt.Fprintf(a.stdout, "reloaded global daemon (pid %d)\n", result.PID)
		return err
	}
	_, err = fmt.Fprintf(a.stdout, "reloaded daemon for %s (pid %d)\n", result.Project, result.PID)
	return err
}

func (a *App) runStatus(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("status", args); handled {
		return err
	}

	fs := newFlagSet("status")
	var projectPath string
	var global bool
	var allHosts bool
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&global, "global", false, "show machine-wide daemon status")
	fs.BoolVar(&allHosts, "all-hosts", false, "aggregate status across all hosts")
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
		if allHosts {
			reader, ok := a.state.(state.AllHostsReader)
			if !ok {
				return fmt.Errorf("status --all-hosts is not supported by this state backend")
			}
			status, err := reader.ProjectStatusAllHosts(ctx, projectPath)
			if err != nil {
				return err
			}
			if jsonOutput {
				return writeJSON(a.stdout, status)
			}
			return writeProjectStatus(a.stdout, status, "", a.version)
		}

		projectStatus, err := a.projectStatus(ctx, projectPath)
		if err != nil {
			return err
		}
		if projectStatus.warning != "" {
			if _, err := fmt.Fprintln(a.stderr, projectStatus.warning); err != nil {
				return err
			}
		}
		if jsonOutput {
			return writeJSON(a.stdout, projectStatus.status)
		}
		return writeProjectStatus(a.stdout, projectStatus.status, projectStatus.daemonBuildCommit, a.version)
	}

	issue = daemon.NormalizeIssueIdentifier(issue)
	if allHosts {
		reader, ok := a.state.(state.AllHostsReader)
		if !ok {
			return fmt.Errorf("status --all-hosts is not supported by this state backend")
		}
		taskStatus, err := reader.TaskStatusAllHosts(ctx, projectPath, issue)
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
	if handled, err := a.writeCommandHelp("assign", args); handled {
		return err
	}

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

func (a *App) runSpawn(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("spawn", args); handled {
		return err
	}

	fs := newFlagSet("spawn")
	var projectPath string
	var session string
	var leadPane string
	var title string
	var agent string
	var prompt string
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.StringVar(&session, "session", "", "amux session name (defaults to AMUX_SESSION)")
	fs.StringVar(&leadPane, "lead-pane", "", "pane to split from")
	fs.StringVar(&title, "title", "", "pane title")
	fs.StringVar(&agent, "agent", "", "agent profile to start in the pane")
	fs.StringVar(&prompt, "prompt", "", "prompt to send after the agent starts")
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
	if strings.TrimSpace(prompt) != "" && strings.TrimSpace(agent) == "" {
		return fmt.Errorf("spawn requires --agent when --prompt is set")
	}

	result, err := a.daemon.Spawn(ctx, daemon.SpawnPaneRequest{
		Project:  projectPath,
		Session:  session,
		LeadPane: leadPane,
		Title:    title,
		Agent:    agent,
		Prompt:   prompt,
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
	if handled, err := a.writeCommandHelp("enqueue", args); handled {
		return err
	}

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
	if handled, err := a.writeCommandHelp("cancel", args); handled {
		return err
	}

	fs := newFlagSet("cancel")
	var projectPath string
	var force bool
	var jsonOutput bool
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.BoolVar(&force, "force", false, "wait for the daemon without the 10s client timeout")
	fs.BoolVar(&jsonOutput, "json", false, "emit JSON output")

	issue, err := parseRequiredSinglePositional(fs, args, "cancel requires ISSUE")
	if err != nil {
		return err
	}

	projectPath, err = a.resolveProject(projectPath)
	if err != nil {
		return err
	}

	cancelCtx := ctx
	cancel := func() {}
	if !force {
		cancelCtx, cancel = context.WithTimeout(ctx, cancelClientTimeout)
	}
	defer cancel()

	result, err := a.daemon.Cancel(cancelCtx, daemon.CancelRequest{
		Project: projectPath,
		Issue:   issue,
	})
	if err != nil {
		if isCancelTimeoutError(err) {
			if force {
				return fmt.Errorf("cancel timed out waiting for the daemon; try `orca stop --force`")
			}
			return fmt.Errorf("cancel timed out after %s waiting for the daemon; try `orca cancel --force %s` or `orca stop --force`", cancelClientTimeout, issue)
		}
		return err
	}

	if jsonOutput {
		return writeJSON(a.stdout, result)
	}

	_, err = fmt.Fprintf(a.stdout, "%s cancelled\n", result.Issue)
	return err
}

func isCancelTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func (a *App) runResume(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("resume", args); handled {
		return err
	}

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
	if handled, err := a.writeCommandHelp("workers", args); handled {
		return err
	}

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
	if handled, err := a.writeCommandHelp("pool", args); handled {
		return err
	}

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
	if handled, err := a.writeCommandHelp("events", args); handled {
		return err
	}

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

func (a *App) runHelp(_ context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("help", args); handled {
		return err
	}
	if len(args) == 0 {
		_, err := fmt.Fprintln(a.stdout, usageText)
		return err
	}

	return writeCommandUsage(a.stdout, args[0])
}

func (a *App) runVersion(_ context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("version", args); handled {
		return err
	}

	_, err := fmt.Fprintf(a.stdout, "orca %s\n", a.version)
	return err
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

func (a *App) writeCommandHelp(command string, args []string) (bool, error) {
	if !wantsCommandHelp(args) {
		return false, nil
	}

	return true, writeCommandUsage(a.stdout, command)
}

func wantsCommandHelp(args []string) bool {
	if len(args) == 1 && args[0] == "help" {
		return true
	}

	for _, arg := range args {
		if isHelpFlag(arg) {
			return true
		}
	}

	return false
}

func lookupHelpUsage(command string) (string, bool) {
	usage, ok := helpRegistry[command]
	return usage, ok
}

func writeCommandUsage(w io.Writer, command string) error {
	usage, ok := lookupHelpUsage(command)
	if !ok {
		return fmt.Errorf("unknown help topic %q", command)
	}

	_, err := fmt.Fprintln(w, usage)
	return err
}

func isHelpFlag(arg string) bool {
	return arg == "--help" || arg == "-h"
}

func writeJSON(w io.Writer, value any) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(value)
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
		if strings.TrimSpace(status.Daemon.Session) != "" {
			if _, err := fmt.Fprintf(w, "session: %s\n", status.Daemon.Session); err != nil {
				return err
			}
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

	if len(status.Daemons) > 0 {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		if _, err := fmt.Fprintln(tw, "\nHOST\tSTATUS\tSESSION\tPID\tUPDATED"); err != nil {
			return err
		}
		for _, daemon := range status.Daemons {
			pid := "-"
			if daemon.PID > 0 {
				pid = strconv.Itoa(daemon.PID)
			}
			if _, err := fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", fallback(daemon.Host), daemon.Status, fallback(daemon.Session), pid, formatTimestamp(daemon.UpdatedAt)); err != nil {
				return err
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}
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
	daemonReason := ""
	if status.Daemon != nil && status.Daemon.Status != "" {
		daemonState = status.Daemon.Status
		daemonReason = strings.TrimSpace(status.Daemon.Reason)
	}
	if daemonState == "unhealthy" && daemonReason != "" {
		return fmt.Sprintf("%s (%s)", daemonState, daemonReason)
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
