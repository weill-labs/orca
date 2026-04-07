package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/weill-labs/orca/internal/cli"
	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

// BuildCommit is set by goreleaser at build time.
var BuildCommit string

type stateStore interface {
	state.Store
	Close() error
}

type appRunner interface {
	Run(context.Context, []string) error
}

type runDependencies struct {
	resolvePaths     func() (daemon.Paths, error)
	openStateStore   func(string) (stateStore, error)
	newController    func(daemon.ControllerOptions) (daemon.Controller, error)
	newApp           func(cli.Options) appRunner
	runDaemonProcess func([]string) error
}

var defaultRunDependencies = runDependencies{
	resolvePaths: daemon.ResolvePaths,
	openStateStore: func(path string) (stateStore, error) {
		return state.OpenSQLite(path)
	},
	newController: func(options daemon.ControllerOptions) (daemon.Controller, error) {
		return daemon.NewLocalController(options)
	},
	newApp: func(options cli.Options) appRunner {
		return cli.New(options)
	},
	runDaemonProcess: runDaemonProcess,
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	return runWithDeps(args, stdout, stderr, defaultRunDependencies)
}

func runWithDeps(args []string, stdout, stderr io.Writer, deps runDependencies) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, cli.UsageText())
		return 1
	}
	if handled, err := cli.WriteHelp(stdout, args); handled {
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	if args[0] == "__daemon-serve" {
		if err := deps.runDaemonProcess(args[1:]); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	if args[0] == "version" {
		fmt.Fprintf(stdout, "orca %s\n", resolvedBuildCommit())
		return 0
	}

	paths, err := deps.resolvePaths()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	store, err := deps.openStateStore(paths.StateDB)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer store.Close()

	controller, err := deps.newController(daemon.ControllerOptions{
		Store: store,
		Paths: paths,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	app := deps.newApp(cli.Options{
		Daemon:  controller,
		State:   store,
		Stdout:  stdout,
		Stderr:  stderr,
		Version: resolvedBuildCommit(),
		Cwd:     os.Getwd,
	})

	if err := app.Run(context.Background(), args); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	return 0
}

func runDaemonProcess(args []string) error {
	fs := flag.NewFlagSet("__daemon-serve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var session string
	var leadPane string
	var stateDB string
	var pidFile string

	fs.StringVar(&session, "session", "", "daemon session")
	fs.StringVar(&leadPane, "lead-pane", "", "lead pane to split from")
	fs.StringVar(&stateDB, "state-db", "", "state db path")
	fs.StringVar(&pidFile, "pid-file", "", "pid file path")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if stateDB == "" {
		return fmt.Errorf("__daemon-serve requires --state-db")
	}
	if pidFile == "" {
		return fmt.Errorf("__daemon-serve requires --pid-file")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	return daemon.RunProcess(ctx, daemon.ServeRequest{
		Session:  session,
		LeadPane: leadPane,
		StateDB:  stateDB,
		PIDFile:  pidFile,
	})
}

func resolvedBuildCommit() string {
	if BuildCommit == "" {
		return "dev"
	}
	return BuildCommit
}
