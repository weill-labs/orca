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
	"github.com/weill-labs/orca/internal/config"
	"github.com/weill-labs/orca/internal/daemon"
	state "github.com/weill-labs/orca/internal/daemonstate"
)

// BuildCommit is set at build time.
var BuildCommit string

var runDaemonServe = daemon.RunProcess

var (
	openSQLiteStateStore = func(path string) (stateStore, error) {
		return state.OpenSQLite(path)
	}
	openPostgresStateStore = func(dsn string) (stateStore, error) {
		return state.OpenPostgres(dsn)
	}
)

type stateStore interface {
	state.Store
	Close() error
}

type appRunner interface {
	Run(context.Context, []string) error
}

type runDependencies struct {
	resolvePaths     func() (daemon.Paths, error)
	bootstrapState   func(context.Context, string, daemon.Paths) error
	openStateStore   func(string) (stateStore, error)
	newController    func(daemon.ControllerOptions) (daemon.Controller, error)
	newApp           func(cli.Options) appRunner
	runMigrateState  func(context.Context, io.Writer, []string) error
	runDaemonProcess func([]string, string) error
}

var defaultRunDependencies = runDependencies{
	resolvePaths: daemon.ResolvePaths,
	bootstrapState: func(ctx context.Context, command string, paths daemon.Paths) error {
		return bootstrapLegacySQLiteState(ctx, command, paths, defaultStateBootstrapDeps)
	},
	openStateStore: openDefaultStateStore,
	newController: func(options daemon.ControllerOptions) (daemon.Controller, error) {
		return daemon.NewLocalController(options)
	},
	newApp: func(options cli.Options) appRunner {
		return cli.New(options)
	},
	runMigrateState:  cli.RunMigrateStateCommand,
	runDaemonProcess: runDaemonProcess,
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func openDefaultStateStore(path string) (stateStore, error) {
	backend, err := config.ResolveStateBackend(path)
	if err != nil {
		return nil, err
	}
	if backend.Kind == config.StateBackendSQLite {
		return openSQLiteStateStore(backend.SQLitePath)
	}
	return openPostgresStateStore(backend.DSN)
}

func run(args []string, stdout, stderr io.Writer) int {
	return runWithDeps(args, stdout, stderr, defaultRunDependencies, BuildCommit)
}

func runWithDeps(args []string, stdout, stderr io.Writer, deps runDependencies, buildCommit string) int {
	buildCommit = resolvedBuildCommit(buildCommit)

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
		if err := deps.runDaemonProcess(args[1:], buildCommit); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	if args[0] == "version" {
		fmt.Fprintf(stdout, "orca %s\n", buildCommit)
		return 0
	}
	if args[0] == "help" {
		fmt.Fprintf(stderr, "unknown help topic %q\n", args[1])
		return 1
	}
	if args[0] == "migrate-state" {
		if err := deps.runMigrateState(context.Background(), stdout, args[1:]); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	paths, err := deps.resolvePaths()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	if deps.bootstrapState != nil {
		if err := deps.bootstrapState(context.Background(), args[0], paths); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
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
		Version: buildCommit,
		Cwd:     os.Getwd,
	})

	if err := app.Run(context.Background(), args); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	return 0
}

func runDaemonProcess(args []string, buildCommit string) error {
	return runDaemonProcessWithServe(args, buildCommit, runDaemonServe)
}

func runDaemonProcessWithServe(args []string, buildCommit string, serve func(context.Context, daemon.ServeRequest) error) error {
	buildCommit = resolvedBuildCommit(buildCommit)

	fs := flag.NewFlagSet("__daemon-serve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var session string
	var stateDB string
	var pidFile string

	fs.StringVar(&session, "session", "", "daemon session")
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

	return serve(ctx, daemon.ServeRequest{
		Session:     session,
		StateDB:     stateDB,
		PIDFile:     pidFile,
		BuildCommit: buildCommit,
	})
}

func resolvedBuildCommit(buildCommit string) string {
	if buildCommit == "" {
		return "dev"
	}
	return buildCommit
}
