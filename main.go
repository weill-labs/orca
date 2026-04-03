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

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, cli.UsageText())
		return 1
	}

	if args[0] == "__daemon-serve" {
		if err := runDaemonProcess(args[1:]); err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		return 0
	}

	if args[0] == "version" {
		fmt.Fprintf(stdout, "orca %s\n", resolvedBuildCommit())
		return 0
	}

	paths, err := daemon.ResolvePaths()
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	store, err := state.OpenSQLite(paths.StateDB)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer store.Close()

	controller, err := daemon.NewLocalController(daemon.ControllerOptions{
		Store: store,
		Paths: paths,
	})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}

	app := cli.New(cli.Options{
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
	var projectPath string
	var leadPane string
	var stateDB string
	var pidFile string

	fs.StringVar(&session, "session", "", "daemon session")
	fs.StringVar(&projectPath, "project", "", "project path")
	fs.StringVar(&leadPane, "lead-pane", "", "lead pane to split from")
	fs.StringVar(&stateDB, "state-db", "", "state db path")
	fs.StringVar(&pidFile, "pid-file", "", "pid file path")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if projectPath == "" {
		return fmt.Errorf("__daemon-serve requires --project")
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
		Project:  projectPath,
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
