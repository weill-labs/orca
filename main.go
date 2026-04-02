package main

import (
	"fmt"
	"os"
)

// BuildCommit is set by goreleaser at build time.
var BuildCommit string

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		commit := BuildCommit
		if commit == "" {
			commit = "dev"
		}
		fmt.Printf("orca %s\n", commit)
		return
	}

	fmt.Fprintln(os.Stderr, "orca: agent orchestration daemon")
	fmt.Fprintln(os.Stderr, "usage: orca <command>")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "commands:")
	fmt.Fprintln(os.Stderr, "  start    Start the orca daemon")
	fmt.Fprintln(os.Stderr, "  stop     Stop the orca daemon")
	fmt.Fprintln(os.Stderr, "  status   Show daemon and task status")
	fmt.Fprintln(os.Stderr, "  assign   Assign an issue to a worker")
	fmt.Fprintln(os.Stderr, "  cancel   Cancel a task")
	fmt.Fprintln(os.Stderr, "  workers  List workers and their state")
	fmt.Fprintln(os.Stderr, "  pool     List clone pool status")
	fmt.Fprintln(os.Stderr, "  version  Print version")
	os.Exit(1)
}
