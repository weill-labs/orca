package cli

import (
	"context"
	"fmt"

	"github.com/weill-labs/orca/internal/daemon"
)

func (a *App) runRefreshCodex(ctx context.Context, args []string) error {
	if handled, err := a.writeCommandHelp("refresh-codex", args); handled {
		return err
	}

	fs := newFlagSet("refresh-codex")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("refresh-codex does not accept positional arguments")
	}

	_, err := fmt.Fprintln(a.stdout, daemon.CodexRefreshCommand)
	return err
}
