package cli

import (
	"fmt"
	"strconv"
	"strings"
)

func enqueueErrorWithProjectHint(err error, target, requestedProject, resolvedProject string) error {
	if err == nil {
		return nil
	}
	requestedProject = strings.TrimSpace(requestedProject)
	resolvedProject = strings.TrimSpace(resolvedProject)
	if requestedProject == "" || resolvedProject == "" || requestedProject == resolvedProject {
		return err
	}
	if !strings.Contains(err.Error(), "active assignment") || strings.Contains(err.Error(), "resolved from clone-pool path") {
		return err
	}
	return fmt.Errorf("%w; resolved from clone-pool path %q to project %q; retry with `%s`", err, requestedProject, resolvedProject, enqueueRetryCommand(target, resolvedProject))
}

func enqueueRetryCommand(target, projectPath string) string {
	return fmt.Sprintf("orca enqueue %s --project %s", cliShellToken(target), cliShellToken(projectPath))
}

func cliShellToken(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "''"
	}
	if strings.ContainsAny(value, " \t\n\"'\\$`") {
		return strconv.Quote(value)
	}
	return value
}
