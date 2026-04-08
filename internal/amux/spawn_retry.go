package amux

import (
	"context"
	"fmt"
	"regexp"
	"strings"
)

var createdWindowPattern = regexp.MustCompile(`(?m)^Created\s+(\S+)\s*$`)

func (c *CLIClient) spawnPane(ctx context.Context, session, atPane, name string) (Pane, error) {
	args := spawnPlacementArgs(atPane)
	args = append(args, "--name", name)

	output, err := c.run(ctx, session, "spawn", args...)
	if err != nil {
		return Pane{}, err
	}
	paneID, err := parseSpawnOutput(string(output))
	if err != nil {
		return Pane{}, err
	}
	return Pane{
		ID:   paneID,
		Name: name,
	}, nil
}

func (c *CLIClient) spawnPaneWithNewWindowFallback(ctx context.Context, session, atPane, name string) (Pane, error) {
	pane, err := c.spawnPane(ctx, session, atPane, name)
	if err == nil || !splitSpaceSpawnError(err) {
		return pane, err
	}

	windowOutput, windowErr := c.run(ctx, session, "new-window")
	if windowErr != nil {
		return Pane{}, fmt.Errorf("create fallback window after split-space failure: %w", windowErr)
	}

	pane, err = c.spawnPane(ctx, session, "", name)
	if err != nil {
		return Pane{}, err
	}
	pane.Window = parseCreatedWindowName(string(windowOutput))
	return pane, nil
}

func splitSpaceSpawnError(err error) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "exit status 1") && strings.Contains(message, "not enough space to split")
}

func parseCreatedWindowName(output string) string {
	match := createdWindowPattern.FindStringSubmatch(strings.TrimSpace(output))
	if len(match) != 2 {
		return ""
	}
	return match[1]
}
