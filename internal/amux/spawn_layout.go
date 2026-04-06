package amux

import "strings"

func spawnPlacementArgs(leadPane string) []string {
	args := []string{"--spiral"}
	if trimmedLeadPane := strings.TrimSpace(leadPane); trimmedLeadPane != "" {
		args = append(args, "--at", trimmedLeadPane)
	}
	return args
}
