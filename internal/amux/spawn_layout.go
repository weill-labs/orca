package amux

import "strings"

func spawnPlacementArgs(leadPane string) []string {
	if trimmedLeadPane := strings.TrimSpace(leadPane); trimmedLeadPane != "" {
		return []string{"--at", trimmedLeadPane}
	}
	return []string{"--auto"}
}
