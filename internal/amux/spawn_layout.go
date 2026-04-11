package amux

import "strings"

func spawnPlacementArgs(window string) []string {
	if w := strings.TrimSpace(window); w != "" {
		return []string{"--auto", "--window", w}
	}
	return []string{"--auto"}
}
