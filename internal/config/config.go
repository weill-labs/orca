package config

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// DetectOrigin returns the URL of the "origin" remote for the git repository
// at projectDir. It falls back to the ORCA_CLONE_ORIGIN environment variable
// if the git remote is not configured.
func DetectOrigin(projectDir string) (string, error) {
	if envOrigin := strings.TrimSpace(os.Getenv("ORCA_CLONE_ORIGIN")); envOrigin != "" {
		return envOrigin, nil
	}

	cmd := exec.Command("git", "-C", projectDir, "remote", "get-url", "origin")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("detect clone origin: no 'origin' remote in %s (set ORCA_CLONE_ORIGIN or run 'git remote add origin <url>'): %w", projectDir, err)
	}

	origin := strings.TrimSpace(string(out))
	if origin == "" {
		return "", fmt.Errorf("detect clone origin: 'origin' remote is empty in %s", projectDir)
	}

	return origin, nil
}
