package pool

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const cloneSetupHookPath = ".orca/setup.sh"

func (m *Manager) runCloneSetupHook(ctx context.Context, clonePath string) {
	scriptPath := filepath.Join(clonePath, cloneSetupHookPath)
	info, err := os.Stat(scriptPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return
		}

		m.logf("pool: inspect clone bootstrap hook %q: %v", scriptPath, err)
		return
	}
	if info.IsDir() {
		m.logf("pool: inspect clone bootstrap hook %q: path is a directory", scriptPath)
		return
	}

	output, err := runCloneSetupHookCommand(ctx, clonePath, scriptPath, m.project)
	if err != nil {
		logCloneSetupHookResult(m.logf, clonePath, output, fmt.Errorf("bash %s %s: %w", cloneSetupHookPath, m.project, err))
		return
	}

	logCloneSetupHookResult(m.logf, clonePath, output, nil)
}

func runCloneSetupHookCommand(ctx context.Context, clonePath, scriptPath, projectPath string) (string, error) {
	cmd := exec.CommandContext(ctx, "bash", scriptPath, projectPath)
	cmd.Dir = clonePath

	output, err := cmd.CombinedOutput()
	return string(output), err
}

func logCloneSetupHookResult(logf func(string, ...any), clonePath, output string, err error) {
	trimmedOutput := strings.TrimSpace(output)
	if err != nil {
		if trimmedOutput == "" {
			logf("pool: clone bootstrap hook failed for %q: %v", clonePath, err)
			return
		}

		logf("pool: clone bootstrap hook failed for %q: %v\n%s", clonePath, err, trimmedOutput)
		return
	}

	if trimmedOutput == "" {
		logf("pool: clone bootstrap hook completed for %q", clonePath)
		return
	}

	logf("pool: clone bootstrap hook completed for %q\n%s", clonePath, trimmedOutput)
}
