package daemon

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

func (execCommandRunner) RunWithEnv(ctx context.Context, dir, name string, env []string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	if len(env) > 0 {
		cmd.Env = mergeCommandEnv(os.Environ(), env)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		if message := strings.TrimSpace(string(output)); message != "" {
			return output, fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, message)
		}
		return output, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}

	return output, nil
}

func mergeCommandEnv(base, overrides []string) []string {
	out := append([]string(nil), base...)
	for _, entry := range overrides {
		key, _, ok := strings.Cut(entry, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			continue
		}
		prefix := key + "="
		filtered := out[:0]
		for _, existing := range out {
			if !strings.HasPrefix(existing, prefix) {
				filtered = append(filtered, existing)
			}
		}
		out = append(filtered, entry)
	}
	return out
}
