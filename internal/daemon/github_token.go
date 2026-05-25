package daemon

import (
	"context"
	"strings"
)

type envCommandRunner interface {
	RunWithEnv(ctx context.Context, dir, name string, env []string, args ...string) ([]byte, error)
}

type gitHubTokenCommandRunner struct {
	base  CommandRunner
	token string
}

func newGitHubTokenCommandRunner(base CommandRunner, token string) CommandRunner {
	token = strings.TrimSpace(token)
	if base == nil || token == "" {
		return base
	}
	return gitHubTokenCommandRunner{base: base, token: token}
}

func (r gitHubTokenCommandRunner) Run(ctx context.Context, dir, name string, args ...string) ([]byte, error) {
	if name != "gh" {
		return r.base.Run(ctx, dir, name, args...)
	}
	if runner, ok := r.base.(envCommandRunner); ok {
		return runner.RunWithEnv(ctx, dir, name, []string{"GH_TOKEN=" + r.token}, args...)
	}
	return r.base.Run(ctx, dir, name, args...)
}
