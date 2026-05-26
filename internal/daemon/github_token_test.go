package daemon

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestGitHubTokenCommandRunnerSetsGHTokenOnlyForGitHubCommands(t *testing.T) {
	t.Parallel()

	commands := newFakeCommands()
	runner := newGitHubTokenCommandRunner(commands, "daemon-token")

	commands.queue("gh", []string{"pr", "list", "--json", "number"}, `[]`, nil)
	if _, err := runner.Run(context.Background(), "/tmp/project", "gh", "pr", "list", "--json", "number"); err != nil {
		t.Fatalf("gh Run() error = %v", err)
	}
	if _, err := runner.Run(context.Background(), "/tmp/project", "git", "status", "--short"); err != nil {
		t.Fatalf("git Run() error = %v", err)
	}

	ghCalls := commands.callsByName("gh")
	if got, want := len(ghCalls), 1; got != want {
		t.Fatalf("gh call count = %d, want %d", got, want)
	}
	if got, want := commandCallEnvValue(ghCalls[0], "GH_TOKEN"), "daemon-token"; got != want {
		t.Fatalf("gh GH_TOKEN env = %q, want %q", got, want)
	}

	gitCalls := commands.callsByName("git")
	if got, want := len(gitCalls), 1; got != want {
		t.Fatalf("git call count = %d, want %d", got, want)
	}
	if got := commandCallEnvValue(gitCalls[0], "GH_TOKEN"); got != "" {
		t.Fatalf("git GH_TOKEN env = %q, want empty", got)
	}
}

func TestGitHubTokenCommandRunnerErrorsWhenBaseCannotInjectEnv(t *testing.T) {
	t.Parallel()

	base := &runOnlyCommandRunner{}
	runner := newGitHubTokenCommandRunner(base, "daemon-token")

	_, err := runner.Run(context.Background(), "/tmp/project", "gh", "pr", "list")
	if err == nil {
		t.Fatal("gh Run() error = nil, want env-capable runner error")
	}
	if got, want := err.Error(), "requires env-capable command runner"; !strings.Contains(got, want) {
		t.Fatalf("gh Run() error = %q, want substring %q", got, want)
	}
	if base.called {
		t.Fatal("base runner was called after token injection failed")
	}
}

func TestMergeCommandEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		base      []string
		overrides []string
		want      []string
	}{
		{
			name:      "replaces existing values",
			base:      []string{"PATH=/bin", "GH_TOKEN=old"},
			overrides: []string{"GH_TOKEN=new", "OTHER=value"},
			want:      []string{"PATH=/bin", "GH_TOKEN=new", "OTHER=value"},
		},
		{
			name:      "removes duplicate base keys before appending override",
			base:      []string{"GH_TOKEN=old-first", "PATH=/bin", "GH_TOKEN=old-second"},
			overrides: []string{"GH_TOKEN=new"},
			want:      []string{"PATH=/bin", "GH_TOKEN=new"},
		},
		{
			name:      "last valid override wins",
			base:      []string{"GH_TOKEN=old", "PATH=/bin"},
			overrides: []string{"GH_TOKEN=first", "GH_TOKEN=second"},
			want:      []string{"PATH=/bin", "GH_TOKEN=second"},
		},
		{
			name:      "skips invalid override entries",
			base:      []string{"PATH=/bin"},
			overrides: []string{"MISSING_SEPARATOR", "   =blank-key", "GH_TOKEN=new"},
			want:      []string{"PATH=/bin", "GH_TOKEN=new"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := mergeCommandEnv(tt.base, tt.overrides)
			if strings.Join(got, "\x00") != strings.Join(tt.want, "\x00") {
				t.Fatalf("mergeCommandEnv() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func commandCallEnvValue(call commandCall, key string) string {
	return envEntryValue(call.Env, key)
}

func envEntryValue(env []string, key string) string {
	prefix := key + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}

type runOnlyCommandRunner struct {
	called bool
	err    error
}

func (r *runOnlyCommandRunner) Run(context.Context, string, string, ...string) ([]byte, error) {
	r.called = true
	if r.err != nil {
		return nil, r.err
	}
	return nil, errors.New("base runner should not be called")
}
