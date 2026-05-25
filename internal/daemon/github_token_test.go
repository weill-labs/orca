package daemon

import (
	"context"
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

func TestMergeCommandEnvReplacesExistingValues(t *testing.T) {
	t.Parallel()

	got := mergeCommandEnv([]string{"PATH=/bin", "GH_TOKEN=old"}, []string{"GH_TOKEN=new", "OTHER=value"})

	if gotValue, want := envEntryValue(got, "PATH"), "/bin"; gotValue != want {
		t.Fatalf("PATH = %q, want %q", gotValue, want)
	}
	if gotValue, want := envEntryValue(got, "GH_TOKEN"), "new"; gotValue != want {
		t.Fatalf("GH_TOKEN = %q, want %q", gotValue, want)
	}
	if gotValue, want := envEntryValue(got, "OTHER"), "value"; gotValue != want {
		t.Fatalf("OTHER = %q, want %q", gotValue, want)
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
