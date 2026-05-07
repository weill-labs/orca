package pool_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/pool"
)

func TestManagerCreateCloneSetupHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
		verify func(t *testing.T, project, clonePath string, logs []string, manager *pool.Manager)
	}{
		{
			name: "runs setup hook once with parent repo path",
			script: `#!/usr/bin/env bash
set -euo pipefail
echo "hook parent: $1"
printf '%s' "$1" > .setup-parent
pwd > .setup-pwd
count=0
if [[ -f .setup-count ]]; then
  count=$(cat .setup-count)
fi
count=$((count + 1))
printf '%s' "$count" > .setup-count
`,
			verify: func(t *testing.T, project, clonePath string, logs []string, manager *pool.Manager) {
				t.Helper()

				if got, want := readTrimmedFile(t, filepath.Join(clonePath, ".setup-parent")), project; got != want {
					t.Fatalf("setup parent = %q, want %q", got, want)
				}
				if got, want := readTrimmedFile(t, filepath.Join(clonePath, ".setup-pwd")), clonePath; got != want {
					t.Fatalf("setup cwd = %q, want %q", got, want)
				}
				if got, want := readTrimmedFile(t, filepath.Join(clonePath, ".setup-count")), "1"; got != want {
					t.Fatalf("setup count after CreateClone() = %q, want %q", got, want)
				}
				if joined := strings.Join(logs, "\n"); !strings.Contains(joined, "hook parent: "+project) {
					t.Fatalf("logs = %q, want hook output with parent path", joined)
				}

				if _, err := manager.Allocate(context.Background(), "LAB-1008", "LAB-1008"); err != nil {
					t.Fatalf("Allocate() error = %v", err)
				}
				if got, want := readTrimmedFile(t, filepath.Join(clonePath, ".setup-count")), "1"; got != want {
					t.Fatalf("setup count after Allocate() = %q, want %q", got, want)
				}
			},
		},
		{
			name: "logs hook failure without blocking clone creation",
			script: `#!/usr/bin/env bash
set -euo pipefail
echo "stdout before failure"
echo "stderr before failure" >&2
exit 23
`,
			verify: func(t *testing.T, project, clonePath string, logs []string, manager *pool.Manager) {
				t.Helper()

				joined := strings.Join(logs, "\n")
				for _, want := range []string{
					"clone bootstrap hook failed",
					"stdout before failure",
					"stderr before failure",
					"exit status 23",
				} {
					if !strings.Contains(joined, want) {
						t.Fatalf("logs = %q, want substring %q", joined, want)
					}
				}
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root := t.TempDir()
			project := filepath.Join(root, "project")
			poolDir := filepath.Join(root, "pool")
			mustMkdir(t, poolDir)
			origin := newOriginWithSetupHook(t, "main", tt.script)
			store := newStore(t)

			logs := make([]string, 0, 4)
			manager := newManager(t, project, staticConfig{
				poolDir:     poolDir,
				cloneOrigin: origin,
			}, store, pool.WithLogf(func(format string, args ...any) {
				logs = append(logs, fmt.Sprintf(format, args...))
			}))

			clonePath, err := manager.CreateClone(context.Background())
			if err != nil {
				t.Fatalf("CreateClone() error = %v", err)
			}

			tt.verify(t, project, clonePath, logs, manager)
		})
	}
}

func newOriginWithSetupHook(t *testing.T, baseBranch, script string) string {
	t.Helper()

	root := t.TempDir()
	source := filepath.Join(root, "source")
	mustRun(t, "", "git", "init", "-b", baseBranch, source)
	mustRun(t, source, "git", "config", "user.name", "Orca Tests")
	mustRun(t, source, "git", "config", "user.email", "orca-tests@example.com")
	mustMkdir(t, filepath.Join(source, ".orca"))
	mustWriteFile(t, filepath.Join(source, "README.md"), "hello")
	mustWriteFile(t, filepath.Join(source, ".gitignore"), ".setup-count\n.setup-parent\n.setup-pwd\n")
	mustWriteFile(t, filepath.Join(source, ".orca", "setup.sh"), script)
	mustRun(t, source, "git", "add", "README.md", ".gitignore", ".orca/setup.sh")
	mustRun(t, source, "git", "commit", "-m", "initial commit")

	origin := filepath.Join(root, "origin.git")
	mustRun(t, "", "git", "clone", "--bare", source, origin)

	return origin
}

func readTrimmedFile(t *testing.T, path string) string {
	t.Helper()

	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}

	return strings.TrimSpace(string(contents))
}
