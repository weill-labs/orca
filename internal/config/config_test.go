package config

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadFile(t *testing.T) {
	t.Parallel()

	type want struct {
		cfg Config
		err string
	}

	tests := []struct {
		name        string
		projectTOML string
		want        want
	}{
		{
			name: "missing repo config returns explicit error",
			want: want{err: ".orca/config.toml"},
		},
		{
			name: "loads project config",
			projectTOML: `
[daemon]
poll_interval = "30s"
notification_pane = "pane-9"

[pool]
pattern = "/tmp/project/amux*"
clone_origin = "git@github.com:weill-labs/orca.git"

[agents.codex]
start_command = "codex --yolo"
postmortem_enabled = true
idle_timeout = "30s"
stuck_timeout = "9m"
stuck_text_patterns = ["tool denied", "approval required"]
go_based = false
nudge_command = "Enter"
max_nudge_retries = 3

[agents.aider]
start_command = "aider"
idle_timeout = "1m"
stuck_timeout = "10m"
stuck_text_patterns = []
go_based = true
nudge_command = "/run\n"
max_nudge_retries = 1
`,
			want: want{
				cfg: Config{
					Daemon: DaemonConfig{
						PollInterval:     30 * time.Second,
						NotificationPane: "pane-9",
					},
					Pool: PoolConfig{
						Pattern:     "/tmp/project/amux*",
						CloneOrigin: "git@github.com:weill-labs/orca.git",
					},
					Agents: map[string]AgentProfile{
						"aider": {
							StartCommand:      "aider",
							IdleTimeout:       time.Minute,
							StuckTimeout:      10 * time.Minute,
							StuckTextPatterns: []string{},
							GoBased:           true,
							NudgeCommand:      "/run\n",
							MaxNudgeRetries:   1,
						},
						"codex": {
							StartCommand:      "codex --yolo",
							PostmortemEnabled: true,
							IdleTimeout:       30 * time.Second,
							StuckTimeout:      9 * time.Minute,
							StuckTextPatterns: []string{"tool denied", "approval required"},
							GoBased:           false,
							NudgeCommand:      "Enter",
							MaxNudgeRetries:   3,
						},
					},
				},
			},
		},
		{
			name: "normalizes legacy enter-like nudge commands",
			projectTOML: `
[agents.codex]
nudge_command = "y\n"

[agents.claude]
nudge_command = "\n"
`,
			want: want{
				cfg: Config{
					Agents: map[string]AgentProfile{
						"claude": {
							NudgeCommand: "Enter",
						},
						"codex": {
							NudgeCommand: "Enter",
						},
					},
				},
			},
		},
		{
			name: "invalid duration returns error",
			projectTOML: `
[daemon]
poll_interval = "soon"
`,
			want: want{err: "parse daemon.poll_interval"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			projectPath := filepath.Join(t.TempDir(), ".orca", "config.toml")
			if tt.projectTOML != "" {
				writeFile(t, projectPath, tt.projectTOML)
			}

			cfg, err := LoadFile(projectPath)
			if tt.want.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.want.err) {
					t.Fatalf("expected error containing %q, got %v", tt.want.err, err)
				}
				if tt.projectTOML == "" && !errors.Is(err, ErrConfigNotFound) {
					t.Fatalf("expected ErrConfigNotFound, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadFile() error = %v", err)
			}

			if !reflect.DeepEqual(cfg, tt.want.cfg) {
				t.Fatalf("LoadFile() mismatch\nwant: %#v\ngot:  %#v", tt.want.cfg, cfg)
			}
		})
	}
}

func TestLoadUsesCanonicalRepoRoot(t *testing.T) {
	repoRoot := newRepoRoot(t)
	subdir := filepath.Join(repoRoot, "internal", "pkg")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", subdir, err)
	}

	homeDir := t.TempDir()
	writeFile(t, filepath.Join(repoRoot, ".orca", "config.toml"), `
[pool]
pattern = "~/project/*"
`)
	t.Setenv("HOME", homeDir)

	cfg, err := Load(subdir)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if got, want := cfg.Pool.Pattern, filepath.Join(homeDir, "project", "*"); got != want {
		t.Fatalf("Load() pattern = %q, want %q", got, want)
	}
}

func TestLoadMissingRepoConfigReturnsCanonicalPath(t *testing.T) {
	repoRoot := newRepoRoot(t)
	subdir := filepath.Join(repoRoot, "cmd", "orca")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", subdir, err)
	}

	_, err := Load(subdir)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("expected ErrConfigNotFound, got %v", err)
	}

	wantPath := filepath.Join(repoRoot, ".orca", "config.toml")
	if !strings.Contains(err.Error(), wantPath) {
		t.Fatalf("expected error containing %q, got %v", wantPath, err)
	}
}

func newRepoRoot(t *testing.T) string {
	t.Helper()

	root := filepath.Join(t.TempDir(), "repo")
	if err := os.MkdirAll(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatalf("MkdirAll(.git): %v", err)
	}

	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("EvalSymlinks(%q): %v", root, err)
	}
	return resolvedRoot
}

func writeFile(t *testing.T, path, contents string) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o644); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
}
