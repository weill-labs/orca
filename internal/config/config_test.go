package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	type want struct {
		cfg Config
		err string
	}

	tests := []struct {
		name        string
		globalTOML  string
		projectTOML string
		want        want
	}{
		{
			name: "missing files returns zero config",
			want: want{cfg: Config{}},
		},
		{
			name: "project overrides global per field",
			globalTOML: `
[daemon]
poll_interval = "30s"
notification_pane = "pane-1"

[pool]
pattern = "/tmp/global/amux*"
clone_origin = "git@github.com:weill-labs/amux.git"

[agents.codex]
start_command = "codex --yolo"
postmortem_enabled = true
idle_timeout = "30s"
stuck_timeout = "5m"
stuck_text_patterns = ["permission prompt"]
nudge_command = "Enter"
max_nudge_retries = 3

[agents.claude]
start_command = "claude --dangerously-skip-permissions"
idle_timeout = "45s"
stuck_timeout = "7m"
stuck_text_patterns = ["tool denied"]
nudge_command = "Enter"
max_nudge_retries = 2
`,
			projectTOML: `
[daemon]
notification_pane = "pane-9"

[pool]
clone_origin = "git@github.com:weill-labs/orca.git"

[agents.codex]
stuck_timeout = "9m"
stuck_text_patterns = ["tool denied", "approval required"]

[agents.aider]
start_command = "aider"
idle_timeout = "1m"
stuck_timeout = "10m"
stuck_text_patterns = []
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
						Pattern:     "/tmp/global/amux*",
						CloneOrigin: "git@github.com:weill-labs/orca.git",
					},
					Agents: map[string]AgentProfile{
						"aider": {
							StartCommand:      "aider",
							IdleTimeout:       time.Minute,
							StuckTimeout:      10 * time.Minute,
							StuckTextPatterns: []string{},
							NudgeCommand:      "/run\n",
							MaxNudgeRetries:   1,
						},
						"claude": {
							StartCommand:      "claude --dangerously-skip-permissions",
							IdleTimeout:       45 * time.Second,
							StuckTimeout:      7 * time.Minute,
							StuckTextPatterns: []string{"tool denied"},
							NudgeCommand:      "Enter",
							MaxNudgeRetries:   2,
						},
						"codex": {
							StartCommand:      "codex --yolo",
							PostmortemEnabled: true,
							IdleTimeout:       30 * time.Second,
							StuckTimeout:      9 * time.Minute,
							StuckTextPatterns: []string{"tool denied", "approval required"},
							NudgeCommand:      "Enter",
							MaxNudgeRetries:   3,
						},
					},
				},
			},
		},
		{
			name: "normalizes legacy enter-like nudge commands",
			globalTOML: `
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
			globalTOML: `
[daemon]
poll_interval = "soon"
`,
			want: want{err: `parse daemon.poll_interval`},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			homeDir := t.TempDir()
			projectDir := filepath.Join(t.TempDir(), "repo")
			globalPath := filepath.Join(homeDir, ".config", "orca", "config.toml")
			projectPath := filepath.Join(projectDir, ".orca", "config.toml")

			if tt.globalTOML != "" {
				writeFile(t, globalPath, tt.globalTOML)
			}
			if tt.projectTOML != "" {
				writeFile(t, projectPath, tt.projectTOML)
			}

			cfg, err := LoadFiles(globalPath, projectPath)
			if tt.want.err != "" {
				if err == nil || !strings.Contains(err.Error(), tt.want.err) {
					t.Fatalf("expected error containing %q, got %v", tt.want.err, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}

			if !reflect.DeepEqual(cfg, tt.want.cfg) {
				t.Fatalf("Load() mismatch\nwant: %#v\ngot:  %#v", tt.want.cfg, cfg)
			}
		})
	}
}

func TestLoadUsesDefaultPaths(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(t.TempDir(), "repo")

	writeFile(t, filepath.Join(homeDir, ".config", "orca", "config.toml"), `
[pool]
pattern = "~/global/*"
`)
	writeFile(t, filepath.Join(projectDir, ".orca", "config.toml"), `
[pool]
pattern = "/tmp/project/*"
`)

	t.Setenv("HOME", homeDir)

	cfg, err := Load(projectDir)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.Pool.Pattern != "/tmp/project/*" {
		t.Fatalf("Load() pattern = %q, want %q", cfg.Pool.Pattern, "/tmp/project/*")
	}
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
