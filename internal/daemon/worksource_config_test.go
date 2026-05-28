package daemon

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/worksource"
)

func TestLoadWorkSourceConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		content   string
		want      workSourceConfig
		wantError string
	}{
		{
			name: "missing project uses defaults",
			want: defaultWorkSourceConfig(),
		},
		{
			name: "repo config without worksource uses defaults",
			content: strings.Join([]string{
				"[agents.codex",
				`start_command = "codex --yolo"`,
				"",
			}, "\n"),
			want: defaultWorkSourceConfig(),
		},
		{
			name: "beads source config",
			content: strings.Join([]string{
				"[worksource]",
				"enabled = true",
				`source = "beads"`,
				`beads_bin = "bd-test"`,
				`agent = "claude"`,
				"",
			}, "\n"),
			want: workSourceConfig{
				Enabled:  true,
				Source:   workSourceBeads,
				BeadsBin: "bd-test",
				Agent:    "claude",
			},
		},
		{
			name: "notification pane config",
			content: strings.Join([]string{
				"[notifications]",
				`notification_pane = "pane-99"`,
				"",
			}, "\n"),
			want: workSourceConfig{
				Enabled:          false,
				Source:           workSourceManual,
				BeadsBin:         defaultBeadsBin,
				Agent:            defaultWorkSourceAgentProfile,
				NotificationPane: "pane-99",
			},
		},
		{
			name: "invalid toml errors when worksource section exists",
			content: strings.Join([]string{
				"[worksource]",
				"enabled =",
				"",
			}, "\n"),
			wantError: "decode repo config",
		},
		{
			name: "unknown source errors",
			content: strings.Join([]string{
				"[worksource]",
				`source = "linear"`,
				"",
			}, "\n"),
			wantError: "worksource.source",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			projectPath := ""
			if tt.content != "" {
				projectPath = writeWorkSourceConfigTestFile(t, tt.content)
			}

			got, err := loadWorkSourceConfig(projectPath)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("loadWorkSourceConfig() error = %v, want substring %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadWorkSourceConfig() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("loadWorkSourceConfig() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestNewWorkSourceFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cfg       workSourceConfig
		wantType  string
		wantError string
	}{
		{
			name:     "manual source",
			cfg:      workSourceConfig{Source: workSourceManual},
			wantType: "worksource.ManualSource",
		},
		{
			name:     "beads source",
			cfg:      workSourceConfig{Source: workSourceBeads, BeadsBin: "bd-test"},
			wantType: "*worksource.BeadsSource",
		},
		{
			name:      "invalid source",
			cfg:       workSourceConfig{Source: "linear"},
			wantError: "worksource.source",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newWorkSourceFromConfig(tt.cfg)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("newWorkSourceFromConfig() error = %v, want substring %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("newWorkSourceFromConfig() error = %v, want nil", err)
			}
			if gotType := reflect.TypeOf(got).String(); gotType != tt.wantType {
				t.Fatalf("newWorkSourceFromConfig() type = %q, want %q", gotType, tt.wantType)
			}
			var _ worksource.Source = got
		})
	}
}

func TestDaemonWorkSourceProject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		project    string
		envProject string
		want       string
		wantError  string
	}{
		{name: "empty"},
		{name: "request project wins", project: "$repo", envProject: "$otherRepo", want: "$repo"},
		{name: "env project fallback", envProject: "$repo", want: "$repo"},
		{name: "invalid request project", project: "\x00", wantError: "resolve daemon project"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			repo := newWorkSourceProjectTestRepo(t)
			otherRepo := newWorkSourceProjectTestRepo(t)
			projectPath := expandWorkSourceProjectTestPath(tt.project, repo, otherRepo)
			envProjectPath := expandWorkSourceProjectTestPath(tt.envProject, repo, otherRepo)
			want := expandWorkSourceProjectTestPath(tt.want, repo, otherRepo)

			got, err := daemonWorkSourceProject(projectPath, envProjectPath)
			if tt.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("daemonWorkSourceProject() error = %v, want substring %q", err, tt.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("daemonWorkSourceProject() error = %v, want nil", err)
			}
			if got != want {
				t.Fatalf("daemonWorkSourceProject() = %q, want %q", got, want)
			}
		})
	}
}

func newWorkSourceProjectTestRepo(t *testing.T) string {
	t.Helper()

	repo := t.TempDir()
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatalf("Mkdir(%q) error = %v", filepath.Join(repo, ".git"), err)
	}
	return repo
}

func expandWorkSourceProjectTestPath(path, repo, otherRepo string) string {
	path = strings.ReplaceAll(path, "$repo", repo)
	return strings.ReplaceAll(path, "$otherRepo", otherRepo)
}

func writeWorkSourceConfigTestFile(t *testing.T, content string) string {
	t.Helper()

	projectPath := t.TempDir()
	configDir := filepath.Join(projectPath, ".orca")
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", configDir, err)
	}
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}
	return projectPath
}
