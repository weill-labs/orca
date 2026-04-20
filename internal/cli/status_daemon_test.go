package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/weill-labs/orca/internal/daemon"
)

func TestBackendMismatchWarningForPIDUsesConfigBackends(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "")
	t.Setenv("ORCA_STATE_DSN", "")

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(strings.Join([]string{
		"[state]",
		`dsn = "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"`,
		"",
	}, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}

	warning := backendMismatchWarningForPID(42, func(pid int) ([]string, error) {
		if got, want := pid, 42; got != want {
			t.Fatalf("readProcessEnviron pid = %d, want %d", got, want)
		}
		return []string{"ORCA_STATE_DSN=postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"}, nil
	}, func() (daemon.Paths, error) {
		return daemon.Paths{StateDB: filepath.Join(configDir, "state.db")}, nil
	})
	if warning != "" {
		t.Fatalf("backendMismatchWarningForPID() = %q, want empty warning", warning)
	}
}

func TestBackendMismatchWarningForPIDFallsBackToDaemonConfigBackend(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "/tmp/orca-shell.db")
	t.Setenv("ORCA_STATE_DSN", "")

	daemonHome := t.TempDir()
	configPath := filepath.Join(daemonHome, ".config", "orca", "config.toml")
	if err := os.MkdirAll(filepath.Dir(configPath), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q) error = %v", filepath.Dir(configPath), err)
	}
	if err := os.WriteFile(configPath, []byte(strings.Join([]string{
		"[state]",
		`dsn = "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"`,
		"",
	}, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}

	warning := backendMismatchWarningForPID(42, func(pid int) ([]string, error) {
		if got, want := pid, 42; got != want {
			t.Fatalf("readProcessEnviron pid = %d, want %d", got, want)
		}
		return []string{"HOME=" + daemonHome}, nil
	}, func() (daemon.Paths, error) {
		t.Fatal("resolvePaths should not be used for daemon config fallback")
		return daemon.Paths{}, nil
	})

	if got, want := warning, "Warning: daemon is running on postgres but this shell reads sqlite. Update ~/.config/orca/config.toml or ORCA_STATE_* overrides so they match."; got != want {
		t.Fatalf("backendMismatchWarningForPID() = %q, want %q", got, want)
	}
}

func TestDaemonStateBackendUsesConfigDirOverride(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(strings.Join([]string{
		"[state]",
		`dsn = "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"`,
		"",
	}, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}

	if got, want := daemonStateBackend([]string{"ORCA_CONFIG_DIR=" + configDir}), stateBackendPostgres; got != want {
		t.Fatalf("daemonStateBackend() = %q, want %q", got, want)
	}
}

func TestDaemonConfigStateDBPathFallsBackToUserHome(t *testing.T) {
	t.Parallel()

	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatalf("UserHomeDir() error = %v", err)
	}

	got, ok := daemonConfigStateDBPath(nil)
	if !ok {
		t.Fatal("daemonConfigStateDBPath() ok = false, want true")
	}
	if want := filepath.Join(homeDir, ".config", "orca", "state.db"); got != want {
		t.Fatalf("daemonConfigStateDBPath() = %q, want %q", got, want)
	}
}

func TestDaemonStateBackendPrefersExplicitEnvironmentOverride(t *testing.T) {
	t.Parallel()

	if got, want := daemonStateBackend([]string{"ORCA_STATE_DB=/tmp/orca.db"}), stateBackendSQLite; got != want {
		t.Fatalf("daemonStateBackend() = %q, want %q", got, want)
	}
}

func TestCurrentStateBackendReturnsUnknownWhenPathsCannotResolve(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "")
	t.Setenv("ORCA_STATE_DSN", "")

	if got, want := currentStateBackend(func() (daemon.Paths, error) {
		return daemon.Paths{}, os.ErrPermission
	}), stateBackendUnknown; got != want {
		t.Fatalf("currentStateBackend() = %q, want %q", got, want)
	}
}

func TestStateBackendFromEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		env  []string
		want string
	}{
		{
			name: "sqlite override path",
			env:  []string{"ORCA_STATE_DB=/tmp/orca.db"},
			want: stateBackendSQLite,
		},
		{
			name: "sqlite dsn",
			env:  []string{"ORCA_STATE_DSN=sqlite:///tmp/orca.db"},
			want: stateBackendSQLite,
		},
		{
			name: "postgres dsn",
			env:  []string{"ORCA_STATE_DSN=postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"},
			want: stateBackendPostgres,
		},
		{
			name: "empty dsn",
			env:  []string{"ORCA_STATE_DSN=   "},
			want: stateBackendUnknown,
		},
		{
			name: "no overrides",
			env:  nil,
			want: stateBackendUnknown,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := stateBackendFromEnv(tt.env); got != tt.want {
				t.Fatalf("stateBackendFromEnv() = %q, want %q", got, tt.want)
			}
		})
	}
}
