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
