package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveStateBackendUsesConfigFileByDefault(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	stateDBPath := filepath.Join(configDir, "state.db")
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(strings.Join([]string{
		"[state]",
		`dsn = "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"`,
		"",
	}, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}

	backend, err := ResolveStateBackend(stateDBPath)
	if err != nil {
		t.Fatalf("ResolveStateBackend() error = %v", err)
	}
	if got, want := backend.Kind, StateBackendPostgres; got != want {
		t.Fatalf("backend.Kind = %q, want %q", got, want)
	}
	if got, want := backend.DSN, "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"; got != want {
		t.Fatalf("backend.DSN = %q, want %q", got, want)
	}
}

func TestResolveStateBackendUsesExplicitSQLiteOverride(t *testing.T) {
	t.Parallel()

	t.Setenv("ORCA_STATE_DB", "sqlite:///tmp/orca-shell.db")

	backend, err := ResolveStateBackend(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatalf("ResolveStateBackend() error = %v", err)
	}
	if got, want := backend.Kind, StateBackendSQLite; got != want {
		t.Fatalf("backend.Kind = %q, want %q", got, want)
	}
	if got, want := backend.SQLitePath, "/tmp/orca-shell.db"; got != want {
		t.Fatalf("backend.SQLitePath = %q, want %q", got, want)
	}
}

func TestResolveStateBackendRequiresConfiguredPostgresDSN(t *testing.T) {
	t.Parallel()

	configDir := t.TempDir()
	stateDBPath := filepath.Join(configDir, "state.db")

	_, err := ResolveStateBackend(stateDBPath)
	if err == nil {
		t.Fatal("ResolveStateBackend() error = nil, want missing config error")
	}
	if got, want := err.Error(), filepath.Join(configDir, "config.toml"); !strings.Contains(got, want) {
		t.Fatalf("ResolveStateBackend() error = %q, want mention of %q", got, want)
	}
	if !strings.Contains(err.Error(), "make dev-postgres") {
		t.Fatalf("ResolveStateBackend() error = %q, want setup hint", err)
	}
}
