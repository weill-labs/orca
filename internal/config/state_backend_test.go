package config

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveStateBackendUsesConfigFileByDefault(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "")
	t.Setenv("ORCA_STATE_DSN", "")

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

func TestResolveStateBackendUsesExplicitPostgresOverride(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "")
	t.Setenv("ORCA_STATE_DSN", "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable")

	backend, err := ResolveStateBackend(filepath.Join(t.TempDir(), "state.db"))
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

func TestResolveStateBackendRequiresConfiguredPostgresDSN(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "")
	t.Setenv("ORCA_STATE_DSN", "")

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

func TestResolveStateBackendRejectsUnsupportedConfigDSN(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "")
	t.Setenv("ORCA_STATE_DSN", "")

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(strings.Join([]string{
		"[state]",
		`dsn = "mysql://localhost/orca"`,
		"",
	}, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}

	_, err := ResolveStateBackend(filepath.Join(configDir, "state.db"))
	if err == nil {
		t.Fatal("ResolveStateBackend() error = nil, want unsupported backend error")
	}
	if !strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("ResolveStateBackend() error = %q, want syntax guidance", err)
	}
}

func TestResolveConfiguredStateBackendIgnoresEnvironmentOverrides(t *testing.T) {
	t.Setenv("ORCA_STATE_DB", "/tmp/orca-shell.db")
	t.Setenv("ORCA_STATE_DSN", "sqlite:///tmp/ignored.db")

	configDir := t.TempDir()
	configPath := filepath.Join(configDir, "config.toml")
	if err := os.WriteFile(configPath, []byte(strings.Join([]string{
		"[state]",
		`dsn = "postgres://orca:orca@127.0.0.1:55432/orca?sslmode=disable"`,
		"",
	}, "\n")), 0o644); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", configPath, err)
	}

	backend, err := ResolveConfiguredStateBackend(filepath.Join(configDir, "state.db"))
	if err != nil {
		t.Fatalf("ResolveConfiguredStateBackend() error = %v", err)
	}
	if got, want := backend.Kind, StateBackendPostgres; got != want {
		t.Fatalf("backend.Kind = %q, want %q", got, want)
	}
}

func TestResolveStateBackendReturnsReadErrors(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("permission denied")
	_, err := resolveStateBackend("/tmp/orca/state.db", func(string) (string, bool) {
		return "", false
	}, func(string) ([]byte, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("resolveStateBackend() error = %v, want wrapped %v", err, wantErr)
	}
}

func TestResolveSQLitePathRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "empty path",
			raw:  "",
			want: "sqlite path is required",
		},
		{
			name: "sqlite uri with host",
			raw:  "sqlite://tmp/orca.db",
			want: "must use sqlite:///absolute/path syntax",
		},
		{
			name: "sqlite uri with relative path",
			raw:  "sqlite:relative/orca.db",
			want: "must include an absolute path",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := ResolveSQLitePath(tt.raw)
			if err == nil {
				t.Fatal("ResolveSQLitePath() error = nil, want non-nil")
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ResolveSQLitePath() error = %q, want substring %q", err, tt.want)
			}
		})
	}
}

func TestResolveSQLitePathAcceptsAbsoluteValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "absolute path",
			raw:  "/tmp/orca.db",
			want: "/tmp/orca.db",
		},
		{
			name: "sqlite uri",
			raw:  "sqlite:///tmp/orca.db",
			want: "/tmp/orca.db",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ResolveSQLitePath(tt.raw)
			if err != nil {
				t.Fatalf("ResolveSQLitePath() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("ResolveSQLitePath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestParseStateBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		source  string
		want    StateBackend
		wantErr string
	}{
		{
			name:   "sqlite backend",
			raw:    "sqlite:///tmp/orca.db",
			source: "config.toml",
			want: StateBackend{
				Kind:       StateBackendSQLite,
				SQLitePath: "/tmp/orca.db",
			},
		},
		{
			name:   "postgresql alias",
			raw:    "postgresql://orca:orca@127.0.0.1:55432/orca?sslmode=disable",
			source: "config.toml",
			want: StateBackend{
				Kind: StateBackendPostgres,
				DSN:  "postgresql://orca:orca@127.0.0.1:55432/orca?sslmode=disable",
			},
		},
		{
			name:    "empty backend",
			raw:     "   ",
			source:  "config.toml",
			wantErr: "state backend in config.toml is empty",
		},
		{
			name:    "invalid sqlite uri",
			raw:     "sqlite://tmp/orca.db",
			source:  "config.toml",
			wantErr: "must use sqlite:///absolute/path syntax",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseStateBackend(tt.raw, tt.source)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("parseStateBackend() error = nil, want non-nil")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseStateBackend() error = %q, want substring %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseStateBackend() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("parseStateBackend() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
