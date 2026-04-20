package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	StateBackendSQLite   = "sqlite"
	StateBackendPostgres = "postgres"
)

type StateBackend struct {
	Kind       string
	SQLitePath string
	DSN        string
}

type rawGlobalConfig struct {
	State rawStateConfig `toml:"state"`
}

type rawStateConfig struct {
	DSN string `toml:"dsn"`
}

func ResolveStateBackend(defaultSQLitePath string) (StateBackend, error) {
	return resolveStateBackend(defaultSQLitePath, os.LookupEnv, os.ReadFile)
}

func ResolveConfiguredStateBackend(defaultSQLitePath string) (StateBackend, error) {
	return resolveStateBackend(defaultSQLitePath, func(string) (string, bool) {
		return "", false
	}, os.ReadFile)
}

func ResolveSQLitePath(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", fmt.Errorf("sqlite path is required")
	}
	if strings.HasPrefix(trimmed, "sqlite:") {
		parsed, err := url.Parse(trimmed)
		if err != nil {
			return "", fmt.Errorf("parse sqlite uri %q: %w", trimmed, err)
		}
		return sqlitePathFromURI(parsed)
	}
	return filepath.Clean(trimmed), nil
}

func resolveStateBackend(defaultSQLitePath string, lookupEnv func(string) (string, bool), readFile func(string) ([]byte, error)) (StateBackend, error) {
	if rawSQLitePath, ok := lookupEnv("ORCA_STATE_DB"); ok && strings.TrimSpace(rawSQLitePath) != "" {
		path, err := ResolveSQLitePath(rawSQLitePath)
		if err != nil {
			return StateBackend{}, err
		}
		return StateBackend{
			Kind:       StateBackendSQLite,
			SQLitePath: path,
		}, nil
	}

	if rawDSN, ok := lookupEnv("ORCA_STATE_DSN"); ok && strings.TrimSpace(rawDSN) != "" {
		return parseStateBackend(rawDSN, "ORCA_STATE_DSN")
	}

	configPath := stateConfigPath(defaultSQLitePath)
	data, err := readFile(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return StateBackend{}, missingStateBackendConfigError(configPath)
		}
		return StateBackend{}, fmt.Errorf("read state config %s: %w", configPath, err)
	}

	var rawConfig rawGlobalConfig
	if _, err := toml.Decode(string(data), &rawConfig); err != nil {
		return StateBackend{}, fmt.Errorf("decode state config %s: %w", configPath, err)
	}
	if strings.TrimSpace(rawConfig.State.DSN) == "" {
		return StateBackend{}, missingStateBackendConfigError(configPath)
	}

	return parseStateBackend(rawConfig.State.DSN, configPath)
}

func parseStateBackend(rawValue, source string) (StateBackend, error) {
	trimmed := strings.TrimSpace(rawValue)
	if trimmed == "" {
		return StateBackend{}, fmt.Errorf("state backend in %s is empty", source)
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return StateBackend{}, fmt.Errorf("parse state backend from %s: %w", source, err)
	}

	switch strings.ToLower(parsed.Scheme) {
	case "sqlite":
		path, err := sqlitePathFromURI(parsed)
		if err != nil {
			return StateBackend{}, err
		}
		return StateBackend{
			Kind:       StateBackendSQLite,
			SQLitePath: path,
		}, nil
	case "postgres", "postgresql":
		return StateBackend{
			Kind: StateBackendPostgres,
			DSN:  trimmed,
		}, nil
	default:
		return StateBackend{}, fmt.Errorf("state backend in %s must use postgres://... or sqlite:///absolute/path syntax", source)
	}
}

func sqlitePathFromURI(parsed *url.URL) (string, error) {
	if parsed == nil {
		return "", fmt.Errorf("sqlite uri is required")
	}
	if parsed.Host != "" {
		return "", fmt.Errorf("sqlite uri %q must use sqlite:///absolute/path syntax", parsed.String())
	}
	if !filepath.IsAbs(parsed.Path) {
		return "", fmt.Errorf("sqlite uri %q must include an absolute path", parsed.String())
	}
	return filepath.Clean(parsed.Path), nil
}

func stateConfigPath(defaultSQLitePath string) string {
	return filepath.Join(filepath.Dir(defaultSQLitePath), "config.toml")
}

func missingStateBackendConfigError(configPath string) error {
	return fmt.Errorf("state backend is not configured; run `make dev-postgres` or set [state].dsn in %s", configPath)
}
