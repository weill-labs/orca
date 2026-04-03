package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

const (
	globalConfigPath  = ".config/orca/config.toml"
	projectConfigDir  = ".orca"
	projectConfigFile = "config.toml"
)

type Config struct {
	Daemon DaemonConfig
	Pool   PoolConfig
	Agents map[string]AgentProfile
}

type DaemonConfig struct {
	PollInterval     time.Duration
	NotificationPane string
}

type PoolConfig struct {
	Pattern     string
	CloneOrigin string
}

type AgentProfile struct {
	StartCommand      string
	PostmortemEnabled bool
	IdleTimeout       time.Duration
	StuckTimeout      time.Duration
	StuckTextPatterns []string
	NudgeCommand      string
	MaxNudgeRetries   int
}

func Load(projectDir string) (Config, error) {
	homeDir, err := currentHomeDir()
	if err != nil {
		return Config{}, fmt.Errorf("resolve home directory: %w", err)
	}

	globalPath := filepath.Join(homeDir, globalConfigPath)
	projectPath := filepath.Join(projectDir, projectConfigDir, projectConfigFile)
	return LoadFiles(globalPath, projectPath)
}

func LoadFiles(globalPath, projectPath string) (Config, error) {
	globalRaw, err := readConfigFile(globalPath)
	if err != nil {
		return Config{}, err
	}

	projectRaw, err := readConfigFile(projectPath)
	if err != nil {
		return Config{}, err
	}

	merged := globalRaw
	merged.merge(projectRaw)

	cfg, err := merged.toConfig()
	if err != nil {
		return Config{}, err
	}

	cfg.expandPaths()
	return cfg, nil
}

func (c *Config) expandPaths() {
	c.Pool.Pattern = expandHome(c.Pool.Pattern)
}

type rawConfig struct {
	Daemon *rawDaemonConfig           `toml:"daemon"`
	Pool   *rawPoolConfig             `toml:"pool"`
	Agents map[string]rawAgentProfile `toml:"agents"`
}

type rawDaemonConfig struct {
	PollInterval     *string `toml:"poll_interval"`
	NotificationPane *string `toml:"notification_pane"`
}

type rawPoolConfig struct {
	Pattern     *string `toml:"pattern"`
	CloneOrigin *string `toml:"clone_origin"`
}

type rawAgentProfile struct {
	StartCommand         *string  `toml:"start_command"`
	PostmortemEnabled    *bool    `toml:"postmortem_enabled"`
	IdleTimeout          *string  `toml:"idle_timeout"`
	StuckTimeout         *string  `toml:"stuck_timeout"`
	StuckTextPatterns    []string `toml:"stuck_text_patterns"`
	HasStuckTextPatterns bool     `toml:"-"`
	NudgeCommand         *string  `toml:"nudge_command"`
	MaxNudgeRetries      *int     `toml:"max_nudge_retries"`
}

func readConfigFile(path string) (rawConfig, error) {
	data, err := os.ReadFile(path)
	if err == nil {
		var cfg rawConfig
		meta, err := toml.Decode(string(data), &cfg)
		if err != nil {
			return rawConfig{}, fmt.Errorf("parse %s: %w", path, err)
		}
		markDefinedFields(&cfg, meta)
		return cfg, nil
	}

	if errors.Is(err, os.ErrNotExist) {
		return rawConfig{}, nil
	}

	return rawConfig{}, fmt.Errorf("read %s: %w", path, err)
}

func (r *rawConfig) merge(override rawConfig) {
	if override.Daemon != nil {
		if r.Daemon == nil {
			r.Daemon = &rawDaemonConfig{}
		}
		r.Daemon.merge(*override.Daemon)
	}

	if override.Pool != nil {
		if r.Pool == nil {
			r.Pool = &rawPoolConfig{}
		}
		r.Pool.merge(*override.Pool)
	}

	if len(override.Agents) > 0 {
		if r.Agents == nil {
			r.Agents = make(map[string]rawAgentProfile, len(override.Agents))
		}
		for name, profile := range override.Agents {
			current := r.Agents[name]
			current.merge(profile)
			r.Agents[name] = current
		}
	}
}

func (r *rawDaemonConfig) merge(override rawDaemonConfig) {
	if override.PollInterval != nil {
		r.PollInterval = override.PollInterval
	}
	if override.NotificationPane != nil {
		r.NotificationPane = override.NotificationPane
	}
}

func (r *rawPoolConfig) merge(override rawPoolConfig) {
	if override.Pattern != nil {
		r.Pattern = override.Pattern
	}
	if override.CloneOrigin != nil {
		r.CloneOrigin = override.CloneOrigin
	}
}

func (r *rawAgentProfile) merge(override rawAgentProfile) {
	if override.StartCommand != nil {
		r.StartCommand = override.StartCommand
	}
	if override.PostmortemEnabled != nil {
		r.PostmortemEnabled = override.PostmortemEnabled
	}
	if override.IdleTimeout != nil {
		r.IdleTimeout = override.IdleTimeout
	}
	if override.StuckTimeout != nil {
		r.StuckTimeout = override.StuckTimeout
	}
	if override.HasStuckTextPatterns {
		r.StuckTextPatterns = override.StuckTextPatterns
		r.HasStuckTextPatterns = true
	}
	if override.NudgeCommand != nil {
		r.NudgeCommand = override.NudgeCommand
	}
	if override.MaxNudgeRetries != nil {
		r.MaxNudgeRetries = override.MaxNudgeRetries
	}
}

func (r rawConfig) toConfig() (Config, error) {
	cfg := Config{}
	if len(r.Agents) > 0 {
		cfg.Agents = make(map[string]AgentProfile, len(r.Agents))
	}

	if r.Daemon != nil {
		var err error
		cfg.Daemon, err = r.Daemon.toConfig()
		if err != nil {
			return Config{}, err
		}
	}

	if r.Pool != nil {
		cfg.Pool = r.Pool.toConfig()
	}

	for name, profile := range r.Agents {
		parsed, err := profile.toConfig("agents." + name)
		if err != nil {
			return Config{}, err
		}
		cfg.Agents[name] = parsed
	}

	return cfg, nil
}

func (r rawDaemonConfig) toConfig() (DaemonConfig, error) {
	cfg := DaemonConfig{}

	if r.PollInterval != nil {
		d, err := time.ParseDuration(*r.PollInterval)
		if err != nil {
			return DaemonConfig{}, fmt.Errorf("parse daemon.poll_interval: %w", err)
		}
		cfg.PollInterval = d
	}
	if r.NotificationPane != nil {
		cfg.NotificationPane = *r.NotificationPane
	}

	return cfg, nil
}

func (r rawPoolConfig) toConfig() PoolConfig {
	cfg := PoolConfig{}
	if r.Pattern != nil {
		cfg.Pattern = *r.Pattern
	}
	if r.CloneOrigin != nil {
		cfg.CloneOrigin = *r.CloneOrigin
	}
	return cfg
}

func (r rawAgentProfile) toConfig(prefix string) (AgentProfile, error) {
	cfg := AgentProfile{}

	if r.StartCommand != nil {
		cfg.StartCommand = *r.StartCommand
	}
	if r.PostmortemEnabled != nil {
		cfg.PostmortemEnabled = *r.PostmortemEnabled
	}

	if r.IdleTimeout != nil {
		d, err := time.ParseDuration(*r.IdleTimeout)
		if err != nil {
			return AgentProfile{}, fmt.Errorf("parse %s.idle_timeout: %w", prefix, err)
		}
		cfg.IdleTimeout = d
	}

	if r.StuckTimeout != nil {
		d, err := time.ParseDuration(*r.StuckTimeout)
		if err != nil {
			return AgentProfile{}, fmt.Errorf("parse %s.stuck_timeout: %w", prefix, err)
		}
		cfg.StuckTimeout = d
	}

	if r.HasStuckTextPatterns {
		if len(r.StuckTextPatterns) == 0 {
			cfg.StuckTextPatterns = []string{}
		} else {
			cfg.StuckTextPatterns = append([]string(nil), r.StuckTextPatterns...)
		}
	}

	if r.NudgeCommand != nil {
		cfg.NudgeCommand = normalizeNudgeCommand(*r.NudgeCommand)
	}

	if r.MaxNudgeRetries != nil {
		cfg.MaxNudgeRetries = *r.MaxNudgeRetries
	}

	return cfg, nil
}

func normalizeNudgeCommand(command string) string {
	if command == "" {
		return ""
	}

	trimmedNewlines := strings.TrimRight(command, "\r\n")
	switch {
	case trimmedNewlines == "":
		return "Enter"
	case strings.EqualFold(trimmedNewlines, "y"):
		return "Enter"
	default:
		return command
	}
}

func expandHome(value string) string {
	if value == "" || value == "~" {
		if value == "~" {
			if homeDir, err := currentHomeDir(); err == nil {
				return homeDir
			}
		}
		return value
	}

	if !strings.HasPrefix(value, "~/") {
		return value
	}

	homeDir, err := currentHomeDir()
	if err != nil {
		return value
	}

	return filepath.Join(homeDir, strings.TrimPrefix(value, "~/"))
}

func currentHomeDir() (string, error) {
	if homeDir := strings.TrimSpace(os.Getenv("HOME")); homeDir != "" {
		return homeDir, nil
	}
	return os.UserHomeDir()
}

func markDefinedFields(cfg *rawConfig, meta toml.MetaData) {
	if cfg == nil {
		return
	}

	for name, profile := range cfg.Agents {
		if meta.IsDefined("agents", name, "stuck_text_patterns") {
			profile.HasStuckTextPatterns = true
			cfg.Agents[name] = profile
		}
	}
}
