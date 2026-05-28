package daemon

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/weill-labs/orca/internal/worksource"
)

const (
	workSourceManual = "manual"
	workSourceBeads  = "beads"
	defaultBeadsBin  = "bd"
)

type workSourceConfig struct {
	Enabled          bool
	Source           string
	BeadsBin         string
	Agent            string
	NotificationPane string
}

type rawRepoConfig struct {
	WorkSource    rawWorkSourceConfig    `toml:"worksource"`
	Notifications rawNotificationsConfig `toml:"notifications"`
}

type rawWorkSourceConfig struct {
	Enabled  bool   `toml:"enabled"`
	Source   string `toml:"source"`
	BeadsBin string `toml:"beads_bin"`
	Agent    string `toml:"agent"`
}

type rawNotificationsConfig struct {
	NotificationPane string `toml:"notification_pane"`
}

func loadWorkSourceConfig(projectPath string) (workSourceConfig, error) {
	cfg := defaultWorkSourceConfig()
	projectPath = strings.TrimSpace(projectPath)
	if projectPath == "" {
		return cfg, nil
	}

	configPath := filepath.Join(projectPath, ".orca", "config.toml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return cfg, nil
		}
		return workSourceConfig{}, fmt.Errorf("read repo config %s: %w", configPath, err)
	}
	if !hasRepoConfigSection(data, "worksource", "notifications") {
		return cfg, nil
	}

	var raw rawRepoConfig
	if _, err := toml.Decode(string(data), &raw); err != nil {
		return workSourceConfig{}, fmt.Errorf("decode repo config %s: %w", configPath, err)
	}

	cfg.Enabled = raw.WorkSource.Enabled
	if source := strings.ToLower(strings.TrimSpace(raw.WorkSource.Source)); source != "" {
		cfg.Source = source
	}
	if beadsBin := strings.TrimSpace(raw.WorkSource.BeadsBin); beadsBin != "" {
		cfg.BeadsBin = beadsBin
	}
	if agent := strings.TrimSpace(raw.WorkSource.Agent); agent != "" {
		cfg.Agent = agent
	}
	cfg.NotificationPane = strings.TrimSpace(raw.Notifications.NotificationPane)
	if err := validateWorkSourceConfig(cfg); err != nil {
		return workSourceConfig{}, fmt.Errorf("decode repo config %s: %w", configPath, err)
	}
	return cfg, nil
}

func hasRepoConfigSection(data []byte, sections ...string) bool {
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		for _, section := range sections {
			if strings.EqualFold(trimmed, "["+section+"]") {
				return true
			}
		}
	}
	return false
}

func defaultWorkSourceConfig() workSourceConfig {
	return workSourceConfig{
		Enabled:  false,
		Source:   workSourceManual,
		BeadsBin: defaultBeadsBin,
		Agent:    defaultWorkSourceAgentProfile,
	}
}

func validateWorkSourceConfig(cfg workSourceConfig) error {
	switch strings.ToLower(strings.TrimSpace(cfg.Source)) {
	case workSourceManual, workSourceBeads:
		return nil
	default:
		return fmt.Errorf("worksource.source must be %q or %q", workSourceManual, workSourceBeads)
	}
}

func newWorkSourceFromConfig(cfg workSourceConfig) (worksource.Source, error) {
	if err := validateWorkSourceConfig(cfg); err != nil {
		return nil, err
	}
	switch strings.ToLower(strings.TrimSpace(cfg.Source)) {
	case workSourceBeads:
		return worksource.NewBeadsSource(cfg.BeadsBin, nil), nil
	default:
		return worksource.ManualSource{}, nil
	}
}
