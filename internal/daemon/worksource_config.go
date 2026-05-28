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

	LandingModePR     = "pr"
	LandingModeDirect = "direct"
)

type workSourceConfig struct {
	Enabled          bool
	Source           string
	BeadsBin         string
	Agent            string
	NotificationPane string
}

type LandingConfig struct {
	Mode        string
	BaseBranch  string
	QualityGate string
}

type IntegrationConfig struct {
	GitHub bool
	Linear bool
}

type rawRepoConfig struct {
	WorkSource    rawWorkSourceConfig    `toml:"worksource"`
	Notifications rawNotificationsConfig `toml:"notifications"`
	Landing       rawLandingConfig       `toml:"landing"`
	Integrations  rawIntegrationConfig   `toml:"integrations"`
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

type rawLandingConfig struct {
	Mode        string `toml:"mode"`
	BaseBranch  string `toml:"base_branch"`
	QualityGate string `toml:"quality_gate"`
}

type rawIntegrationConfig struct {
	GitHub bool `toml:"github"`
	Linear bool `toml:"linear"`
}

func loadWorkSourceConfig(projectPath string) (workSourceConfig, error) {
	cfg, _, _, err := loadRepoConfig(projectPath)
	return cfg, err
}

func loadLandingConfig(projectPath string) (LandingConfig, error) {
	_, cfg, _, err := loadRepoConfig(projectPath)
	return cfg, err
}

func loadIntegrationConfig(projectPath string) (IntegrationConfig, error) {
	_, _, cfg, err := loadRepoConfig(projectPath)
	return cfg, err
}

func loadRepoConfig(projectPath string) (workSourceConfig, LandingConfig, IntegrationConfig, error) {
	workSource := defaultWorkSourceConfig()
	landing := defaultLandingConfig()
	integrations := defaultIntegrationConfig()
	projectPath = strings.TrimSpace(projectPath)
	if projectPath == "" {
		return workSource, landing, integrations, nil
	}

	configPath := filepath.Join(projectPath, ".orca", "config.toml")
	data, err := os.ReadFile(configPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return workSource, landing, integrations, nil
		}
		return workSourceConfig{}, LandingConfig{}, IntegrationConfig{}, fmt.Errorf("read repo config %s: %w", configPath, err)
	}
	if !hasRepoConfigSection(data, "worksource", "notifications", "landing", "integrations") {
		return workSource, landing, integrations, nil
	}

	var raw rawRepoConfig
	if _, err := toml.Decode(string(data), &raw); err != nil {
		return workSourceConfig{}, LandingConfig{}, IntegrationConfig{}, fmt.Errorf("decode repo config %s: %w", configPath, err)
	}

	workSource.Enabled = raw.WorkSource.Enabled
	if source := strings.ToLower(strings.TrimSpace(raw.WorkSource.Source)); source != "" {
		workSource.Source = source
	}
	if beadsBin := strings.TrimSpace(raw.WorkSource.BeadsBin); beadsBin != "" {
		workSource.BeadsBin = beadsBin
	}
	if agent := strings.TrimSpace(raw.WorkSource.Agent); agent != "" {
		workSource.Agent = agent
	}
	workSource.NotificationPane = strings.TrimSpace(raw.Notifications.NotificationPane)
	if mode := strings.ToLower(strings.TrimSpace(raw.Landing.Mode)); mode != "" {
		landing.Mode = mode
	}
	if baseBranch := strings.TrimSpace(raw.Landing.BaseBranch); baseBranch != "" {
		landing.BaseBranch = baseBranch
	}
	landing.QualityGate = strings.TrimSpace(raw.Landing.QualityGate)
	integrations.GitHub = raw.Integrations.GitHub
	integrations.Linear = raw.Integrations.Linear
	if err := validateWorkSourceConfig(workSource); err != nil {
		return workSourceConfig{}, LandingConfig{}, IntegrationConfig{}, fmt.Errorf("decode repo config %s: %w", configPath, err)
	}
	if err := validateLandingConfig(landing); err != nil {
		return workSourceConfig{}, LandingConfig{}, IntegrationConfig{}, fmt.Errorf("decode repo config %s: %w", configPath, err)
	}
	return workSource, landing, integrations, nil
}

func hasRepoConfigSection(data []byte, sections ...string) bool {
	for _, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(line)
		if index := strings.Index(trimmed, "#"); index >= 0 {
			trimmed = strings.TrimSpace(trimmed[:index])
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

func defaultLandingConfig() LandingConfig {
	return LandingConfig{
		Mode:       LandingModeDirect,
		BaseBranch: "main",
	}
}

func defaultIntegrationConfig() IntegrationConfig {
	return IntegrationConfig{}
}

func validateWorkSourceConfig(cfg workSourceConfig) error {
	switch strings.ToLower(strings.TrimSpace(cfg.Source)) {
	case workSourceManual, workSourceBeads:
		return nil
	default:
		return fmt.Errorf("worksource.source must be %q or %q", workSourceManual, workSourceBeads)
	}
}

func validateLandingConfig(cfg LandingConfig) error {
	switch strings.ToLower(strings.TrimSpace(cfg.Mode)) {
	case "", LandingModePR, LandingModeDirect:
	default:
		return fmt.Errorf("landing.mode must be %q or %q", LandingModePR, LandingModeDirect)
	}
	if strings.TrimSpace(cfg.BaseBranch) == "" {
		return fmt.Errorf("landing.base_branch must not be empty")
	}
	return nil
}

func normalizeLandingConfig(cfg LandingConfig) LandingConfig {
	out := defaultLandingConfig()
	if mode := strings.ToLower(strings.TrimSpace(cfg.Mode)); mode != "" {
		out.Mode = mode
	}
	if baseBranch := strings.TrimSpace(cfg.BaseBranch); baseBranch != "" {
		out.BaseBranch = baseBranch
	}
	out.QualityGate = strings.TrimSpace(cfg.QualityGate)
	return out
}

func (c LandingConfig) directMode() bool {
	return strings.EqualFold(strings.TrimSpace(c.Mode), LandingModeDirect)
}

func (c IntegrationConfig) githubEnabled() bool {
	return c.GitHub
}

func (c IntegrationConfig) linearEnabled() bool {
	return c.Linear
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
