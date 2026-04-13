package relay

import (
	"fmt"
	"os"
	"strings"
)

const defaultPort = "8080"

type Config struct {
	WebhookSecret string
	RelayToken    string
	Port          string
}

func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		WebhookSecret: strings.TrimSpace(os.Getenv("WEBHOOK_SECRET")),
		RelayToken:    strings.TrimSpace(os.Getenv("ORCA_RELAY_TOKEN")),
		Port:          strings.TrimSpace(os.Getenv("PORT")),
	}
	if cfg.Port == "" {
		cfg.Port = defaultPort
	}

	var missing []string
	if cfg.WebhookSecret == "" {
		missing = append(missing, "WEBHOOK_SECRET")
	}
	if cfg.RelayToken == "" {
		missing = append(missing, "ORCA_RELAY_TOKEN")
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required env vars: %s", strings.Join(missing, ", "))
	}

	return cfg, nil
}

func (c Config) Addr() string {
	if strings.HasPrefix(c.Port, ":") {
		return c.Port
	}
	return ":" + c.Port
}
