package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

func LoadEditable(explicitPath string) (Config, string, bool, error) {
	path, exists, err := resolveConfigPath(explicitPath, true)
	if err != nil {
		return Config{}, "", false, err
	}

	cfg := Default()
	if exists {
		content, err := os.ReadFile(path)
		if err != nil {
			return Config{}, "", false, fmt.Errorf("read config file: %w", err)
		}
		decoder := yaml.NewDecoder(strings.NewReader(string(content)))
		decoder.KnownFields(true)
		if err := decoder.Decode(&cfg); err != nil {
			return Config{}, "", false, fmt.Errorf("decode config file: %w", err)
		}
	}

	return cfg, path, exists, nil
}

func Save(path string, cfg Config) error {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return fmt.Errorf("config path is required")
	}

	output := cfg
	output.Runtime = RuntimeConfig{}
	content, err := yaml.Marshal(output)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(trimmed), 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	if err := os.WriteFile(trimmed, content, 0o600); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}
	return nil
}

func ResolveWritePath(explicitPath string) (string, error) {
	path, _, err := resolveConfigPath(explicitPath, true)
	return path, err
}
