package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	ProductName           = "BareS3"
	defaultConfigFilename = "config.yml"
)

type Config struct {
	App     AppConfig     `yaml:"app"`
	Paths   PathsConfig   `yaml:"paths"`
	Listen  ListenConfig  `yaml:"listen"`
	Auth    AuthConfig    `yaml:"auth"`
	Storage StorageConfig `yaml:"storage"`
	Logging LoggingConfig `yaml:"logging"`
	Runtime RuntimeConfig `yaml:"-"`
}

type AppConfig struct {
	Env string `yaml:"env"`
}

type PathsConfig struct {
	DataDir string `yaml:"data_dir"`
	LogDir  string `yaml:"log_dir"`
}

type ListenConfig struct {
	Admin string `yaml:"admin"`
	S3    string `yaml:"s3"`
	File  string `yaml:"file"`
}

type AuthConfig struct {
	AdminUsername     string `yaml:"admin_username"`
	AdminPasswordHash string `yaml:"admin_password_hash"`
	JWTSecret         string `yaml:"jwt_secret"`
}

type StorageConfig struct {
	PublicBaseURL  string `yaml:"public_base_url"`
	Region         string `yaml:"region"`
	TmpDir         string `yaml:"tmp_dir"`
	MetadataLayout string `yaml:"metadata_layout"`
}

type LoggingConfig struct {
	Level        string `yaml:"level"`
	Format       string `yaml:"format"`
	RotateSizeMB int    `yaml:"rotate_size_mb"`
	RotateKeep   int    `yaml:"rotate_keep"`
}

type RuntimeConfig struct {
	BaseDir    string
	ConfigPath string
	ConfigUsed bool
}

func Default() Config {
	return Config{
		App: AppConfig{
			Env: "development",
		},
		Paths: PathsConfig{
			DataDir: "./data",
			LogDir:  "./logs",
		},
		Listen: ListenConfig{
			Admin: "127.0.0.1:19080",
			S3:    "0.0.0.0:9000",
			File:  "0.0.0.0:9001",
		},
		Auth: AuthConfig{
			AdminUsername: "admin",
		},
		Storage: StorageConfig{
			Region:         "home-lab-1",
			MetadataLayout: "hidden-dir",
		},
		Logging: LoggingConfig{
			Level:        "info",
			Format:       "pretty",
			RotateSizeMB: 16,
			RotateKeep:   10,
		},
	}
}

func Load(explicitPath string) (Config, error) {
	cfg := Default()
	lookupPath, exists, err := lookupConfigPath(explicitPath)
	if err != nil {
		return Config{}, err
	}

	baseDir := executableDir
	if exists {
		content, err := os.ReadFile(lookupPath)
		if err != nil {
			return Config{}, fmt.Errorf("read config file: %w", err)
		}

		decoder := yaml.NewDecoder(strings.NewReader(string(content)))
		decoder.KnownFields(true)
		if err := decoder.Decode(&cfg); err != nil {
			return Config{}, fmt.Errorf("decode config file: %w", err)
		}

		baseDir = filepath.Dir(lookupPath)
		cfg.Runtime.ConfigPath = lookupPath
		cfg.Runtime.ConfigUsed = true
	}

	cfg.Runtime.BaseDir = baseDir
	cfg.Paths.DataDir = resolvePath(baseDir, cfg.Paths.DataDir, "data")
	cfg.Paths.LogDir = resolvePath(baseDir, cfg.Paths.LogDir, "logs")

	if strings.TrimSpace(cfg.Storage.TmpDir) == "" {
		cfg.Storage.TmpDir = filepath.Join(cfg.Paths.DataDir, ".bares3", "tmp")
	} else {
		cfg.Storage.TmpDir = resolvePath(baseDir, cfg.Storage.TmpDir, "")
	}

	if strings.TrimSpace(cfg.Storage.PublicBaseURL) == "" {
		cfg.Storage.PublicBaseURL = derivePublicBaseURL(cfg.Listen.File)
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Validate() error {
	addresses := map[string]string{
		"listen.admin": c.Listen.Admin,
		"listen.s3":    c.Listen.S3,
		"listen.file":  c.Listen.File,
	}

	seen := make(map[string]string, len(addresses))
	for name, addr := range addresses {
		if _, err := net.ResolveTCPAddr("tcp", addr); err != nil {
			return fmt.Errorf("%s is invalid: %w", name, err)
		}
		if previous, ok := seen[addr]; ok {
			return fmt.Errorf("%s duplicates %s (%s)", name, previous, addr)
		}
		seen[addr] = name
	}

	if strings.TrimSpace(c.Paths.DataDir) == "" {
		return errors.New("paths.data_dir must not be empty")
	}
	if strings.TrimSpace(c.Paths.LogDir) == "" {
		return errors.New("paths.log_dir must not be empty")
	}
	if strings.TrimSpace(c.Storage.TmpDir) == "" {
		return errors.New("storage.tmp_dir must not be empty")
	}

	format := strings.ToLower(strings.TrimSpace(c.Logging.Format))
	if format != "" && format != "pretty" && format != "json" {
		return fmt.Errorf("logging.format must be pretty or json, got %q", c.Logging.Format)
	}

	if c.Logging.RotateSizeMB <= 0 {
		return errors.New("logging.rotate_size_mb must be greater than zero")
	}
	if c.Logging.RotateKeep < 0 {
		return errors.New("logging.rotate_keep must not be negative")
	}

	return nil
}

var executableDir = func() string {
	path, err := os.Executable()
	if err != nil {
		return "."
	}
	return filepath.Dir(path)
}()

func lookupConfigPath(explicitPath string) (string, bool, error) {
	if strings.TrimSpace(explicitPath) != "" {
		resolved, err := filepath.Abs(explicitPath)
		if err != nil {
			return "", false, fmt.Errorf("resolve explicit config path: %w", err)
		}
		if _, err := os.Stat(resolved); err != nil {
			return "", false, fmt.Errorf("stat explicit config path: %w", err)
		}
		return resolved, true, nil
	}

	path := filepath.Join(executableDir, defaultConfigFilename)
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return path, false, nil
		}
		return "", false, fmt.Errorf("stat implicit config path: %w", err)
	}
	return path, true, nil
}

func resolvePath(baseDir, value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		trimmed = fallback
	}
	if trimmed == "" {
		return ""
	}
	if filepath.IsAbs(trimmed) {
		return filepath.Clean(trimmed)
	}
	return filepath.Clean(filepath.Join(baseDir, trimmed))
}

func derivePublicBaseURL(listenAddr string) string {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return ""
	}
	if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
		host = "127.0.0.1"
	}
	return (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, port)}).String()
}
