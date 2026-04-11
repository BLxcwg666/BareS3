package config

import "testing"

func TestValidateAllowsDefaultConfiguration(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Auth.Console.PasswordHash = "hash"
	cfg.Auth.Console.SessionSecret = "secret"
	cfg.Paths.TmpDir = "./tmp"
	cfg.Settings.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Settings.S3BaseURL = "http://127.0.0.1:9000"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected default config to validate, got %v", err)
	}
}

func TestValidateAllowsConfigurationWithoutDeprecatedBlocks(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Auth.Console.PasswordHash = "hash"
	cfg.Auth.Console.SessionSecret = "secret"
	cfg.Paths.TmpDir = "./tmp"
	cfg.Settings.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Settings.S3BaseURL = "http://127.0.0.1:9000"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected config without deprecated blocks to validate, got %v", err)
	}
}
