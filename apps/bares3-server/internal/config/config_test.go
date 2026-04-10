package config

import "testing"

func TestValidateAllowsDefaultConfigurationWithoutS3Credentials(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Auth.Console.PasswordHash = "hash"
	cfg.Auth.Console.SessionSecret = "secret"
	cfg.Paths.TmpDir = "./tmp"
	cfg.Settings.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Settings.S3BaseURL = "http://127.0.0.1:9000"

	if cfg.Auth.S3.AccessKeyID != "" || cfg.Auth.S3.SecretAccessKey != "" {
		t.Fatalf("expected default config to leave S3 credentials unset, got %+v", cfg.Auth.S3)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected default config without S3 credentials to validate, got %v", err)
	}
}

func TestValidateRejectsBundledS3CredentialsOutsideDevelopment(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.App.Env = "production"
	cfg.Auth.Console.PasswordHash = "hash"
	cfg.Auth.Console.SessionSecret = "secret"
	cfg.Auth.S3.AccessKeyID = defaultDevAccessKeyID
	cfg.Auth.S3.SecretAccessKey = defaultDevSecretKey
	cfg.Paths.TmpDir = "./tmp"
	cfg.Settings.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Settings.S3BaseURL = "http://127.0.0.1:9000"

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected production config to reject bundled S3 credentials")
	}
}

func TestValidateAllowsEmptyS3Credentials(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.App.Env = "production"
	cfg.Auth.Console.PasswordHash = "hash"
	cfg.Auth.Console.SessionSecret = "secret"
	cfg.Auth.S3.AccessKeyID = ""
	cfg.Auth.S3.SecretAccessKey = ""
	cfg.Paths.TmpDir = "./tmp"
	cfg.Settings.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Settings.S3BaseURL = "http://127.0.0.1:9000"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected empty S3 credentials to validate, got %v", err)
	}
}

func TestValidateAllowsSyncToggle(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Auth.Console.PasswordHash = "hash"
	cfg.Auth.Console.SessionSecret = "secret"
	cfg.Paths.TmpDir = "./tmp"
	cfg.Settings.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Settings.S3BaseURL = "http://127.0.0.1:9000"
	cfg.Sync.Enabled = true

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected sync toggle to validate, got %v", err)
	}
}
