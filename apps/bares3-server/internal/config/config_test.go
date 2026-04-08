package config

import "testing"

func TestValidateAllowsBundledS3CredentialsInDevelopment(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.Auth.Console.PasswordHash = "hash"
	cfg.Auth.Console.SessionSecret = "secret"
	cfg.Storage.TmpDir = "./tmp"
	cfg.Storage.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Storage.S3BaseURL = "http://127.0.0.1:9000"

	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected development defaults to validate, got %v", err)
	}
}

func TestValidateRejectsBundledS3CredentialsOutsideDevelopment(t *testing.T) {
	t.Parallel()

	cfg := Default()
	cfg.App.Env = "production"
	cfg.Auth.Console.PasswordHash = "hash"
	cfg.Auth.Console.SessionSecret = "secret"
	cfg.Storage.TmpDir = "./tmp"
	cfg.Storage.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Storage.S3BaseURL = "http://127.0.0.1:9000"

	if err := cfg.Validate(); err == nil {
		t.Fatalf("expected production config to reject bundled S3 credentials")
	}
}
