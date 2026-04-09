package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"bares3-server/internal/config"
)

func DefaultRuntimeSettings(cfg config.Config) RuntimeSettings {
	settings := RuntimeSettings{
		PublicBaseURL:  strings.TrimSpace(cfg.Settings.PublicBaseURL),
		S3BaseURL:      strings.TrimSpace(cfg.Settings.S3BaseURL),
		Region:         strings.TrimSpace(cfg.Settings.Region),
		MetadataLayout: strings.TrimSpace(cfg.Settings.MetadataLayout),
		MaxBytes:       cfg.Settings.MaxBytes,
	}
	if settings.PublicBaseURL == "" {
		settings.PublicBaseURL = derivePublicBaseURL(cfg.Listen.File)
	}
	if settings.S3BaseURL == "" {
		settings.S3BaseURL = derivePublicBaseURL(cfg.Listen.S3)
	}
	if settings.Region == "" {
		settings.Region = "home-lab-1"
	}
	if settings.MetadataLayout == "" {
		settings.MetadataLayout = "hidden-dir"
	}
	return settings
}

func (s *Store) RuntimeSettings(ctx context.Context) (RuntimeSettings, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeSettings{}, err
	}
	if cached, ok := s.runtimeSettings.Load().(RuntimeSettings); ok && cached.Region != "" {
		return cached, nil
	}
	raw, err := s.metadata.getSyncState(runtimeSettingsStateName)
	if errors.Is(err, os.ErrNotExist) {
		return RuntimeSettings{}, os.ErrNotExist
	}
	if err != nil {
		return RuntimeSettings{}, err
	}
	settings := RuntimeSettings{}
	if err := json.Unmarshal([]byte(raw), &settings); err != nil {
		return RuntimeSettings{}, fmt.Errorf("decode runtime settings: %w", err)
	}
	if err := validateRuntimeSettings(settings); err != nil {
		return RuntimeSettings{}, err
	}
	s.runtimeSettings.Store(settings)
	return settings, nil
}

func (s *Store) SetRuntimeSettings(ctx context.Context, settings RuntimeSettings) (RuntimeSettings, error) {
	if err := ctx.Err(); err != nil {
		return RuntimeSettings{}, err
	}
	normalized := RuntimeSettings{
		PublicBaseURL:  strings.TrimSpace(settings.PublicBaseURL),
		S3BaseURL:      strings.TrimSpace(settings.S3BaseURL),
		Region:         strings.TrimSpace(settings.Region),
		MetadataLayout: strings.TrimSpace(settings.MetadataLayout),
		MaxBytes:       settings.MaxBytes,
		UpdatedAt:      time.Now().UTC(),
	}
	if err := validateRuntimeSettings(normalized); err != nil {
		return RuntimeSettings{}, err
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return RuntimeSettings{}, fmt.Errorf("encode runtime settings: %w", err)
	}
	if err := s.metadata.upsertSyncState(runtimeSettingsStateName, string(encoded)); err != nil {
		return RuntimeSettings{}, err
	}
	s.runtimeSettings.Store(normalized)
	s.instanceQuota.Store(normalized.MaxBytes)
	s.metadataLayout = normalized.MetadataLayout
	return normalized, nil
}

func validateRuntimeSettings(settings RuntimeSettings) error {
	if strings.TrimSpace(settings.PublicBaseURL) == "" {
		return errors.New("settings.public_base_url must not be empty")
	}
	if _, err := url.Parse(settings.PublicBaseURL); err != nil {
		return fmt.Errorf("settings.public_base_url is invalid: %w", err)
	}
	if strings.TrimSpace(settings.S3BaseURL) == "" {
		return errors.New("settings.s3_base_url must not be empty")
	}
	if _, err := url.Parse(settings.S3BaseURL); err != nil {
		return fmt.Errorf("settings.s3_base_url is invalid: %w", err)
	}
	if strings.TrimSpace(settings.Region) == "" {
		return errors.New("settings.region must not be empty")
	}
	if layout := strings.ToLower(strings.TrimSpace(settings.MetadataLayout)); layout != "hidden-dir" {
		return fmt.Errorf("settings.metadata_layout must be hidden-dir, got %q", settings.MetadataLayout)
	}
	if settings.MaxBytes < 0 {
		return errors.New("settings.max_bytes must not be negative")
	}
	return nil
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
