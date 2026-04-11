package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"
)

type domainSettingsState struct {
	Items     []PublicDomainBinding `json:"items"`
	UpdatedAt time.Time             `json:"updated_at"`
}

type legacyRuntimeDomainSettings struct {
	DomainBindings []PublicDomainBinding `json:"domain_bindings,omitempty"`
}

func (s *Store) PublicDomainBindings(ctx context.Context) ([]PublicDomainBinding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if cached, ok := s.publicDomainBindings.Load().([]PublicDomainBinding); ok {
		return NormalizePublicDomainBindings(cached), nil
	}
	raw, err := s.metadata.getSyncState(domainSettingsStateName)
	if errors.Is(err, os.ErrNotExist) {
		return nil, os.ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	state := domainSettingsState{}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return nil, fmt.Errorf("decode domain settings: %w", err)
	}
	bindings := NormalizePublicDomainBindings(state.Items)
	if err := validatePublicDomainBindings(bindings); err != nil {
		return nil, err
	}
	s.publicDomainBindings.Store(bindings)
	return bindings, nil
}

func (s *Store) SetPublicDomainBindings(ctx context.Context, bindings []PublicDomainBinding) ([]PublicDomainBinding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalized := NormalizePublicDomainBindings(bindings)
	if err := validatePublicDomainBindings(normalized); err != nil {
		return nil, err
	}
	current, err := s.PublicDomainBindings(ctx)
	missing := errors.Is(err, os.ErrNotExist)
	if missing {
		current = []PublicDomainBinding{}
	} else if err != nil {
		return nil, err
	}
	if !missing && reflect.DeepEqual(current, normalized) {
		return normalized, nil
	}
	encoded, err := json.Marshal(domainSettingsState{Items: normalized, UpdatedAt: time.Now().UTC()})
	if err != nil {
		return nil, fmt.Errorf("encode domain settings: %w", err)
	}
	if err := s.metadata.upsertSyncState(domainSettingsStateName, string(encoded)); err != nil {
		return nil, err
	}
	if err := s.recordDomainUpdateEvent(normalized); err != nil {
		return nil, err
	}
	s.publicDomainBindings.Store(normalized)
	return normalized, nil
}

func (s *Store) ApplyReplicaDomainBindings(ctx context.Context, bindings []PublicDomainBinding) ([]PublicDomainBinding, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	normalized := NormalizePublicDomainBindings(bindings)
	if err := validatePublicDomainBindings(normalized); err != nil {
		return nil, err
	}
	current, err := s.PublicDomainBindings(ctx)
	missing := errors.Is(err, os.ErrNotExist)
	if missing {
		current = []PublicDomainBinding{}
	} else if err != nil {
		return nil, err
	}
	if !missing && reflect.DeepEqual(current, normalized) {
		return normalized, nil
	}
	encoded, err := json.Marshal(domainSettingsState{Items: normalized, UpdatedAt: time.Now().UTC()})
	if err != nil {
		return nil, fmt.Errorf("encode replica domain settings: %w", err)
	}
	if err := s.metadata.upsertSyncState(domainSettingsStateName, string(encoded)); err != nil {
		return nil, err
	}
	s.publicDomainBindings.Store(normalized)
	return normalized, nil
}

func (s *Store) bootstrapDomainSettings() error {
	ctx := context.Background()
	bindings, err := s.PublicDomainBindings(ctx)
	if err == nil {
		s.publicDomainBindings.Store(bindings)
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	legacyBindings, err := s.legacyPublicDomainBindings()
	if err != nil {
		return err
	}
	_, err = s.SetPublicDomainBindings(ctx, legacyBindings)
	return err
}

func (s *Store) legacyPublicDomainBindings() ([]PublicDomainBinding, error) {
	raw, err := s.metadata.getSyncState(runtimeSettingsStateName)
	if errors.Is(err, os.ErrNotExist) {
		return []PublicDomainBinding{}, nil
	}
	if err != nil {
		return nil, err
	}
	legacy := legacyRuntimeDomainSettings{}
	if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
		return nil, fmt.Errorf("decode legacy runtime domain settings: %w", err)
	}
	bindings := NormalizePublicDomainBindings(legacy.DomainBindings)
	if err := validatePublicDomainBindings(bindings); err != nil {
		return nil, err
	}
	return bindings, nil
}

func validatePublicDomainBindings(bindings []PublicDomainBinding) error {
	for index, binding := range NormalizePublicDomainBindings(bindings) {
		if binding.Host == "" {
			return fmt.Errorf("settings.domain_bindings[%d].host must not be empty", index)
		}
		if binding.Bucket == "" {
			return fmt.Errorf("settings.domain_bindings[%d].bucket must not be empty", index)
		}
		if err := validateBucketName(binding.Bucket); err != nil {
			return fmt.Errorf("settings.domain_bindings[%d].bucket %w", index, err)
		}
		if binding.SPAFallback && !binding.IndexDocument {
			return fmt.Errorf("settings.domain_bindings[%d].spa_fallback requires index_document", index)
		}
	}
	seenHosts := make(map[string]int, len(bindings))
	for index, binding := range NormalizePublicDomainBindings(bindings) {
		if previous, ok := seenHosts[binding.Host]; ok {
			return fmt.Errorf("settings.domain_bindings[%d].host duplicates settings.domain_bindings[%d].host", index, previous)
		}
		seenHosts[binding.Host] = index
	}
	return nil
}
