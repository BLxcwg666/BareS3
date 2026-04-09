package s3creds

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	controlDirName        = ".bares3"
	credentialsFileName   = "s3-credentials.json"
	credentialSourceStore = "managed"
	credentialSourceBoot  = "config"
	accessKeyPrefix       = "BS3"
	accessKeyLength       = 20

	PermissionReadWrite = "read_write"
	PermissionReadOnly  = "read_only"
)

var (
	ErrCredentialNotFound = errors.New("s3 credential not found")
	ErrNoActiveCredential = errors.New("no active s3 credential")
	ErrInvalidLabel       = errors.New("invalid s3 credential label")
	ErrInvalidPermission  = errors.New("invalid s3 credential permission")
	ErrCredentialActive   = errors.New("s3 credential must be revoked before deletion")
)

type BootstrapCredential struct {
	AccessKeyID     string
	SecretAccessKey string
}

type Credential struct {
	AccessKeyID     string     `json:"access_key_id"`
	SecretAccessKey string     `json:"secret_access_key"`
	Label           string     `json:"label,omitempty"`
	Source          string     `json:"source,omitempty"`
	Permission      string     `json:"permission,omitempty"`
	Buckets         []string   `json:"buckets,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	LastUsedAt      *time.Time `json:"last_used_at,omitempty"`
	RevokedAt       *time.Time `json:"revoked_at,omitempty"`
}

type PublicCredential struct {
	AccessKeyID string     `json:"access_key_id"`
	Label       string     `json:"label,omitempty"`
	Source      string     `json:"source,omitempty"`
	Permission  string     `json:"permission"`
	Buckets     []string   `json:"buckets"`
	CreatedAt   time.Time  `json:"created_at"`
	LastUsedAt  *time.Time `json:"last_used_at,omitempty"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
	Status      string     `json:"status"`
}

type CreateInput struct {
	Label      string
	Permission string
	Buckets    []string
}

type UpdateInput struct {
	AccessKeyID string
	Label       string
	Permission  string
	Buckets     []string
}

type Store struct {
	path   string
	logger *zap.Logger
	now    func() time.Time
	mu     sync.Mutex
}

func New(dataDir string, bootstrap BootstrapCredential, logger *zap.Logger) (*Store, error) {
	trimmed := strings.TrimSpace(dataDir)
	if trimmed == "" {
		return nil, fmt.Errorf("s3 credential data dir is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	dir := filepath.Join(trimmed, controlDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create s3 credential dir: %w", err)
	}

	store := &Store{
		path:   filepath.Join(dir, credentialsFileName),
		logger: logger,
		now:    time.Now,
	}
	if err := store.ensureBootstrap(bootstrap); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) List(ctx context.Context) ([]PublicCredential, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return nil, err
	}
	result := make([]PublicCredential, 0, len(items))
	for _, item := range items {
		result = append(result, item.Public())
	}
	return result, nil
}

func (s *Store) Create(ctx context.Context, input CreateInput) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}
	trimmedLabel := strings.TrimSpace(input.Label)
	if len(trimmedLabel) > 128 {
		return Credential{}, fmt.Errorf("%w: label must be 128 characters or fewer", ErrInvalidLabel)
	}
	permission := normalizePermission(input.Permission)
	if err := validatePermission(permission); err != nil {
		return Credential{}, err
	}
	buckets := normalizeBuckets(input.Buckets)

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return Credential{}, err
	}

	accessKeyID, err := generateAccessKeyID(existingAccessKeyIDs(items))
	if err != nil {
		return Credential{}, err
	}
	secretAccessKey, err := generateSecretAccessKey()
	if err != nil {
		return Credential{}, err
	}

	credential := Credential{
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		Label:           trimmedLabel,
		Source:          credentialSourceStore,
		Permission:      permission,
		Buckets:         buckets,
		CreatedAt:       s.now().UTC(),
	}
	items = append(items, credential)
	if err := s.writeAllLocked(items); err != nil {
		return Credential{}, err
	}

	s.logger.Info("s3 credential created", zap.String("access_key_id", credential.AccessKeyID), zap.String("label", credential.Label))
	return credential, nil
}

func (s *Store) Update(ctx context.Context, input UpdateInput) (PublicCredential, error) {
	if err := ctx.Err(); err != nil {
		return PublicCredential{}, err
	}
	trimmedAccessKeyID := strings.TrimSpace(input.AccessKeyID)
	trimmedLabel := strings.TrimSpace(input.Label)
	if len(trimmedLabel) > 128 {
		return PublicCredential{}, fmt.Errorf("%w: label must be 128 characters or fewer", ErrInvalidLabel)
	}
	permission := normalizePermission(input.Permission)
	if err := validatePermission(permission); err != nil {
		return PublicCredential{}, err
	}
	buckets := normalizeBuckets(input.Buckets)

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return PublicCredential{}, err
	}
	for index := range items {
		if items[index].AccessKeyID != trimmedAccessKeyID {
			continue
		}
		items[index].Label = trimmedLabel
		items[index].Permission = permission
		items[index].Buckets = buckets
		if err := s.writeAllLocked(items); err != nil {
			return PublicCredential{}, err
		}
		s.logger.Info("s3 credential updated", zap.String("access_key_id", trimmedAccessKeyID), zap.String("permission", permission), zap.Int("bucket_count", len(buckets)))
		return items[index].Public(), nil
	}

	return PublicCredential{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmedAccessKeyID)
}

func (s *Store) Revoke(ctx context.Context, accessKeyID string) (PublicCredential, error) {
	if err := ctx.Err(); err != nil {
		return PublicCredential{}, err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return PublicCredential{}, err
	}

	for index := range items {
		if items[index].AccessKeyID != trimmed {
			continue
		}
		if items[index].RevokedAt == nil {
			now := s.now().UTC()
			items[index].RevokedAt = &now
			if err := s.writeAllLocked(items); err != nil {
				return PublicCredential{}, err
			}
			s.logger.Info("s3 credential revoked", zap.String("access_key_id", trimmed))
		}
		return items[index].Public(), nil
	}

	return PublicCredential{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmed)
}

func (s *Store) Delete(ctx context.Context, accessKeyID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return err
	}

	for index := range items {
		if items[index].AccessKeyID != trimmed {
			continue
		}
		if items[index].RevokedAt == nil {
			return fmt.Errorf("%w: %s", ErrCredentialActive, trimmed)
		}
		items = append(items[:index], items[index+1:]...)
		if err := s.writeAllLocked(items); err != nil {
			return err
		}
		s.logger.Info("s3 credential deleted", zap.String("access_key_id", trimmed))
		return nil
	}

	return fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmed)
}

func (s *Store) LookupSecret(ctx context.Context, accessKeyID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return "", err
	}
	for _, item := range items {
		if item.AccessKeyID == trimmed && item.RevokedAt == nil {
			return item.SecretAccessKey, nil
		}
	}
	return "", fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmed)
}

func (s *Store) GetActive(ctx context.Context, accessKeyID string) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return Credential{}, err
	}
	for _, item := range items {
		if item.AccessKeyID == trimmed && item.RevokedAt == nil {
			return item, nil
		}
	}
	return Credential{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmed)
}

func (s *Store) Touch(ctx context.Context, accessKeyID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return err
	}
	for index := range items {
		if items[index].AccessKeyID != trimmed || items[index].RevokedAt != nil {
			continue
		}
		now := s.now().UTC()
		items[index].LastUsedAt = &now
		return s.writeAllLocked(items)
	}
	return fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmed)
}

func (s *Store) DefaultCredential(ctx context.Context) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return Credential{}, err
	}
	for _, item := range items {
		if item.RevokedAt == nil {
			return item, nil
		}
	}
	return Credential{}, ErrNoActiveCredential
}

func (s *Store) FindForOperation(ctx context.Context, bucket string, write bool) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}
	trimmedBucket := strings.TrimSpace(bucket)

	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return Credential{}, err
	}
	for _, item := range items {
		if item.RevokedAt != nil || !item.AllowsBucket(trimmedBucket) || !item.AllowsOperation(write) {
			continue
		}
		return item, nil
	}
	return Credential{}, ErrNoActiveCredential
}

func (c Credential) Public() PublicCredential {
	return PublicCredential{
		AccessKeyID: c.AccessKeyID,
		Label:       c.Label,
		Source:      c.Source,
		Permission:  normalizePermission(c.Permission),
		Buckets:     normalizeBuckets(c.Buckets),
		CreatedAt:   c.CreatedAt,
		LastUsedAt:  c.LastUsedAt,
		RevokedAt:   c.RevokedAt,
		Status:      c.Status(),
	}
}

func (c Credential) AllowsBucket(bucket string) bool {
	buckets := normalizeBuckets(c.Buckets)
	if len(buckets) == 0 {
		return true
	}
	trimmedBucket := strings.TrimSpace(bucket)
	for _, candidate := range buckets {
		if candidate == trimmedBucket {
			return true
		}
	}
	return false
}

func (c Credential) AllowsOperation(write bool) bool {
	if !write {
		return true
	}
	return normalizePermission(c.Permission) == PermissionReadWrite
}

func (c Credential) Status() string {
	if c.RevokedAt != nil {
		return "revoked"
	}
	return "active"
}

func (s *Store) ensureBootstrap(bootstrap BootstrapCredential) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	items, err := s.readAllLocked()
	if err != nil {
		return err
	}
	if len(items) > 0 {
		return nil
	}
	if strings.TrimSpace(bootstrap.AccessKeyID) == "" || strings.TrimSpace(bootstrap.SecretAccessKey) == "" {
		return s.writeAllLocked(nil)
	}

	credential := Credential{
		AccessKeyID:     strings.TrimSpace(bootstrap.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(bootstrap.SecretAccessKey),
		Label:           "Imported from config",
		Source:          credentialSourceBoot,
		Permission:      PermissionReadWrite,
		CreatedAt:       s.now().UTC(),
	}
	if err := s.writeAllLocked([]Credential{credential}); err != nil {
		return err
	}
	s.logger.Info("bootstrapped s3 credential from config", zap.String("access_key_id", credential.AccessKeyID))
	return nil
}

func (s *Store) readAllLocked() ([]Credential, error) {
	content, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []Credential{}, nil
		}
		return nil, fmt.Errorf("read s3 credentials: %w", err)
	}
	if len(content) == 0 {
		return []Credential{}, nil
	}

	items := []Credential{}
	if err := json.Unmarshal(content, &items); err != nil {
		return nil, fmt.Errorf("decode s3 credentials: %w", err)
	}
	for index := range items {
		items[index].Permission = normalizePermission(items[index].Permission)
		items[index].Buckets = normalizeBuckets(items[index].Buckets)
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].RevokedAt == nil && items[j].RevokedAt != nil {
			return true
		}
		if items[i].RevokedAt != nil && items[j].RevokedAt == nil {
			return false
		}
		return items[i].CreatedAt.After(items[j].CreatedAt)
	})
	return items, nil
}

func (s *Store) writeAllLocked(items []Credential) error {
	content, err := json.MarshalIndent(items, "", "  ")
	if err != nil {
		return fmt.Errorf("encode s3 credentials: %w", err)
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), "s3-credentials-*.json")
	if err != nil {
		return fmt.Errorf("stage s3 credentials: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = os.Remove(tmpPath)
	}()
	if _, err := tmp.Write(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write staged s3 credentials: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close staged s3 credentials: %w", err)
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return fmt.Errorf("chmod staged s3 credentials: %w", err)
	}
	if err := replaceFile(tmpPath, s.path); err != nil {
		return fmt.Errorf("replace s3 credentials: %w", err)
	}
	return nil
}

func replaceFile(sourcePath, destinationPath string) error {
	if err := os.Rename(sourcePath, destinationPath); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrExist) && !errors.Is(err, os.ErrPermission) {
		if removeErr := os.Remove(destinationPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return err
		}
		return os.Rename(sourcePath, destinationPath)
	}
	if err := os.Remove(destinationPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(sourcePath, destinationPath)
}

func existingAccessKeyIDs(items []Credential) map[string]struct{} {
	set := make(map[string]struct{}, len(items))
	for _, item := range items {
		set[item.AccessKeyID] = struct{}{}
	}
	return set
}

func generateAccessKeyID(existing map[string]struct{}) (string, error) {
	encoder := base32.StdEncoding.WithPadding(base32.NoPadding)
	for range 16 {
		raw := make([]byte, 11)
		if _, err := rand.Read(raw); err != nil {
			return "", fmt.Errorf("generate access key id: %w", err)
		}
		value := accessKeyPrefix + strings.ToUpper(encoder.EncodeToString(raw))
		if len(value) > accessKeyLength {
			value = value[:accessKeyLength]
		}
		if _, ok := existing[value]; !ok {
			return value, nil
		}
	}
	return "", fmt.Errorf("generate access key id: exhausted retries")
}

func generateSecretAccessKey() (string, error) {
	raw := make([]byte, 30)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate secret access key: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func normalizePermission(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", PermissionReadWrite:
		return PermissionReadWrite
	case PermissionReadOnly:
		return PermissionReadOnly
	default:
		return strings.ToLower(strings.TrimSpace(value))
	}
}

func validatePermission(value string) error {
	switch normalizePermission(value) {
	case PermissionReadWrite, PermissionReadOnly:
		return nil
	default:
		return fmt.Errorf("%w: permission must be read_write or read_only", ErrInvalidPermission)
	}
}

func normalizeBuckets(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	sort.Strings(result)
	return result
}
