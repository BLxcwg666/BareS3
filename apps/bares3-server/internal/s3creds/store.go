package s3creds

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"bares3-server/internal/statedb"
	"github.com/uptrace/bun"
	"go.uber.org/zap"
)

const (
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

	storeMigrations = []statedb.Migration{{
		Name: "s3_credentials_v1",
		Statements: []string{`
			CREATE TABLE IF NOT EXISTS s3_credentials (
				access_key_id TEXT PRIMARY KEY,
				secret_access_key TEXT NOT NULL,
				label TEXT NOT NULL DEFAULT '',
				source TEXT NOT NULL DEFAULT '',
				permission TEXT NOT NULL,
				buckets_json TEXT NOT NULL DEFAULT '[]',
				created_at TEXT NOT NULL,
				last_used_at TEXT,
				revoked_at TEXT
			)
		`},
	}}
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
	dataDir string
	db      *bun.DB
	logger  *zap.Logger
	now     func() time.Time
}

type credentialRecord struct {
	bun.BaseModel `bun:"table:s3_credentials"`

	AccessKeyID     string         `bun:"access_key_id,pk"`
	SecretAccessKey string         `bun:"secret_access_key"`
	Label           string         `bun:"label"`
	Source          string         `bun:"source"`
	Permission      string         `bun:"permission"`
	BucketsJSON     string         `bun:"buckets_json"`
	CreatedAt       string         `bun:"created_at"`
	LastUsedAt      sql.NullString `bun:"last_used_at"`
	RevokedAt       sql.NullString `bun:"revoked_at"`
}

func New(dataDir string, bootstrap BootstrapCredential, logger *zap.Logger) (*Store, error) {
	trimmed := strings.TrimSpace(dataDir)
	if trimmed == "" {
		return nil, fmt.Errorf("s3 credential data dir is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	sqlDB, err := statedb.Open(trimmed)
	if err != nil {
		return nil, err
	}
	bunDB := statedb.Wrap(sqlDB)
	store := &Store{dataDir: trimmed, db: bunDB, logger: logger, now: time.Now}

	if err := statedb.EnsureMigrations(sqlDB, storeMigrations); err != nil {
		_ = bunDB.Close()
		return nil, fmt.Errorf("initialize s3 credential schema: %w", err)
	}
	if err := store.ensureBootstrap(bunDB, bootstrap); err != nil {
		_ = bunDB.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *Store) List(ctx context.Context) ([]PublicCredential, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	records := make([]credentialRecord, 0)
	err = db.NewSelect().Model(&records).OrderExpr("revoked_at IS NOT NULL ASC, created_at DESC, access_key_id ASC").Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("list s3 credentials: %w", err)
	}

	result := make([]PublicCredential, 0, len(records))
	for _, record := range records {
		item, err := record.Credential()
		if err != nil {
			return nil, err
		}
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
	bucketsJSON, err := encodeBuckets(buckets)
	if err != nil {
		return Credential{}, err
	}

	db, err := s.openDB()
	if err != nil {
		return Credential{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	for range 16 {
		accessKeyID, err := generateAccessKeyID(nil)
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
		record := newCredentialRecord(credential)
		record.BucketsJSON = bucketsJSON
		_, err = db.NewInsert().Model(&record).Exec(ctx)
		if err == nil {
			s.logger.Info("s3 credential created", zap.String("access_key_id", credential.AccessKeyID), zap.String("label", credential.Label))
			return credential, nil
		}
		if !isUniqueConstraint(err) {
			return Credential{}, fmt.Errorf("insert s3 credential: %w", err)
		}
	}

	return Credential{}, fmt.Errorf("generate access key id: exhausted retries")
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
	bucketsJSON, err := encodeBuckets(buckets)
	if err != nil {
		return PublicCredential{}, err
	}

	db, err := s.openDB()
	if err != nil {
		return PublicCredential{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return PublicCredential{}, fmt.Errorf("begin s3 credential update: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	result, err := tx.NewUpdate().Model((*credentialRecord)(nil)).
		Set("label = ?", trimmedLabel).
		Set("permission = ?", permission).
		Set("buckets_json = ?", bucketsJSON).
		Where("access_key_id = ?", trimmedAccessKeyID).
		Exec(ctx)
	if err != nil {
		return PublicCredential{}, fmt.Errorf("update s3 credential: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return PublicCredential{}, fmt.Errorf("inspect s3 credential update: %w", err)
	}
	if rowsAffected == 0 {
		return PublicCredential{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmedAccessKeyID)
	}

	updated, err := loadCredential(ctx, tx, trimmedAccessKeyID)
	if err != nil {
		return PublicCredential{}, err
	}
	if err := tx.Commit(); err != nil {
		return PublicCredential{}, fmt.Errorf("commit s3 credential update: %w", err)
	}

	s.logger.Info("s3 credential updated", zap.String("access_key_id", trimmedAccessKeyID), zap.String("permission", permission), zap.Int("bucket_count", len(buckets)))
	return updated.Public(), nil
}

func (s *Store) Revoke(ctx context.Context, accessKeyID string) (PublicCredential, error) {
	if err := ctx.Err(); err != nil {
		return PublicCredential{}, err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	db, err := s.openDB()
	if err != nil {
		return PublicCredential{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return PublicCredential{}, fmt.Errorf("begin s3 credential revoke: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	credential, err := loadCredential(ctx, tx, trimmed)
	if err != nil {
		return PublicCredential{}, err
	}
	if credential.RevokedAt == nil {
		revokedAt := s.now().UTC()
		if _, err := tx.NewUpdate().Model((*credentialRecord)(nil)).
			Set("revoked_at = ?", formatTime(revokedAt)).
			Where("access_key_id = ?", trimmed).
			Exec(ctx); err != nil {
			return PublicCredential{}, fmt.Errorf("revoke s3 credential: %w", err)
		}
		credential.RevokedAt = &revokedAt
		s.logger.Info("s3 credential revoked", zap.String("access_key_id", trimmed))
	}
	if err := tx.Commit(); err != nil {
		return PublicCredential{}, fmt.Errorf("commit s3 credential revoke: %w", err)
	}
	return credential.Public(), nil
}

func (s *Store) Delete(ctx context.Context, accessKeyID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin s3 credential delete: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	credential, err := loadCredential(ctx, tx, trimmed)
	if err != nil {
		return err
	}
	if credential.RevokedAt == nil {
		return fmt.Errorf("%w: %s", ErrCredentialActive, trimmed)
	}
	if _, err := tx.NewDelete().Model((*credentialRecord)(nil)).Where("access_key_id = ?", trimmed).Exec(ctx); err != nil {
		return fmt.Errorf("delete s3 credential: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit s3 credential delete: %w", err)
	}

	s.logger.Info("s3 credential deleted", zap.String("access_key_id", trimmed))
	return nil
}

func (s *Store) LookupSecret(ctx context.Context, accessKeyID string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	db, err := s.openDB()
	if err != nil {
		return "", err
	}
	defer func() {
		_ = db.Close()
	}()

	record := new(credentialRecord)
	err = db.NewSelect().Model(record).
		Column("secret_access_key").
		Where("access_key_id = ?", trimmed).
		Where("revoked_at IS NULL").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmed)
	}
	if err != nil {
		return "", fmt.Errorf("lookup s3 credential secret: %w", err)
	}
	return record.SecretAccessKey, nil
}

func (s *Store) GetActive(ctx context.Context, accessKeyID string) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	db, err := s.openDB()
	if err != nil {
		return Credential{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	credential, err := loadCredential(ctx, db, trimmed)
	if err != nil {
		return Credential{}, err
	}
	if credential.RevokedAt != nil {
		return Credential{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmed)
	}
	return credential, nil
}

func (s *Store) Touch(ctx context.Context, accessKeyID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	trimmed := strings.TrimSpace(accessKeyID)

	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	result, err := db.NewUpdate().Model((*credentialRecord)(nil)).
		Set("last_used_at = ?", formatTime(s.now().UTC())).
		Where("access_key_id = ?", trimmed).
		Where("revoked_at IS NULL").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("touch s3 credential: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect s3 credential touch: %w", err)
	}
	if rowsAffected == 0 {
		return fmt.Errorf("%w: %s", ErrCredentialNotFound, trimmed)
	}
	return nil
}

func (s *Store) DefaultCredential(ctx context.Context) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}

	db, err := s.openDB()
	if err != nil {
		return Credential{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	record := new(credentialRecord)
	err = db.NewSelect().Model(record).
		Where("revoked_at IS NULL").
		OrderExpr("created_at DESC, access_key_id ASC").
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Credential{}, ErrNoActiveCredential
	}
	if err != nil {
		return Credential{}, fmt.Errorf("load default s3 credential: %w", err)
	}
	credential, err := record.Credential()
	if err != nil {
		return Credential{}, err
	}
	return credential, nil
}

func (s *Store) FindForOperation(ctx context.Context, bucket string, write bool) (Credential, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, err
	}
	trimmedBucket := strings.TrimSpace(bucket)

	db, err := s.openDB()
	if err != nil {
		return Credential{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	records := make([]credentialRecord, 0)
	err = db.NewSelect().Model(&records).
		Where("revoked_at IS NULL").
		OrderExpr("created_at DESC, access_key_id ASC").
		Scan(ctx)
	if err != nil {
		return Credential{}, fmt.Errorf("list active s3 credentials: %w", err)
	}

	for _, record := range records {
		credential, err := record.Credential()
		if err != nil {
			return Credential{}, err
		}
		if credential.AllowsBucket(trimmedBucket) && credential.AllowsOperation(write) {
			return credential, nil
		}
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

func (s *Store) ensureBootstrap(db bun.IDB, bootstrap BootstrapCredential) error {
	hasCredentials, err := hasAnyCredential(db)
	if err != nil {
		return err
	}
	if hasCredentials {
		return nil
	}
	if strings.TrimSpace(bootstrap.AccessKeyID) == "" || strings.TrimSpace(bootstrap.SecretAccessKey) == "" {
		return nil
	}

	credential := Credential{
		AccessKeyID:     strings.TrimSpace(bootstrap.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(bootstrap.SecretAccessKey),
		Label:           "Imported from config",
		Source:          credentialSourceBoot,
		Permission:      PermissionReadWrite,
		Buckets:         []string{},
		CreatedAt:       s.now().UTC(),
	}
	bucketsJSON, err := encodeBuckets(credential.Buckets)
	if err != nil {
		return err
	}
	record := newCredentialRecord(credential)
	record.BucketsJSON = bucketsJSON
	if _, err := db.NewInsert().Model(&record).Exec(context.Background()); err != nil {
		return fmt.Errorf("bootstrap s3 credential: %w", err)
	}
	s.logger.Info("bootstrapped s3 credential from config", zap.String("access_key_id", credential.AccessKeyID))
	return nil
}

func (s *Store) openDB() (*statedb.BunSession, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("open s3 credential db: store is closed")
	}
	return statedb.Session(s.db), nil
}

func loadCredential(ctx context.Context, queryer bun.IDB, accessKeyID string) (Credential, error) {
	record := new(credentialRecord)
	err := queryer.NewSelect().Model(record).Where("access_key_id = ?", accessKeyID).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Credential{}, fmt.Errorf("%w: %s", ErrCredentialNotFound, accessKeyID)
	}
	if err != nil {
		return Credential{}, fmt.Errorf("load s3 credential: %w", err)
	}
	credential, err := record.Credential()
	if err != nil {
		return Credential{}, err
	}
	return credential, nil
}

func newCredentialRecord(credential Credential) credentialRecord {
	return credentialRecord{
		AccessKeyID:     strings.TrimSpace(credential.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(credential.SecretAccessKey),
		Label:           strings.TrimSpace(credential.Label),
		Source:          strings.TrimSpace(credential.Source),
		Permission:      normalizePermission(credential.Permission),
		BucketsJSON:     "[]",
		CreatedAt:       formatTime(credential.CreatedAt),
		LastUsedAt:      formatNullableTime(credential.LastUsedAt),
		RevokedAt:       formatNullableTime(credential.RevokedAt),
	}
}

func (r credentialRecord) Credential() (Credential, error) {
	buckets, err := decodeBuckets(r.BucketsJSON)
	if err != nil {
		return Credential{}, err
	}
	createdAtValue, err := parseTime(r.CreatedAt)
	if err != nil {
		return Credential{}, err
	}
	lastUsedAtValue, err := parseNullableTime(r.LastUsedAt)
	if err != nil {
		return Credential{}, err
	}
	revokedAtValue, err := parseNullableTime(r.RevokedAt)
	if err != nil {
		return Credential{}, err
	}
	return Credential{
		AccessKeyID:     strings.TrimSpace(r.AccessKeyID),
		SecretAccessKey: strings.TrimSpace(r.SecretAccessKey),
		Label:           strings.TrimSpace(r.Label),
		Source:          strings.TrimSpace(r.Source),
		Permission:      normalizePermission(r.Permission),
		Buckets:         buckets,
		CreatedAt:       createdAtValue,
		LastUsedAt:      lastUsedAtValue,
		RevokedAt:       revokedAtValue,
	}, nil
}

func hasAnyCredential(db bun.IDB) (bool, error) {
	count, err := db.NewSelect().Model((*credentialRecord)(nil)).Count(context.Background())
	if err != nil {
		return false, fmt.Errorf("count s3 credentials: %w", err)
	}
	return count > 0, nil
}

func encodeBuckets(buckets []string) (string, error) {
	content, err := json.Marshal(normalizeBuckets(buckets))
	if err != nil {
		return "", fmt.Errorf("encode s3 credential buckets: %w", err)
	}
	return string(content), nil
}

func decodeBuckets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return []string{}, nil
	}
	buckets := []string{}
	if err := json.Unmarshal([]byte(raw), &buckets); err != nil {
		return nil, fmt.Errorf("decode s3 credential buckets: %w", err)
	}
	return normalizeBuckets(buckets), nil
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func formatNullableTime(value *time.Time) sql.NullString {
	if value == nil {
		return sql.NullString{}
	}
	return sql.NullString{String: formatTime(value.UTC()), Valid: true}
}

func parseTime(raw string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse s3 credential time: %w", err)
	}
	return parsed.UTC(), nil
}

func parseNullableTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid || strings.TrimSpace(value.String) == "" {
		return nil, nil
	}
	parsed, err := parseTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func isUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "unique constraint failed")
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
