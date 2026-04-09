package sharelink

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"bares3-server/internal/statedb"
	"github.com/uptrace/bun"
	"go.uber.org/zap"
)

const (
	minExpiry     = time.Minute
	maxExpiry     = 365 * 24 * time.Hour
	idLengthBytes = 16
)

var (
	ErrNotFound      = errors.New("share link not found")
	ErrInvalidID     = errors.New("invalid share link id")
	ErrInvalidExpiry = errors.New("invalid share link expiry")
	ErrNotRevoked    = errors.New("share link must be revoked or expired before removal")
	ErrExpired       = errors.New("share link expired")
	ErrRevoked       = errors.New("share link revoked")

	storeMigrations = []statedb.Migration{{
		Name: "share_links_v1",
		Statements: []string{`
			CREATE TABLE IF NOT EXISTS share_links (
				id TEXT PRIMARY KEY,
				bucket TEXT NOT NULL,
				key TEXT NOT NULL,
				filename TEXT NOT NULL,
				content_type TEXT NOT NULL DEFAULT '',
				size INTEGER NOT NULL,
				created_by TEXT NOT NULL DEFAULT '',
				created_at TEXT NOT NULL,
				expires_at TEXT NOT NULL,
				revoked_at TEXT
			)
		`, `CREATE INDEX IF NOT EXISTS share_links_bucket_key_idx ON share_links (bucket, key)`},
	}}
)

type Link struct {
	ID          string     `json:"id"`
	Bucket      string     `json:"bucket"`
	Key         string     `json:"key"`
	Filename    string     `json:"filename"`
	ContentType string     `json:"content_type,omitempty"`
	Size        int64      `json:"size"`
	CreatedBy   string     `json:"created_by,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
}

type CreateInput struct {
	Bucket      string
	Key         string
	Filename    string
	ContentType string
	Size        int64
	CreatedBy   string
	Expires     time.Duration
}

type Store struct {
	dataDir string
	db      *bun.DB
	logger  *zap.Logger
	now     func() time.Time
}

type linkRecord struct {
	bun.BaseModel `bun:"table:share_links"`

	ID          string         `bun:"id,pk"`
	Bucket      string         `bun:"bucket"`
	Key         string         `bun:"key"`
	Filename    string         `bun:"filename"`
	ContentType string         `bun:"content_type"`
	Size        int64          `bun:"size"`
	CreatedBy   string         `bun:"created_by"`
	CreatedAt   string         `bun:"created_at"`
	ExpiresAt   string         `bun:"expires_at"`
	RevokedAt   sql.NullString `bun:"revoked_at"`
}

func New(dataDir string, logger *zap.Logger) (*Store, error) {
	trimmed := strings.TrimSpace(dataDir)
	if trimmed == "" {
		return nil, fmt.Errorf("share link data dir is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	sqlDB, err := statedb.Open(trimmed)
	if err != nil {
		return nil, err
	}
	bunDB := statedb.Wrap(sqlDB)

	if err := statedb.EnsureMigrations(sqlDB, storeMigrations); err != nil {
		_ = bunDB.Close()
		return nil, fmt.Errorf("initialize share link schema: %w", err)
	}
	return &Store{dataDir: trimmed, db: bunDB, logger: logger, now: time.Now}, nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *Store) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	if _, err := db.NewRaw("SELECT 1").Exec(ctx); err != nil {
		return fmt.Errorf("check share link db: %w", err)
	}
	return nil
}

func (s *Store) Create(ctx context.Context, input CreateInput) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}

	bucket := strings.TrimSpace(input.Bucket)
	key := strings.TrimSpace(input.Key)
	if bucket == "" || key == "" {
		return Link{}, fmt.Errorf("share link bucket and key are required")
	}
	if input.Expires < minExpiry || input.Expires > maxExpiry {
		return Link{}, fmt.Errorf("%w: expiry must be between %s and %s", ErrInvalidExpiry, minExpiry, maxExpiry)
	}

	db, err := s.openDB()
	if err != nil {
		return Link{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	for range 16 {
		id, err := newID()
		if err != nil {
			return Link{}, err
		}
		createdAt := s.now().UTC()
		filename := strings.TrimSpace(input.Filename)
		if filename == "" {
			filename = path.Base(key)
		}
		link := Link{
			ID:          id,
			Bucket:      bucket,
			Key:         key,
			Filename:    filename,
			ContentType: strings.TrimSpace(input.ContentType),
			Size:        input.Size,
			CreatedBy:   strings.TrimSpace(input.CreatedBy),
			CreatedAt:   createdAt,
			ExpiresAt:   createdAt.Add(input.Expires),
		}
		record := newLinkRecord(link)
		_, err = db.NewInsert().Model(&record).Exec(ctx)
		if err == nil {
			s.logger.Info(
				"share link created",
				zap.String("id", link.ID),
				zap.String("bucket", link.Bucket),
				zap.String("key", link.Key),
				zap.Time("expires_at", link.ExpiresAt),
			)
			return link, nil
		}
		if !isUniqueConstraint(err) {
			return Link{}, fmt.Errorf("insert share link: %w", err)
		}
	}

	return Link{}, fmt.Errorf("generate share link id: exhausted retries")
}

func (s *Store) Get(ctx context.Context, id string) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}

	db, err := s.openDB()
	if err != nil {
		return Link{}, err
	}
	defer func() {
		_ = db.Close()
	}()
	return s.readLink(ctx, db, id)
}

func (s *Store) GetActive(ctx context.Context, id string) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}

	db, err := s.openDB()
	if err != nil {
		return Link{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	link, err := s.readLink(ctx, db, id)
	if err != nil {
		return Link{}, err
	}

	switch link.Status(s.now()) {
	case "revoked":
		return Link{}, fmt.Errorf("%w: %s", ErrRevoked, id)
	case "expired":
		return Link{}, fmt.Errorf("%w: %s", ErrExpired, id)
	default:
		return link, nil
	}
}

func (s *Store) List(ctx context.Context) ([]Link, error) {
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

	records := make([]linkRecord, 0)
	err = db.NewSelect().Model(&records).OrderExpr("created_at DESC, id DESC").Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("list share links: %w", err)
	}

	links := make([]Link, 0, len(records))
	for _, record := range records {
		link, err := record.Link()
		if err != nil {
			return nil, err
		}
		links = append(links, link)
	}
	return links, nil
}

func (s *Store) ReassignObject(ctx context.Context, sourceBucket, sourceKey, destinationBucket, destinationKey string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	sourceBucket = strings.TrimSpace(sourceBucket)
	sourceKey = strings.TrimSpace(sourceKey)
	destinationBucket = strings.TrimSpace(destinationBucket)
	destinationKey = strings.TrimSpace(destinationKey)
	if sourceBucket == "" || sourceKey == "" || destinationBucket == "" || destinationKey == "" {
		return 0, fmt.Errorf("share link source and destination are required")
	}

	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()

	result, err := db.NewUpdate().Model((*linkRecord)(nil)).
		Set("bucket = ?", destinationBucket).
		Set("key = ?", destinationKey).
		Set("filename = ?", path.Base(destinationKey)).
		Where("bucket = ?", sourceBucket).
		Where("key = ?", sourceKey).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("reassign share links for object move: %w", err)
	}
	updated, err := rowsAffected(result, "inspect share link object reassignment")
	if err != nil {
		return 0, err
	}
	if updated > 0 {
		s.logger.Info(
			"share links reassigned for object move",
			zap.String("source_bucket", sourceBucket),
			zap.String("source_key", sourceKey),
			zap.String("destination_bucket", destinationBucket),
			zap.String("destination_key", destinationKey),
			zap.Int("count", updated),
		)
	}
	return updated, nil
}

func (s *Store) ReassignBucket(ctx context.Context, sourceBucket, destinationBucket string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	sourceBucket = strings.TrimSpace(sourceBucket)
	destinationBucket = strings.TrimSpace(destinationBucket)
	if sourceBucket == "" || destinationBucket == "" {
		return 0, fmt.Errorf("share link source and destination buckets are required")
	}

	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()

	result, err := db.NewUpdate().Model((*linkRecord)(nil)).
		Set("bucket = ?", destinationBucket).
		Where("bucket = ?", sourceBucket).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("reassign share links for bucket rename: %w", err)
	}
	updated, err := rowsAffected(result, "inspect share link bucket reassignment")
	if err != nil {
		return 0, err
	}
	if updated > 0 {
		s.logger.Info(
			"share links reassigned for bucket rename",
			zap.String("source_bucket", sourceBucket),
			zap.String("destination_bucket", destinationBucket),
			zap.Int("count", updated),
		)
	}
	return updated, nil
}

func (s *Store) RemoveByObject(ctx context.Context, bucket, key string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	bucket = strings.TrimSpace(bucket)
	key = strings.TrimSpace(key)
	if bucket == "" || key == "" {
		return 0, fmt.Errorf("share link bucket and key are required")
	}

	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()

	result, err := db.NewDelete().Model((*linkRecord)(nil)).
		Where("bucket = ?", bucket).
		Where("key = ?", key).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("remove share links for object: %w", err)
	}
	removed, err := rowsAffected(result, "inspect share link object removal")
	if err != nil {
		return 0, err
	}
	if removed > 0 {
		s.logger.Info(
			"share links removed for deleted object",
			zap.String("bucket", bucket),
			zap.String("key", key),
			zap.Int("count", removed),
		)
	}
	return removed, nil
}

func (s *Store) RemoveByBucket(ctx context.Context, bucket string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	bucket = strings.TrimSpace(bucket)
	if bucket == "" {
		return 0, fmt.Errorf("share link bucket is required")
	}

	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()

	result, err := db.NewDelete().Model((*linkRecord)(nil)).Where("bucket = ?", bucket).Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("remove share links for bucket: %w", err)
	}
	removed, err := rowsAffected(result, "inspect share link bucket removal")
	if err != nil {
		return 0, err
	}
	if removed > 0 {
		s.logger.Info(
			"share links removed for deleted bucket",
			zap.String("bucket", bucket),
			zap.Int("count", removed),
		)
	}
	return removed, nil
}

func (s *Store) RemoveByPrefix(ctx context.Context, bucket, prefix string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	bucket = strings.TrimSpace(bucket)
	prefix = normalizePrefix(prefix)
	if bucket == "" || prefix == "" {
		return 0, fmt.Errorf("share link bucket and prefix are required")
	}

	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()

	result, err := db.NewDelete().Model((*linkRecord)(nil)).
		Where("bucket = ?", bucket).
		Where("key LIKE ? ESCAPE '\\'", likePrefixPattern(prefix)).
		Exec(ctx)
	if err != nil {
		return 0, fmt.Errorf("remove share links for prefix: %w", err)
	}
	removed, err := rowsAffected(result, "inspect share link prefix removal")
	if err != nil {
		return 0, err
	}
	if removed > 0 {
		s.logger.Info(
			"share links removed for deleted prefix",
			zap.String("bucket", bucket),
			zap.String("prefix", prefix),
			zap.Int("count", removed),
		)
	}
	return removed, nil
}

func (s *Store) ReassignPrefix(ctx context.Context, sourceBucket, sourcePrefix, destinationBucket, destinationPrefix string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	sourceBucket = strings.TrimSpace(sourceBucket)
	destinationBucket = strings.TrimSpace(destinationBucket)
	sourcePrefix = normalizePrefix(sourcePrefix)
	destinationPrefix = normalizePrefix(destinationPrefix)
	if sourceBucket == "" || destinationBucket == "" || sourcePrefix == "" {
		return 0, fmt.Errorf("share link source prefix and destination bucket are required")
	}

	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin share link prefix reassignment: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	type pendingUpdate struct {
		ID  string `bun:"id"`
		Key string `bun:"key"`
	}
	updates := make([]pendingUpdate, 0)
	if err := tx.NewSelect().Model((*linkRecord)(nil)).Column("id", "key").
		Where("bucket = ?", sourceBucket).
		Where("key LIKE ? ESCAPE '\\'", likePrefixPattern(sourcePrefix)).
		Scan(ctx, &updates); err != nil {
		return 0, fmt.Errorf("list share links for prefix reassignment: %w", err)
	}

	for _, item := range updates {
		relative := strings.TrimPrefix(item.Key, sourcePrefix)
		nextKey := destinationPrefix + relative
		if _, err := tx.NewUpdate().Model((*linkRecord)(nil)).
			Set("bucket = ?", destinationBucket).
			Set("key = ?", nextKey).
			Set("filename = ?", path.Base(nextKey)).
			Where("id = ?", item.ID).
			Exec(ctx); err != nil {
			return 0, fmt.Errorf("update share link for prefix reassignment: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit share link prefix reassignment: %w", err)
	}

	if len(updates) > 0 {
		s.logger.Info(
			"share links reassigned for prefix move",
			zap.String("source_bucket", sourceBucket),
			zap.String("source_prefix", sourcePrefix),
			zap.String("destination_bucket", destinationBucket),
			zap.String("destination_prefix", destinationPrefix),
			zap.Int("count", len(updates)),
		)
	}
	return len(updates), nil
}

func (s *Store) ActiveCount(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()

	count, err := db.NewSelect().Model((*linkRecord)(nil)).
		Where("revoked_at IS NULL").
		Where("expires_at >= ?", formatTime(s.now().UTC())).
		Count(ctx)
	if err != nil {
		return 0, fmt.Errorf("count active share links: %w", err)
	}
	return count, nil
}

func (s *Store) Revoke(ctx context.Context, id string) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}

	db, err := s.openDB()
	if err != nil {
		return Link{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Link{}, fmt.Errorf("begin share link revoke: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	link, err := s.readLink(ctx, tx, id)
	if err != nil {
		return Link{}, err
	}
	if link.RevokedAt == nil {
		revokedAt := s.now().UTC()
		if _, err := tx.NewUpdate().Model((*linkRecord)(nil)).
			Set("revoked_at = ?", formatTime(revokedAt)).
			Where("id = ?", link.ID).
			Exec(ctx); err != nil {
			return Link{}, fmt.Errorf("revoke share link: %w", err)
		}
		link.RevokedAt = &revokedAt
		s.logger.Info(
			"share link revoked",
			zap.String("id", link.ID),
			zap.String("bucket", link.Bucket),
			zap.String("key", link.Key),
		)
	}
	if err := tx.Commit(); err != nil {
		return Link{}, fmt.Errorf("commit share link revoke: %w", err)
	}
	return link, nil
}

func (s *Store) Remove(ctx context.Context, id string) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}

	db, err := s.openDB()
	if err != nil {
		return Link{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return Link{}, fmt.Errorf("begin share link removal: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	link, err := s.readLink(ctx, tx, id)
	if err != nil {
		return Link{}, err
	}
	if link.RevokedAt == nil && link.Status(s.now()) != "expired" {
		return Link{}, fmt.Errorf("%w: %s", ErrNotRevoked, link.ID)
	}
	if _, err := tx.NewDelete().Model((*linkRecord)(nil)).Where("id = ?", link.ID).Exec(ctx); err != nil {
		return Link{}, fmt.Errorf("remove share link: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return Link{}, fmt.Errorf("commit share link removal: %w", err)
	}

	s.logger.Info(
		"share link removed",
		zap.String("id", link.ID),
		zap.String("bucket", link.Bucket),
		zap.String("key", link.Key),
	)
	return link, nil
}

func (l Link) Status(now time.Time) string {
	if l.RevokedAt != nil {
		return "revoked"
	}
	if !l.ExpiresAt.IsZero() && now.UTC().After(l.ExpiresAt.UTC()) {
		return "expired"
	}
	return "active"
}

func (s *Store) readLink(ctx context.Context, queryer bun.IDB, id string) (Link, error) {
	validated, err := validateID(id)
	if err != nil {
		return Link{}, err
	}

	record := new(linkRecord)
	err = queryer.NewSelect().Model(record).Where("id = ?", validated).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return Link{}, fmt.Errorf("%w: %s", ErrNotFound, validated)
	}
	if err != nil {
		return Link{}, fmt.Errorf("read share link: %w", err)
	}
	link, err := record.Link()
	if err != nil {
		return Link{}, err
	}
	return link, nil
}

func (s *Store) openDB() (*statedb.BunSession, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("open share link db: store is closed")
	}
	return statedb.Session(s.db), nil
}

func newLinkRecord(link Link) linkRecord {
	return linkRecord{
		ID:          strings.ToLower(strings.TrimSpace(link.ID)),
		Bucket:      strings.TrimSpace(link.Bucket),
		Key:         strings.TrimSpace(link.Key),
		Filename:    strings.TrimSpace(link.Filename),
		ContentType: strings.TrimSpace(link.ContentType),
		Size:        link.Size,
		CreatedBy:   strings.TrimSpace(link.CreatedBy),
		CreatedAt:   formatTime(link.CreatedAt),
		ExpiresAt:   formatTime(link.ExpiresAt),
		RevokedAt:   formatNullableTime(link.RevokedAt),
	}
}

func (r linkRecord) Link() (Link, error) {
	createdAtValue, err := parseTime(r.CreatedAt)
	if err != nil {
		return Link{}, err
	}
	expiresAtValue, err := parseTime(r.ExpiresAt)
	if err != nil {
		return Link{}, err
	}
	revokedAtValue, err := parseNullableTime(r.RevokedAt)
	if err != nil {
		return Link{}, err
	}
	return Link{
		ID:          strings.ToLower(r.ID),
		Bucket:      strings.TrimSpace(r.Bucket),
		Key:         strings.TrimSpace(r.Key),
		Filename:    strings.TrimSpace(r.Filename),
		ContentType: strings.TrimSpace(r.ContentType),
		Size:        r.Size,
		CreatedBy:   strings.TrimSpace(r.CreatedBy),
		CreatedAt:   createdAtValue,
		ExpiresAt:   expiresAtValue,
		RevokedAt:   revokedAtValue,
	}, nil
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
		return time.Time{}, fmt.Errorf("parse share link time: %w", err)
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

func rowsAffected(result sql.Result, context string) (int, error) {
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("%s: %w", context, err)
	}
	return int(count), nil
}

func likePrefixPattern(prefix string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(prefix) + "%"
}

func isUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), "unique constraint failed")
}

func validateID(id string) (string, error) {
	trimmed := strings.TrimSpace(id)
	if len(trimmed) != idLengthBytes*2 {
		return "", fmt.Errorf("%w: share link id must be %d hex chars", ErrInvalidID, idLengthBytes*2)
	}
	if _, err := hex.DecodeString(trimmed); err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidID, trimmed)
	}
	return strings.ToLower(trimmed), nil
}

func newID() (string, error) {
	buf := make([]byte, idLengthBytes)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate share link id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func normalizePrefix(value string) string {
	trimmed := strings.Trim(strings.ReplaceAll(strings.TrimSpace(value), "\\", "/"), "/")
	if trimmed == "" {
		return ""
	}
	return trimmed + "/"
}
