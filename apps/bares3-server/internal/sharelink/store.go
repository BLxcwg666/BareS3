package sharelink

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	controlDirName = ".bares3"
	linksDirName   = "sharelinks"
	minExpiry      = time.Minute
	maxExpiry      = 365 * 24 * time.Hour
	idLengthBytes  = 16
)

var (
	ErrNotFound      = errors.New("share link not found")
	ErrInvalidID     = errors.New("invalid share link id")
	ErrInvalidExpiry = errors.New("invalid share link expiry")
	ErrNotRevoked    = errors.New("share link must be revoked or expired before removal")
	ErrExpired       = errors.New("share link expired")
	ErrRevoked       = errors.New("share link revoked")
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
	dir    string
	logger *zap.Logger
	now    func() time.Time
	mu     sync.Mutex
}

func New(dataDir string, logger *zap.Logger) (*Store, error) {
	trimmed := strings.TrimSpace(dataDir)
	if trimmed == "" {
		return nil, fmt.Errorf("share link data dir is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	dir := filepath.Join(trimmed, controlDirName, linksDirName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create share link dir: %w", err)
	}

	return &Store{dir: dir, logger: logger, now: time.Now}, nil
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

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.writeLink(link); err != nil {
		return Link{}, err
	}

	s.logger.Info(
		"share link created",
		zap.String("id", link.ID),
		zap.String("bucket", link.Bucket),
		zap.String("key", link.Key),
		zap.Time("expires_at", link.ExpiresAt),
	)
	return link, nil
}

func (s *Store) Get(ctx context.Context, id string) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}
	return s.readLink(id)
}

func (s *Store) GetActive(ctx context.Context, id string) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}

	link, err := s.readLink(id)
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
	return s.listLocked(ctx)
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

	s.mu.Lock()
	defer s.mu.Unlock()

	links, err := s.listLocked(ctx)
	if err != nil {
		return 0, err
	}

	updated := 0
	for _, link := range links {
		if link.Bucket != sourceBucket || link.Key != sourceKey {
			continue
		}
		link.Bucket = destinationBucket
		link.Key = destinationKey
		link.Filename = path.Base(destinationKey)
		if err := s.writeLink(link); err != nil {
			return updated, err
		}
		updated += 1
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

	s.mu.Lock()
	defer s.mu.Unlock()

	links, err := s.listLocked(ctx)
	if err != nil {
		return 0, err
	}

	updated := 0
	for _, link := range links {
		if link.Bucket != sourceBucket {
			continue
		}
		link.Bucket = destinationBucket
		if err := s.writeLink(link); err != nil {
			return updated, err
		}
		updated += 1
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

	s.mu.Lock()
	defer s.mu.Unlock()

	links, err := s.listLocked(ctx)
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, link := range links {
		if link.Bucket != bucket || link.Key != key {
			continue
		}
		if err := os.Remove(s.linkPath(link.ID)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return removed, fmt.Errorf("remove share link for object: %w", err)
		}
		removed += 1
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

	s.mu.Lock()
	defer s.mu.Unlock()

	links, err := s.listLocked(ctx)
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, link := range links {
		if link.Bucket != bucket {
			continue
		}
		if err := os.Remove(s.linkPath(link.ID)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return removed, fmt.Errorf("remove share link for bucket: %w", err)
		}
		removed += 1
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

	s.mu.Lock()
	defer s.mu.Unlock()

	links, err := s.listLocked(ctx)
	if err != nil {
		return 0, err
	}

	removed := 0
	for _, link := range links {
		if link.Bucket != bucket || !strings.HasPrefix(link.Key, prefix) {
			continue
		}
		if err := os.Remove(s.linkPath(link.ID)); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return removed, fmt.Errorf("remove share link for prefix: %w", err)
		}
		removed += 1
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

	s.mu.Lock()
	defer s.mu.Unlock()

	links, err := s.listLocked(ctx)
	if err != nil {
		return 0, err
	}

	updated := 0
	for _, link := range links {
		if link.Bucket != sourceBucket || !strings.HasPrefix(link.Key, sourcePrefix) {
			continue
		}
		relative := strings.TrimPrefix(link.Key, sourcePrefix)
		link.Bucket = destinationBucket
		link.Key = destinationPrefix + relative
		link.Filename = path.Base(link.Key)
		if err := s.writeLink(link); err != nil {
			return updated, err
		}
		updated += 1
	}

	if updated > 0 {
		s.logger.Info(
			"share links reassigned for prefix move",
			zap.String("source_bucket", sourceBucket),
			zap.String("source_prefix", sourcePrefix),
			zap.String("destination_bucket", destinationBucket),
			zap.String("destination_prefix", destinationPrefix),
			zap.Int("count", updated),
		)
	}

	return updated, nil
}

func (s *Store) listLocked(ctx context.Context) ([]Link, error) {

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read share link dir: %w", err)
	}

	links := make([]Link, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		link := Link{}
		if err := readJSONFile(filepath.Join(s.dir, entry.Name()), &link); err != nil {
			return nil, err
		}
		links = append(links, link)
	}

	sort.Slice(links, func(i, j int) bool {
		if links[i].CreatedAt.Equal(links[j].CreatedAt) {
			return links[i].ID > links[j].ID
		}
		return links[i].CreatedAt.After(links[j].CreatedAt)
	})

	return links, nil
}

func (s *Store) ActiveCount(ctx context.Context) (int, error) {
	links, err := s.List(ctx)
	if err != nil {
		return 0, err
	}

	count := 0
	now := s.now()
	for _, link := range links {
		if link.Status(now) == "active" {
			count += 1
		}
	}
	return count, nil
}

func (s *Store) Revoke(ctx context.Context, id string) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	link, err := s.readLink(id)
	if err != nil {
		return Link{}, err
	}
	if link.RevokedAt != nil {
		return link, nil
	}

	revokedAt := s.now().UTC()
	link.RevokedAt = &revokedAt
	if err := s.writeLink(link); err != nil {
		return Link{}, err
	}

	s.logger.Info(
		"share link revoked",
		zap.String("id", link.ID),
		zap.String("bucket", link.Bucket),
		zap.String("key", link.Key),
	)
	return link, nil
}

func (s *Store) Remove(ctx context.Context, id string) (Link, error) {
	if err := ctx.Err(); err != nil {
		return Link{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	link, err := s.readLink(id)
	if err != nil {
		return Link{}, err
	}
	if link.RevokedAt == nil && link.Status(s.now()) != "expired" {
		return Link{}, fmt.Errorf("%w: %s", ErrNotRevoked, link.ID)
	}
	if err := os.Remove(s.linkPath(link.ID)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Link{}, fmt.Errorf("%w: %s", ErrNotFound, link.ID)
		}
		return Link{}, fmt.Errorf("remove share link: %w", err)
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

func (s *Store) readLink(id string) (Link, error) {
	validated, err := validateID(id)
	if err != nil {
		return Link{}, err
	}

	link := Link{}
	if err := readJSONFile(s.linkPath(validated), &link); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Link{}, fmt.Errorf("%w: %s", ErrNotFound, validated)
		}
		return Link{}, fmt.Errorf("read share link: %w", err)
	}
	return link, nil
}

func (s *Store) writeLink(link Link) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return fmt.Errorf("create share link dir: %w", err)
	}

	stagedPath, err := writeJSONTemp(s.dir, "share-link-*", link)
	if err != nil {
		return fmt.Errorf("stage share link: %w", err)
	}
	defer func() {
		_ = os.Remove(stagedPath)
	}()

	if err := replaceFile(stagedPath, s.linkPath(link.ID)); err != nil {
		return fmt.Errorf("write share link: %w", err)
	}
	return nil
}

func (s *Store) linkPath(id string) string {
	return filepath.Join(s.dir, id+".json")
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

func writeJSONTemp(dir, pattern string, value any) (string, error) {
	file, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return "", err
	}
	path := file.Name()

	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return "", err
	}
	if err := file.Close(); err != nil {
		return "", err
	}

	return path, nil
}

func readJSONFile(path string, value any) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(content, value)
}

func replaceFile(fromPath, toPath string) error {
	if err := os.Rename(fromPath, toPath); err == nil {
		return nil
	}

	if err := os.Remove(toPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return os.Rename(fromPath, toPath)
}

func normalizePrefix(value string) string {
	trimmed := strings.Trim(strings.ReplaceAll(strings.TrimSpace(value), "\\", "/"), "/")
	if trimmed == "" {
		return ""
	}
	return trimmed + "/"
}
