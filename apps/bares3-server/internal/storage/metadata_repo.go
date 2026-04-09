package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"bares3-server/internal/statedb"
	"github.com/uptrace/bun"
	"go.uber.org/zap"
)

var storageMetadataMigrations = []statedb.Migration{
	{
		Name: "storage_buckets_v1",
		Statements: []string{`
			CREATE TABLE IF NOT EXISTS storage_buckets (
				name TEXT PRIMARY KEY,
				created_at TEXT NOT NULL,
				metadata_layout TEXT NOT NULL,
				access_mode TEXT NOT NULL,
				access_policy_json TEXT NOT NULL DEFAULT '{}',
				quota_bytes INTEGER NOT NULL DEFAULT 0,
				tags_json TEXT NOT NULL DEFAULT '[]',
				note TEXT NOT NULL DEFAULT ''
			)
		`},
	},
	{
		Name: "storage_objects_v1",
		Statements: []string{
			`
			CREATE TABLE IF NOT EXISTS storage_objects (
				bucket TEXT NOT NULL,
				key TEXT NOT NULL,
				size INTEGER NOT NULL DEFAULT 0,
				etag TEXT NOT NULL DEFAULT '',
				content_type TEXT NOT NULL DEFAULT '',
				cache_control TEXT NOT NULL DEFAULT '',
				content_disposition TEXT NOT NULL DEFAULT '',
				user_metadata_json TEXT NOT NULL DEFAULT '{}',
				last_modified TEXT NOT NULL,
				PRIMARY KEY (bucket, key),
				FOREIGN KEY (bucket) REFERENCES storage_buckets(name) ON UPDATE CASCADE ON DELETE CASCADE
			)
		`,
			`CREATE INDEX IF NOT EXISTS storage_objects_bucket_key_idx ON storage_objects (bucket, key)`,
		},
	},
	{
		Name: "storage_objects_checksum_v1",
		Statements: []string{
			`ALTER TABLE storage_objects ADD COLUMN checksum_sha256 TEXT NOT NULL DEFAULT ''`,
		},
	},
	{
		Name: "storage_objects_revision_v1",
		Statements: []string{
			`ALTER TABLE storage_objects ADD COLUMN revision INTEGER NOT NULL DEFAULT 0`,
			`ALTER TABLE storage_objects ADD COLUMN origin_node_id TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE storage_objects ADD COLUMN last_change_id TEXT NOT NULL DEFAULT ''`,
		},
	},
	{
		Name: "storage_bucket_usage_history_v1",
		Statements: []string{
			`
			CREATE TABLE IF NOT EXISTS storage_bucket_usage_history (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				bucket TEXT NOT NULL,
				recorded_at TEXT NOT NULL,
				used_bytes INTEGER NOT NULL DEFAULT 0,
				object_count INTEGER NOT NULL DEFAULT 0,
				quota_bytes INTEGER NOT NULL DEFAULT 0,
				FOREIGN KEY (bucket) REFERENCES storage_buckets(name) ON UPDATE CASCADE ON DELETE CASCADE
			)
		`,
			`CREATE INDEX IF NOT EXISTS storage_bucket_usage_history_bucket_recorded_idx ON storage_bucket_usage_history (bucket, recorded_at, id)`,
		},
	},
}

type metadataStore struct {
	dataDir string
	db      *bun.DB
}

func newMetadataStore(dataDir string, logger *zap.Logger) (*metadataStore, error) {
	if strings.TrimSpace(dataDir) == "" {
		return nil, fmt.Errorf("storage metadata data dir is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	sqlDB, err := statedb.Open(dataDir)
	if err != nil {
		return nil, fmt.Errorf("open storage metadata db: %w", err)
	}
	bunDB := statedb.Wrap(sqlDB)
	if err := statedb.EnsureMigrations(sqlDB, allStorageMigrations()); err != nil {
		_ = bunDB.Close()
		return nil, fmt.Errorf("initialize storage metadata schema: %w", err)
	}
	store := &metadataStore{dataDir: dataDir, db: bunDB}
	return store, nil
}

func (s *metadataStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	err := s.db.Close()
	s.db = nil
	return err
}

func (s *metadataStore) getBucket(name string) (bucketMetadata, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return bucketMetadata{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	record := new(storageBucketRecord)
	err = db.NewSelect().Model(record).Where("name = ?", strings.TrimSpace(name)).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return bucketMetadata{}, os.ErrNotExist
	}
	if err != nil {
		return bucketMetadata{}, fmt.Errorf("read bucket metadata: %w", err)
	}
	meta, err := record.BucketMetadata()
	if err != nil {
		return bucketMetadata{}, err
	}
	return meta, nil
}

func (s *metadataStore) listBuckets() ([]bucketMetadata, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	records := make([]storageBucketRecord, 0)
	err = db.NewSelect().Model(&records).OrderExpr("name ASC").Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("list bucket metadata: %w", err)
	}

	items := make([]bucketMetadata, 0, len(records))
	for _, record := range records {
		meta, err := record.BucketMetadata()
		if err != nil {
			return nil, fmt.Errorf("scan bucket metadata: %w", err)
		}
		items = append(items, meta)
	}
	return items, nil
}

func (s *metadataStore) bucketUsage(name string) (BucketInfo, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return BucketInfo{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	usage := struct {
		UsedBytes   int64 `bun:"used_bytes"`
		ObjectCount int   `bun:"object_count"`
	}{}
	if err := db.NewSelect().Model((*storageObjectRecord)(nil)).
		ColumnExpr("COALESCE(SUM(size), 0) AS used_bytes").
		ColumnExpr("COUNT(*) AS object_count").
		Where("bucket = ?", strings.TrimSpace(name)).
		Scan(ctx, &usage); err != nil {
		return BucketInfo{}, fmt.Errorf("query bucket usage: %w", err)
	}
	return BucketInfo{UsedBytes: usage.UsedBytes, ObjectCount: usage.ObjectCount}, nil
}

func (s *metadataStore) listObjects(bucket, prefix string) ([]objectMetadata, error) {
	bucket = strings.TrimSpace(bucket)
	prefix = strings.TrimSpace(prefix)
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	query := db.NewSelect().Model((*storageObjectRecord)(nil)).Where("bucket = ?", bucket)
	if prefix != "" {
		query = query.Where("key LIKE ? ESCAPE '\\'", likePrefixPattern(prefix))
	}

	records := make([]storageObjectRecord, 0)
	err = query.OrderExpr("key ASC").Scan(ctx, &records)
	if err != nil {
		return nil, fmt.Errorf("list object metadata: %w", err)
	}

	items := make([]objectMetadata, 0, len(records))
	for _, record := range records {
		meta, err := record.ObjectMetadata()
		if err != nil {
			return nil, fmt.Errorf("scan object metadata: %w", err)
		}
		items = append(items, meta)
	}
	return items, nil
}

func (s *metadataStore) getObject(bucket, key string) (objectMetadata, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return objectMetadata{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	record := new(storageObjectRecord)
	err = db.NewSelect().Model(record).
		Where("bucket = ?", strings.TrimSpace(bucket)).
		Where("key = ?", strings.TrimSpace(key)).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return objectMetadata{}, os.ErrNotExist
	}
	if err != nil {
		return objectMetadata{}, fmt.Errorf("read object metadata: %w", err)
	}
	meta, err := record.ObjectMetadata()
	if err != nil {
		return objectMetadata{}, err
	}
	return meta, nil
}

func (s *metadataStore) upsertBucket(meta bucketMetadata) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	record, err := newStorageBucketRecord(meta)
	if err != nil {
		return err
	}

	_, err = db.NewInsert().Model(&record).
		On("CONFLICT (name) DO UPDATE").
		Set("created_at = EXCLUDED.created_at").
		Set("metadata_layout = EXCLUDED.metadata_layout").
		Set("access_mode = EXCLUDED.access_mode").
		Set("access_policy_json = EXCLUDED.access_policy_json").
		Set("quota_bytes = EXCLUDED.quota_bytes").
		Set("tags_json = EXCLUDED.tags_json").
		Set("note = EXCLUDED.note").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("upsert bucket metadata: %w", err)
	}
	return nil
}

func (s *metadataStore) renameBucket(sourceName, destinationName string, meta bucketMetadata) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	record, err := newStorageBucketRecord(meta)
	if err != nil {
		return err
	}
	result, err := db.NewUpdate().Model((*storageBucketRecord)(nil)).
		Set("name = ?", destinationName).
		Set("created_at = ?", record.CreatedAt).
		Set("metadata_layout = ?", record.MetadataLayout).
		Set("access_mode = ?", record.AccessMode).
		Set("access_policy_json = ?", record.AccessPolicyJSON).
		Set("quota_bytes = ?", record.QuotaBytes).
		Set("tags_json = ?", record.TagsJSON).
		Set("note = ?", record.Note).
		Where("name = ?", sourceName).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("rename bucket metadata: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect bucket metadata rename: %w", err)
	}
	if rowsAffected == 0 {
		return os.ErrNotExist
	}
	return nil
}

func (s *metadataStore) deleteBucket(name string) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	if _, err := db.NewDelete().Model((*storageBucketRecord)(nil)).Where("name = ?", strings.TrimSpace(name)).Exec(ctx); err != nil {
		return fmt.Errorf("delete bucket metadata: %w", err)
	}
	return nil
}

func (s *metadataStore) upsertObject(meta objectMetadata) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	record, err := newStorageObjectRecord(meta)
	if err != nil {
		return err
	}
	_, err = db.NewInsert().Model(&record).
		On("CONFLICT (bucket, key) DO UPDATE").
		Set("size = EXCLUDED.size").
		Set("etag = EXCLUDED.etag").
		Set("checksum_sha256 = EXCLUDED.checksum_sha256").
		Set("revision = EXCLUDED.revision").
		Set("origin_node_id = EXCLUDED.origin_node_id").
		Set("last_change_id = EXCLUDED.last_change_id").
		Set("content_type = EXCLUDED.content_type").
		Set("cache_control = EXCLUDED.cache_control").
		Set("content_disposition = EXCLUDED.content_disposition").
		Set("user_metadata_json = EXCLUDED.user_metadata_json").
		Set("last_modified = EXCLUDED.last_modified").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("upsert object metadata: %w", err)
	}
	return nil
}

func (s *metadataStore) moveObject(sourceBucket, sourceKey string, meta objectMetadata) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	record, err := newStorageObjectRecord(meta)
	if err != nil {
		return err
	}
	result, err := db.NewUpdate().Model((*storageObjectRecord)(nil)).
		Set("bucket = ?", record.Bucket).
		Set("key = ?", record.Key).
		Set("size = ?", record.Size).
		Set("etag = ?", record.ETag).
		Set("checksum_sha256 = ?", record.ChecksumSHA256).
		Set("revision = ?", record.Revision).
		Set("origin_node_id = ?", record.OriginNodeID).
		Set("last_change_id = ?", record.LastChangeID).
		Set("content_type = ?", record.ContentType).
		Set("cache_control = ?", record.CacheControl).
		Set("content_disposition = ?", record.ContentDisposition).
		Set("user_metadata_json = ?", record.UserMetadataJSON).
		Set("last_modified = ?", record.LastModified).
		Where("bucket = ?", strings.TrimSpace(sourceBucket)).
		Where("key = ?", strings.TrimSpace(sourceKey)).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("move object metadata: %w", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect object metadata move: %w", err)
	}
	if rowsAffected == 0 {
		return os.ErrNotExist
	}
	return nil
}

func (s *metadataStore) deleteObject(bucket, key string) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	_, err = db.NewDelete().Model((*storageObjectRecord)(nil)).
		Where("bucket = ?", strings.TrimSpace(bucket)).
		Where("key = ?", strings.TrimSpace(key)).
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("delete object metadata: %w", err)
	}
	return nil
}

func (s *metadataStore) listBucketUsageHistory(bucket string, limit int) ([]BucketUsageSample, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	records := make([]storageBucketUsageRecord, 0)
	err = db.NewSelect().Model(&records).
		Where("bucket = ?", strings.TrimSpace(bucket)).
		OrderExpr("id ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("list bucket usage history: %w", err)
	}

	if limit > 0 && len(records) > limit {
		records = records[len(records)-limit:]
	}

	items := make([]BucketUsageSample, 0, len(records))
	for _, record := range records {
		sample, err := record.BucketUsageSample()
		if err != nil {
			return nil, fmt.Errorf("scan bucket usage history: %w", err)
		}
		items = append(items, sample)
	}
	return items, nil
}

func (s *metadataStore) appendBucketUsageSample(bucket string, sample BucketUsageSample) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	record := newStorageBucketUsageRecord(bucket, sample)
	if _, err := db.NewInsert().Model(&record).Exec(ctx); err != nil {
		return fmt.Errorf("append bucket usage history: %w", err)
	}

	type staleUsageRecord struct {
		ID int64 `bun:"id"`
	}
	records := make([]staleUsageRecord, 0)
	if err := db.NewSelect().Model((*storageBucketUsageRecord)(nil)).
		Column("id").
		Where("bucket = ?", strings.TrimSpace(bucket)).
		OrderExpr("id DESC").
		Scan(ctx, &records); err != nil {
		return fmt.Errorf("list stale bucket usage history: %w", err)
	}
	stale := records
	if len(stale) > bucketUsageHistoryLimit {
		stale = stale[bucketUsageHistoryLimit:]
	} else {
		stale = nil
	}
	if len(stale) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(stale))
	for _, item := range stale {
		ids = append(ids, item.ID)
	}
	if _, err := db.NewDelete().Model((*storageBucketUsageRecord)(nil)).Where("id IN (?)", bun.In(ids)).Exec(ctx); err != nil {
		return fmt.Errorf("trim bucket usage history: %w", err)
	}
	return nil
}

func (s *metadataStore) listObjectsMissingChecksum() ([]objectMetadata, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	records := make([]storageObjectRecord, 0)
	err = db.NewSelect().Model(&records).
		Where("COALESCE(checksum_sha256, '') = ''").
		OrderExpr("bucket ASC, key ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("list object metadata missing checksum: %w", err)
	}

	items := make([]objectMetadata, 0, len(records))
	for _, record := range records {
		meta, err := record.ObjectMetadata()
		if err != nil {
			return nil, fmt.Errorf("scan object metadata missing checksum: %w", err)
		}
		items = append(items, meta)
	}
	return items, nil
}

func (s *metadataStore) openDB() (*statedb.BunSession, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("open storage metadata db: store is closed")
	}
	return statedb.Session(s.db), nil
}

func formatMetadataTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseMetadataTime(raw string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse metadata time: %w", err)
	}
	return parsed.UTC(), nil
}

func decodeJSONField[T any](raw string, target *T, empty T) error {
	if strings.TrimSpace(raw) == "" {
		*target = empty
		return nil
	}
	if err := json.Unmarshal([]byte(raw), target); err != nil {
		return err
	}
	return nil
}

func likePrefixPattern(prefix string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(prefix) + "%"
}
