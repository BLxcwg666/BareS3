package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"

	"bares3-server/internal/statedb"
)

var storageMultipartMigrations = []statedb.Migration{
	{
		Name: "storage_multipart_uploads_v1",
		Statements: []string{
			`
			CREATE TABLE IF NOT EXISTS storage_multipart_uploads (
				upload_id TEXT PRIMARY KEY,
				bucket TEXT NOT NULL,
				key TEXT NOT NULL,
				content_type TEXT NOT NULL DEFAULT '',
				cache_control TEXT NOT NULL DEFAULT '',
				content_disposition TEXT NOT NULL DEFAULT '',
				user_metadata_json TEXT NOT NULL DEFAULT '{}',
				created_at TEXT NOT NULL,
				FOREIGN KEY (bucket) REFERENCES storage_buckets(name) ON UPDATE CASCADE ON DELETE CASCADE
			)
		`,
			`CREATE INDEX IF NOT EXISTS storage_multipart_uploads_bucket_key_idx ON storage_multipart_uploads (bucket, key)`,
		},
	},
	{
		Name: "storage_multipart_parts_v1",
		Statements: []string{`
			CREATE TABLE IF NOT EXISTS storage_multipart_parts (
				upload_id TEXT NOT NULL,
				part_number INTEGER NOT NULL,
				etag TEXT NOT NULL DEFAULT '',
				size INTEGER NOT NULL DEFAULT 0,
				last_modified TEXT NOT NULL,
				PRIMARY KEY (upload_id, part_number),
				FOREIGN KEY (upload_id) REFERENCES storage_multipart_uploads(upload_id) ON UPDATE CASCADE ON DELETE CASCADE
			)
		`},
	},
}

func allStorageMigrations() []statedb.Migration {
	migrations := make([]statedb.Migration, 0, len(storageMetadataMigrations)+len(storageMultipartMigrations))
	migrations = append(migrations, storageMetadataMigrations...)
	migrations = append(migrations, storageMultipartMigrations...)
	return migrations
}

func (s *metadataStore) getMultipartUpload(bucket, key, uploadID string) (multipartUploadMetadata, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return multipartUploadMetadata{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	record := new(storageMultipartUploadRecord)
	err = db.NewSelect().Model(record).Where("upload_id = ?", strings.TrimSpace(uploadID)).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return multipartUploadMetadata{}, os.ErrNotExist
	}
	if err != nil {
		return multipartUploadMetadata{}, fmt.Errorf("read multipart upload metadata: %w", err)
	}
	meta, err := record.MultipartUploadMetadata()
	if err != nil {
		return multipartUploadMetadata{}, err
	}
	if meta.Bucket != strings.TrimSpace(bucket) || meta.Key != strings.TrimSpace(key) {
		return multipartUploadMetadata{}, os.ErrNotExist
	}
	return meta, nil
}

func (s *metadataStore) listMultipartUploads(bucket string) ([]multipartUploadMetadata, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	records := make([]storageMultipartUploadRecord, 0)
	err = db.NewSelect().Model(&records).
		Where("bucket = ?", strings.TrimSpace(bucket)).
		OrderExpr("key ASC, created_at ASC, upload_id ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("list multipart upload metadata: %w", err)
	}

	items := make([]multipartUploadMetadata, 0, len(records))
	for _, record := range records {
		meta, err := record.MultipartUploadMetadata()
		if err != nil {
			return nil, fmt.Errorf("scan multipart upload metadata: %w", err)
		}
		items = append(items, meta)
	}
	return items, nil
}

func (s *metadataStore) upsertMultipartUpload(meta multipartUploadMetadata) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	record, err := newStorageMultipartUploadRecord(meta)
	if err != nil {
		return err
	}
	_, err = db.NewInsert().Model(&record).
		On("CONFLICT (upload_id) DO UPDATE").
		Set("bucket = EXCLUDED.bucket").
		Set("key = EXCLUDED.key").
		Set("content_type = EXCLUDED.content_type").
		Set("cache_control = EXCLUDED.cache_control").
		Set("content_disposition = EXCLUDED.content_disposition").
		Set("user_metadata_json = EXCLUDED.user_metadata_json").
		Set("created_at = EXCLUDED.created_at").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("upsert multipart upload metadata: %w", err)
	}
	return nil
}

func (s *metadataStore) deleteMultipartUpload(uploadID string) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	if _, err := db.NewDelete().Model((*storageMultipartUploadRecord)(nil)).Where("upload_id = ?", strings.TrimSpace(uploadID)).Exec(ctx); err != nil {
		return fmt.Errorf("delete multipart upload metadata: %w", err)
	}
	return nil
}

func (s *metadataStore) listMultipartParts(uploadID string) ([]multipartPartMetadata, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	records := make([]storageMultipartPartRecord, 0)
	err = db.NewSelect().Model(&records).
		Where("upload_id = ?", strings.TrimSpace(uploadID)).
		OrderExpr("part_number ASC").
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("list multipart part metadata: %w", err)
	}

	items := make([]multipartPartMetadata, 0, len(records))
	for _, record := range records {
		meta, err := record.MultipartPartMetadata()
		if err != nil {
			return nil, fmt.Errorf("scan multipart part metadata: %w", err)
		}
		items = append(items, meta)
	}
	return items, nil
}

func (s *metadataStore) upsertMultipartPart(uploadID string, meta multipartPartMetadata) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	record := newStorageMultipartPartRecord(uploadID, meta)
	_, err = db.NewInsert().Model(&record).
		On("CONFLICT (upload_id, part_number) DO UPDATE").
		Set("etag = EXCLUDED.etag").
		Set("size = EXCLUDED.size").
		Set("last_modified = EXCLUDED.last_modified").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("upsert multipart part metadata: %w", err)
	}
	return nil
}

func scanMultipartUploadMetadata(scanner interface{ Scan(dest ...any) error }) (multipartUploadMetadata, error) {
	var (
		meta             multipartUploadMetadata
		userMetadataJSON string
		createdAt        string
	)
	if err := scanner.Scan(&meta.UploadID, &meta.Bucket, &meta.Key, &meta.ContentType, &meta.CacheControl, &meta.ContentDisposition, &userMetadataJSON, &createdAt); err != nil {
		return multipartUploadMetadata{}, err
	}
	parsedCreatedAt, err := parseMetadataTime(createdAt)
	if err != nil {
		return multipartUploadMetadata{}, err
	}
	meta.CreatedAt = parsedCreatedAt
	if err := decodeJSONField(userMetadataJSON, &meta.UserMetadata, map[string]string{}); err != nil {
		return multipartUploadMetadata{}, fmt.Errorf("decode multipart upload user metadata: %w", err)
	}
	meta.UserMetadata = cloneStringMap(meta.UserMetadata)
	meta.ContentType = strings.TrimSpace(meta.ContentType)
	meta.CacheControl = strings.TrimSpace(meta.CacheControl)
	meta.ContentDisposition = strings.TrimSpace(meta.ContentDisposition)
	return meta, nil
}

func scanMultipartPartMetadata(scanner interface{ Scan(dest ...any) error }) (multipartPartMetadata, error) {
	var (
		meta         multipartPartMetadata
		lastModified string
	)
	if err := scanner.Scan(&meta.PartNumber, &meta.ETag, &meta.Size, &lastModified); err != nil {
		return multipartPartMetadata{}, err
	}
	parsedLastModified, err := parseMetadataTime(lastModified)
	if err != nil {
		return multipartPartMetadata{}, err
	}
	meta.LastModified = parsedLastModified
	meta.ETag = strings.TrimSpace(meta.ETag)
	return meta, nil
}
