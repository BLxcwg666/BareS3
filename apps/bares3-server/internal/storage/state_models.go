package storage

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/uptrace/bun"
)

type storageBucketRecord struct {
	bun.BaseModel `bun:"table:storage_buckets"`

	Name               string `bun:"name,pk"`
	CreatedAt          string `bun:"created_at"`
	MetadataLayout     string `bun:"metadata_layout"`
	AccessMode         string `bun:"access_mode"`
	AccessPolicyJSON   string `bun:"access_policy_json"`
	ReplicationEnabled bool   `bun:"replication_enabled"`
	QuotaBytes         int64  `bun:"quota_bytes"`
	TagsJSON           string `bun:"tags_json"`
	Note               string `bun:"note"`
}

type storageObjectRecord struct {
	bun.BaseModel `bun:"table:storage_objects"`

	Bucket             string `bun:"bucket,pk"`
	Key                string `bun:"key,pk"`
	Size               int64  `bun:"size"`
	ETag               string `bun:"etag"`
	ChecksumSHA256     string `bun:"checksum_sha256"`
	Revision           int64  `bun:"revision"`
	OriginNodeID       string `bun:"origin_node_id"`
	LastChangeID       string `bun:"last_change_id"`
	ContentType        string `bun:"content_type"`
	CacheControl       string `bun:"cache_control"`
	ContentDisposition string `bun:"content_disposition"`
	UserMetadataJSON   string `bun:"user_metadata_json"`
	LastModified       string `bun:"last_modified"`
}

type storageBucketUsageRecord struct {
	bun.BaseModel `bun:"table:storage_bucket_usage_history"`

	ID          int64  `bun:"id,pk,autoincrement"`
	Bucket      string `bun:"bucket"`
	RecordedAt  string `bun:"recorded_at"`
	UsedBytes   int64  `bun:"used_bytes"`
	ObjectCount int    `bun:"object_count"`
	QuotaBytes  int64  `bun:"quota_bytes"`
}

type storageMultipartUploadRecord struct {
	bun.BaseModel `bun:"table:storage_multipart_uploads"`

	UploadID           string `bun:"upload_id,pk"`
	Bucket             string `bun:"bucket"`
	Key                string `bun:"key"`
	ContentType        string `bun:"content_type"`
	CacheControl       string `bun:"cache_control"`
	ContentDisposition string `bun:"content_disposition"`
	UserMetadataJSON   string `bun:"user_metadata_json"`
	CreatedAt          string `bun:"created_at"`
}

type storageMultipartPartRecord struct {
	bun.BaseModel `bun:"table:storage_multipart_parts"`

	UploadID     string `bun:"upload_id,pk"`
	PartNumber   int    `bun:"part_number,pk"`
	ETag         string `bun:"etag"`
	Size         int64  `bun:"size"`
	LastModified string `bun:"last_modified"`
}

func newStorageBucketRecord(meta bucketMetadata) (storageBucketRecord, error) {
	accessPolicyJSON, err := json.Marshal(NormalizeBucketAccessPolicy(meta.AccessPolicy))
	if err != nil {
		return storageBucketRecord{}, fmt.Errorf("encode bucket access policy: %w", err)
	}
	tagsJSON, err := json.Marshal(normalizeBucketTags(meta.Tags))
	if err != nil {
		return storageBucketRecord{}, fmt.Errorf("encode bucket tags: %w", err)
	}
	return storageBucketRecord{
		Name:               strings.TrimSpace(meta.Name),
		CreatedAt:          formatMetadataTime(meta.CreatedAt),
		MetadataLayout:     strings.TrimSpace(meta.MetadataLayout),
		AccessMode:         NormalizeBucketAccessMode(meta.AccessMode),
		AccessPolicyJSON:   string(accessPolicyJSON),
		ReplicationEnabled: meta.ReplicationEnabled,
		QuotaBytes:         meta.QuotaBytes,
		TagsJSON:           string(tagsJSON),
		Note:               strings.TrimSpace(meta.Note),
	}, nil
}

func (r storageBucketRecord) BucketMetadata() (bucketMetadata, error) {
	createdAt, err := parseMetadataTime(r.CreatedAt)
	if err != nil {
		return bucketMetadata{}, err
	}
	meta := bucketMetadata{
		Name:               strings.TrimSpace(r.Name),
		CreatedAt:          createdAt,
		MetadataLayout:     strings.TrimSpace(r.MetadataLayout),
		AccessMode:         NormalizeBucketAccessMode(r.AccessMode),
		ReplicationEnabled: r.ReplicationEnabled,
		QuotaBytes:         r.QuotaBytes,
		Note:               strings.TrimSpace(r.Note),
	}
	if err := decodeJSONField(r.AccessPolicyJSON, &meta.AccessPolicy, BucketAccessPolicy{}); err != nil {
		return bucketMetadata{}, fmt.Errorf("decode bucket access policy: %w", err)
	}
	meta.AccessPolicy = NormalizeBucketAccessPolicy(meta.AccessPolicy)
	if err := decodeJSONField(r.TagsJSON, &meta.Tags, []string{}); err != nil {
		return bucketMetadata{}, fmt.Errorf("decode bucket tags: %w", err)
	}
	meta.Tags = normalizeBucketTags(meta.Tags)
	return meta, nil
}

func newStorageObjectRecord(meta objectMetadata) (storageObjectRecord, error) {
	userMetadataJSON, err := json.Marshal(cloneStringMap(meta.UserMetadata))
	if err != nil {
		return storageObjectRecord{}, fmt.Errorf("encode object user metadata: %w", err)
	}
	return storageObjectRecord{
		Bucket:             strings.TrimSpace(meta.Bucket),
		Key:                strings.TrimSpace(meta.Key),
		Size:               meta.Size,
		ETag:               strings.TrimSpace(meta.ETag),
		ChecksumSHA256:     strings.TrimSpace(meta.ChecksumSHA256),
		Revision:           meta.Revision,
		OriginNodeID:       strings.TrimSpace(meta.OriginNodeID),
		LastChangeID:       strings.TrimSpace(meta.LastChangeID),
		ContentType:        strings.TrimSpace(meta.ContentType),
		CacheControl:       strings.TrimSpace(meta.CacheControl),
		ContentDisposition: strings.TrimSpace(meta.ContentDisposition),
		UserMetadataJSON:   string(userMetadataJSON),
		LastModified:       formatMetadataTime(meta.LastModified),
	}, nil
}

func (r storageObjectRecord) ObjectMetadata() (objectMetadata, error) {
	lastModified, err := parseMetadataTime(r.LastModified)
	if err != nil {
		return objectMetadata{}, err
	}
	meta := objectMetadata{
		Bucket:             strings.TrimSpace(r.Bucket),
		Key:                strings.TrimSpace(r.Key),
		Size:               r.Size,
		ETag:               strings.TrimSpace(r.ETag),
		ChecksumSHA256:     strings.TrimSpace(r.ChecksumSHA256),
		Revision:           r.Revision,
		OriginNodeID:       strings.TrimSpace(r.OriginNodeID),
		LastChangeID:       strings.TrimSpace(r.LastChangeID),
		ContentType:        strings.TrimSpace(r.ContentType),
		CacheControl:       strings.TrimSpace(r.CacheControl),
		ContentDisposition: strings.TrimSpace(r.ContentDisposition),
		LastModified:       lastModified,
	}
	if err := decodeJSONField(r.UserMetadataJSON, &meta.UserMetadata, map[string]string{}); err != nil {
		return objectMetadata{}, fmt.Errorf("decode object user metadata: %w", err)
	}
	meta.UserMetadata = cloneStringMap(meta.UserMetadata)
	return meta, nil
}

func newStorageBucketUsageRecord(bucket string, sample BucketUsageSample) storageBucketUsageRecord {
	return storageBucketUsageRecord{
		Bucket:      strings.TrimSpace(bucket),
		RecordedAt:  formatMetadataTime(sample.RecordedAt),
		UsedBytes:   sample.UsedBytes,
		ObjectCount: sample.ObjectCount,
		QuotaBytes:  sample.QuotaBytes,
	}
}

func (r storageBucketUsageRecord) BucketUsageSample() (BucketUsageSample, error) {
	recordedAt, err := parseMetadataTime(r.RecordedAt)
	if err != nil {
		return BucketUsageSample{}, err
	}
	return BucketUsageSample{
		RecordedAt:  recordedAt,
		UsedBytes:   r.UsedBytes,
		ObjectCount: r.ObjectCount,
		QuotaBytes:  r.QuotaBytes,
	}, nil
}

func newStorageMultipartUploadRecord(meta multipartUploadMetadata) (storageMultipartUploadRecord, error) {
	userMetadataJSON, err := json.Marshal(cloneStringMap(meta.UserMetadata))
	if err != nil {
		return storageMultipartUploadRecord{}, fmt.Errorf("encode multipart upload user metadata: %w", err)
	}
	return storageMultipartUploadRecord{
		UploadID:           strings.TrimSpace(meta.UploadID),
		Bucket:             strings.TrimSpace(meta.Bucket),
		Key:                strings.TrimSpace(meta.Key),
		ContentType:        strings.TrimSpace(meta.ContentType),
		CacheControl:       strings.TrimSpace(meta.CacheControl),
		ContentDisposition: strings.TrimSpace(meta.ContentDisposition),
		UserMetadataJSON:   string(userMetadataJSON),
		CreatedAt:          formatMetadataTime(meta.CreatedAt),
	}, nil
}

func (r storageMultipartUploadRecord) MultipartUploadMetadata() (multipartUploadMetadata, error) {
	createdAt, err := parseMetadataTime(r.CreatedAt)
	if err != nil {
		return multipartUploadMetadata{}, err
	}
	meta := multipartUploadMetadata{
		UploadID:           strings.TrimSpace(r.UploadID),
		Bucket:             strings.TrimSpace(r.Bucket),
		Key:                strings.TrimSpace(r.Key),
		ContentType:        strings.TrimSpace(r.ContentType),
		CacheControl:       strings.TrimSpace(r.CacheControl),
		ContentDisposition: strings.TrimSpace(r.ContentDisposition),
		CreatedAt:          createdAt,
	}
	if err := decodeJSONField(r.UserMetadataJSON, &meta.UserMetadata, map[string]string{}); err != nil {
		return multipartUploadMetadata{}, fmt.Errorf("decode multipart upload user metadata: %w", err)
	}
	meta.UserMetadata = cloneStringMap(meta.UserMetadata)
	return meta, nil
}

func newStorageMultipartPartRecord(uploadID string, meta multipartPartMetadata) storageMultipartPartRecord {
	return storageMultipartPartRecord{
		UploadID:     strings.TrimSpace(uploadID),
		PartNumber:   meta.PartNumber,
		ETag:         strings.TrimSpace(meta.ETag),
		Size:         meta.Size,
		LastModified: formatMetadataTime(meta.LastModified),
	}
}

func (r storageMultipartPartRecord) MultipartPartMetadata() (multipartPartMetadata, error) {
	lastModified, err := parseMetadataTime(r.LastModified)
	if err != nil {
		return multipartPartMetadata{}, err
	}
	return multipartPartMetadata{
		PartNumber:   r.PartNumber,
		ETag:         strings.TrimSpace(r.ETag),
		Size:         r.Size,
		LastModified: lastModified,
	}, nil
}

func nullableString(value string) sql.NullString {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: trimmed, Valid: true}
}
