package storage

import (
	"encoding/json"
	"time"

	"bares3-server/internal/sharelink"
)

const (
	SyncStatusPending     = "pending"
	SyncStatusVerifying   = "verifying"
	SyncStatusDownloading = "downloading"
	SyncStatusReady       = "ready"
	SyncStatusError       = "error"
	SyncStatusConflict    = "conflict"

	SyncEventBucketUpsert     = "bucket_upsert"
	SyncEventBucketDelete     = "bucket_delete"
	SyncEventObjectUpsert     = "object_upsert"
	SyncEventObjectDelete     = "object_delete"
	SyncEventDomainUpdate     = "domain_update"
	SyncEventShareLinksUpdate = "sharelinks_update"

	runtimeSettingsStateName  = "runtime_settings"
	domainSettingsStateName   = "domain_settings"
	syncSettingsStateName     = "sync_settings"
	SyncSourceSeededReconcile = "seeded_reconcile"
)

type RuntimeSettings struct {
	PublicBaseURL  string    `json:"public_base_url"`
	S3BaseURL      string    `json:"s3_base_url"`
	Region         string    `json:"region"`
	MetadataLayout string    `json:"metadata_layout"`
	MaxBytes       int64     `json:"max_bytes"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type PublicDomainBinding struct {
	Host          string `json:"host"`
	Bucket        string `json:"bucket"`
	Prefix        string `json:"prefix,omitempty"`
	IndexDocument bool   `json:"index_document"`
	SPAFallback   bool   `json:"spa_fallback"`
}

func (b *PublicDomainBinding) UnmarshalJSON(data []byte) error {
	type rawBinding PublicDomainBinding
	payload := rawBinding{IndexDocument: true}
	if err := json.Unmarshal(data, &payload); err != nil {
		return err
	}
	*b = PublicDomainBinding(payload)
	return nil
}

type ReplicaObjectMetadata struct {
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	Size               int64             `json:"size"`
	ETag               string            `json:"etag"`
	ChecksumSHA256     string            `json:"checksum_sha256"`
	Revision           int64             `json:"revision"`
	OriginNodeID       string            `json:"origin_node_id,omitempty"`
	LastChangeID       string            `json:"last_change_id,omitempty"`
	ContentType        string            `json:"content_type"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	UserMetadata       map[string]string `json:"user_metadata,omitempty"`
	LastModified       time.Time         `json:"last_modified"`
}

type SyncObjectStatus struct {
	Bucket                 string    `json:"bucket"`
	Key                    string    `json:"key"`
	Status                 string    `json:"status"`
	ExpectedChecksumSHA256 string    `json:"expected_checksum_sha256,omitempty"`
	LastError              string    `json:"last_error,omitempty"`
	Source                 string    `json:"source,omitempty"`
	BaselineNodeID         string    `json:"baseline_node_id,omitempty"`
	UpdatedAt              time.Time `json:"updated_at"`
}

type SyncSettings struct {
	Enabled   bool      `json:"enabled"`
	UpdatedAt time.Time `json:"updated_at"`
}

type SyncStatusCounts struct {
	Pending     int `json:"pending"`
	Verifying   int `json:"verifying"`
	Downloading int `json:"downloading"`
	Ready       int `json:"ready"`
	Error       int `json:"error"`
	Conflict    int `json:"conflict"`
}

type SyncStatusSummary struct {
	BaselineNodeID string `json:"baseline_node_id,omitempty"`
	LastError      string `json:"last_error,omitempty"`
}

type SyncConflictItem struct {
	Bucket         string    `json:"bucket"`
	Key            string    `json:"key"`
	Source         string    `json:"source,omitempty"`
	BaselineNodeID string    `json:"baseline_node_id,omitempty"`
	LastError      string    `json:"last_error,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type SyncEvent struct {
	Cursor     int64                  `json:"cursor"`
	Kind       string                 `json:"kind"`
	Bucket     string                 `json:"bucket"`
	Key        string                 `json:"key,omitempty"`
	BucketData *ReplicaBucketInput    `json:"bucket_data,omitempty"`
	ObjectData *ReplicaObjectMetadata `json:"object_data,omitempty"`
	DomainData []PublicDomainBinding  `json:"domain_data,omitempty"`
	ShareLinks []sharelink.Link       `json:"share_links,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
}

func NormalizeSyncStatus(value string) string {
	switch value {
	case SyncStatusPending, SyncStatusDownloading, SyncStatusReady, SyncStatusError, SyncStatusConflict:
		return value
	case SyncStatusVerifying:
		return value
	default:
		return SyncStatusPending
	}
}

func replicaBucketInputFromMetadata(meta bucketMetadata) ReplicaBucketInput {
	return ReplicaBucketInput{
		Name:           meta.Name,
		CreatedAt:      meta.CreatedAt,
		MetadataLayout: meta.MetadataLayout,
		AccessMode:     meta.AccessMode,
		AccessPolicy:   meta.AccessPolicy,
		QuotaBytes:     meta.QuotaBytes,
		Tags:           cloneStringSlice(meta.Tags),
		Note:           meta.Note,
	}
}

func replicaObjectMetadataFromObjectInfo(info ObjectInfo) ReplicaObjectMetadata {
	return ReplicaObjectMetadata{
		Bucket:             info.Bucket,
		Key:                info.Key,
		Size:               info.Size,
		ETag:               info.ETag,
		ChecksumSHA256:     info.ChecksumSHA256,
		Revision:           info.Revision,
		OriginNodeID:       info.OriginNodeID,
		LastChangeID:       info.LastChangeID,
		ContentType:        info.ContentType,
		CacheControl:       info.CacheControl,
		ContentDisposition: info.ContentDisposition,
		UserMetadata:       cloneStringMap(info.UserMetadata),
		LastModified:       info.LastModified,
	}
}

func replicaObjectMetadataFromObjectMeta(meta objectMetadata) ReplicaObjectMetadata {
	return ReplicaObjectMetadata{
		Bucket:             meta.Bucket,
		Key:                meta.Key,
		Size:               meta.Size,
		ETag:               meta.ETag,
		ChecksumSHA256:     meta.ChecksumSHA256,
		Revision:           meta.Revision,
		OriginNodeID:       meta.OriginNodeID,
		LastChangeID:       meta.LastChangeID,
		ContentType:        meta.ContentType,
		CacheControl:       meta.CacheControl,
		ContentDisposition: meta.ContentDisposition,
		UserMetadata:       cloneStringMap(meta.UserMetadata),
		LastModified:       meta.LastModified,
	}
}

func objectMetadataFromObjectInfo(info ObjectInfo) objectMetadata {
	return objectMetadata{
		Bucket:             info.Bucket,
		Key:                info.Key,
		Size:               info.Size,
		ETag:               info.ETag,
		ChecksumSHA256:     info.ChecksumSHA256,
		Revision:           info.Revision,
		OriginNodeID:       info.OriginNodeID,
		LastChangeID:       info.LastChangeID,
		ContentType:        info.ContentType,
		CacheControl:       info.CacheControl,
		ContentDisposition: info.ContentDisposition,
		UserMetadata:       cloneStringMap(info.UserMetadata),
		LastModified:       info.LastModified,
	}
}
