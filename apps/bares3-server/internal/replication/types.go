package replication

import (
	"time"

	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
)

const (
	HeaderAccessToken    = "X-BareS3-Sync-Token"
	HeaderChecksumSHA256 = "X-BareS3-Checksum-SHA256"
	HeaderETag           = "X-BareS3-ETag"
	HeaderRevision       = "X-BareS3-Revision"
	HeaderOriginNodeID   = "X-BareS3-Origin-Node-ID"
	HeaderLastChangeID   = "X-BareS3-Last-Change-ID"
)

type Manifest struct {
	GeneratedAt       time.Time                     `json:"generated_at"`
	Full              bool                          `json:"full"`
	HasMore           bool                          `json:"has_more,omitempty"`
	Cursor            int64                         `json:"cursor"`
	DomainsChanged    bool                          `json:"domains_changed,omitempty"`
	Domains           []storage.PublicDomainBinding `json:"domains,omitempty"`
	ShareLinksChanged bool                          `json:"share_links_changed,omitempty"`
	ShareLinks        []sharelink.Link              `json:"share_links,omitempty"`
	Buckets           []BucketManifest              `json:"buckets,omitempty"`
	Objects           []ObjectManifest              `json:"objects,omitempty"`
	DeletedBuckets    []string                      `json:"deleted_buckets,omitempty"`
	DeletedObjects    []DeletedObjectManifest       `json:"deleted_objects,omitempty"`
}

type SourceStatus struct {
	Cursor      int64 `json:"cursor"`
	UsedBytes   int64 `json:"used_bytes"`
	BucketCount int   `json:"bucket_count"`
	ObjectCount int   `json:"object_count"`
}

type DeletedObjectManifest struct {
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

type BucketManifest struct {
	Name               string                     `json:"name"`
	CreatedAt          time.Time                  `json:"created_at"`
	MetadataLayout     string                     `json:"metadata_layout"`
	AccessMode         string                     `json:"access_mode"`
	AccessPolicy       storage.BucketAccessPolicy `json:"access_policy"`
	ReplicationEnabled bool                       `json:"replication_enabled"`
	QuotaBytes         int64                      `json:"quota_bytes"`
	Tags               []string                   `json:"tags,omitempty"`
	Note               string                     `json:"note,omitempty"`
}

type ObjectManifest struct {
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
