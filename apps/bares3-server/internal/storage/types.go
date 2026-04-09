package storage

import (
	"errors"
	"io"
	"time"
)

var (
	ErrBucketExists          = errors.New("bucket already exists")
	ErrBucketNotFound        = errors.New("bucket not found")
	ErrBucketNotEmpty        = errors.New("bucket not empty")
	ErrBucketQuotaExceeded   = errors.New("bucket quota exceeded")
	ErrObjectNotFound        = errors.New("object not found")
	ErrObjectExists          = errors.New("object already exists")
	ErrInstanceQuotaExceeded = errors.New("instance quota exceeded")
	ErrInvalidMetadata       = errors.New("invalid metadata")
	ErrInvalidBucketAccess   = errors.New("invalid bucket access")
	ErrInvalidQuota          = errors.New("invalid quota")
	ErrInvalidMove           = errors.New("invalid move")
	ErrUploadNotFound        = errors.New("multipart upload not found")
	ErrInvalidPart           = errors.New("invalid multipart part")
	ErrInvalidPartOrder      = errors.New("invalid multipart part order")
	ErrInvalidPartNumber     = errors.New("invalid multipart part number")
	ErrInvalidBucketName     = errors.New("invalid bucket name")
	ErrInvalidObjectKey      = errors.New("invalid object key")
)

type BucketInfo struct {
	Name           string    `json:"name"`
	Path           string    `json:"path"`
	MetadataPath   string    `json:"metadata_path"`
	CreatedAt      time.Time `json:"created_at"`
	MetadataLayout string    `json:"metadata_layout"`
	AccessMode     string    `json:"access_mode"`
	QuotaBytes     int64     `json:"quota_bytes,omitempty"`
	Tags           []string  `json:"tags,omitempty"`
	Note           string    `json:"note,omitempty"`
	UsedBytes      int64     `json:"used_bytes"`
	ObjectCount    int       `json:"object_count"`
}

type CreateBucketInput struct {
	Name         string
	AccessMode   string
	AccessPolicy BucketAccessPolicy
	QuotaBytes   int64
}

type BucketAccessRule struct {
	Prefix string `json:"prefix"`
	Action string `json:"action"`
	Note   string `json:"note,omitempty"`
}

type BucketAccessPolicy struct {
	DefaultAction string             `json:"default_action"`
	Rules         []BucketAccessRule `json:"rules"`
}

type BucketAccessConfig struct {
	Mode   string             `json:"mode"`
	Policy BucketAccessPolicy `json:"policy"`
}

type UpdateBucketAccessInput struct {
	Name   string
	Mode   string
	Policy BucketAccessPolicy
}

type BucketUsageSample struct {
	RecordedAt  time.Time `json:"recorded_at"`
	UsedBytes   int64     `json:"used_bytes"`
	ObjectCount int       `json:"object_count"`
	QuotaBytes  int64     `json:"quota_bytes,omitempty"`
}

type UpdateBucketInput struct {
	Name       string
	NewName    string
	AccessMode string
	QuotaBytes int64
	Tags       []string
	Note       string
}

type ObjectInfo struct {
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	Path               string            `json:"path"`
	MetadataPath       string            `json:"metadata_path"`
	Size               int64             `json:"size"`
	ETag               string            `json:"etag"`
	ContentType        string            `json:"content_type"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	UserMetadata       map[string]string `json:"user_metadata,omitempty"`
	LastModified       time.Time         `json:"last_modified"`
}

type objectMetadata struct {
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	Size               int64             `json:"size"`
	ETag               string            `json:"etag"`
	ContentType        string            `json:"content_type"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	UserMetadata       map[string]string `json:"user_metadata,omitempty"`
	LastModified       time.Time         `json:"last_modified"`
}

type PutObjectInput struct {
	Bucket             string
	Key                string
	Body               io.Reader
	ContentType        string
	CacheControl       string
	ContentDisposition string
	UserMetadata       map[string]string
}

type ListObjectsOptions struct {
	Prefix string
	Query  string
	After  string
	Limit  int
}

type ListObjectsPage struct {
	Items      []ObjectInfo
	HasMore    bool
	NextCursor string
}

type MoveObjectInput struct {
	SourceBucket      string
	SourceKey         string
	DestinationBucket string
	DestinationKey    string
}

type MovePrefixInput struct {
	SourceBucket      string
	SourcePrefix      string
	DestinationBucket string
	DestinationPrefix string
}

type MoveResult struct {
	Kind              string `json:"kind"`
	SourceBucket      string `json:"source_bucket"`
	SourceKey         string `json:"source_key,omitempty"`
	SourcePrefix      string `json:"source_prefix,omitempty"`
	DestinationBucket string `json:"destination_bucket"`
	DestinationKey    string `json:"destination_key,omitempty"`
	DestinationPrefix string `json:"destination_prefix,omitempty"`
	MovedCount        int    `json:"moved_count"`
}

type UpdateObjectMetadataInput struct {
	Bucket             string
	Key                string
	ContentType        string
	CacheControl       string
	ContentDisposition string
	UserMetadata       map[string]string
}

type InitiateMultipartUploadInput struct {
	Bucket             string
	Key                string
	ContentType        string
	CacheControl       string
	ContentDisposition string
	UserMetadata       map[string]string
}

type MultipartUploadInfo struct {
	UploadID           string            `json:"upload_id"`
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	ContentType        string            `json:"content_type,omitempty"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	UserMetadata       map[string]string `json:"user_metadata,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
}

type UploadPartInput struct {
	Bucket     string
	Key        string
	UploadID   string
	PartNumber int
	Body       io.Reader
}

type MultipartPartInfo struct {
	PartNumber   int       `json:"part_number"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}

type CompletedPart struct {
	PartNumber int
	ETag       string
}

type bucketMetadata struct {
	Name           string             `json:"name"`
	CreatedAt      time.Time          `json:"created_at"`
	MetadataLayout string             `json:"metadata_layout"`
	AccessMode     string             `json:"access_mode,omitempty"`
	AccessPolicy   BucketAccessPolicy `json:"access_policy,omitempty"`
	QuotaBytes     int64              `json:"quota_bytes,omitempty"`
	Tags           []string           `json:"tags,omitempty"`
	Note           string             `json:"note,omitempty"`
}

type multipartUploadMetadata struct {
	UploadID           string            `json:"upload_id"`
	Bucket             string            `json:"bucket"`
	Key                string            `json:"key"`
	ContentType        string            `json:"content_type,omitempty"`
	CacheControl       string            `json:"cache_control,omitempty"`
	ContentDisposition string            `json:"content_disposition,omitempty"`
	UserMetadata       map[string]string `json:"user_metadata,omitempty"`
	CreatedAt          time.Time         `json:"created_at"`
}

type multipartPartMetadata struct {
	PartNumber   int       `json:"part_number"`
	ETag         string    `json:"etag"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"last_modified"`
}
