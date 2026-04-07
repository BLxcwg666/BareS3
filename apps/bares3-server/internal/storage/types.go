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
	ErrInstanceQuotaExceeded = errors.New("instance quota exceeded")
	ErrInvalidQuota          = errors.New("invalid quota")
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
	QuotaBytes     int64     `json:"quota_bytes,omitempty"`
	UsedBytes      int64     `json:"used_bytes"`
	ObjectCount    int       `json:"object_count"`
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
	Limit  int
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
	Name           string    `json:"name"`
	CreatedAt      time.Time `json:"created_at"`
	MetadataLayout string    `json:"metadata_layout"`
	QuotaBytes     int64     `json:"quota_bytes,omitempty"`
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
