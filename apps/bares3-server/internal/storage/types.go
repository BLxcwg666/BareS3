package storage

import (
	"errors"
	"io"
	"time"
)

var (
	ErrBucketExists      = errors.New("bucket already exists")
	ErrBucketNotFound    = errors.New("bucket not found")
	ErrBucketNotEmpty    = errors.New("bucket not empty")
	ErrObjectNotFound    = errors.New("object not found")
	ErrInvalidBucketName = errors.New("invalid bucket name")
	ErrInvalidObjectKey  = errors.New("invalid object key")
)

type BucketInfo struct {
	Name           string    `json:"name"`
	Path           string    `json:"path"`
	MetadataPath   string    `json:"metadata_path"`
	CreatedAt      time.Time `json:"created_at"`
	MetadataLayout string    `json:"metadata_layout"`
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

type bucketMetadata struct {
	Name           string    `json:"name"`
	CreatedAt      time.Time `json:"created_at"`
	MetadataLayout string    `json:"metadata_layout"`
}
