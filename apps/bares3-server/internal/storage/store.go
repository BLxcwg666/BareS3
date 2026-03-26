package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"bares3-server/internal/config"
	"go.uber.org/zap"
)

type Store struct {
	dataDir        string
	tmpDir         string
	metadataLayout string
	logger         *zap.Logger
}

func New(cfg config.Config, logger *zap.Logger) *Store {
	return &Store{
		dataDir:        cfg.Paths.DataDir,
		tmpDir:         cfg.Storage.TmpDir,
		metadataLayout: cfg.Storage.MetadataLayout,
		logger:         logger,
	}
}

func (s *Store) CreateBucket(ctx context.Context, name string) (BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return BucketInfo{}, err
	}
	if err := validateBucketName(name); err != nil {
		return BucketInfo{}, err
	}

	root := s.bucketRoot(name)
	if info, err := os.Stat(root); err == nil {
		if info.IsDir() {
			return BucketInfo{}, fmt.Errorf("%w: %s", ErrBucketExists, name)
		}
		return BucketInfo{}, fmt.Errorf("%w: bucket path exists but is not a directory", ErrBucketExists)
	} else if !errors.Is(err, os.ErrNotExist) {
		return BucketInfo{}, err
	}

	if err := os.MkdirAll(s.bucketMetaDir(name), 0o755); err != nil {
		return BucketInfo{}, fmt.Errorf("create bucket directories: %w", err)
	}

	meta := bucketMetadata{
		Name:           name,
		CreatedAt:      time.Now().UTC(),
		MetadataLayout: s.metadataLayout,
	}

	if err := s.writeBucketMetadata(name, meta); err != nil {
		return BucketInfo{}, err
	}

	info := BucketInfo{
		Name:           meta.Name,
		Path:           root,
		MetadataPath:   s.bucketMetadataPath(name),
		CreatedAt:      meta.CreatedAt,
		MetadataLayout: meta.MetadataLayout,
	}

	s.logger.Info("bucket created", zap.String("bucket", name), zap.String("path", root))
	return info, nil
}

func (s *Store) GetBucket(ctx context.Context, name string) (BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return BucketInfo{}, err
	}
	if err := validateBucketName(name); err != nil {
		return BucketInfo{}, err
	}

	root := s.bucketRoot(name)
	info, err := os.Stat(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return BucketInfo{}, fmt.Errorf("%w: %s", ErrBucketNotFound, name)
		}
		return BucketInfo{}, err
	}
	if !info.IsDir() {
		return BucketInfo{}, fmt.Errorf("%w: %s", ErrBucketNotFound, name)
	}

	meta, err := s.readBucketMetadata(name)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return BucketInfo{}, err
	}

	createdAt := info.ModTime().UTC()
	metadataLayout := s.metadataLayout
	if err == nil {
		createdAt = meta.CreatedAt
		metadataLayout = meta.MetadataLayout
	}

	return BucketInfo{
		Name:           name,
		Path:           root,
		MetadataPath:   s.bucketMetadataPath(name),
		CreatedAt:      createdAt,
		MetadataLayout: metadataLayout,
	}, nil
}

func (s *Store) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(s.dataDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	buckets := make([]BucketInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		if err := validateBucketName(name); err != nil {
			continue
		}
		bucket, err := s.GetBucket(ctx, name)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, bucket)
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func (s *Store) PutObject(ctx context.Context, input PutObjectInput) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if input.Body == nil {
		return ObjectInfo{}, fmt.Errorf("%w: object body must not be nil", ErrInvalidObjectKey)
	}
	if _, err := s.GetBucket(ctx, input.Bucket); err != nil {
		return ObjectInfo{}, err
	}

	objectPath, metadataPath, err := s.resolveObjectPaths(input.Bucket, input.Key)
	if err != nil {
		return ObjectInfo{}, err
	}

	stagingDir := joinPath(s.tmpDir, input.Bucket)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return ObjectInfo{}, fmt.Errorf("create staging dir: %w", err)
	}

	stagedObject, err := os.CreateTemp(stagingDir, "obj-*")
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("create staged object file: %w", err)
	}
	stagedObjectPath := stagedObject.Name()
	defer func() {
		_ = os.Remove(stagedObjectPath)
	}()

	hasher := md5.New()
	buf := make([]byte, 32*1024)
	firstBytes := make([]byte, 0, 512)
	var size int64

	for {
		if err := ctx.Err(); err != nil {
			_ = stagedObject.Close()
			return ObjectInfo{}, err
		}

		n, readErr := input.Body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if len(firstBytes) < 512 {
				want := 512 - len(firstBytes)
				if want > len(chunk) {
					want = len(chunk)
				}
				firstBytes = append(firstBytes, chunk[:want]...)
			}

			if _, err := stagedObject.Write(chunk); err != nil {
				_ = stagedObject.Close()
				return ObjectInfo{}, fmt.Errorf("write staged object: %w", err)
			}
			if _, err := hasher.Write(chunk); err != nil {
				_ = stagedObject.Close()
				return ObjectInfo{}, fmt.Errorf("hash object data: %w", err)
			}
			size += int64(n)
		}

		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = stagedObject.Close()
			return ObjectInfo{}, fmt.Errorf("read object body: %w", readErr)
		}
	}

	if err := stagedObject.Sync(); err != nil {
		_ = stagedObject.Close()
		return ObjectInfo{}, fmt.Errorf("sync staged object: %w", err)
	}
	if err := stagedObject.Close(); err != nil {
		return ObjectInfo{}, fmt.Errorf("close staged object: %w", err)
	}

	contentType := strings.TrimSpace(input.ContentType)
	if contentType == "" {
		if len(firstBytes) > 0 {
			contentType = http.DetectContentType(firstBytes)
		} else {
			contentType = "application/octet-stream"
		}
	}

	meta := objectMetadata{
		Bucket:             input.Bucket,
		Key:                input.Key,
		Size:               size,
		ETag:               hex.EncodeToString(hasher.Sum(nil)),
		ContentType:        contentType,
		CacheControl:       strings.TrimSpace(input.CacheControl),
		ContentDisposition: strings.TrimSpace(input.ContentDisposition),
		UserMetadata:       cloneStringMap(input.UserMetadata),
		LastModified:       time.Now().UTC(),
	}
	info := ObjectInfo{
		Bucket:             meta.Bucket,
		Key:                meta.Key,
		Path:               objectPath,
		MetadataPath:       metadataPath,
		Size:               meta.Size,
		ETag:               meta.ETag,
		ContentType:        meta.ContentType,
		CacheControl:       meta.CacheControl,
		ContentDisposition: meta.ContentDisposition,
		UserMetadata:       cloneStringMap(meta.UserMetadata),
		LastModified:       meta.LastModified,
	}

	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		return ObjectInfo{}, fmt.Errorf("create object parent dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(metadataPath), 0o755); err != nil {
		return ObjectInfo{}, fmt.Errorf("create metadata parent dir: %w", err)
	}

	stagedMetadataPath, err := writeJSONTemp(filepath.Dir(metadataPath), "meta-*", meta)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("stage metadata file: %w", err)
	}
	defer func() {
		_ = os.Remove(stagedMetadataPath)
	}()

	if err := replaceFile(stagedObjectPath, objectPath); err != nil {
		return ObjectInfo{}, fmt.Errorf("commit object file: %w", err)
	}
	if err := replaceFile(stagedMetadataPath, metadataPath); err != nil {
		return ObjectInfo{}, fmt.Errorf("commit metadata file: %w", err)
	}

	s.logger.Info(
		"object stored",
		zap.String("bucket", input.Bucket),
		zap.String("key", input.Key),
		zap.Int64("size", size),
		zap.String("etag", info.ETag),
	)

	return info, nil
}

func (s *Store) StatObject(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return ObjectInfo{}, err
	}

	objectPath, metadataPath, err := s.resolveObjectPaths(bucket, key)
	if err != nil {
		return ObjectInfo{}, err
	}

	fileInfo, err := os.Stat(objectPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ObjectInfo{}, fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, key)
		}
		return ObjectInfo{}, err
	}

	meta := objectMetadata{}
	if err := readJSONFile(metadataPath, &meta); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return ObjectInfo{}, err
		}
		return ObjectInfo{
			Bucket:       bucket,
			Key:          key,
			Path:         objectPath,
			MetadataPath: metadataPath,
			Size:         fileInfo.Size(),
			ContentType:  fallbackContentType(objectPath),
			LastModified: fileInfo.ModTime().UTC(),
		}, nil
	}

	object := ObjectInfo{
		Bucket:             meta.Bucket,
		Key:                meta.Key,
		Path:               objectPath,
		MetadataPath:       metadataPath,
		Size:               meta.Size,
		ETag:               meta.ETag,
		ContentType:        meta.ContentType,
		CacheControl:       meta.CacheControl,
		ContentDisposition: meta.ContentDisposition,
		UserMetadata:       cloneStringMap(meta.UserMetadata),
		LastModified:       meta.LastModified,
	}

	if object.Size == 0 && fileInfo.Size() > 0 {
		object.Size = fileInfo.Size()
	}
	if object.LastModified.IsZero() {
		object.LastModified = fileInfo.ModTime().UTC()
	}
	if strings.TrimSpace(object.ContentType) == "" {
		object.ContentType = fallbackContentType(objectPath)
	}

	return object, nil
}

func (s *Store) OpenObject(ctx context.Context, bucket, key string) (*os.File, ObjectInfo, error) {
	object, err := s.StatObject(ctx, bucket, key)
	if err != nil {
		return nil, ObjectInfo{}, err
	}

	file, err := os.Open(object.Path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ObjectInfo{}, fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, key)
		}
		return nil, ObjectInfo{}, err
	}

	return file, object, nil
}

func (s *Store) bucketRoot(name string) string {
	return joinPath(s.dataDir, name)
}

func (s *Store) bucketControlDir(name string) string {
	return joinPath(s.bucketRoot(name), controlDirName)
}

func (s *Store) bucketMetaDir(name string) string {
	return joinPath(s.bucketControlDir(name), metaDirName)
}

func (s *Store) bucketMetadataPath(name string) string {
	return joinPath(s.bucketControlDir(name), bucketMetaName)
}

func (s *Store) readBucketMetadata(name string) (bucketMetadata, error) {
	meta := bucketMetadata{}
	if err := readJSONFile(s.bucketMetadataPath(name), &meta); err != nil {
		return bucketMetadata{}, err
	}
	return meta, nil
}

func (s *Store) writeBucketMetadata(name string, meta bucketMetadata) error {
	path := s.bucketMetadataPath(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	staged, err := writeJSONTemp(filepath.Dir(path), "bucket-*", meta)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(staged)
	}()
	return replaceFile(staged, path)
}

func (s *Store) resolveObjectPaths(bucket, key string) (string, string, error) {
	if err := validateBucketName(bucket); err != nil {
		return "", "", err
	}
	segments, err := encodeObjectKey(key)
	if err != nil {
		return "", "", err
	}

	objectPath := joinPath(append([]string{s.bucketRoot(bucket)}, segments...)...)
	metaSegments := append([]string{s.bucketMetaDir(bucket)}, segments...)
	metaSegments[len(metaSegments)-1] += ".json"
	metadataPath := joinPath(metaSegments...)

	return objectPath, metadataPath, nil
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

func fallbackContentType(path string) string {
	if value := mime.TypeByExtension(filepath.Ext(path)); value != "" {
		return value
	}
	return "application/octet-stream"
}

func cloneStringMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}
