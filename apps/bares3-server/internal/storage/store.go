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
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bares3-server/internal/config"
	"go.uber.org/zap"
)

type Store struct {
	dataDir        string
	tmpDir         string
	metadataLayout string
	commitMu       sync.Mutex
	instanceQuota  atomic.Int64
	logger         *zap.Logger
}

const bucketUsageHistoryLimit = 60

func New(cfg config.Config, logger *zap.Logger) *Store {
	store := &Store{
		dataDir:        cfg.Paths.DataDir,
		tmpDir:         cfg.Storage.TmpDir,
		metadataLayout: cfg.Storage.MetadataLayout,
		logger:         logger,
	}
	store.instanceQuota.Store(cfg.Storage.MaxBytes)
	return store
}

func (s *Store) CreateBucket(ctx context.Context, name string, quotaBytes int64) (BucketInfo, error) {
	return s.CreateBucketWithOptions(ctx, CreateBucketInput{
		Name:       name,
		QuotaBytes: quotaBytes,
		AccessMode: BucketAccessPrivate,
	})
}

func (s *Store) CreateBucketWithOptions(ctx context.Context, input CreateBucketInput) (BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return BucketInfo{}, err
	}
	name := strings.TrimSpace(input.Name)
	if err := validateBucketName(name); err != nil {
		return BucketInfo{}, err
	}
	if err := validateQuotaBytes(input.QuotaBytes); err != nil {
		return BucketInfo{}, err
	}
	if err := validateBucketAccessMode(input.AccessMode); err != nil {
		return BucketInfo{}, err
	}
	accessMode := NormalizeBucketAccessMode(input.AccessMode)
	if instanceQuota := s.InstanceQuotaBytes(); instanceQuota > 0 && input.QuotaBytes > instanceQuota {
		return BucketInfo{}, fmt.Errorf("%w: bucket quota %d exceeds instance quota %d", ErrInvalidQuota, input.QuotaBytes, instanceQuota)
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
		AccessMode:     accessMode,
		QuotaBytes:     input.QuotaBytes,
	}

	if err := s.writeBucketMetadata(name, meta); err != nil {
		return BucketInfo{}, err
	}
	if err := s.recordBucketUsageSample(name, 0, 0, meta.QuotaBytes); err != nil {
		return BucketInfo{}, err
	}

	info := BucketInfo{
		Name:           meta.Name,
		Path:           root,
		MetadataPath:   s.bucketMetadataPath(name),
		CreatedAt:      meta.CreatedAt,
		MetadataLayout: meta.MetadataLayout,
		AccessMode:     meta.AccessMode,
		QuotaBytes:     meta.QuotaBytes,
		Tags:           cloneStringSlice(meta.Tags),
		Note:           meta.Note,
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
	accessMode := BucketAccessPrivate
	quotaBytes := int64(0)
	tags := []string(nil)
	note := ""
	if err == nil {
		createdAt = meta.CreatedAt
		metadataLayout = meta.MetadataLayout
		accessMode = NormalizeBucketAccessMode(meta.AccessMode)
		quotaBytes = meta.QuotaBytes
		tags = cloneStringSlice(meta.Tags)
		note = meta.Note
	}

	return BucketInfo{
		Name:           name,
		Path:           root,
		MetadataPath:   s.bucketMetadataPath(name),
		CreatedAt:      createdAt,
		MetadataLayout: metadataLayout,
		AccessMode:     accessMode,
		QuotaBytes:     quotaBytes,
		Tags:           tags,
		Note:           note,
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
		usage, err := s.bucketUsage(ctx, name)
		if err != nil {
			return nil, err
		}
		bucket.UsedBytes = usage.UsedBytes
		bucket.ObjectCount = usage.ObjectCount
		buckets = append(buckets, bucket)
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

func (s *Store) UpdateBucket(ctx context.Context, input UpdateBucketInput) (BucketInfo, error) {
	if err := ctx.Err(); err != nil {
		return BucketInfo{}, err
	}

	name := strings.TrimSpace(input.Name)
	if err := validateBucketName(name); err != nil {
		return BucketInfo{}, err
	}

	nextName := strings.TrimSpace(input.NewName)
	if nextName == "" {
		nextName = name
	}
	if err := validateBucketName(nextName); err != nil {
		return BucketInfo{}, err
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	currentBucket, err := s.GetBucket(ctx, name)
	if err != nil {
		return BucketInfo{}, err
	}
	usage, err := s.bucketUsage(ctx, name)
	if err != nil {
		return BucketInfo{}, err
	}
	currentBucket.UsedBytes = usage.UsedBytes
	currentBucket.ObjectCount = usage.ObjectCount
	nextAccessMode := strings.TrimSpace(input.AccessMode)
	if nextAccessMode == "" {
		nextAccessMode = currentBucket.AccessMode
	}
	if err := validateBucketAccessMode(nextAccessMode); err != nil {
		return BucketInfo{}, err
	}
	nextAccessMode = NormalizeBucketAccessMode(nextAccessMode)
	if err := validateQuotaBytes(input.QuotaBytes); err != nil {
		return BucketInfo{}, err
	}

	if input.QuotaBytes > 0 && currentBucket.UsedBytes > input.QuotaBytes {
		return BucketInfo{}, fmt.Errorf("%w: quota %d is below current usage %d", ErrInvalidQuota, input.QuotaBytes, currentBucket.UsedBytes)
	}
	if instanceQuota := s.InstanceQuotaBytes(); instanceQuota > 0 && input.QuotaBytes > instanceQuota {
		return BucketInfo{}, fmt.Errorf("%w: bucket quota %d exceeds instance quota %d", ErrInvalidQuota, input.QuotaBytes, instanceQuota)
	}

	meta, err := s.readBucketMetadata(name)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return BucketInfo{}, err
		}
		meta = bucketMetadata{
			Name:           currentBucket.Name,
			CreatedAt:      currentBucket.CreatedAt,
			MetadataLayout: currentBucket.MetadataLayout,
			AccessMode:     currentBucket.AccessMode,
			QuotaBytes:     currentBucket.QuotaBytes,
			Tags:           cloneStringSlice(currentBucket.Tags),
			Note:           currentBucket.Note,
		}
	}

	meta.Name = nextName
	meta.MetadataLayout = currentBucket.MetadataLayout
	meta.AccessMode = nextAccessMode
	meta.QuotaBytes = input.QuotaBytes
	meta.Tags = normalizeBucketTags(input.Tags)
	meta.Note = strings.TrimSpace(input.Note)

	renamed := false
	if nextName != name {
		if _, err := os.Stat(s.bucketRoot(nextName)); err == nil {
			return BucketInfo{}, fmt.Errorf("%w: %s", ErrBucketExists, nextName)
		} else if !errors.Is(err, os.ErrNotExist) {
			return BucketInfo{}, err
		}
		if err := os.Rename(s.bucketRoot(name), s.bucketRoot(nextName)); err != nil {
			return BucketInfo{}, fmt.Errorf("rename bucket dir: %w", err)
		}
		renamed = true
	}

	if err := s.writeBucketMetadata(nextName, meta); err != nil {
		if renamed {
			_ = os.Rename(s.bucketRoot(nextName), s.bucketRoot(name))
		}
		return BucketInfo{}, err
	}
	if renamed {
		if err := s.rewriteBucketObjectMetadata(nextName); err != nil {
			return BucketInfo{}, err
		}
		if err := s.rewriteBucketMultipartMetadata(nextName); err != nil {
			return BucketInfo{}, err
		}
	}
	if input.QuotaBytes != currentBucket.QuotaBytes {
		if err := s.recordBucketUsageSample(nextName, currentBucket.UsedBytes, currentBucket.ObjectCount, input.QuotaBytes); err != nil {
			return BucketInfo{}, err
		}
	}

	updated := currentBucket
	updated.Name = nextName
	updated.Path = s.bucketRoot(nextName)
	updated.MetadataPath = s.bucketMetadataPath(nextName)
	updated.AccessMode = nextAccessMode
	updated.QuotaBytes = input.QuotaBytes
	updated.Tags = cloneStringSlice(meta.Tags)
	updated.Note = meta.Note

	s.logger.Info(
		"bucket updated",
		zap.String("bucket", name),
		zap.String("updated_bucket", nextName),
		zap.String("access_mode", updated.AccessMode),
		zap.Int64("quota_bytes", updated.QuotaBytes),
	)

	return updated, nil
}

func (s *Store) ListBucketUsageHistory(ctx context.Context, name string, limit int) ([]BucketUsageSample, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if limit == 0 {
		if _, err := s.GetBucket(ctx, name); err != nil {
			return nil, err
		}
		return []BucketUsageSample{}, nil
	}

	bucket, err := s.GetBucket(ctx, name)
	if err != nil {
		return nil, err
	}

	history, err := s.readBucketUsageHistory(name)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		snapshot, snapshotErr := s.currentBucketUsageSample(ctx, name)
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		snapshot.QuotaBytes = bucket.QuotaBytes
		history = []BucketUsageSample{snapshot}
	}
	if len(history) == 0 {
		snapshot, snapshotErr := s.currentBucketUsageSample(ctx, name)
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		history = []BucketUsageSample{snapshot}
	}
	if limit > 0 && len(history) > limit {
		history = history[len(history)-limit:]
	}

	items := make([]BucketUsageSample, len(history))
	copy(items, history)
	return items, nil
}

func (s *Store) InstanceQuotaBytes() int64 {
	return s.instanceQuota.Load()
}

func (s *Store) SetInstanceQuotaBytes(value int64) error {
	if err := validateQuotaBytes(value); err != nil {
		return err
	}
	s.instanceQuota.Store(value)
	return nil
}

func (s *Store) UsageSummary(ctx context.Context) (int64, int, error) {
	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return 0, 0, err
	}

	var usedBytes int64
	activeLinks := 0
	for _, bucket := range buckets {
		usedBytes += bucket.UsedBytes
		activeLinks += bucket.ObjectCount
	}

	return usedBytes, activeLinks, nil
}

func (s *Store) bucketUsage(ctx context.Context, bucket string) (BucketInfo, error) {
	root := s.bucketRoot(bucket)
	usedBytes := int64(0)
	objectCount := 0

	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			if path != root && filepath.Base(path) == controlDirName {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		usedBytes += info.Size()
		objectCount += 1
		return nil
	})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return BucketInfo{}, nil
		}
		return BucketInfo{}, err
	}

	return BucketInfo{UsedBytes: usedBytes, ObjectCount: objectCount}, nil
}

func validateQuotaBytes(value int64) error {
	if value < 0 {
		return fmt.Errorf("%w: quota must not be negative", ErrInvalidQuota)
	}
	return nil
}

func (s *Store) ListObjects(ctx context.Context, bucket string, options ListObjectsOptions) ([]ObjectInfo, error) {
	page, err := s.ListObjectsPage(ctx, bucket, options)
	if err != nil {
		return nil, err
	}
	return page.Items, nil
}

func (s *Store) ListObjectsPage(ctx context.Context, bucket string, options ListObjectsOptions) (ListObjectsPage, error) {
	objects, err := s.collectObjects(ctx, bucket, options.Prefix)
	if err != nil {
		return ListObjectsPage{}, err
	}

	objects = filterObjectsByQuery(objects, options.Query)
	objects = applyObjectsAfterCursor(objects, options.After)

	page := ListObjectsPage{Items: objects}
	if options.Limit > 0 && len(objects) > options.Limit {
		page.Items = objects[:options.Limit]
		page.HasMore = true
		page.NextCursor = page.Items[len(page.Items)-1].Key
	}

	return page, nil
}

func (s *Store) collectObjects(ctx context.Context, bucket, prefix string) ([]ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return nil, err
	}

	prefix = strings.TrimSpace(prefix)
	objectsByKey := make(map[string]ObjectInfo)

	metadataRoot := s.bucketMetaDir(bucket)
	if err := filepath.WalkDir(metadataRoot, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if filepath.Ext(path) != ".json" {
			return nil
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		meta := objectMetadata{}
		if err := readJSONFile(path, &meta); err != nil {
			return err
		}
		if prefix != "" && !strings.HasPrefix(meta.Key, prefix) {
			return nil
		}

		objectPath, metadataPath, err := s.resolveObjectPaths(bucket, meta.Key)
		if err != nil {
			return err
		}
		objectsByKey[meta.Key] = ObjectInfo{
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
		return nil
	}); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}

	if err := filepath.WalkDir(s.bucketRoot(bucket), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path != s.bucketRoot(bucket) && filepath.Base(path) == controlDirName {
				return filepath.SkipDir
			}
			return nil
		}

		relPath, err := filepath.Rel(s.bucketRoot(bucket), path)
		if err != nil {
			return err
		}
		key := filepath.ToSlash(relPath)
		if prefix != "" && !strings.HasPrefix(key, prefix) {
			return nil
		}
		if _, ok := objectsByKey[key]; ok {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		info, err := entry.Info()
		if err != nil {
			return err
		}
		objectsByKey[key] = ObjectInfo{
			Bucket:       bucket,
			Key:          key,
			Path:         path,
			MetadataPath: "",
			Size:         info.Size(),
			ContentType:  fallbackContentType(path),
			LastModified: info.ModTime().UTC(),
		}
		return nil
	}); err != nil {
		return nil, err
	}

	objects := make([]ObjectInfo, 0, len(objectsByKey))
	for _, object := range objectsByKey {
		objects = append(objects, object)
	}

	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	return objects, nil
}

func filterObjectsByQuery(objects []ObjectInfo, query string) []ObjectInfo {
	trimmed := strings.ToLower(strings.TrimSpace(query))
	if trimmed == "" {
		return objects
	}

	filtered := make([]ObjectInfo, 0, len(objects))
	for _, object := range objects {
		if objectMatchesQuery(object, trimmed) {
			filtered = append(filtered, object)
		}
	}
	return filtered
}

func objectMatchesQuery(object ObjectInfo, query string) bool {
	if strings.Contains(strings.ToLower(object.Key), query) {
		return true
	}
	if strings.Contains(strings.ToLower(object.ContentType), query) {
		return true
	}
	if strings.Contains(strings.ToLower(object.CacheControl), query) {
		return true
	}
	if strings.Contains(strings.ToLower(object.ETag), query) {
		return true
	}
	for key, value := range object.UserMetadata {
		if strings.Contains(strings.ToLower(key), query) || strings.Contains(strings.ToLower(value), query) {
			return true
		}
	}
	return false
}

func applyObjectsAfterCursor(objects []ObjectInfo, after string) []ObjectInfo {
	trimmed := strings.TrimSpace(after)
	if trimmed == "" || len(objects) == 0 {
		return objects
	}
	start := sort.Search(len(objects), func(index int) bool {
		return objects[index].Key > trimmed
	})
	if start >= len(objects) {
		return []ObjectInfo{}
	}
	return objects[start:]
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

	if _, _, err := s.resolveObjectPaths(input.Bucket, input.Key); err != nil {
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
	info, err := s.commitObjectWithQuota(ctx, input.Bucket, input.Key, stagedObjectPath, meta)
	if err != nil {
		return ObjectInfo{}, err
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

func (s *Store) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return err
	}
	if err := s.deleteObjectLocked(bucket, key); err != nil {
		return err
	}
	return s.recordBucketUsageSamples(ctx, bucket)
}

func (s *Store) DeletePrefix(ctx context.Context, bucket, prefix string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	bucket = strings.TrimSpace(bucket)
	prefix = normalizeMovePrefix(prefix)
	if bucket == "" || prefix == "" {
		return 0, fmt.Errorf("%w: bucket and prefix are required", ErrInvalidObjectKey)
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if _, err := s.GetBucket(ctx, bucket); err != nil {
		return 0, err
	}

	objects, err := s.ListObjects(ctx, bucket, ListObjectsOptions{Prefix: prefix})
	if err != nil {
		return 0, err
	}
	if len(objects) == 0 {
		return 0, fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, prefix)
	}

	deleted := 0
	for _, object := range objects {
		if err := s.deleteObjectLocked(bucket, object.Key); err != nil {
			return deleted, err
		}
		deleted += 1
	}
	if err := s.recordBucketUsageSamples(ctx, bucket); err != nil {
		return deleted, err
	}

	s.logger.Info("prefix deleted", zap.String("bucket", bucket), zap.String("prefix", prefix), zap.Int("count", deleted))
	return deleted, nil
}

func (s *Store) UpdateObjectMetadata(ctx context.Context, input UpdateObjectMetadataInput) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	bucket := strings.TrimSpace(input.Bucket)
	key := strings.TrimSpace(input.Key)
	if bucket == "" || key == "" {
		return ObjectInfo{}, fmt.Errorf("%w: bucket and key are required", ErrInvalidMetadata)
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	object, err := s.StatObject(ctx, bucket, key)
	if err != nil {
		return ObjectInfo{}, err
	}

	updatedAt := time.Now().UTC()
	meta := objectMetadata{
		Bucket:             bucket,
		Key:                key,
		Size:               object.Size,
		ETag:               object.ETag,
		ContentType:        strings.TrimSpace(input.ContentType),
		CacheControl:       strings.TrimSpace(input.CacheControl),
		ContentDisposition: strings.TrimSpace(input.ContentDisposition),
		UserMetadata:       cloneStringMap(input.UserMetadata),
		LastModified:       updatedAt,
	}
	if meta.ContentType == "" {
		meta.ContentType = fallbackContentType(object.Path)
	}

	metadataPath := object.MetadataPath
	if strings.TrimSpace(metadataPath) == "" {
		_, metadataPath, err = s.resolveObjectPaths(bucket, key)
		if err != nil {
			return ObjectInfo{}, err
		}
	}
	stagedMetadataPath, err := writeJSONTemp(filepath.Dir(metadataPath), "meta-*", meta)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("stage metadata file: %w", err)
	}
	defer func() {
		_ = os.Remove(stagedMetadataPath)
	}()
	if err := replaceFile(stagedMetadataPath, metadataPath); err != nil {
		return ObjectInfo{}, fmt.Errorf("write metadata file: %w", err)
	}

	updated := object
	updated.MetadataPath = metadataPath
	updated.ContentType = meta.ContentType
	updated.CacheControl = meta.CacheControl
	updated.ContentDisposition = meta.ContentDisposition
	updated.UserMetadata = cloneStringMap(meta.UserMetadata)
	updated.LastModified = meta.LastModified

	s.logger.Info("object metadata updated", zap.String("bucket", bucket), zap.String("key", key))
	return updated, nil
}

func (s *Store) deleteObjectLocked(bucket, key string) error {

	objectPath, metadataPath, err := s.resolveObjectPaths(bucket, key)
	if err != nil {
		return err
	}

	removedObject := false
	if err := os.Remove(objectPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove object file: %w", err)
		}
	} else {
		removedObject = true
	}

	removedMetadata := false
	if err := os.Remove(metadataPath); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove metadata file: %w", err)
		}
	} else {
		removedMetadata = true
	}

	if !removedObject && !removedMetadata {
		return fmt.Errorf("%w: %s/%s", ErrObjectNotFound, bucket, key)
	}

	pruneEmptyParents(filepath.Dir(objectPath), s.bucketRoot(bucket))
	pruneEmptyParents(filepath.Dir(metadataPath), s.bucketMetaDir(bucket))

	s.logger.Info("object deleted", zap.String("bucket", bucket), zap.String("key", key))
	return nil
}

func (s *Store) MoveObject(ctx context.Context, input MoveObjectInput) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}

	sourceBucket := strings.TrimSpace(input.SourceBucket)
	sourceKey := strings.TrimSpace(input.SourceKey)
	destinationBucket := strings.TrimSpace(input.DestinationBucket)
	destinationKey := strings.TrimSpace(input.DestinationKey)
	if sourceBucket == "" || sourceKey == "" || destinationBucket == "" || destinationKey == "" {
		return ObjectInfo{}, fmt.Errorf("%w: source and destination are required", ErrInvalidMove)
	}
	if sourceBucket == destinationBucket && sourceKey == destinationKey {
		return ObjectInfo{}, fmt.Errorf("%w: source and destination are identical", ErrInvalidMove)
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	object, err := s.StatObject(ctx, sourceBucket, sourceKey)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := s.ensureDestinationBucketQuota(ctx, sourceBucket, destinationBucket, object.Size); err != nil {
		return ObjectInfo{}, err
	}
	if err := s.ensureDestinationAvailable(destinationBucket, destinationKey); err != nil {
		return ObjectInfo{}, err
	}

	moved, err := s.moveObjectLocked(object, destinationBucket, destinationKey)
	if err != nil {
		return ObjectInfo{}, err
	}
	if object.Bucket != moved.Bucket {
		if err := s.recordBucketUsageSamples(ctx, object.Bucket, moved.Bucket); err != nil {
			return ObjectInfo{}, err
		}
	}
	return moved, nil
}

func (s *Store) MovePrefix(ctx context.Context, input MovePrefixInput) (MoveResult, error) {
	if err := ctx.Err(); err != nil {
		return MoveResult{}, err
	}

	sourceBucket := strings.TrimSpace(input.SourceBucket)
	destinationBucket := strings.TrimSpace(input.DestinationBucket)
	sourcePrefix := normalizeMovePrefix(input.SourcePrefix)
	destinationPrefix := normalizeMovePrefix(input.DestinationPrefix)
	if sourceBucket == "" || destinationBucket == "" || sourcePrefix == "" {
		return MoveResult{}, fmt.Errorf("%w: source bucket, source prefix, and destination bucket are required", ErrInvalidMove)
	}
	if sourceBucket == destinationBucket && sourcePrefix == destinationPrefix {
		return MoveResult{}, fmt.Errorf("%w: source and destination are identical", ErrInvalidMove)
	}
	if sourceBucket == destinationBucket && strings.HasPrefix(destinationPrefix, sourcePrefix) {
		return MoveResult{}, fmt.Errorf("%w: cannot move a folder into itself", ErrInvalidMove)
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	objects, err := s.ListObjects(ctx, sourceBucket, ListObjectsOptions{Prefix: sourcePrefix})
	if err != nil {
		return MoveResult{}, err
	}
	if len(objects) == 0 {
		return MoveResult{}, fmt.Errorf("%w: %s/%s", ErrObjectNotFound, sourceBucket, sourcePrefix)
	}

	type movePlan struct {
		object         ObjectInfo
		destinationKey string
	}
	plans := make([]movePlan, 0, len(objects))
	plannedKeys := make(map[string]struct{}, len(objects))
	totalBytes := int64(0)
	for _, object := range objects {
		relative := strings.TrimPrefix(object.Key, sourcePrefix)
		destinationKey := destinationPrefix + relative
		if destinationKey == "" {
			return MoveResult{}, fmt.Errorf("%w: invalid destination prefix", ErrInvalidMove)
		}
		if _, exists := plannedKeys[destinationKey]; exists {
			return MoveResult{}, fmt.Errorf("%w: destination contains duplicate keys", ErrInvalidMove)
		}
		plannedKeys[destinationKey] = struct{}{}
		plans = append(plans, movePlan{object: object, destinationKey: destinationKey})
		totalBytes += object.Size
	}

	if err := s.ensureDestinationBucketQuota(ctx, sourceBucket, destinationBucket, totalBytes); err != nil {
		return MoveResult{}, err
	}
	for _, plan := range plans {
		if plan.object.Bucket == destinationBucket && plan.object.Key == plan.destinationKey {
			return MoveResult{}, fmt.Errorf("%w: source and destination are identical", ErrInvalidMove)
		}
		if err := s.ensureDestinationAvailable(destinationBucket, plan.destinationKey); err != nil {
			return MoveResult{}, err
		}
	}

	for _, plan := range plans {
		if _, err := s.moveObjectLocked(plan.object, destinationBucket, plan.destinationKey); err != nil {
			return MoveResult{}, err
		}
	}
	if sourceBucket != destinationBucket {
		if err := s.recordBucketUsageSamples(ctx, sourceBucket, destinationBucket); err != nil {
			return MoveResult{}, err
		}
	}

	s.logger.Info(
		"prefix moved",
		zap.String("source_bucket", sourceBucket),
		zap.String("source_prefix", sourcePrefix),
		zap.String("destination_bucket", destinationBucket),
		zap.String("destination_prefix", destinationPrefix),
		zap.Int("count", len(plans)),
	)

	return MoveResult{
		Kind:              "prefix",
		SourceBucket:      sourceBucket,
		SourcePrefix:      sourcePrefix,
		DestinationBucket: destinationBucket,
		DestinationPrefix: destinationPrefix,
		MovedCount:        len(plans),
	}, nil
}

func (s *Store) DeleteBucket(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if _, err := s.GetBucket(ctx, name); err != nil {
		return err
	}

	root := s.bucketRoot(name)
	if bucketHasObjects(root) {
		return fmt.Errorf("%w: %s", ErrBucketNotEmpty, name)
	}

	if err := os.RemoveAll(root); err != nil {
		return fmt.Errorf("remove bucket dir: %w", err)
	}

	s.logger.Info("bucket deleted", zap.String("bucket", name), zap.String("path", root))
	return nil
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

func (s *Store) bucketUsageHistoryPath(name string) string {
	return joinPath(s.bucketControlDir(name), bucketHistoryName)
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

func (s *Store) readBucketUsageHistory(name string) ([]BucketUsageSample, error) {
	items := []BucketUsageSample{}
	if err := readJSONFile(s.bucketUsageHistoryPath(name), &items); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) writeBucketUsageHistory(name string, items []BucketUsageSample) error {
	path := s.bucketUsageHistoryPath(name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	staged, err := writeJSONTemp(filepath.Dir(path), "bucket-history-*", items)
	if err != nil {
		return err
	}
	defer func() {
		_ = os.Remove(staged)
	}()
	return replaceFile(staged, path)
}

func (s *Store) currentBucketUsageSample(ctx context.Context, bucket string) (BucketUsageSample, error) {
	info, err := s.GetBucket(ctx, bucket)
	if err != nil {
		return BucketUsageSample{}, err
	}
	usage, err := s.bucketUsage(ctx, bucket)
	if err != nil {
		return BucketUsageSample{}, err
	}
	return BucketUsageSample{
		RecordedAt:  time.Now().UTC(),
		UsedBytes:   usage.UsedBytes,
		ObjectCount: usage.ObjectCount,
		QuotaBytes:  info.QuotaBytes,
	}, nil
}

func (s *Store) recordBucketUsageSamples(ctx context.Context, buckets ...string) error {
	seen := make(map[string]struct{}, len(buckets))
	for _, bucket := range buckets {
		bucket = strings.TrimSpace(bucket)
		if bucket == "" {
			continue
		}
		if _, exists := seen[bucket]; exists {
			continue
		}
		seen[bucket] = struct{}{}

		snapshot, err := s.currentBucketUsageSample(ctx, bucket)
		if err != nil {
			return err
		}
		if err := s.recordBucketUsageSample(bucket, snapshot.UsedBytes, snapshot.ObjectCount, snapshot.QuotaBytes); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) recordBucketUsageSample(bucket string, usedBytes int64, objectCount int, quotaBytes int64) error {
	history, err := s.readBucketUsageHistory(bucket)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	history = append(history, BucketUsageSample{
		RecordedAt:  time.Now().UTC(),
		UsedBytes:   usedBytes,
		ObjectCount: objectCount,
		QuotaBytes:  quotaBytes,
	})
	if len(history) > bucketUsageHistoryLimit {
		history = history[len(history)-bucketUsageHistoryLimit:]
	}
	return s.writeBucketUsageHistory(bucket, history)
}

func (s *Store) rewriteBucketObjectMetadata(bucket string) error {
	root := s.bucketMetaDir(bucket)
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}

		meta := objectMetadata{}
		if err := readJSONFile(path, &meta); err != nil {
			return err
		}
		if meta.Bucket == bucket {
			return nil
		}

		meta.Bucket = bucket
		staged, err := writeJSONTemp(filepath.Dir(path), "meta-*", meta)
		if err != nil {
			return err
		}
		defer func() {
			_ = os.Remove(staged)
		}()
		return replaceFile(staged, path)
	})
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
	if runtime.GOOS == "windows" {
		if value := mime.TypeByExtension(strings.ToLower(filepath.Ext(path))); value != "" {
			return value
		}
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

func cloneStringSlice(input []string) []string {
	if len(input) == 0 {
		return nil
	}
	cloned := make([]string, len(input))
	copy(cloned, input)
	return cloned
}

func normalizeBucketTags(tags []string) []string {
	if len(tags) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(tags))
	normalized := make([]string, 0, len(tags))
	for _, tag := range tags {
		trimmed := strings.TrimSpace(tag)
		if trimmed == "" {
			continue
		}
		if _, exists := seen[trimmed]; exists {
			continue
		}
		seen[trimmed] = struct{}{}
		normalized = append(normalized, trimmed)
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func (s *Store) ensureDestinationBucketQuota(ctx context.Context, sourceBucket, destinationBucket string, movedBytes int64) error {
	if sourceBucket == destinationBucket {
		return nil
	}

	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return err
	}
	for _, bucket := range buckets {
		if bucket.Name != destinationBucket {
			continue
		}
		if bucket.QuotaBytes > 0 && bucket.UsedBytes+movedBytes > bucket.QuotaBytes {
			return fmt.Errorf("%w: %s exceeds %d bytes", ErrBucketQuotaExceeded, destinationBucket, bucket.QuotaBytes)
		}
		return nil
	}

	return fmt.Errorf("%w: %s", ErrBucketNotFound, destinationBucket)
}

func (s *Store) ensureDestinationAvailable(bucket, key string) error {
	objectPath, metadataPath, err := s.resolveObjectPaths(bucket, key)
	if err != nil {
		return err
	}
	if _, err := os.Stat(objectPath); err == nil {
		return fmt.Errorf("%w: %s/%s", ErrObjectExists, bucket, key)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if _, err := os.Stat(metadataPath); err == nil {
		return fmt.Errorf("%w: %s/%s", ErrObjectExists, bucket, key)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *Store) moveObjectLocked(object ObjectInfo, destinationBucket, destinationKey string) (ObjectInfo, error) {
	sourceMetadataPath := object.MetadataPath
	if strings.TrimSpace(sourceMetadataPath) == "" {
		_, resolvedMetadataPath, err := s.resolveObjectPaths(object.Bucket, object.Key)
		if err != nil {
			return ObjectInfo{}, err
		}
		sourceMetadataPath = resolvedMetadataPath
	}

	destinationObjectPath, destinationMetadataPath, err := s.resolveObjectPaths(destinationBucket, destinationKey)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destinationObjectPath), 0o755); err != nil {
		return ObjectInfo{}, fmt.Errorf("create destination object dir: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(destinationMetadataPath), 0o755); err != nil {
		return ObjectInfo{}, fmt.Errorf("create destination metadata dir: %w", err)
	}

	updatedAt := time.Now().UTC()
	meta := objectMetadata{
		Bucket:             destinationBucket,
		Key:                destinationKey,
		Size:               object.Size,
		ETag:               object.ETag,
		ContentType:        object.ContentType,
		CacheControl:       object.CacheControl,
		ContentDisposition: object.ContentDisposition,
		UserMetadata:       cloneStringMap(object.UserMetadata),
		LastModified:       updatedAt,
	}
	stagedMetadataPath, err := writeJSONTemp(filepath.Dir(destinationMetadataPath), "meta-*", meta)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("stage destination metadata: %w", err)
	}
	defer func() {
		_ = os.Remove(stagedMetadataPath)
	}()

	if err := os.Rename(object.Path, destinationObjectPath); err != nil {
		return ObjectInfo{}, fmt.Errorf("move object file: %w", err)
	}
	if err := replaceFile(stagedMetadataPath, destinationMetadataPath); err != nil {
		_ = os.Rename(destinationObjectPath, object.Path)
		return ObjectInfo{}, fmt.Errorf("write moved metadata: %w", err)
	}
	if err := os.Chtimes(destinationObjectPath, updatedAt, updatedAt); err == nil {
		meta.LastModified = updatedAt
	}
	if filepath.Clean(sourceMetadataPath) != filepath.Clean(destinationMetadataPath) {
		if err := os.Remove(sourceMetadataPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return ObjectInfo{}, fmt.Errorf("remove old metadata file: %w", err)
		}
	}

	pruneEmptyParents(filepath.Dir(object.Path), s.bucketRoot(object.Bucket))
	pruneEmptyParents(filepath.Dir(sourceMetadataPath), s.bucketMetaDir(object.Bucket))

	moved := ObjectInfo{
		Bucket:             destinationBucket,
		Key:                destinationKey,
		Path:               destinationObjectPath,
		MetadataPath:       destinationMetadataPath,
		Size:               object.Size,
		ETag:               object.ETag,
		ContentType:        object.ContentType,
		CacheControl:       object.CacheControl,
		ContentDisposition: object.ContentDisposition,
		UserMetadata:       cloneStringMap(object.UserMetadata),
		LastModified:       meta.LastModified,
	}

	s.logger.Info(
		"object moved",
		zap.String("source_bucket", object.Bucket),
		zap.String("source_key", object.Key),
		zap.String("destination_bucket", destinationBucket),
		zap.String("destination_key", destinationKey),
	)
	return moved, nil
}

func normalizeMovePrefix(value string) string {
	trimmed := strings.Trim(strings.ReplaceAll(strings.TrimSpace(value), "\\", "/"), "/")
	if trimmed == "" {
		return ""
	}
	return trimmed + "/"
}

func pruneEmptyParents(startPath, stopPath string) {
	current := filepath.Clean(startPath)
	stop := filepath.Clean(stopPath)

	for current != stop && current != "." && current != string(filepath.Separator) {
		if err := os.Remove(current); err != nil {
			break
		}
		current = filepath.Dir(current)
	}
}

func bucketHasObjects(root string) bool {
	hasObjects := false
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		if entry.IsDir() {
			if filepath.Base(path) == controlDirName {
				if dirHasEntries(joinPath(path, multipartDirName)) {
					hasObjects = true
					return errors.New("stop")
				}
				return filepath.SkipDir
			}
			return nil
		}
		hasObjects = true
		return errors.New("stop")
	})
	return hasObjects
}

func dirHasEntries(path string) bool {
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}
	return len(entries) > 0
}
