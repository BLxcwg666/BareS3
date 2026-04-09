package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

func (s *Store) ApplyReplicaBucket(ctx context.Context, input ReplicaBucketInput) (BucketInfo, error) {
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
	accessPolicy := NormalizeBucketAccessPolicy(input.AccessPolicy)
	if accessPolicy.DefaultAction == "" && len(accessPolicy.Rules) == 0 {
		accessPolicy = PresetBucketAccessPolicy(accessMode)
	}
	if err := validateBucketAccessPolicy(accessPolicy); err != nil {
		return BucketInfo{}, err
	}
	if instanceQuota := s.InstanceQuotaBytes(); instanceQuota > 0 && input.QuotaBytes > instanceQuota {
		return BucketInfo{}, fmt.Errorf("%w: bucket quota %d exceeds instance quota %d", ErrInvalidQuota, input.QuotaBytes, instanceQuota)
	}

	meta := bucketMetadata{
		Name:           name,
		CreatedAt:      input.CreatedAt.UTC(),
		MetadataLayout: strings.TrimSpace(input.MetadataLayout),
		AccessMode:     accessMode,
		AccessPolicy:   accessPolicy,
		QuotaBytes:     input.QuotaBytes,
		Tags:           normalizeBucketTags(input.Tags),
		Note:           strings.TrimSpace(input.Note),
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = time.Now().UTC()
	}
	if meta.MetadataLayout == "" {
		meta.MetadataLayout = s.metadataLayout
	}

	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	root := s.bucketRoot(name)
	if info, err := os.Stat(root); err == nil {
		if !info.IsDir() {
			return BucketInfo{}, fmt.Errorf("%w: bucket path exists but is not a directory", ErrBucketExists)
		}
	} else if !os.IsNotExist(err) {
		return BucketInfo{}, err
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return BucketInfo{}, fmt.Errorf("create bucket directories: %w", err)
	}
	if err := s.writeBucketMetadata(name, meta); err != nil {
		return BucketInfo{}, err
	}
	if err := s.recordBucketUsageSamples(ctx, name); err != nil {
		return BucketInfo{}, err
	}
	return s.GetBucket(ctx, name)
}

func (s *Store) ApplyReplicaObject(ctx context.Context, input ReplicaObjectInput) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	if input.Body == nil {
		return ObjectInfo{}, fmt.Errorf("%w: replica object body must not be nil", ErrInvalidMetadata)
	}
	if strings.TrimSpace(input.ETag) == "" {
		return ObjectInfo{}, fmt.Errorf("%w: replica object etag must not be empty", ErrInvalidMetadata)
	}
	expectedChecksum := strings.ToLower(strings.TrimSpace(input.ChecksumSHA256))
	if expectedChecksum == "" {
		return ObjectInfo{}, fmt.Errorf("%w: replica object checksum must not be empty", ErrInvalidMetadata)
	}
	if input.Size < 0 {
		return ObjectInfo{}, fmt.Errorf("%w: replica object size must not be negative", ErrInvalidMetadata)
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

	stagedObject, err := os.CreateTemp(stagingDir, "replica-*")
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("create staged replica object file: %w", err)
	}
	stagedObjectPath := stagedObject.Name()
	defer func() {
		_ = os.Remove(stagedObjectPath)
	}()

	hasher := sha256.New()
	buf := make([]byte, 32*1024)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			_ = stagedObject.Close()
			return ObjectInfo{}, err
		}

		n, readErr := input.Body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if _, err := stagedObject.Write(chunk); err != nil {
				_ = stagedObject.Close()
				return ObjectInfo{}, fmt.Errorf("write staged replica object: %w", err)
			}
			if _, err := hasher.Write(chunk); err != nil {
				_ = stagedObject.Close()
				return ObjectInfo{}, fmt.Errorf("hash replica object checksum: %w", err)
			}
			size += int64(n)
		}

		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = stagedObject.Close()
			return ObjectInfo{}, fmt.Errorf("read replica object body: %w", readErr)
		}
	}

	if err := stagedObject.Sync(); err != nil {
		_ = stagedObject.Close()
		return ObjectInfo{}, fmt.Errorf("sync staged replica object: %w", err)
	}
	if err := stagedObject.Close(); err != nil {
		return ObjectInfo{}, fmt.Errorf("close staged replica object: %w", err)
	}

	actualChecksum := hex.EncodeToString(hasher.Sum(nil))
	if actualChecksum != expectedChecksum {
		return ObjectInfo{}, fmt.Errorf("%w: expected %s got %s", ErrChecksumMismatch, expectedChecksum, actualChecksum)
	}
	if size != input.Size {
		return ObjectInfo{}, fmt.Errorf("%w: expected %d bytes got %d", ErrChecksumMismatch, input.Size, size)
	}

	if current, err := s.readObjectMetadata(input.Bucket, input.Key); err == nil {
		if strings.TrimSpace(current.LastChangeID) != "" && strings.TrimSpace(current.LastChangeID) == strings.TrimSpace(input.LastChangeID) {
			return s.StatObject(ctx, input.Bucket, input.Key)
		}
		if !input.Force && current.Revision > input.Revision {
			return ObjectInfo{}, fmt.Errorf("%w: local revision %d is newer than remote revision %d for %s/%s", ErrObjectConflict, current.Revision, input.Revision, input.Bucket, input.Key)
		}
	}

	contentType := strings.TrimSpace(input.ContentType)
	if contentType == "" {
		contentType = fallbackContentType(input.Key)
	}
	lastModified := input.LastModified.UTC()
	if lastModified.IsZero() {
		lastModified = time.Now().UTC()
	}

	meta := objectMetadata{
		Bucket:             input.Bucket,
		Key:                input.Key,
		Size:               size,
		ETag:               strings.TrimSpace(input.ETag),
		ChecksumSHA256:     actualChecksum,
		Revision:           input.Revision,
		OriginNodeID:       strings.TrimSpace(input.OriginNodeID),
		LastChangeID:       strings.TrimSpace(input.LastChangeID),
		ContentType:        contentType,
		CacheControl:       strings.TrimSpace(input.CacheControl),
		ContentDisposition: strings.TrimSpace(input.ContentDisposition),
		UserMetadata:       cloneStringMap(input.UserMetadata),
		LastModified:       lastModified,
	}

	info, err := s.commitObjectWithQuota(ctx, input.Bucket, input.Key, stagedObjectPath, meta)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := os.Chtimes(info.Path, lastModified, lastModified); err == nil {
		info.LastModified = lastModified
	}
	return info, nil
}
