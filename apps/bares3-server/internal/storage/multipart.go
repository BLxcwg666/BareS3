package storage

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

const multipartDirName = "multipart"

func (s *Store) InitiateMultipartUpload(ctx context.Context, input InitiateMultipartUploadInput) (MultipartUploadInfo, error) {
	if err := ctx.Err(); err != nil {
		return MultipartUploadInfo{}, err
	}
	if _, err := s.GetBucket(ctx, input.Bucket); err != nil {
		return MultipartUploadInfo{}, err
	}
	if _, _, err := s.resolveObjectPaths(input.Bucket, input.Key); err != nil {
		return MultipartUploadInfo{}, err
	}

	uploadID, err := newUploadID()
	if err != nil {
		return MultipartUploadInfo{}, err
	}

	meta := multipartUploadMetadata{
		UploadID:           uploadID,
		Bucket:             input.Bucket,
		Key:                input.Key,
		ContentType:        strings.TrimSpace(input.ContentType),
		CacheControl:       strings.TrimSpace(input.CacheControl),
		ContentDisposition: strings.TrimSpace(input.ContentDisposition),
		UserMetadata:       cloneStringMap(input.UserMetadata),
		CreatedAt:          time.Now().UTC(),
	}

	partsDir := s.multipartUploadPartsDir(input.Bucket, uploadID)
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		return MultipartUploadInfo{}, fmt.Errorf("create multipart upload dir: %w", err)
	}
	if err := s.writeMultipartUploadMetadata(input.Bucket, uploadID, meta); err != nil {
		return MultipartUploadInfo{}, err
	}

	info := MultipartUploadInfo{
		UploadID:           meta.UploadID,
		Bucket:             meta.Bucket,
		Key:                meta.Key,
		ContentType:        meta.ContentType,
		CacheControl:       meta.CacheControl,
		ContentDisposition: meta.ContentDisposition,
		UserMetadata:       cloneStringMap(meta.UserMetadata),
		CreatedAt:          meta.CreatedAt,
	}

	s.logger.Info("multipart upload initiated", zap.String("bucket", meta.Bucket), zap.String("key", meta.Key), zap.String("upload_id", meta.UploadID))
	return info, nil
}

func (s *Store) UploadPart(ctx context.Context, input UploadPartInput) (MultipartPartInfo, error) {
	if err := ctx.Err(); err != nil {
		return MultipartPartInfo{}, err
	}
	if input.Body == nil {
		return MultipartPartInfo{}, fmt.Errorf("%w: part body must not be nil", ErrInvalidPart)
	}
	if err := validatePartNumber(input.PartNumber); err != nil {
		return MultipartPartInfo{}, err
	}
	if _, err := s.readMultipartUpload(input.Bucket, input.Key, input.UploadID); err != nil {
		return MultipartPartInfo{}, err
	}

	stagingDir := joinPath(s.tmpDir, input.Bucket, multipartDirName, input.UploadID)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return MultipartPartInfo{}, fmt.Errorf("create multipart staging dir: %w", err)
	}

	stagedPart, err := os.CreateTemp(stagingDir, "part-*")
	if err != nil {
		return MultipartPartInfo{}, fmt.Errorf("create staged part file: %w", err)
	}
	stagedPartPath := stagedPart.Name()
	defer func() {
		_ = os.Remove(stagedPartPath)
	}()

	hasher := md5.New()
	buf := make([]byte, 32*1024)
	var size int64
	for {
		if err := ctx.Err(); err != nil {
			_ = stagedPart.Close()
			return MultipartPartInfo{}, err
		}
		n, readErr := input.Body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if _, err := stagedPart.Write(chunk); err != nil {
				_ = stagedPart.Close()
				return MultipartPartInfo{}, fmt.Errorf("write staged part: %w", err)
			}
			if _, err := hasher.Write(chunk); err != nil {
				_ = stagedPart.Close()
				return MultipartPartInfo{}, fmt.Errorf("hash part data: %w", err)
			}
			size += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			_ = stagedPart.Close()
			return MultipartPartInfo{}, fmt.Errorf("read part body: %w", readErr)
		}
	}

	if err := stagedPart.Sync(); err != nil {
		_ = stagedPart.Close()
		return MultipartPartInfo{}, fmt.Errorf("sync staged part: %w", err)
	}
	if err := stagedPart.Close(); err != nil {
		return MultipartPartInfo{}, fmt.Errorf("close staged part: %w", err)
	}

	meta := multipartPartMetadata{
		PartNumber:   input.PartNumber,
		ETag:         hex.EncodeToString(hasher.Sum(nil)),
		Size:         size,
		LastModified: time.Now().UTC(),
	}

	partsDir := s.multipartUploadPartsDir(input.Bucket, input.UploadID)
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		return MultipartPartInfo{}, fmt.Errorf("create multipart parts dir: %w", err)
	}

	if err := replaceFile(stagedPartPath, s.multipartPartPath(input.Bucket, input.UploadID, input.PartNumber)); err != nil {
		return MultipartPartInfo{}, fmt.Errorf("commit part file: %w", err)
	}
	if err := s.writeMultipartPartMetadata(input.Bucket, input.UploadID, meta); err != nil {
		_ = os.Remove(s.multipartPartPath(input.Bucket, input.UploadID, input.PartNumber))
		return MultipartPartInfo{}, err
	}

	info := MultipartPartInfo{PartNumber: meta.PartNumber, ETag: meta.ETag, Size: meta.Size, LastModified: meta.LastModified}
	s.logger.Info("multipart part stored", zap.String("bucket", input.Bucket), zap.String("key", input.Key), zap.String("upload_id", input.UploadID), zap.Int("part_number", input.PartNumber), zap.Int64("size", size))
	return info, nil
}

func (s *Store) ListParts(ctx context.Context, bucket, key, uploadID string) ([]MultipartPartInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := s.readMultipartUpload(bucket, key, uploadID); err != nil {
		return nil, err
	}

	metas, err := s.metadata.listMultipartParts(uploadID)
	if err != nil {
		return nil, err
	}
	parts := make([]MultipartPartInfo, 0, len(metas))
	for _, meta := range metas {
		parts = append(parts, MultipartPartInfo{PartNumber: meta.PartNumber, ETag: meta.ETag, Size: meta.Size, LastModified: meta.LastModified})
	}
	return parts, nil
}

func (s *Store) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, completed []CompletedPart) (ObjectInfo, error) {
	if err := ctx.Err(); err != nil {
		return ObjectInfo{}, err
	}
	meta, err := s.readMultipartUpload(bucket, key, uploadID)
	if err != nil {
		return ObjectInfo{}, err
	}
	if len(completed) == 0 {
		return ObjectInfo{}, fmt.Errorf("%w: completion request must include at least one part", ErrInvalidPart)
	}

	parts, err := s.ListParts(ctx, bucket, key, uploadID)
	if err != nil {
		return ObjectInfo{}, err
	}
	partsByNumber := make(map[int]MultipartPartInfo, len(parts))
	for _, part := range parts {
		partsByNumber[part.PartNumber] = part
	}

	stagingDir := joinPath(s.tmpDir, bucket)
	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return ObjectInfo{}, fmt.Errorf("create multipart commit dir: %w", err)
	}
	stagedObject, err := os.CreateTemp(stagingDir, "multipart-*")
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("create multipart object file: %w", err)
	}
	stagedObjectPath := stagedObject.Name()
	defer func() {
		_ = os.Remove(stagedObjectPath)
	}()

	compositeHash := md5.New()
	var size int64
	firstBytes := make([]byte, 0, 512)
	buffer := make([]byte, 32*1024)
	lastPartNumber := 0

	for _, item := range completed {
		if err := validatePartNumber(item.PartNumber); err != nil {
			_ = stagedObject.Close()
			return ObjectInfo{}, err
		}
		if item.PartNumber <= lastPartNumber {
			_ = stagedObject.Close()
			return ObjectInfo{}, fmt.Errorf("%w: part numbers must be strictly increasing", ErrInvalidPartOrder)
		}
		lastPartNumber = item.PartNumber

		part, ok := partsByNumber[item.PartNumber]
		if !ok {
			_ = stagedObject.Close()
			return ObjectInfo{}, fmt.Errorf("%w: part %d is missing", ErrInvalidPart, item.PartNumber)
		}
		if expected := normalizeETag(item.ETag); expected != "" && !strings.EqualFold(expected, part.ETag) {
			_ = stagedObject.Close()
			return ObjectInfo{}, fmt.Errorf("%w: part %d etag mismatch", ErrInvalidPart, item.PartNumber)
		}

		decodedETag, err := hex.DecodeString(part.ETag)
		if err != nil {
			decodedETag = []byte(part.ETag)
		}
		if _, err := compositeHash.Write(decodedETag); err != nil {
			_ = stagedObject.Close()
			return ObjectInfo{}, fmt.Errorf("hash multipart etag: %w", err)
		}

		partFile, err := os.Open(s.multipartPartPath(bucket, uploadID, item.PartNumber))
		if err != nil {
			_ = stagedObject.Close()
			if errors.Is(err, os.ErrNotExist) {
				return ObjectInfo{}, fmt.Errorf("%w: part %d file is missing", ErrInvalidPart, item.PartNumber)
			}
			return ObjectInfo{}, fmt.Errorf("open multipart part: %w", err)
		}

		if len(firstBytes) < 512 {
			probe := make([]byte, 512-len(firstBytes))
			n, _ := partFile.Read(probe)
			if n > 0 {
				firstBytes = append(firstBytes, probe[:n]...)
			}
			if _, err := partFile.Seek(0, io.SeekStart); err != nil {
				_ = partFile.Close()
				_ = stagedObject.Close()
				return ObjectInfo{}, fmt.Errorf("rewind multipart part: %w", err)
			}
		}

		written, err := io.CopyBuffer(stagedObject, partFile, buffer)
		_ = partFile.Close()
		if err != nil {
			_ = stagedObject.Close()
			return ObjectInfo{}, fmt.Errorf("append multipart part: %w", err)
		}
		size += written
	}

	if err := stagedObject.Sync(); err != nil {
		_ = stagedObject.Close()
		return ObjectInfo{}, fmt.Errorf("sync multipart object: %w", err)
	}
	if err := stagedObject.Close(); err != nil {
		return ObjectInfo{}, fmt.Errorf("close multipart object: %w", err)
	}

	contentType := meta.ContentType
	if contentType == "" {
		if len(firstBytes) > 0 {
			contentType = http.DetectContentType(firstBytes)
		} else {
			contentType = "application/octet-stream"
		}
	}

	objectMeta := objectMetadata{
		Bucket:             meta.Bucket,
		Key:                meta.Key,
		Size:               size,
		ETag:               hex.EncodeToString(compositeHash.Sum(nil)) + "-" + strconv.Itoa(len(completed)),
		ContentType:        contentType,
		CacheControl:       meta.CacheControl,
		ContentDisposition: meta.ContentDisposition,
		UserMetadata:       cloneStringMap(meta.UserMetadata),
		LastModified:       time.Now().UTC(),
	}

	object, err := s.commitObjectWithQuota(ctx, bucket, key, stagedObjectPath, objectMeta)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := os.RemoveAll(s.multipartUploadDir(bucket, uploadID)); err != nil {
		return ObjectInfo{}, fmt.Errorf("cleanup multipart upload dir: %w", err)
	}
	if err := s.metadata.deleteMultipartUpload(uploadID); err != nil {
		return ObjectInfo{}, err
	}

	s.logger.Info("multipart upload completed", zap.String("bucket", bucket), zap.String("key", key), zap.String("upload_id", uploadID), zap.Int("parts", len(completed)), zap.Int64("size", size))
	return object, nil
}

func (s *Store) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := s.readMultipartUpload(bucket, key, uploadID); err != nil {
		return err
	}
	if err := os.RemoveAll(s.multipartUploadDir(bucket, uploadID)); err != nil {
		return fmt.Errorf("remove multipart upload dir: %w", err)
	}
	if err := s.metadata.deleteMultipartUpload(uploadID); err != nil {
		return err
	}
	pruneEmptyParents(s.bucketMultipartDir(bucket), s.bucketControlDir(bucket))
	s.logger.Info("multipart upload aborted", zap.String("bucket", bucket), zap.String("key", key), zap.String("upload_id", uploadID))
	return nil
}

func (s *Store) bucketMultipartDir(name string) string {
	return joinPath(s.bucketControlDir(name), multipartDirName)
}

func (s *Store) multipartUploadDir(bucket, uploadID string) string {
	return joinPath(s.bucketMultipartDir(bucket), uploadID)
}

func (s *Store) multipartUploadPartsDir(bucket, uploadID string) string {
	return joinPath(s.multipartUploadDir(bucket, uploadID), "parts")
}

func (s *Store) multipartPartPath(bucket, uploadID string, partNumber int) string {
	return joinPath(s.multipartUploadPartsDir(bucket, uploadID), fmt.Sprintf("%05d.part", partNumber))
}

func (s *Store) readMultipartUpload(bucket, key, uploadID string) (multipartUploadMetadata, error) {
	meta, err := s.metadata.getMultipartUpload(bucket, key, uploadID)
	if errors.Is(err, os.ErrNotExist) {
		return multipartUploadMetadata{}, fmt.Errorf("%w: %s", ErrUploadNotFound, uploadID)
	}
	if err != nil {
		return multipartUploadMetadata{}, err
	}
	return meta, nil
}

func (s *Store) writeMultipartUploadMetadata(bucket, uploadID string, meta multipartUploadMetadata) error {
	return s.metadata.upsertMultipartUpload(meta)
}

func (s *Store) writeMultipartPartMetadata(bucket, uploadID string, meta multipartPartMetadata) error {
	_ = bucket
	return s.metadata.upsertMultipartPart(uploadID, meta)
}

func (s *Store) commitObjectWithQuota(ctx context.Context, bucket, key, stagedObjectPath string, meta objectMetadata) (ObjectInfo, error) {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()

	existingSize, err := s.currentObjectSize(bucket, key)
	if err != nil {
		return ObjectInfo{}, err
	}

	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return ObjectInfo{}, err
	}

	instanceQuota := s.InstanceQuotaBytes()
	totalUsedBytes := int64(0)
	bucketUsedBytes := int64(0)
	bucketQuotaBytes := int64(0)
	bucketFound := false
	for _, item := range buckets {
		totalUsedBytes += item.UsedBytes
		if item.Name == bucket {
			bucketFound = true
			bucketUsedBytes = item.UsedBytes
			bucketQuotaBytes = item.QuotaBytes
		}
	}
	if !bucketFound {
		return ObjectInfo{}, fmt.Errorf("%w: %s", ErrBucketNotFound, bucket)
	}

	nextBucketUsedBytes := bucketUsedBytes - existingSize + meta.Size
	if bucketQuotaBytes > 0 && nextBucketUsedBytes > bucketQuotaBytes {
		return ObjectInfo{}, fmt.Errorf("%w: %s exceeds %d bytes", ErrBucketQuotaExceeded, bucket, bucketQuotaBytes)
	}

	nextTotalUsedBytes := totalUsedBytes - existingSize + meta.Size
	if instanceQuota > 0 && nextTotalUsedBytes > instanceQuota {
		return ObjectInfo{}, fmt.Errorf("%w: total usage exceeds %d bytes", ErrInstanceQuotaExceeded, instanceQuota)
	}

	info, err := s.commitObjectFromStaged(bucket, key, stagedObjectPath, meta)
	if err != nil {
		return ObjectInfo{}, err
	}
	if err := s.recordBucketUsageSamples(ctx, bucket); err != nil {
		return ObjectInfo{}, err
	}
	return info, nil
}

func (s *Store) currentObjectSize(bucket, key string) (int64, error) {
	objectPath, _, err := s.resolveObjectPaths(bucket, key)
	if err != nil {
		return 0, err
	}

	info, err := os.Stat(objectPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}
	if info.IsDir() {
		return 0, fmt.Errorf("%w: %s/%s", ErrInvalidObjectKey, bucket, key)
	}
	return info.Size(), nil
}

func (s *Store) commitObjectFromStaged(bucket, key, stagedObjectPath string, meta objectMetadata) (ObjectInfo, error) {
	objectPath, _, err := s.resolveObjectPaths(bucket, key)
	if err != nil {
		return ObjectInfo{}, err
	}

	if err := os.MkdirAll(filepath.Dir(objectPath), 0o755); err != nil {
		return ObjectInfo{}, fmt.Errorf("create object parent dir: %w", err)
	}

	if err := replaceFile(stagedObjectPath, objectPath); err != nil {
		return ObjectInfo{}, fmt.Errorf("commit object file: %w", err)
	}
	if err := s.writeObjectMetadata(meta); err != nil {
		_ = os.Rename(objectPath, stagedObjectPath)
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:             meta.Bucket,
		Key:                meta.Key,
		Path:               objectPath,
		MetadataPath:       "",
		Size:               meta.Size,
		ETag:               meta.ETag,
		ContentType:        meta.ContentType,
		CacheControl:       meta.CacheControl,
		ContentDisposition: meta.ContentDisposition,
		UserMetadata:       cloneStringMap(meta.UserMetadata),
		LastModified:       meta.LastModified,
	}, nil
}

func newUploadID() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate multipart upload id: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

func validatePartNumber(value int) error {
	if value < 1 || value > 10000 {
		return fmt.Errorf("%w: part number must be between 1 and 10000", ErrInvalidPartNumber)
	}
	return nil
}

func normalizeETag(value string) string {
	return strings.Trim(strings.TrimSpace(value), `"`)
}
