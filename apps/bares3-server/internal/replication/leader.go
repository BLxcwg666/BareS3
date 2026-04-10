package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/remotes"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func NewLeaderHandler(cfg config.Config, store *storage.Store, logger *zap.Logger) http.Handler {
	if logger == nil {
		logger = zap.NewNop()
	}
	router := chi.NewRouter()
	remoteStore, err := remotes.New(cfg.Paths.DataDir, logger)
	if err != nil {
		panic(fmt.Sprintf("initialize replication store for auth: %v", err))
	}
	router.Use(requireReplicationAuth(remoteStore))
	router.Get("/manifest", func(w http.ResponseWriter, r *http.Request) {
		cursor := int64(0)
		if rawCursor := strings.TrimSpace(r.URL.Query().Get("cursor")); rawCursor != "" {
			parsed, err := strconv.ParseInt(rawCursor, 10, 64)
			if err != nil || parsed < 0 {
				writeJSONError(w, http.StatusBadRequest, errors.New("cursor must be a non-negative integer"))
				return
			}
			cursor = parsed
		}
		manifest, err := buildManifest(r.Context(), store, cursor)
		if err != nil {
			logger.Error("build sync manifest failed", zap.Error(err))
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, manifest)
	})
	router.Get("/status", func(w http.ResponseWriter, r *http.Request) {
		status, err := buildSourceStatus(r.Context(), store)
		if err != nil {
			logger.Error("build sync source status failed", zap.Error(err))
			writeJSONError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, status)
	})
	router.Get("/stream", func(w http.ResponseWriter, r *http.Request) {
		serveSyncStream(w, r, store, logger)
	})
	router.Get("/object", func(w http.ResponseWriter, r *http.Request) {
		bucket := strings.TrimSpace(r.URL.Query().Get("bucket"))
		key := strings.TrimSpace(r.URL.Query().Get("key"))
		if bucket == "" || key == "" {
			writeJSONError(w, http.StatusBadRequest, errors.New("bucket and key are required"))
			return
		}

		file, object, err := store.OpenObject(r.Context(), bucket, key)
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, storage.ErrBucketNotFound) || errors.Is(err, storage.ErrObjectNotFound) {
				status = http.StatusNotFound
			}
			writeJSONError(w, status, err)
			return
		}
		defer file.Close()

		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", strconv.FormatInt(object.Size, 10))
		w.Header().Set(HeaderETag, object.ETag)
		w.Header().Set(HeaderChecksumSHA256, object.ChecksumSHA256)
		w.Header().Set(HeaderRevision, strconv.FormatInt(object.Revision, 10))
		w.Header().Set(HeaderOriginNodeID, object.OriginNodeID)
		w.Header().Set(HeaderLastChangeID, object.LastChangeID)
		w.Header().Set("Last-Modified", object.LastModified.UTC().Format(http.TimeFormat))
		w.WriteHeader(http.StatusOK)
		if _, err := io.Copy(w, file); err != nil {
			logger.Warn("stream sync object failed", zap.String("bucket", bucket), zap.String("key", key), zap.Error(err))
		}
	})
	return router
}

func buildManifest(ctx context.Context, store *storage.Store, afterCursor int64) (Manifest, error) {
	currentCursor, err := store.CurrentSyncCursor(ctx)
	if err != nil {
		return Manifest{}, err
	}
	if afterCursor <= 0 || afterCursor > currentCursor {
		return buildFullManifest(ctx, store, currentCursor)
	}
	return buildIncrementalManifest(ctx, store, afterCursor)
}

func buildFullManifest(ctx context.Context, store *storage.Store, cursor int64) (Manifest, error) {
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		return Manifest{}, err
	}

	manifest := Manifest{
		GeneratedAt: time.Now().UTC(),
		Full:        true,
		Cursor:      cursor,
		Buckets:     make([]BucketManifest, 0, len(buckets)),
		Objects:     make([]ObjectManifest, 0),
	}
	for _, bucket := range buckets {
		access, err := store.GetBucketAccessConfig(ctx, bucket.Name)
		if err != nil {
			return Manifest{}, err
		}
		manifest.Buckets = append(manifest.Buckets, bucketManifestFromBucketInfo(bucket, access))

		objects, err := store.ListObjects(ctx, bucket.Name, storage.ListObjectsOptions{})
		if err != nil {
			return Manifest{}, err
		}
		for _, object := range objects {
			manifest.Objects = append(manifest.Objects, objectManifestFromObjectInfo(object))
		}
	}
	return manifest, nil
}

func buildIncrementalManifest(ctx context.Context, store *storage.Store, afterCursor int64) (Manifest, error) {
	events, err := store.ListSyncEvents(ctx, afterCursor, 5000)
	if err != nil {
		return Manifest{}, err
	}
	manifest := Manifest{
		GeneratedAt: time.Now().UTC(),
		Full:        false,
		Cursor:      afterCursor,
	}
	if len(events) == 0 {
		return manifest, nil
	}
	bucketUpserts := make(map[string]BucketManifest)
	bucketDeletes := make(map[string]struct{})
	objectUpserts := make(map[string]ObjectManifest)
	objectDeletes := make(map[string]DeletedObjectManifest)

	for _, event := range events {
		manifest.Cursor = event.Cursor
		switch event.Kind {
		case storage.SyncEventBucketUpsert:
			delete(bucketDeletes, event.Bucket)
			if event.BucketData != nil {
				bucketUpserts[event.Bucket] = bucketManifestFromReplicaBucket(*event.BucketData)
			}
		case storage.SyncEventBucketDelete:
			delete(bucketUpserts, event.Bucket)
			bucketDeletes[event.Bucket] = struct{}{}
		case storage.SyncEventObjectUpsert:
			delete(objectDeletes, objectID(event.Bucket, event.Key))
			if event.ObjectData != nil {
				objectUpserts[objectID(event.Bucket, event.Key)] = objectManifestFromReplicaObject(*event.ObjectData)
			}
		case storage.SyncEventObjectDelete:
			delete(objectUpserts, objectID(event.Bucket, event.Key))
			objectDeletes[objectID(event.Bucket, event.Key)] = DeletedObjectManifest{Bucket: event.Bucket, Key: event.Key}
		}
	}
	for bucket := range bucketDeletes {
		for id, object := range objectUpserts {
			if object.Bucket == bucket {
				delete(objectUpserts, id)
			}
		}
		for id, object := range objectDeletes {
			if object.Bucket == bucket {
				delete(objectDeletes, id)
			}
		}
	}

	manifest.Buckets = sortedBucketManifestValues(bucketUpserts)
	manifest.Objects = sortedObjectManifestValues(objectUpserts)
	manifest.DeletedBuckets = sortedStringKeys(bucketDeletes)
	manifest.DeletedObjects = sortedDeletedObjectValues(objectDeletes)
	currentCursor, err := store.CurrentSyncCursor(ctx)
	if err != nil {
		return Manifest{}, err
	}
	manifest.HasMore = manifest.Cursor < currentCursor
	return manifest, nil
}

func buildSourceStatus(ctx context.Context, store *storage.Store) (SourceStatus, error) {
	usedBytes, objectCount, err := store.UsageSummary(ctx)
	if err != nil {
		return SourceStatus{}, err
	}
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		return SourceStatus{}, err
	}
	cursor, err := store.CurrentSyncCursor(ctx)
	if err != nil {
		return SourceStatus{}, err
	}
	return SourceStatus{Cursor: cursor, UsedBytes: usedBytes, BucketCount: len(buckets), ObjectCount: objectCount}, nil
}

func requireReplicationAuth(remoteStore *remotes.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if token := strings.TrimSpace(r.Header.Get(HeaderAccessToken)); token != "" {
				if _, err := remoteStore.AccessTokenByValue(r.Context(), token); err == nil {
					next.ServeHTTP(w, r)
					return
				} else if err != nil && !errors.Is(err, remotes.ErrTokenNotFound) && !errors.Is(err, remotes.ErrTokenRevoked) {
					writeJSONError(w, http.StatusInternalServerError, err)
					return
				}
			}
			writeJSONError(w, http.StatusUnauthorized, errors.New("invalid sync credentials"))
		})
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeJSONError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]any{
		"status":  "error",
		"message": err.Error(),
	})
}

func cloneMap(input map[string]string) map[string]string {
	if len(input) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(input))
	for key, value := range input {
		cloned[key] = value
	}
	return cloned
}

func bucketManifestFromBucketInfo(bucket storage.BucketInfo, access storage.BucketAccessConfig) BucketManifest {
	return BucketManifest{
		Name:           bucket.Name,
		CreatedAt:      bucket.CreatedAt,
		MetadataLayout: bucket.MetadataLayout,
		AccessMode:     access.Mode,
		AccessPolicy:   access.Policy,
		QuotaBytes:     bucket.QuotaBytes,
		Tags:           append([]string(nil), bucket.Tags...),
		Note:           bucket.Note,
	}
}

func bucketManifestFromReplicaBucket(bucket storage.ReplicaBucketInput) BucketManifest {
	return BucketManifest{
		Name:           bucket.Name,
		CreatedAt:      bucket.CreatedAt,
		MetadataLayout: bucket.MetadataLayout,
		AccessMode:     bucket.AccessMode,
		AccessPolicy:   bucket.AccessPolicy,
		QuotaBytes:     bucket.QuotaBytes,
		Tags:           append([]string(nil), bucket.Tags...),
		Note:           bucket.Note,
	}
}

func objectManifestFromObjectInfo(object storage.ObjectInfo) ObjectManifest {
	return ObjectManifest{
		Bucket:             object.Bucket,
		Key:                object.Key,
		Size:               object.Size,
		ETag:               object.ETag,
		ChecksumSHA256:     object.ChecksumSHA256,
		Revision:           object.Revision,
		OriginNodeID:       object.OriginNodeID,
		LastChangeID:       object.LastChangeID,
		ContentType:        object.ContentType,
		CacheControl:       object.CacheControl,
		ContentDisposition: object.ContentDisposition,
		UserMetadata:       cloneMap(object.UserMetadata),
		LastModified:       object.LastModified,
	}
}

func objectManifestFromReplicaObject(object storage.ReplicaObjectMetadata) ObjectManifest {
	return ObjectManifest{
		Bucket:             object.Bucket,
		Key:                object.Key,
		Size:               object.Size,
		ETag:               object.ETag,
		ChecksumSHA256:     object.ChecksumSHA256,
		Revision:           object.Revision,
		OriginNodeID:       object.OriginNodeID,
		LastChangeID:       object.LastChangeID,
		ContentType:        object.ContentType,
		CacheControl:       object.CacheControl,
		ContentDisposition: object.ContentDisposition,
		UserMetadata:       cloneMap(object.UserMetadata),
		LastModified:       object.LastModified,
	}
}

func sortedBucketManifestValues(values map[string]BucketManifest) []BucketManifest {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]BucketManifest, 0, len(keys))
	for _, key := range keys {
		items = append(items, values[key])
	}
	return items
}

func sortedObjectManifestValues(values map[string]ObjectManifest) []ObjectManifest {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]ObjectManifest, 0, len(keys))
	for _, key := range keys {
		items = append(items, values[key])
	}
	return items
}

func sortedStringKeys(values map[string]struct{}) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func sortedDeletedObjectValues(values map[string]DeletedObjectManifest) []DeletedObjectManifest {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]DeletedObjectManifest, 0, len(keys))
	for _, key := range keys {
		items = append(items, values[key])
	}
	return items
}
