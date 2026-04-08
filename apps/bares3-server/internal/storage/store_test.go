package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"bares3-server/internal/config"
	"go.uber.org/zap"
)

func TestCreateBucketAndList(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	created, err := store.CreateBucketWithOptions(context.Background(), CreateBucketInput{
		Name:       "gallery",
		QuotaBytes: 5 * 1024,
		AccessMode: BucketAccessPublic,
	})
	if err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if created.Name != "gallery" {
		t.Fatalf("unexpected bucket name: %s", created.Name)
	}
	if created.AccessMode != BucketAccessPublic {
		t.Fatalf("unexpected bucket access mode: %s", created.AccessMode)
	}
	if created.QuotaBytes != 5*1024 {
		t.Fatalf("unexpected bucket quota: %d", created.QuotaBytes)
	}

	buckets, err := store.ListBuckets(context.Background())
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	if len(buckets) != 1 || buckets[0].Name != "gallery" {
		t.Fatalf("unexpected buckets: %+v", buckets)
	}
	if buckets[0].AccessMode != BucketAccessPublic {
		t.Fatalf("unexpected listed access mode: %s", buckets[0].AccessMode)
	}
	if buckets[0].QuotaBytes != 5*1024 {
		t.Fatalf("unexpected listed quota: %d", buckets[0].QuotaBytes)
	}
	if buckets[0].UsedBytes != 0 || buckets[0].ObjectCount != 0 {
		t.Fatalf("unexpected empty bucket usage: %+v", buckets[0])
	}
}

func TestUpdateBucketRenamesAndPersistsMetadata(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 10*1024); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/a.txt",
		Body:   bytes.NewBufferString("hello"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	updated, err := store.UpdateBucket(ctx, UpdateBucketInput{
		Name:       "gallery",
		NewName:    "archive",
		AccessMode: BucketAccessPublic,
		QuotaBytes: 20 * 1024,
		Tags:       []string{"media", "launch", "media"},
		Note:       "Launch assets",
	})
	if err != nil {
		t.Fatalf("UpdateBucket failed: %v", err)
	}
	if updated.Name != "archive" {
		t.Fatalf("unexpected updated bucket name: %s", updated.Name)
	}
	if updated.QuotaBytes != 20*1024 {
		t.Fatalf("unexpected updated quota: %d", updated.QuotaBytes)
	}
	if updated.AccessMode != BucketAccessPublic {
		t.Fatalf("unexpected updated access mode: %s", updated.AccessMode)
	}
	if len(updated.Tags) != 2 || updated.Tags[0] != "media" || updated.Tags[1] != "launch" {
		t.Fatalf("unexpected updated tags: %+v", updated.Tags)
	}
	if updated.Note != "Launch assets" {
		t.Fatalf("unexpected updated note: %q", updated.Note)
	}

	if _, err := store.GetBucket(ctx, "gallery"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected old bucket to be gone, got %v", err)
	}
	bucket, err := store.GetBucket(ctx, "archive")
	if err != nil {
		t.Fatalf("GetBucket archive failed: %v", err)
	}
	if len(bucket.Tags) != 2 || bucket.Note != "Launch assets" {
		t.Fatalf("unexpected persisted metadata: %+v", bucket)
	}
	if bucket.AccessMode != BucketAccessPublic {
		t.Fatalf("unexpected persisted access mode: %s", bucket.AccessMode)
	}

	object, err := store.StatObject(ctx, "archive", "notes/a.txt")
	if err != nil {
		t.Fatalf("StatObject archive failed: %v", err)
	}
	if object.Bucket != "archive" {
		t.Fatalf("unexpected moved object bucket: %s", object.Bucket)
	}

	history, err := store.ListBucketUsageHistory(ctx, "archive", 10)
	if err != nil {
		t.Fatalf("ListBucketUsageHistory failed: %v", err)
	}
	if len(history) < 2 {
		t.Fatalf("expected usage history entries, got %+v", history)
	}
	last := history[len(history)-1]
	if last.QuotaBytes != 20*1024 || last.UsedBytes != int64(len("hello")) || last.ObjectCount != 1 {
		t.Fatalf("unexpected latest usage history sample: %+v", last)
	}
}

func TestBucketUsageHistoryTracksMutations(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/a.txt",
		Body:   bytes.NewBufferString("abc"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if err := store.DeleteObject(ctx, "gallery", "notes/a.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	history, err := store.ListBucketUsageHistory(ctx, "gallery", 10)
	if err != nil {
		t.Fatalf("ListBucketUsageHistory failed: %v", err)
	}
	if len(history) != 3 {
		t.Fatalf("expected 3 usage history entries, got %+v", history)
	}
	if history[0].UsedBytes != 0 || history[0].ObjectCount != 0 {
		t.Fatalf("unexpected initial history sample: %+v", history[0])
	}
	if history[1].UsedBytes != 3 || history[1].ObjectCount != 1 {
		t.Fatalf("unexpected upload history sample: %+v", history[1])
	}
	if history[2].UsedBytes != 0 || history[2].ObjectCount != 0 {
		t.Fatalf("unexpected delete history sample: %+v", history[2])
	}
}

func TestPutObjectEnforcesBucketQuota(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 10); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	if _, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/a.txt",
		Body:   bytes.NewBufferString("12345"),
	}); err != nil {
		t.Fatalf("initial PutObject failed: %v", err)
	}

	if _, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/b.txt",
		Body:   bytes.NewBufferString("123456"),
	}); !errors.Is(err, ErrBucketQuotaExceeded) {
		t.Fatalf("expected ErrBucketQuotaExceeded, got %v", err)
	}
}

func TestPutObjectEnforcesInstanceQuota(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if err := store.SetInstanceQuotaBytes(10); err != nil {
		t.Fatalf("SetInstanceQuotaBytes failed: %v", err)
	}
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket gallery failed: %v", err)
	}
	if _, err := store.CreateBucket(context.Background(), "docs", 0); err != nil {
		t.Fatalf("CreateBucket docs failed: %v", err)
	}

	if _, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/a.txt",
		Body:   bytes.NewBufferString("123456"),
	}); err != nil {
		t.Fatalf("initial PutObject failed: %v", err)
	}

	if _, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket: "docs",
		Key:    "notes/b.txt",
		Body:   bytes.NewBufferString("12345"),
	}); !errors.Is(err, ErrInstanceQuotaExceeded) {
		t.Fatalf("expected ErrInstanceQuotaExceeded, got %v", err)
	}
}

func TestPutObjectWritesDataAndMetadata(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	object, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket:       "gallery",
		Key:          "2026/launch/mock-02.png",
		Body:         bytes.NewBufferString("hello world"),
		CacheControl: "public, max-age=3600",
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	content, err := os.ReadFile(object.Path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(content) != "hello world" {
		t.Fatalf("unexpected object content: %q", string(content))
	}

	var storedMeta ObjectInfo
	metaContent, err := os.ReadFile(object.MetadataPath)
	if err != nil {
		t.Fatalf("Read metadata failed: %v", err)
	}
	if err := json.Unmarshal(metaContent, &storedMeta); err != nil {
		t.Fatalf("Unmarshal metadata failed: %v", err)
	}
	if storedMeta.Key != object.Key || storedMeta.Bucket != object.Bucket {
		t.Fatalf("unexpected stored metadata: %+v", storedMeta)
	}
	if storedMeta.ETag == "" {
		t.Fatalf("expected non-empty etag")
	}
	if storedMeta.CacheControl != "public, max-age=3600" {
		t.Fatalf("unexpected cache control: %q", storedMeta.CacheControl)
	}

	stated, err := store.StatObject(context.Background(), "gallery", "2026/launch/mock-02.png")
	if err != nil {
		t.Fatalf("StatObject failed: %v", err)
	}
	if stated.Size != int64(len("hello world")) {
		t.Fatalf("unexpected object size: %d", stated.Size)
	}
	if stated.ETag != storedMeta.ETag {
		t.Fatalf("etag mismatch: %s != %s", stated.ETag, storedMeta.ETag)
	}
}

func TestPutObjectReplacesExistingFile(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	if _, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/keep.txt",
		Body:   bytes.NewBufferString("first"),
	}); err != nil {
		t.Fatalf("initial PutObject failed: %v", err)
	}

	updated, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/keep.txt",
		Body:   bytes.NewBufferString("second version"),
	})
	if err != nil {
		t.Fatalf("second PutObject failed: %v", err)
	}

	content, err := os.ReadFile(updated.Path)
	if err != nil {
		t.Fatalf("Read updated object failed: %v", err)
	}
	if string(content) != "second version" {
		t.Fatalf("unexpected updated content: %q", string(content))
	}
	if updated.Size != int64(len("second version")) {
		t.Fatalf("unexpected updated size: %d", updated.Size)
	}
}

func TestListObjectsSupportsPrefixAndLimit(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	fixtures := []string{
		"2026/launch/mock-01.png",
		"2026/launch/mock-02.png",
		"notes/readme.txt",
	}
	for _, key := range fixtures {
		if _, err := store.PutObject(context.Background(), PutObjectInput{
			Bucket: "gallery",
			Key:    key,
			Body:   bytes.NewBufferString(key),
		}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	items, err := store.ListObjects(context.Background(), "gallery", ListObjectsOptions{
		Prefix: "2026/launch/",
		Limit:  1,
	})
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Key != "2026/launch/mock-01.png" {
		t.Fatalf("unexpected first key: %s", items[0].Key)
	}

	allItems, err := store.ListObjects(context.Background(), "gallery", ListObjectsOptions{})
	if err != nil {
		t.Fatalf("ListObjects(all) failed: %v", err)
	}
	if len(allItems) != len(fixtures) {
		t.Fatalf("expected %d items, got %d", len(fixtures), len(allItems))
	}
}

func TestListObjectsPageSupportsQueryAndCursor(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	fixtures := []struct {
		key         string
		contentType string
	}{
		{key: "docs/alpha.txt", contentType: "text/plain"},
		{key: "docs/beta.txt", contentType: "text/plain"},
		{key: "images/cover.png", contentType: "image/png"},
	}
	for _, fixture := range fixtures {
		if _, err := store.PutObject(context.Background(), PutObjectInput{
			Bucket:      "gallery",
			Key:         fixture.key,
			Body:        bytes.NewBufferString(fixture.key),
			ContentType: fixture.contentType,
		}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", fixture.key, err)
		}
	}

	queryPage, err := store.ListObjectsPage(context.Background(), "gallery", ListObjectsOptions{
		Query: "text/plain",
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListObjectsPage(query) failed: %v", err)
	}
	if len(queryPage.Items) != 1 || queryPage.Items[0].Key != "docs/alpha.txt" {
		t.Fatalf("unexpected first query page: %+v", queryPage.Items)
	}
	if !queryPage.HasMore || queryPage.NextCursor != "docs/alpha.txt" {
		t.Fatalf("expected next cursor for query page, got %+v", queryPage)
	}

	nextPage, err := store.ListObjectsPage(context.Background(), "gallery", ListObjectsOptions{
		Query: "text/plain",
		After: queryPage.NextCursor,
		Limit: 1,
	})
	if err != nil {
		t.Fatalf("ListObjectsPage(after) failed: %v", err)
	}
	if len(nextPage.Items) != 1 || nextPage.Items[0].Key != "docs/beta.txt" {
		t.Fatalf("unexpected second query page: %+v", nextPage.Items)
	}
	if nextPage.HasMore {
		t.Fatalf("expected second query page to be final, got %+v", nextPage)
	}
}

func TestDeleteObjectRemovesDataAndMetadata(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	object, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/remove.txt",
		Body:   bytes.NewBufferString("bye"),
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if err := store.DeleteObject(context.Background(), "gallery", "notes/remove.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	if _, err := os.Stat(object.Path); !os.IsNotExist(err) {
		t.Fatalf("expected object file to be removed, got %v", err)
	}
	if _, err := os.Stat(object.MetadataPath); !os.IsNotExist(err) {
		t.Fatalf("expected metadata file to be removed, got %v", err)
	}
}

func TestMoveObjectRenamesAcrossBuckets(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket gallery failed: %v", err)
	}
	if _, err := store.CreateBucket(context.Background(), "archive", 0); err != nil {
		t.Fatalf("CreateBucket archive failed: %v", err)
	}
	if _, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket:      "gallery",
		Key:         "notes/readme.txt",
		Body:        bytes.NewBufferString("hello"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	moved, err := store.MoveObject(context.Background(), MoveObjectInput{
		SourceBucket:      "gallery",
		SourceKey:         "notes/readme.txt",
		DestinationBucket: "archive",
		DestinationKey:    "moved/readme.txt",
	})
	if err != nil {
		t.Fatalf("MoveObject failed: %v", err)
	}
	if moved.Bucket != "archive" || moved.Key != "moved/readme.txt" {
		t.Fatalf("unexpected moved object: %+v", moved)
	}
	if _, err := store.StatObject(context.Background(), "gallery", "notes/readme.txt"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected old object to be gone, got %v", err)
	}
	stated, err := store.StatObject(context.Background(), "archive", "moved/readme.txt")
	if err != nil {
		t.Fatalf("StatObject moved failed: %v", err)
	}
	if stated.ContentType != "text/plain" {
		t.Fatalf("unexpected moved content type: %s", stated.ContentType)
	}
}

func TestMovePrefixMovesFolderContents(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket gallery failed: %v", err)
	}
	if _, err := store.CreateBucket(context.Background(), "archive", 0); err != nil {
		t.Fatalf("CreateBucket archive failed: %v", err)
	}
	fixtures := []string{"2026/launch/mock-01.png", "2026/launch/mock-02.png"}
	for _, key := range fixtures {
		if _, err := store.PutObject(context.Background(), PutObjectInput{
			Bucket: "gallery",
			Key:    key,
			Body:   bytes.NewBufferString(key),
		}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	result, err := store.MovePrefix(context.Background(), MovePrefixInput{
		SourceBucket:      "gallery",
		SourcePrefix:      "2026/launch/",
		DestinationBucket: "archive",
		DestinationPrefix: "imports/launch/",
	})
	if err != nil {
		t.Fatalf("MovePrefix failed: %v", err)
	}
	if result.MovedCount != len(fixtures) {
		t.Fatalf("unexpected moved count: %d", result.MovedCount)
	}
	items, err := store.ListObjects(context.Background(), "archive", ListObjectsOptions{Prefix: "imports/launch/"})
	if err != nil {
		t.Fatalf("ListObjects archive failed: %v", err)
	}
	if len(items) != len(fixtures) {
		t.Fatalf("expected %d moved items, got %d", len(fixtures), len(items))
	}
	sourceItems, err := store.ListObjects(context.Background(), "gallery", ListObjectsOptions{Prefix: "2026/launch/"})
	if err != nil {
		t.Fatalf("ListObjects source after move failed: %v", err)
	}
	if len(sourceItems) != 0 {
		t.Fatalf("expected moved source prefix to be empty, got %d items", len(sourceItems))
	}
	if _, err := store.StatObject(context.Background(), "gallery", "2026/launch/mock-01.png"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected source object removed, got %v", err)
	}
}

func TestUpdateObjectMetadataPersistsChanges(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket:      "gallery",
		Key:         "notes/readme.txt",
		Body:        bytes.NewBufferString("hello"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	updated, err := store.UpdateObjectMetadata(context.Background(), UpdateObjectMetadataInput{
		Bucket:             "gallery",
		Key:                "notes/readme.txt",
		ContentType:        "text/markdown",
		ContentDisposition: "inline",
		CacheControl:       "public, max-age=60",
		UserMetadata:       map[string]string{"author": "bare"},
	})
	if err != nil {
		t.Fatalf("UpdateObjectMetadata failed: %v", err)
	}
	if updated.ContentType != "text/markdown" || updated.ContentDisposition != "inline" || updated.CacheControl != "public, max-age=60" {
		t.Fatalf("unexpected updated object metadata: %+v", updated)
	}
	if updated.UserMetadata["author"] != "bare" {
		t.Fatalf("expected updated user metadata, got %+v", updated.UserMetadata)
	}

	stated, err := store.StatObject(context.Background(), "gallery", "notes/readme.txt")
	if err != nil {
		t.Fatalf("StatObject failed: %v", err)
	}
	if stated.ContentType != "text/markdown" || stated.CacheControl != "public, max-age=60" || stated.ContentDisposition != "inline" {
		t.Fatalf("unexpected persisted metadata: %+v", stated)
	}
}

func TestDeletePrefixRemovesNestedObjects(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	fixtures := []string{"folder/a.txt", "folder/deep/b.txt", "other/c.txt"}
	for _, key := range fixtures {
		if _, err := store.PutObject(context.Background(), PutObjectInput{Bucket: "gallery", Key: key, Body: bytes.NewBufferString(key)}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	deleted, err := store.DeletePrefix(context.Background(), "gallery", "folder/")
	if err != nil {
		t.Fatalf("DeletePrefix failed: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("expected 2 deleted objects, got %d", deleted)
	}
	remaining, err := store.ListObjects(context.Background(), "gallery", ListObjectsOptions{})
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}
	if len(remaining) != 1 || remaining[0].Key != "other/c.txt" {
		t.Fatalf("unexpected remaining objects: %+v", remaining)
	}
}

func TestDeleteBucketRequiresEmptyBucket(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(context.Background(), PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/file.txt",
		Body:   bytes.NewBufferString("data"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	if err := store.DeleteBucket(context.Background(), "gallery"); !errors.Is(err, ErrBucketNotEmpty) {
		t.Fatalf("expected ErrBucketNotEmpty, got %v", err)
	}
	if err := store.DeleteObject(context.Background(), "gallery", "notes/file.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	if err := store.DeleteBucket(context.Background(), "gallery"); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}
	if _, err := store.GetBucket(context.Background(), "gallery"); !errors.Is(err, ErrBucketNotFound) {
		t.Fatalf("expected ErrBucketNotFound after delete, got %v", err)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")

	for _, dir := range []string{cfg.Paths.DataDir, cfg.Paths.LogDir, cfg.Storage.TmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) failed: %v", dir, err)
		}
	}

	return New(cfg, zap.NewNop())
}
