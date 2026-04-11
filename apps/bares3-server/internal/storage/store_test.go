package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	if created.ReplicationEnabled {
		t.Fatalf("expected replication to default off, got %+v", created)
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
	if buckets[0].ReplicationEnabled {
		t.Fatalf("expected listed replication to default off, got %+v", buckets[0])
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
		Name:               "gallery",
		NewName:            "archive",
		AccessMode:         BucketAccessPublic,
		ReplicationEnabled: true,
		QuotaBytes:         20 * 1024,
		Tags:               []string{"media", "launch", "media"},
		Note:               "Launch assets",
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
	if !updated.ReplicationEnabled {
		t.Fatalf("expected updated replication enabled, got %+v", updated)
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
	if !bucket.ReplicationEnabled {
		t.Fatalf("expected persisted replication enabled, got %+v", bucket)
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

func TestPublicDomainBindingsPersistAcrossStoreRestart(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Paths.TmpDir = filepath.Join(root, "tmp")
	cfg.Settings.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Settings.S3BaseURL = "http://127.0.0.1:9000"

	store := New(cfg, zap.NewNop())
	if _, err := store.SetPublicDomainBindings(context.Background(), []PublicDomainBinding{{Host: "cdn.example.com", Bucket: "gallery", Prefix: "site", IndexDocument: false, SPAFallback: false}}); err != nil {
		t.Fatalf("SetPublicDomainBindings failed: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	restarted := New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = restarted.Close()
	})
	reloaded, err := restarted.PublicDomainBindings(context.Background())
	if err != nil {
		t.Fatalf("PublicDomainBindings after restart failed: %v", err)
	}
	if len(reloaded) != 1 {
		t.Fatalf("expected persisted domain bindings, got %+v", reloaded)
	}
	binding := reloaded[0]
	if binding.Host != "cdn.example.com" || binding.Bucket != "gallery" || binding.Prefix != "site" || binding.IndexDocument || binding.SPAFallback {
		t.Fatalf("unexpected persisted binding after restart: %+v", binding)
	}
}

func TestBucketAccessConfigEvaluatesCustomRules(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	updated, err := store.UpdateBucketAccess(ctx, UpdateBucketAccessInput{
		Name: "gallery",
		Mode: BucketAccessCustom,
		Policy: BucketAccessPolicy{
			DefaultAction: BucketAccessActionAuthenticated,
			Rules: []BucketAccessRule{
				{Prefix: "images/", Action: BucketAccessActionPublic, Note: "Public images"},
				{Prefix: "secret/", Action: BucketAccessActionDeny, Note: "No reads"},
			},
		},
	})
	if err != nil {
		t.Fatalf("UpdateBucketAccess failed: %v", err)
	}
	if updated.Mode != BucketAccessCustom {
		t.Fatalf("unexpected access mode: %s", updated.Mode)
	}
	if updated.Policy.DefaultAction != BucketAccessActionAuthenticated || len(updated.Policy.Rules) != 2 {
		t.Fatalf("unexpected access policy: %+v", updated.Policy)
	}

	config, err := store.GetBucketAccessConfig(ctx, "gallery")
	if err != nil {
		t.Fatalf("GetBucketAccessConfig failed: %v", err)
	}
	if config.Mode != BucketAccessCustom || len(config.Policy.Rules) != 2 {
		t.Fatalf("unexpected persisted access config: %+v", config)
	}

	publicAction, err := store.ResolveBucketObjectAccess(ctx, "gallery", "images/cover.png")
	if err != nil {
		t.Fatalf("ResolveBucketObjectAccess public failed: %v", err)
	}
	if publicAction != BucketAccessActionPublic {
		t.Fatalf("unexpected public action: %s", publicAction)
	}

	authAction, err := store.ResolveBucketObjectAccess(ctx, "gallery", "notes/readme.txt")
	if err != nil {
		t.Fatalf("ResolveBucketObjectAccess auth failed: %v", err)
	}
	if authAction != BucketAccessActionAuthenticated {
		t.Fatalf("unexpected auth action: %s", authAction)
	}

	denyAction, err := store.ResolveBucketObjectAccess(ctx, "gallery", "secret/plan.txt")
	if err != nil {
		t.Fatalf("ResolveBucketObjectAccess deny failed: %v", err)
	}
	if denyAction != BucketAccessActionDeny {
		t.Fatalf("unexpected deny action: %s", denyAction)
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
	if object.MetadataPath != "" {
		t.Fatalf("expected metadata path to be empty, got %q", object.MetadataPath)
	}
	expectedChecksum := checksumHexForTest("hello world")
	if object.ChecksumSHA256 != expectedChecksum {
		t.Fatalf("unexpected object checksum: %s", object.ChecksumSHA256)
	}
	_, metadataPath, err := store.resolveObjectPaths("gallery", "2026/launch/mock-02.png")
	if err != nil {
		t.Fatalf("resolveObjectPaths failed: %v", err)
	}
	if metadataPath != "" {
		t.Fatalf("expected resolveObjectPaths to return empty metadata path, got %q", metadataPath)
	}

	stated, err := store.StatObject(context.Background(), "gallery", "2026/launch/mock-02.png")
	if err != nil {
		t.Fatalf("StatObject failed: %v", err)
	}
	if stated.Size != int64(len("hello world")) {
		t.Fatalf("unexpected object size: %d", stated.Size)
	}
	if stated.ETag == "" {
		t.Fatalf("expected non-empty etag in sqlite metadata")
	}
	if stated.ChecksumSHA256 != expectedChecksum {
		t.Fatalf("unexpected checksum from sqlite metadata: %s", stated.ChecksumSHA256)
	}
	if stated.CacheControl != "public, max-age=3600" {
		t.Fatalf("unexpected cache control from sqlite metadata: %q", stated.CacheControl)
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

func TestListObjectsPageSupportsDelimiterAndOffset(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	fixtures := []string{
		"Basic/alpha.txt",
		"Basic/beta.txt",
		"Backups/2026/alpha.txt",
		"Backups/2026/beta.txt",
		"404.html",
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

	firstPage, err := store.ListObjectsPage(context.Background(), "gallery", ListObjectsOptions{
		Delimiter: "/",
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("ListObjectsPage(first) failed: %v", err)
	}
	if firstPage.TotalCount != 3 {
		t.Fatalf("expected 3 browser entries, got %+v", firstPage)
	}
	if len(firstPage.Prefixes) != 2 || firstPage.Prefixes[0] != "Backups/" || firstPage.Prefixes[1] != "Basic/" {
		t.Fatalf("unexpected first page prefixes: %+v", firstPage.Prefixes)
	}
	if len(firstPage.Items) != 0 {
		t.Fatalf("expected first page to contain only prefixes, got %+v", firstPage.Items)
	}

	secondPage, err := store.ListObjectsPage(context.Background(), "gallery", ListObjectsOptions{
		Delimiter: "/",
		Offset:    2,
		Limit:     2,
	})
	if err != nil {
		t.Fatalf("ListObjectsPage(second) failed: %v", err)
	}
	if len(secondPage.Prefixes) != 0 || len(secondPage.Items) != 1 || secondPage.Items[0].Key != "404.html" {
		t.Fatalf("unexpected second page: %+v", secondPage)
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
	if _, err := store.StatObject(context.Background(), "gallery", "notes/remove.txt"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("expected sqlite metadata to be removed, got %v", err)
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
	if stated.ChecksumSHA256 != checksumHexForTest("hello") {
		t.Fatalf("unexpected moved checksum: %s", stated.ChecksumSHA256)
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
	if stated.ChecksumSHA256 != checksumHexForTest("hello") {
		t.Fatalf("unexpected persisted checksum after metadata update: %s", stated.ChecksumSHA256)
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

func TestNewBackfillsMissingObjectChecksums(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	store := newTestStoreAt(t, root)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/legacy.txt",
		Body:   bytes.NewBufferString("legacy data"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	db, err := store.metadata.openDB()
	if err != nil {
		t.Fatalf("openDB failed: %v", err)
	}
	if _, err := db.NewUpdate().Model((*storageObjectRecord)(nil)).
		Set("checksum_sha256 = ''").
		Where("bucket = ?", "gallery").
		Where("key = ?", "notes/legacy.txt").
		Exec(ctx); err != nil {
		_ = db.Close()
		t.Fatalf("clear checksum failed: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close db failed: %v", err)
	}

	meta, err := store.readObjectMetadata("gallery", "notes/legacy.txt")
	if err != nil {
		t.Fatalf("readObjectMetadata failed: %v", err)
	}
	if meta.ChecksumSHA256 != "" {
		t.Fatalf("expected checksum to be cleared before reopen, got %s", meta.ChecksumSHA256)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened := newTestStoreAt(t, root)
	stated, err := reopened.StatObject(ctx, "gallery", "notes/legacy.txt")
	if err != nil {
		t.Fatalf("StatObject after reopen failed: %v", err)
	}
	expectedChecksum := checksumHexForTest("legacy data")
	if stated.ChecksumSHA256 != expectedChecksum {
		t.Fatalf("unexpected backfilled checksum: %s", stated.ChecksumSHA256)
	}
}

func TestStatObjectServesExistingVersionWhileObjectStatusIsNotReady(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/readme.txt",
		Body:   bytes.NewBufferString("hello"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if err := store.SetObjectSyncStatus(ctx, SyncObjectStatus{
		Bucket:                 "gallery",
		Key:                    "notes/readme.txt",
		Status:                 SyncStatusPending,
		ExpectedChecksumSHA256: checksumHexForTest("hello"),
	}); err != nil {
		t.Fatalf("SetObjectSyncStatus existing object failed: %v", err)
	}
	stated, err := store.StatObject(ctx, "gallery", "notes/readme.txt")
	if err != nil {
		t.Fatalf("expected existing object to remain readable, got %v", err)
	}
	if stated.ChecksumSHA256 != checksumHexForTest("hello") {
		t.Fatalf("unexpected existing object checksum: %s", stated.ChecksumSHA256)
	}

	if err := store.SetObjectSyncStatus(ctx, SyncObjectStatus{
		Bucket:                 "gallery",
		Key:                    "notes/pending.txt",
		Status:                 SyncStatusDownloading,
		ExpectedChecksumSHA256: checksumHexForTest("pending"),
	}); err != nil {
		t.Fatalf("SetObjectSyncStatus missing object failed: %v", err)
	}
	if _, err := store.StatObject(ctx, "gallery", "notes/pending.txt"); !errors.Is(err, ErrObjectSyncing) {
		t.Fatalf("expected ErrObjectSyncing for pending object, got %v", err)
	}

	if err := store.SetObjectSyncStatus(ctx, SyncObjectStatus{
		Bucket:                 "gallery",
		Key:                    "notes/readme.txt",
		Status:                 SyncStatusReady,
		ExpectedChecksumSHA256: checksumHexForTest("hello"),
	}); err != nil {
		t.Fatalf("SetObjectSyncStatus ready failed: %v", err)
	}
	if _, err := store.StatObject(ctx, "gallery", "notes/readme.txt"); err != nil {
		t.Fatalf("expected ready object to stat successfully, got %v", err)
	}
}

func TestApplyReplicaObjectRejectsOlderRevision(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	local, err := store.PutObject(ctx, PutObjectInput{Bucket: "gallery", Key: "notes/readme.txt", Body: bytes.NewBufferString("hello")})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	_, err = store.ApplyReplicaObject(ctx, ReplicaObjectInput{
		Bucket:         "gallery",
		Key:            "notes/readme.txt",
		Body:           bytes.NewBufferString("remote"),
		Size:           int64(len("remote")),
		ETag:           "etag-remote",
		ChecksumSHA256: checksumHexForTest("remote"),
		Revision:       local.Revision - 1,
		OriginNodeID:   "node-b",
		LastChangeID:   "change-b-1",
		LastModified:   time.Now().UTC(),
	})
	if !errors.Is(err, ErrObjectConflict) {
		t.Fatalf("expected ErrObjectConflict, got %v", err)
	}
}

func TestMultipartLegacySessionIgnoresLegacyFiles(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Paths.TmpDir = filepath.Join(root, "tmp")

	for _, dir := range []string{cfg.Paths.DataDir, cfg.Paths.LogDir, cfg.Paths.TmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) failed: %v", dir, err)
		}
	}

	createdAt := time.Date(2026, time.April, 9, 12, 0, 0, 0, time.UTC)
	bucketControlDir := filepath.Join(cfg.Paths.DataDir, "gallery", ".bares3")
	partsDir := filepath.Join(bucketControlDir, multipartDirName, "upload-123", "parts")
	if err := os.MkdirAll(partsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(parts) failed: %v", err)
	}
	writeJSONFileForTest(t, filepath.Join(bucketControlDir, bucketMetaName), bucketMetadata{
		Name:           "gallery",
		CreatedAt:      createdAt,
		MetadataLayout: "hidden-dir",
		AccessMode:     BucketAccessPrivate,
		AccessPolicy:   PresetBucketAccessPolicy(BucketAccessPrivate),
	})
	writeJSONFileForTest(t, filepath.Join(bucketControlDir, multipartDirName, "upload-123", "upload.json"), multipartUploadMetadata{
		UploadID:           "upload-123",
		Bucket:             "gallery",
		Key:                "archive/big.txt",
		ContentType:        "text/plain",
		ContentDisposition: "attachment",
		CreatedAt:          createdAt,
	})
	writeJSONFileForTest(t, filepath.Join(partsDir, "00001.json"), multipartPartMetadata{
		PartNumber:   1,
		ETag:         "part-one",
		Size:         6,
		LastModified: createdAt.Add(time.Minute),
	})

	store := New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})

	if _, err := store.readMultipartUpload("gallery", "archive/big.txt", "upload-123"); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("expected legacy multipart upload metadata to be ignored, got %v", err)
	}
	if _, err := store.ListParts(context.Background(), "gallery", "archive/big.txt", "upload-123"); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("expected legacy multipart parts to be ignored, got %v", err)
	}
}

func newTestStore(t *testing.T) *Store {
	t.Helper()

	return newTestStoreAt(t, t.TempDir())
}

func newTestStoreAt(t *testing.T, root string) *Store {
	t.Helper()

	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Paths.TmpDir = filepath.Join(root, "tmp")

	for _, dir := range []string{cfg.Paths.DataDir, cfg.Paths.LogDir, cfg.Paths.TmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) failed: %v", dir, err)
		}
	}

	store := New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func checksumHexForTest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func writeJSONFileForTest(t *testing.T, path string, value any) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%s) failed: %v", filepath.Dir(path), err)
	}
	content, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("Marshal(%s) failed: %v", path, err)
	}
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("WriteFile(%s) failed: %v", path, err)
	}
}
