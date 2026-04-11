package replication_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/remotes"
	"bares3-server/internal/replication"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
)

func TestLeaderHandlerRequiresAccessToken(t *testing.T) {
	t.Parallel()

	leaderCfg := newSyncConfig(t, t.TempDir())
	leaderStore := newStoreForTest(t, leaderCfg)
	server := newLeaderServer(t, leaderCfg, leaderStore)
	defer server.Close()

	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/internal/sync/manifest", nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext failed: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unexpected status: %d", response.StatusCode)
	}
}

func TestWorkerSyncOnceReplicatesMultipartObjects(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	leaderCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "leader"))
	leaderStore := newStoreForTest(t, leaderCfg)
	if _, err := leaderStore.CreateBucketWithOptions(ctx, storage.CreateBucketInput{
		Name:       "gallery",
		AccessMode: storage.BucketAccessPublic,
		QuotaBytes: 1024,
	}); err != nil {
		t.Fatalf("CreateBucketWithOptions failed: %v", err)
	}
	init, err := leaderStore.InitiateMultipartUpload(ctx, storage.InitiateMultipartUploadInput{
		Bucket:             "gallery",
		Key:                "docs/readme.txt",
		ContentType:        "text/plain",
		CacheControl:       "public, max-age=60",
		ContentDisposition: "inline",
		UserMetadata:       map[string]string{"author": "bare"},
	})
	if err != nil {
		t.Fatalf("InitiateMultipartUpload failed: %v", err)
	}
	partOne, err := leaderStore.UploadPart(ctx, storage.UploadPartInput{
		Bucket:     "gallery",
		Key:        "docs/readme.txt",
		UploadID:   init.UploadID,
		PartNumber: 1,
		Body:       bytes.NewBufferString("hello "),
	})
	if err != nil {
		t.Fatalf("UploadPart(1) failed: %v", err)
	}
	partTwo, err := leaderStore.UploadPart(ctx, storage.UploadPartInput{
		Bucket:     "gallery",
		Key:        "docs/readme.txt",
		UploadID:   init.UploadID,
		PartNumber: 2,
		Body:       bytes.NewBufferString("world"),
	})
	if err != nil {
		t.Fatalf("UploadPart(2) failed: %v", err)
	}
	leaderObject, err := leaderStore.CompleteMultipartUpload(ctx, "gallery", "docs/readme.txt", init.UploadID, []storage.CompletedPart{{PartNumber: 1, ETag: partOne.ETag}, {PartNumber: 2, ETag: partTwo.ETag}})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload failed: %v", err)
	}

	server := newLeaderServer(t, leaderCfg, leaderStore)
	defer server.Close()

	followerCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "follower"))
	followerStore := newStoreForTest(t, followerCfg)
	configuredRemote, err := configureRemoteForTest(ctx, leaderCfg, followerCfg, server.URL, "Leader A", remotes.BootstrapModeFull)
	if err != nil {
		t.Fatalf("configureRemoteForTest failed: %v", err)
	}
	if _, err := followerStore.SetSyncSettings(ctx, storage.SyncSettings{Enabled: true}); err != nil {
		t.Fatalf("SetSyncSettings failed: %v", err)
	}
	worker := replication.NewWorker(followerCfg, followerStore, zap.NewNop())
	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce failed: %v", err)
	}

	bucket, err := followerStore.GetBucket(ctx, "gallery")
	if err != nil {
		t.Fatalf("GetBucket failed: %v", err)
	}
	if bucket.AccessMode != storage.BucketAccessPublic || bucket.QuotaBytes != 1024 {
		t.Fatalf("unexpected replicated bucket: %+v", bucket)
	}
	access, err := followerStore.GetBucketAccessConfig(ctx, "gallery")
	if err != nil {
		t.Fatalf("GetBucketAccessConfig failed: %v", err)
	}
	if access.Mode != storage.BucketAccessPublic {
		t.Fatalf("unexpected replicated access mode: %s", access.Mode)
	}

	followerObject, err := followerStore.StatObject(ctx, "gallery", "docs/readme.txt")
	if err != nil {
		t.Fatalf("StatObject failed: %v", err)
	}
	if followerObject.ETag != leaderObject.ETag {
		t.Fatalf("expected replicated etag %s, got %s", leaderObject.ETag, followerObject.ETag)
	}
	if followerObject.ChecksumSHA256 != leaderObject.ChecksumSHA256 {
		t.Fatalf("expected replicated checksum %s, got %s", leaderObject.ChecksumSHA256, followerObject.ChecksumSHA256)
	}
	if followerObject.ContentType != "text/plain" || followerObject.CacheControl != "public, max-age=60" || followerObject.ContentDisposition != "inline" {
		t.Fatalf("unexpected replicated object metadata: %+v", followerObject)
	}
	if followerObject.UserMetadata["author"] != "bare" {
		t.Fatalf("unexpected replicated user metadata: %+v", followerObject.UserMetadata)
	}
	content, err := os.ReadFile(followerObject.Path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}
	if string(content) != "hello world" {
		t.Fatalf("unexpected replicated content: %q", string(content))
	}
	status, err := followerStore.GetObjectSyncStatus(ctx, "gallery", "docs/readme.txt")
	if err != nil {
		t.Fatalf("GetObjectSyncStatus failed: %v", err)
	}
	if status.Status != storage.SyncStatusReady {
		t.Fatalf("expected ready sync status, got %+v", status)
	}
	remoteStore := newRemoteStoreForTest(t, followerCfg)
	trackedRemote, err := remoteStore.GetRemote(ctx, configuredRemote.ID)
	if err != nil {
		t.Fatalf("GetRemote failed: %v", err)
	}
	if trackedRemote.Status != remotes.RemoteStatusIdle {
		t.Fatalf("expected idle remote status, got %+v", trackedRemote)
	}
	if trackedRemote.LastSyncStartedAt == nil || trackedRemote.LastSyncAt == nil {
		t.Fatalf("expected sync timestamps, got %+v", trackedRemote)
	}
	if trackedRemote.ObjectsTotal != 1 || trackedRemote.ObjectsCompleted != 1 {
		t.Fatalf("unexpected object progress: %+v", trackedRemote)
	}
	if trackedRemote.BytesTotal <= 0 || trackedRemote.BytesCompleted <= 0 {
		t.Fatalf("expected byte progress to be recorded, got %+v", trackedRemote)
	}
}

func TestWorkerSyncOnceDeletesRemovedObjectsAndBuckets(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	leaderCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "leader"))
	leaderStore := newStoreForTest(t, leaderCfg)
	if _, err := leaderStore.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := leaderStore.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: "notes/a.txt", Body: bytes.NewBufferString("hello")}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	server := newLeaderServer(t, leaderCfg, leaderStore)
	defer server.Close()

	followerCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "follower"))
	followerStore := newStoreForTest(t, followerCfg)
	if _, err := configureRemoteForTest(ctx, leaderCfg, followerCfg, server.URL, "Leader A", remotes.BootstrapModeFull); err != nil {
		t.Fatalf("configureRemoteForTest failed: %v", err)
	}
	if _, err := followerStore.SetSyncSettings(ctx, storage.SyncSettings{Enabled: true}); err != nil {
		t.Fatalf("SetSyncSettings failed: %v", err)
	}
	worker := replication.NewWorker(followerCfg, followerStore, zap.NewNop())
	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("initial SyncOnce failed: %v", err)
	}

	if err := leaderStore.DeleteObject(ctx, "gallery", "notes/a.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}
	if err := leaderStore.DeleteBucket(ctx, "gallery"); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}
	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("second SyncOnce failed: %v", err)
	}

	if _, err := followerStore.StatObject(ctx, "gallery", "notes/a.txt"); !errors.Is(err, storage.ErrObjectNotFound) && !errors.Is(err, storage.ErrBucketNotFound) {
		t.Fatalf("expected follower object to be removed, got %v", err)
	}
	if _, err := followerStore.GetBucket(ctx, "gallery"); !errors.Is(err, storage.ErrBucketNotFound) {
		t.Fatalf("expected follower bucket to be removed, got %v", err)
	}
}

func TestLeaderHandlerReturnsIncrementalManifestFromCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	leaderCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "leader"))
	leaderStore := newStoreForTest(t, leaderCfg)
	if _, err := leaderStore.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := leaderStore.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: "notes/a.txt", Body: bytes.NewBufferString("hello")}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	server := newLeaderServer(t, leaderCfg, leaderStore)
	defer server.Close()
	remoteToken, err := issueAccessTokenForTest(ctx, leaderCfg)
	if err != nil {
		t.Fatalf("issueAccessTokenForTest failed: %v", err)
	}

	fullManifest := fetchManifestForTest(t, server.URL+"/internal/sync/manifest", replication.HeaderAccessToken, remoteToken)
	if !fullManifest.Full {
		t.Fatalf("expected full manifest on initial request")
	}
	if fullManifest.Cursor == 0 {
		t.Fatalf("expected non-zero cursor after initial mutations")
	}

	if _, err := leaderStore.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: "notes/b.txt", Body: bytes.NewBufferString("world")}); err != nil {
		t.Fatalf("second PutObject failed: %v", err)
	}
	if err := leaderStore.DeleteObject(ctx, "gallery", "notes/a.txt"); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	deltaManifest := fetchManifestForTest(t, server.URL+"/internal/sync/manifest?cursor="+fmt.Sprintf("%d", fullManifest.Cursor), replication.HeaderAccessToken, remoteToken)
	if deltaManifest.Full {
		t.Fatalf("expected incremental manifest for cursor request")
	}
	if len(deltaManifest.Objects) != 1 || deltaManifest.Objects[0].Key != "notes/b.txt" {
		t.Fatalf("unexpected incremental object upserts: %+v", deltaManifest.Objects)
	}
	if len(deltaManifest.DeletedObjects) != 1 || deltaManifest.DeletedObjects[0].Key != "notes/a.txt" {
		t.Fatalf("unexpected incremental object deletes: %+v", deltaManifest.DeletedObjects)
	}
}

func TestLeaderHandlerReturnsDomainUpdatesInIncrementalManifest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	leaderCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "leader"))
	leaderStore := newStoreForTest(t, leaderCfg)

	server := newLeaderServer(t, leaderCfg, leaderStore)
	defer server.Close()
	remoteToken, err := issueAccessTokenForTest(ctx, leaderCfg)
	if err != nil {
		t.Fatalf("issueAccessTokenForTest failed: %v", err)
	}

	fullManifest := fetchManifestForTest(t, server.URL+"/internal/sync/manifest", replication.HeaderAccessToken, remoteToken)
	if !fullManifest.DomainsChanged {
		t.Fatalf("expected full manifest to include domain bindings")
	}
	if len(fullManifest.Domains) != 0 {
		t.Fatalf("expected empty initial domain bindings, got %+v", fullManifest.Domains)
	}

	updated, err := leaderStore.SetPublicDomainBindings(ctx, []storage.PublicDomainBinding{{Host: "cdn.example.com", Bucket: "gallery", Prefix: "site", IndexDocument: true}})
	if err != nil {
		t.Fatalf("SetPublicDomainBindings failed: %v", err)
	}

	deltaManifest := fetchManifestForTest(t, server.URL+"/internal/sync/manifest?cursor="+fmt.Sprintf("%d", fullManifest.Cursor), replication.HeaderAccessToken, remoteToken)
	if deltaManifest.Full {
		t.Fatalf("expected incremental manifest for cursor request")
	}
	if !deltaManifest.DomainsChanged {
		t.Fatalf("expected incremental manifest to include domain change")
	}
	if !reflect.DeepEqual(deltaManifest.Domains, updated) {
		t.Fatalf("unexpected incremental domain bindings: %+v", deltaManifest.Domains)
	}
}

func TestWorkerSyncOnceReplicatesDomainBindings(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	leaderCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "leader"))
	leaderStore := newStoreForTest(t, leaderCfg)
	if _, err := leaderStore.SetPublicDomainBindings(ctx, []storage.PublicDomainBinding{{Host: "cdn.example.com", Bucket: "gallery", Prefix: "site", IndexDocument: true}}); err != nil {
		t.Fatalf("SetPublicDomainBindings failed: %v", err)
	}

	server := newLeaderServer(t, leaderCfg, leaderStore)
	defer server.Close()

	followerCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "follower"))
	followerStore := newStoreForTest(t, followerCfg)
	if _, err := configureRemoteForTest(ctx, leaderCfg, followerCfg, server.URL, "Leader A", remotes.BootstrapModeFull); err != nil {
		t.Fatalf("configureRemoteForTest failed: %v", err)
	}
	if _, err := followerStore.SetSyncSettings(ctx, storage.SyncSettings{Enabled: true}); err != nil {
		t.Fatalf("SetSyncSettings failed: %v", err)
	}

	worker := replication.NewWorker(followerCfg, followerStore, zap.NewNop())
	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce failed: %v", err)
	}

	bindings, err := followerStore.PublicDomainBindings(ctx)
	if err != nil {
		t.Fatalf("PublicDomainBindings failed: %v", err)
	}
	want := []storage.PublicDomainBinding{{Host: "cdn.example.com", Bucket: "gallery", Prefix: "site", IndexDocument: true}}
	if !reflect.DeepEqual(bindings, want) {
		t.Fatalf("unexpected replicated domain bindings: %+v", bindings)
	}
}

func TestWorkerSyncOnceKeepsExistingObjectReadableWhileUpdating(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	leaderCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "leader"))
	leaderStore := newStoreForTest(t, leaderCfg)
	if _, err := leaderStore.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := leaderStore.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: "notes/readme.txt", Body: bytes.NewBufferString("new version")}); err != nil {
		t.Fatalf("leader PutObject failed: %v", err)
	}

	server := newLeaderServer(t, leaderCfg, leaderStore)
	defer server.Close()

	followerCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "follower"))
	followerStore := newStoreForTest(t, followerCfg)
	if _, err := configureRemoteForTest(ctx, leaderCfg, followerCfg, server.URL, "Leader A", remotes.BootstrapModeFull); err != nil {
		t.Fatalf("configureRemoteForTest failed: %v", err)
	}
	if _, err := followerStore.SetSyncSettings(ctx, storage.SyncSettings{Enabled: true}); err != nil {
		t.Fatalf("SetSyncSettings failed: %v", err)
	}
	if _, err := followerStore.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("follower CreateBucket failed: %v", err)
	}
	if _, err := followerStore.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: "notes/readme.txt", Body: bytes.NewBufferString("old version")}); err != nil {
		t.Fatalf("follower PutObject failed: %v", err)
	}
	if err := followerStore.SetObjectSyncStatus(ctx, storage.SyncObjectStatus{Bucket: "gallery", Key: "notes/readme.txt", Status: storage.SyncStatusDownloading}); err != nil {
		t.Fatalf("SetObjectSyncStatus failed: %v", err)
	}

	before, _, err := followerStore.OpenObject(ctx, "gallery", "notes/readme.txt")
	if err != nil {
		t.Fatalf("OpenObject before sync failed: %v", err)
	}
	beforeBody, err := io.ReadAll(before)
	_ = before.Close()
	if err != nil {
		t.Fatalf("ReadAll before sync failed: %v", err)
	}
	if string(beforeBody) != "old version" {
		t.Fatalf("expected old version before sync, got %q", string(beforeBody))
	}

	worker := replication.NewWorker(followerCfg, followerStore, zap.NewNop())
	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce failed: %v", err)
	}

	afterFile, afterObject, err := followerStore.OpenObject(ctx, "gallery", "notes/readme.txt")
	if err != nil {
		t.Fatalf("OpenObject after sync failed: %v", err)
	}
	afterBody, err := io.ReadAll(afterFile)
	_ = afterFile.Close()
	if err != nil {
		t.Fatalf("ReadAll after sync failed: %v", err)
	}
	if string(afterBody) != "new version" {
		t.Fatalf("expected new version after sync, got %q", string(afterBody))
	}
	status, err := followerStore.GetObjectSyncStatus(ctx, "gallery", "notes/readme.txt")
	if err != nil {
		t.Fatalf("GetObjectSyncStatus failed: %v", err)
	}
	if status.Status != storage.SyncStatusReady {
		t.Fatalf("expected ready status after sync, got %+v", status)
	}
	if afterObject.ChecksumSHA256 != checksumHexForString("new version") {
		t.Fatalf("unexpected checksum after sync: %s", afterObject.ChecksumSHA256)
	}
}

func TestWorkerSyncOnceCatchesUpAcrossPagedIncrementalManifests(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	leaderCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "leader"))
	leaderStore := newStoreForTest(t, leaderCfg)
	if _, err := leaderStore.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	for i := range 5005 {
		key := fmt.Sprintf("notes/%04d.txt", i)
		if _, err := leaderStore.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: key, Body: bytes.NewBufferString(key)}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	server := newLeaderServer(t, leaderCfg, leaderStore)
	defer server.Close()

	followerCfg := newSyncConfig(t, filepath.Join(t.TempDir(), "follower"))
	followerStore := newStoreForTest(t, followerCfg)
	if _, err := configureRemoteForTest(ctx, leaderCfg, followerCfg, server.URL, "Leader A", remotes.BootstrapModeFromNow); err != nil {
		t.Fatalf("configureRemoteForTest failed: %v", err)
	}
	if _, err := followerStore.SetSyncSettings(ctx, storage.SyncSettings{Enabled: true}); err != nil {
		t.Fatalf("SetSyncSettings failed: %v", err)
	}

	remoteStore := newRemoteStoreForTest(t, followerCfg)
	items, err := remoteStore.ListRemotes(ctx)
	if err != nil {
		t.Fatalf("ListRemotes failed: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected one remote, got %d", len(items))
	}
	zero := int64(0)
	if err := remoteStore.UpdateRemoteState(ctx, remotes.UpdateRemoteStateInput{ID: items[0].ID, Cursor: &zero}); err != nil {
		t.Fatalf("UpdateRemoteState failed: %v", err)
	}

	worker := replication.NewWorker(followerCfg, followerStore, zap.NewNop())
	if err := worker.SyncOnce(ctx); err != nil {
		t.Fatalf("SyncOnce failed: %v", err)
	}

	for _, key := range []string{"notes/0000.txt", "notes/4999.txt", "notes/5004.txt"} {
		object, err := followerStore.StatObject(ctx, "gallery", key)
		if err != nil {
			t.Fatalf("StatObject(%s) failed: %v", key, err)
		}
		if object.Key != key {
			t.Fatalf("unexpected object after catch-up: %+v", object)
		}
	}
	trackedRemote, err := remoteStore.GetRemote(ctx, items[0].ID)
	if err != nil {
		t.Fatalf("GetRemote failed: %v", err)
	}
	currentCursor, err := leaderStore.CurrentSyncCursor(ctx)
	if err != nil {
		t.Fatalf("CurrentSyncCursor failed: %v", err)
	}
	if trackedRemote.Cursor != currentCursor {
		t.Fatalf("expected remote cursor %d, got %+v", currentCursor, trackedRemote)
	}
	if trackedRemote.Status != remotes.RemoteStatusIdle {
		t.Fatalf("expected idle remote after catch-up, got %+v", trackedRemote)
	}
	if trackedRemote.ObjectsCompleted != trackedRemote.ObjectsTotal {
		t.Fatalf("expected completed objects to match plan after catch-up, got %+v", trackedRemote)
	}
	if trackedRemote.ObjectsTotal == 0 {
		t.Fatalf("expected catch-up run to record non-zero object plan, got %+v", trackedRemote)
	}
	if trackedRemote.LastSyncAt == nil {
		t.Fatalf("expected sync timestamp after catch-up, got %+v", trackedRemote)
	}
	if trackedRemote.LastSyncAt.Before(trackedRemote.UpdatedAt.Add(-time.Minute)) {
		t.Fatalf("unexpected stale sync timestamp: %+v", trackedRemote)
	}
}

func newSyncConfig(t *testing.T, root string) config.Config {
	t.Helper()

	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Paths.TmpDir = filepath.Join(root, "tmp")
	cfg.Settings.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Settings.S3BaseURL = "http://127.0.0.1:9000"
	return cfg
}

func newStoreForTest(t *testing.T, cfg config.Config) *storage.Store {
	t.Helper()
	for _, dir := range []string{cfg.Paths.DataDir, cfg.Paths.LogDir, cfg.Paths.TmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s) failed: %v", dir, err)
		}
	}
	store := storage.New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func newLeaderServer(t *testing.T, cfg config.Config, store *storage.Store) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/internal/sync/", http.StripPrefix("/internal/sync", replication.NewLeaderHandler(cfg, store, zap.NewNop())))
	return httptest.NewServer(mux)
}

func newRemoteStoreForTest(t *testing.T, cfg config.Config) *remotes.Store {
	t.Helper()
	store, err := remotes.New(cfg.Paths.DataDir, zap.NewNop())
	if err != nil {
		t.Fatalf("remotes.New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func issueAccessTokenForTest(ctx context.Context, cfg config.Config) (string, error) {
	store, err := remotes.New(cfg.Paths.DataDir, zap.NewNop())
	if err != nil {
		return "", err
	}
	defer func() {
		_ = store.Close()
	}()
	token, err := store.CreateAccessToken(ctx, remotes.CreateAccessTokenInput{Label: "test-token"})
	if err != nil {
		return "", err
	}
	return token.Token, nil
}

func configureRemoteForTest(ctx context.Context, sourceCfg, followerCfg config.Config, endpoint, displayName, bootstrapMode string) (remotes.Remote, error) {
	token, err := issueAccessTokenForTest(ctx, sourceCfg)
	if err != nil {
		return remotes.Remote{}, err
	}
	remoteStore, err := remotes.New(followerCfg.Paths.DataDir, zap.NewNop())
	if err != nil {
		return remotes.Remote{}, err
	}
	defer func() {
		_ = remoteStore.Close()
	}()
	return remoteStore.CreateRemote(ctx, remotes.CreateRemoteInput{DisplayName: displayName, Endpoint: endpoint, Token: token, BootstrapMode: bootstrapMode})
}

func fetchManifestForTest(t *testing.T, requestURL string, headerName string, headerValue string) replication.Manifest {
	t.Helper()
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, requestURL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext failed: %v", err)
	}
	request.Header.Set(headerName, headerValue)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("unexpected manifest status: %d", response.StatusCode)
	}
	manifest := replication.Manifest{}
	if err := json.NewDecoder(response.Body).Decode(&manifest); err != nil {
		t.Fatalf("Decode failed: %v", err)
	}
	return manifest
}

func checksumHexForString(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
