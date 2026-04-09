package fileserve

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
)

func TestServeShareLinks(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Storage.PublicBaseURL = "http://127.0.0.1:9001"

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, storage.PutObjectInput{
		Bucket:      "gallery",
		Key:         "notes/readme.txt",
		Body:        bytes.NewBufferString("share me"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	links := newShareLinksForTest(t, cfg.Paths.DataDir)
	link, err := links.Create(ctx, sharelink.CreateInput{
		Bucket:  "gallery",
		Key:     "notes/readme.txt",
		Expires: time.Hour,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	handler := newHandler(cfg, store, links, zap.NewNop())

	openRequest := httptest.NewRequest(http.MethodGet, "/s/"+link.ID, nil)
	openRecorder := httptest.NewRecorder()
	handler.ServeHTTP(openRecorder, openRequest)
	if openRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected open status: %d body=%s", openRecorder.Code, openRecorder.Body.String())
	}
	if body := strings.TrimSpace(openRecorder.Body.String()); body != "share me" {
		t.Fatalf("unexpected open body: %q", body)
	}

	downloadRequest := httptest.NewRequest(http.MethodGet, "/dl/"+link.ID, nil)
	downloadRecorder := httptest.NewRecorder()
	handler.ServeHTTP(downloadRecorder, downloadRequest)
	if downloadRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected download status: %d body=%s", downloadRecorder.Code, downloadRecorder.Body.String())
	}
	if disposition := downloadRecorder.Header().Get("Content-Disposition"); !strings.Contains(disposition, "attachment") {
		t.Fatalf("expected attachment content disposition, got %q", disposition)
	}

	if _, err := links.Revoke(ctx, link.ID); err != nil {
		t.Fatalf("Revoke failed: %v", err)
	}

	revokedRequest := httptest.NewRequest(http.MethodGet, "/s/"+link.ID, nil)
	revokedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(revokedRecorder, revokedRequest)
	if revokedRecorder.Code != http.StatusGone {
		t.Fatalf("unexpected revoked status: %d body=%s", revokedRecorder.Code, revokedRecorder.Body.String())
	}
}

func TestServePublicBucketRouteHonorsAccessMode(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Storage.PublicBaseURL = "http://127.0.0.1:9001"

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, storage.PutObjectInput{
		Bucket:      "gallery",
		Key:         "notes/readme.txt",
		Body:        bytes.NewBufferString("public me"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	handler := newHandler(cfg, store, newShareLinksForTest(t, cfg.Paths.DataDir), zap.NewNop())

	privateRequest := httptest.NewRequest(http.MethodGet, "/pub/gallery/notes/readme.txt", nil)
	privateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(privateRecorder, privateRequest)
	if privateRecorder.Code != http.StatusForbidden {
		t.Fatalf("unexpected private access status: %d body=%s", privateRecorder.Code, privateRecorder.Body.String())
	}

	if _, err := store.UpdateBucket(ctx, storage.UpdateBucketInput{
		Name:       "gallery",
		AccessMode: storage.BucketAccessPublic,
		QuotaBytes: 0,
	}); err != nil {
		t.Fatalf("UpdateBucket failed: %v", err)
	}

	publicRequest := httptest.NewRequest(http.MethodGet, "/pub/gallery/notes/readme.txt", nil)
	publicRecorder := httptest.NewRecorder()
	handler.ServeHTTP(publicRecorder, publicRequest)
	if publicRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected public access status: %d body=%s", publicRecorder.Code, publicRecorder.Body.String())
	}
	if body := strings.TrimSpace(publicRecorder.Body.String()); body != "public me" {
		t.Fatalf("unexpected public access body: %q", body)
	}
}

func TestServeCustomBucketAccessRules(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Storage.PublicBaseURL = "http://127.0.0.1:9001"

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	fixtures := []struct {
		key  string
		body string
	}{
		{key: "images/hero.txt", body: "public"},
		{key: "notes/readme.txt", body: "auth"},
		{key: "secret/plan.txt", body: "deny"},
	}
	for _, fixture := range fixtures {
		if _, err := store.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: fixture.key, Body: bytes.NewBufferString(fixture.body), ContentType: "text/plain"}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", fixture.key, err)
		}
	}
	if _, err := store.UpdateBucketAccess(ctx, storage.UpdateBucketAccessInput{
		Name: "gallery",
		Mode: storage.BucketAccessCustom,
		Policy: storage.BucketAccessPolicy{
			DefaultAction: storage.BucketAccessActionAuthenticated,
			Rules: []storage.BucketAccessRule{
				{Prefix: "images/", Action: storage.BucketAccessActionPublic},
				{Prefix: "secret/", Action: storage.BucketAccessActionDeny},
			},
		},
	}); err != nil {
		t.Fatalf("UpdateBucketAccess failed: %v", err)
	}

	links := newShareLinksForTest(t, cfg.Paths.DataDir)
	authLink, err := links.Create(ctx, sharelink.CreateInput{Bucket: "gallery", Key: "notes/readme.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create auth link failed: %v", err)
	}
	denyLink, err := links.Create(ctx, sharelink.CreateInput{Bucket: "gallery", Key: "secret/plan.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create deny link failed: %v", err)
	}

	handler := newHandler(cfg, store, links, zap.NewNop())

	publicRequest := httptest.NewRequest(http.MethodGet, "/pub/gallery/images/hero.txt", nil)
	publicRecorder := httptest.NewRecorder()
	handler.ServeHTTP(publicRecorder, publicRequest)
	if publicRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected public custom status: %d body=%s", publicRecorder.Code, publicRecorder.Body.String())
	}

	authRequest := httptest.NewRequest(http.MethodGet, "/pub/gallery/notes/readme.txt", nil)
	authRecorder := httptest.NewRecorder()
	handler.ServeHTTP(authRecorder, authRequest)
	if authRecorder.Code != http.StatusForbidden {
		t.Fatalf("unexpected authenticated-only public status: %d body=%s", authRecorder.Code, authRecorder.Body.String())
	}

	denyRequest := httptest.NewRequest(http.MethodGet, "/pub/gallery/secret/plan.txt", nil)
	denyRecorder := httptest.NewRecorder()
	handler.ServeHTTP(denyRecorder, denyRequest)
	if denyRecorder.Code != http.StatusForbidden {
		t.Fatalf("unexpected denied public status: %d body=%s", denyRecorder.Code, denyRecorder.Body.String())
	}

	authLinkRequest := httptest.NewRequest(http.MethodGet, "/s/"+authLink.ID, nil)
	authLinkRecorder := httptest.NewRecorder()
	handler.ServeHTTP(authLinkRecorder, authLinkRequest)
	if authLinkRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected authenticated share link status: %d body=%s", authLinkRecorder.Code, authLinkRecorder.Body.String())
	}

	denyLinkRequest := httptest.NewRequest(http.MethodGet, "/s/"+denyLink.ID, nil)
	denyLinkRecorder := httptest.NewRecorder()
	handler.ServeHTTP(denyLinkRecorder, denyLinkRequest)
	if denyLinkRecorder.Code != http.StatusForbidden {
		t.Fatalf("unexpected denied share link status: %d body=%s", denyLinkRecorder.Code, denyLinkRecorder.Body.String())
	}
}

func newStorageStoreForTest(t *testing.T, cfg config.Config) *storage.Store {
	t.Helper()
	store := storage.New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	return store
}

func newShareLinksForTest(t *testing.T, dataDir string) *sharelink.Store {
	t.Helper()
	links, err := sharelink.New(dataDir, zap.NewNop())
	if err != nil {
		t.Fatalf("sharelink.New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = links.Close()
	})
	return links
}
