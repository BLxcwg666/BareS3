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

	store := storage.New(cfg, zap.NewNop())
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

	links, err := sharelink.New(cfg.Paths.DataDir, zap.NewNop())
	if err != nil {
		t.Fatalf("sharelink.New failed: %v", err)
	}
	link, err := links.Create(ctx, sharelink.CreateInput{
		Bucket:  "gallery",
		Key:     "notes/readme.txt",
		Expires: time.Hour,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	handler := NewHandler(cfg, store, zap.NewNop())

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

	store := storage.New(cfg, zap.NewNop())
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

	handler := NewHandler(cfg, store, zap.NewNop())

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
