package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"bares3-server/internal/config"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
)

func TestLoginAndProtectedRuntime(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	hash, err := consoleauth.HashPassword("secret-password")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}
	cfg.Auth.Console.PasswordHash = hash
	cfg.Auth.Console.SessionSecret = "test-session-secret"

	store := storage.New(cfg, zap.NewNop())
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery"); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, storage.PutObjectInput{
		Bucket:      "gallery",
		Key:         "launch/mock-02.png",
		Body:        bytes.NewBufferString("mock-02"),
		ContentType: "image/png",
	}); err != nil {
		t.Fatalf("PutObject mock-02 failed: %v", err)
	}
	if _, err := store.PutObject(ctx, storage.PutObjectInput{
		Bucket:      "gallery",
		Key:         "launch/mock-03.png",
		Body:        bytes.NewBufferString("mock-03"),
		ContentType: "image/png",
	}); err != nil {
		t.Fatalf("PutObject mock-03 failed: %v", err)
	}

	handler := NewHandler(cfg, store, zap.NewNop())

	unauthorized := httptest.NewRequest(http.MethodGet, "/api/v1/runtime", nil)
	unauthorizedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(unauthorizedRecorder, unauthorized)
	if unauthorizedRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("unexpected unauthorized status: %d body=%s", unauthorizedRecorder.Code, unauthorizedRecorder.Body.String())
	}

	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "secret-password"})
	loginRequest := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(loginBody))
	loginRequest.Header.Set("Content-Type", "application/json")
	loginRecorder := httptest.NewRecorder()
	handler.ServeHTTP(loginRecorder, loginRequest)
	if loginRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected login status: %d body=%s", loginRecorder.Code, loginRecorder.Body.String())
	}
	if len(loginRecorder.Result().Cookies()) == 0 {
		t.Fatalf("expected session cookie after login")
	}

	runtimeRequest := httptest.NewRequest(http.MethodGet, "/api/v1/runtime", nil)
	runtimeRequest.AddCookie(loginRecorder.Result().Cookies()[0])
	runtimeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(runtimeRecorder, runtimeRequest)
	if runtimeRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected runtime status: %d body=%s", runtimeRecorder.Code, runtimeRecorder.Body.String())
	}

	payload := struct {
		Storage struct {
			BucketCount     int `json:"bucket_count"`
			ActiveLinkCount int `json:"active_link_count"`
		} `json:"storage"`
	}{}
	if err := json.Unmarshal(runtimeRecorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal runtime payload: %v", err)
	}
	if payload.Storage.BucketCount != 1 {
		t.Fatalf("unexpected bucket count: %d", payload.Storage.BucketCount)
	}
	if payload.Storage.ActiveLinkCount != 2 {
		t.Fatalf("unexpected active link count: %d", payload.Storage.ActiveLinkCount)
	}
}
