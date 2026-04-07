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
	cfg.Storage.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Storage.S3BaseURL = "http://127.0.0.1:9000"
	hash, err := consoleauth.HashPassword("secret-password")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}
	cfg.Auth.Console.PasswordHash = hash
	cfg.Auth.Console.SessionSecret = "test-session-secret"

	store := storage.New(cfg, zap.NewNop())
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
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

	auditRequest := httptest.NewRequest(http.MethodGet, "/api/v1/audit/events?limit=5", nil)
	auditRequest.AddCookie(loginRecorder.Result().Cookies()[0])
	auditRecorder := httptest.NewRecorder()
	handler.ServeHTTP(auditRecorder, auditRequest)
	if auditRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected audit status: %d body=%s", auditRecorder.Code, auditRecorder.Body.String())
	}

	payload := struct {
		Storage struct {
			MaxBytes        int `json:"max_bytes"`
			UsedBytes       int `json:"used_bytes"`
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
	if payload.Storage.MaxBytes != 0 {
		t.Fatalf("unexpected max bytes: %d", payload.Storage.MaxBytes)
	}
	if payload.Storage.UsedBytes != len("mock-02")+len("mock-03") {
		t.Fatalf("unexpected used bytes: %d", payload.Storage.UsedBytes)
	}
	if payload.Storage.ActiveLinkCount != 0 {
		t.Fatalf("unexpected active link count: %d", payload.Storage.ActiveLinkCount)
	}

	auditPayload := struct {
		Items []struct {
			Action string `json:"action"`
			Title  string `json:"title"`
			Actor  string `json:"actor"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(auditRecorder.Body.Bytes(), &auditPayload); err != nil {
		t.Fatalf("unmarshal audit payload: %v", err)
	}
	if len(auditPayload.Items) == 0 {
		t.Fatalf("expected at least one audit event")
	}
	if auditPayload.Items[0].Action != "auth.login" {
		t.Fatalf("unexpected latest audit action: %s", auditPayload.Items[0].Action)
	}
	if auditPayload.Items[0].Actor != "admin" {
		t.Fatalf("unexpected latest audit actor: %s", auditPayload.Items[0].Actor)
	}
}

func TestUpdateStorageSettingsPersistsAndAppliesImmediately(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Storage.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Storage.S3BaseURL = "http://127.0.0.1:9000"
	hash, err := consoleauth.HashPassword("secret-password")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}
	cfg.Auth.Console.PasswordHash = hash
	cfg.Auth.Console.SessionSecret = "test-session-secret"
	cfg.Runtime.ConfigPath = filepath.Join(root, "config.yml")
	cfg.Runtime.ConfigUsed = true
	if err := config.Save(cfg.Runtime.ConfigPath, cfg); err != nil {
		t.Fatalf("Save config failed: %v", err)
	}

	store := storage.New(cfg, zap.NewNop())
	handler := NewHandler(cfg, store, zap.NewNop())

	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "secret-password"})
	loginRequest := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(loginBody))
	loginRequest.Header.Set("Content-Type", "application/json")
	loginRecorder := httptest.NewRecorder()
	handler.ServeHTTP(loginRecorder, loginRequest)
	if loginRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected login status: %d body=%s", loginRecorder.Code, loginRecorder.Body.String())
	}
	cookies := loginRecorder.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatalf("expected session cookie after login")
	}

	body, _ := json.Marshal(map[string]int64{"max_bytes": 1024})
	request := httptest.NewRequest(http.MethodPut, "/api/v1/settings/storage", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(cookies[0])
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected settings status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if got := store.InstanceQuotaBytes(); got != 1024 {
		t.Fatalf("unexpected in-memory instance quota: %d", got)
	}

	stored, _, _, err := config.LoadEditable(cfg.Runtime.ConfigPath)
	if err != nil {
		t.Fatalf("LoadEditable failed: %v", err)
	}
	if stored.Storage.MaxBytes != 1024 {
		t.Fatalf("unexpected stored max bytes: %d", stored.Storage.MaxBytes)
	}
}

func TestCreateListAndRevokeShareLinks(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Storage.PublicBaseURL = "http://127.0.0.1:9001"
	cfg.Storage.S3BaseURL = "http://127.0.0.1:9000"
	hash, err := consoleauth.HashPassword("secret-password")
	if err != nil {
		t.Fatalf("HashPassword failed: %v", err)
	}
	cfg.Auth.Console.PasswordHash = hash
	cfg.Auth.Console.SessionSecret = "test-session-secret"

	store := storage.New(cfg, zap.NewNop())
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, storage.PutObjectInput{
		Bucket:      "gallery",
		Key:         "notes/share-me.txt",
		Body:        bytes.NewBufferString("share me"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	handler := NewHandler(cfg, store, zap.NewNop())
	cookie := loginCookie(t, handler)

	createBody, _ := json.Marshal(map[string]any{
		"bucket":          "gallery",
		"key":             "notes/share-me.txt",
		"expires_seconds": 3600,
	})
	createRequest := httptest.NewRequest(http.MethodPost, "/api/v1/share-links", bytes.NewReader(createBody))
	createRequest.Header.Set("Content-Type", "application/json")
	createRequest.AddCookie(cookie)
	createRecorder := httptest.NewRecorder()
	handler.ServeHTTP(createRecorder, createRequest)
	if createRecorder.Code != http.StatusCreated {
		t.Fatalf("unexpected create share link status: %d body=%s", createRecorder.Code, createRecorder.Body.String())
	}

	created := struct {
		ID          string `json:"id"`
		Status      string `json:"status"`
		URL         string `json:"url"`
		DownloadURL string `json:"download_url"`
	}{}
	if err := json.Unmarshal(createRecorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal create share link payload: %v", err)
	}
	if created.ID == "" || created.Status != "active" {
		t.Fatalf("unexpected created share link payload: %+v", created)
	}
	if created.URL == "" || created.DownloadURL == "" {
		t.Fatalf("expected generated URLs in share link payload: %+v", created)
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/share-links", nil)
	listRequest.AddCookie(cookie)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list share links status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}

	listed := struct {
		Items []struct {
			ID     string `json:"id"`
			Status string `json:"status"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("unmarshal list share links payload: %v", err)
	}
	if len(listed.Items) != 1 || listed.Items[0].ID != created.ID || listed.Items[0].Status != "active" {
		t.Fatalf("unexpected listed share links payload: %+v", listed.Items)
	}

	runtimeRequest := httptest.NewRequest(http.MethodGet, "/api/v1/runtime", nil)
	runtimeRequest.AddCookie(cookie)
	runtimeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(runtimeRecorder, runtimeRequest)
	if runtimeRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected runtime status after share link create: %d body=%s", runtimeRecorder.Code, runtimeRecorder.Body.String())
	}
	runtimePayload := struct {
		Storage struct {
			ActiveLinkCount int `json:"active_link_count"`
		} `json:"storage"`
	}{}
	if err := json.Unmarshal(runtimeRecorder.Body.Bytes(), &runtimePayload); err != nil {
		t.Fatalf("unmarshal runtime share link payload: %v", err)
	}
	if runtimePayload.Storage.ActiveLinkCount != 1 {
		t.Fatalf("unexpected active link count after create: %d", runtimePayload.Storage.ActiveLinkCount)
	}

	revokeRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/share-links/"+created.ID, nil)
	revokeRequest.AddCookie(cookie)
	revokeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(revokeRecorder, revokeRequest)
	if revokeRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected revoke share link status: %d body=%s", revokeRecorder.Code, revokeRecorder.Body.String())
	}

	revoked := struct {
		ID     string `json:"id"`
		Status string `json:"status"`
	}{}
	if err := json.Unmarshal(revokeRecorder.Body.Bytes(), &revoked); err != nil {
		t.Fatalf("unmarshal revoke share link payload: %v", err)
	}
	if revoked.ID != created.ID || revoked.Status != "revoked" {
		t.Fatalf("unexpected revoked share link payload: %+v", revoked)
	}

	removeRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/share-links/"+created.ID+"/remove", nil)
	removeRequest.AddCookie(cookie)
	removeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(removeRecorder, removeRequest)
	if removeRecorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected remove share link status: %d body=%s", removeRecorder.Code, removeRecorder.Body.String())
	}

	listAfterRemoveRequest := httptest.NewRequest(http.MethodGet, "/api/v1/share-links", nil)
	listAfterRemoveRequest.AddCookie(cookie)
	listAfterRemoveRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listAfterRemoveRecorder, listAfterRemoveRequest)
	if listAfterRemoveRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list share links after remove status: %d body=%s", listAfterRemoveRecorder.Code, listAfterRemoveRecorder.Body.String())
	}

	listedAfterRemove := struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(listAfterRemoveRecorder.Body.Bytes(), &listedAfterRemove); err != nil {
		t.Fatalf("unmarshal list share links after remove payload: %v", err)
	}
	if len(listedAfterRemove.Items) != 0 {
		t.Fatalf("expected no share links after remove, got %+v", listedAfterRemove.Items)
	}
}

func loginCookie(t *testing.T, handler http.Handler) *http.Cookie {
	t.Helper()

	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "secret-password"})
	loginRequest := httptest.NewRequest(http.MethodPost, "/api/v1/auth/login", bytes.NewReader(loginBody))
	loginRequest.Header.Set("Content-Type", "application/json")
	loginRecorder := httptest.NewRecorder()
	handler.ServeHTTP(loginRecorder, loginRequest)
	if loginRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected login status: %d body=%s", loginRecorder.Code, loginRecorder.Body.String())
	}
	cookies := loginRecorder.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatalf("expected session cookie after login")
	}
	return cookies[0]
}
