package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/sharelink"
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

func TestMoveBrowserEntries(t *testing.T) {
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
		t.Fatalf("CreateBucket gallery failed: %v", err)
	}
	if _, err := store.CreateBucket(ctx, "archive", 0); err != nil {
		t.Fatalf("CreateBucket archive failed: %v", err)
	}
	for _, key := range []string{"notes/readme.txt", "folder/a.txt", "folder/b.txt"} {
		if _, err := store.PutObject(ctx, storage.PutObjectInput{
			Bucket: "gallery",
			Key:    key,
			Body:   bytes.NewBufferString(key),
		}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	handler := NewHandler(cfg, store, zap.NewNop())
	cookie := loginCookie(t, handler)

	shareOneBody, _ := json.Marshal(map[string]any{
		"bucket":          "gallery",
		"key":             "notes/readme.txt",
		"expires_seconds": 3600,
	})
	shareOneRequest := httptest.NewRequest(http.MethodPost, "/api/v1/share-links", bytes.NewReader(shareOneBody))
	shareOneRequest.Header.Set("Content-Type", "application/json")
	shareOneRequest.AddCookie(cookie)
	shareOneRecorder := httptest.NewRecorder()
	handler.ServeHTTP(shareOneRecorder, shareOneRequest)
	if shareOneRecorder.Code != http.StatusCreated {
		t.Fatalf("unexpected create first share link status: %d body=%s", shareOneRecorder.Code, shareOneRecorder.Body.String())
	}

	shareTwoBody, _ := json.Marshal(map[string]any{
		"bucket":          "gallery",
		"key":             "folder/a.txt",
		"expires_seconds": 3600,
	})
	shareTwoRequest := httptest.NewRequest(http.MethodPost, "/api/v1/share-links", bytes.NewReader(shareTwoBody))
	shareTwoRequest.Header.Set("Content-Type", "application/json")
	shareTwoRequest.AddCookie(cookie)
	shareTwoRecorder := httptest.NewRecorder()
	handler.ServeHTTP(shareTwoRecorder, shareTwoRequest)
	if shareTwoRecorder.Code != http.StatusCreated {
		t.Fatalf("unexpected create second share link status: %d body=%s", shareTwoRecorder.Code, shareTwoRecorder.Body.String())
	}

	moveObjectBody, _ := json.Marshal(map[string]string{
		"kind":               "object",
		"source_bucket":      "gallery",
		"source_key":         "notes/readme.txt",
		"destination_bucket": "archive",
		"destination_key":    "moved/readme.txt",
	})
	moveObjectRequest := httptest.NewRequest(http.MethodPost, "/api/v1/browser/move", bytes.NewReader(moveObjectBody))
	moveObjectRequest.Header.Set("Content-Type", "application/json")
	moveObjectRequest.AddCookie(cookie)
	moveObjectRecorder := httptest.NewRecorder()
	handler.ServeHTTP(moveObjectRecorder, moveObjectRequest)
	if moveObjectRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected move object status: %d body=%s", moveObjectRecorder.Code, moveObjectRecorder.Body.String())
	}

	movePrefixBody, _ := json.Marshal(map[string]string{
		"kind":               "prefix",
		"source_bucket":      "gallery",
		"source_prefix":      "folder/",
		"destination_bucket": "archive",
		"destination_prefix": "imports/folder/",
	})
	movePrefixRequest := httptest.NewRequest(http.MethodPost, "/api/v1/browser/move", bytes.NewReader(movePrefixBody))
	movePrefixRequest.Header.Set("Content-Type", "application/json")
	movePrefixRequest.AddCookie(cookie)
	movePrefixRecorder := httptest.NewRecorder()
	handler.ServeHTTP(movePrefixRecorder, movePrefixRequest)
	if movePrefixRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected move prefix status: %d body=%s", movePrefixRecorder.Code, movePrefixRecorder.Body.String())
	}

	listArchiveRequest := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/archive/objects", nil)
	listArchiveRequest.AddCookie(cookie)
	listArchiveRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listArchiveRecorder, listArchiveRequest)
	if listArchiveRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected archive objects status: %d body=%s", listArchiveRecorder.Code, listArchiveRecorder.Body.String())
	}

	archiveItems := struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(listArchiveRecorder.Body.Bytes(), &archiveItems); err != nil {
		t.Fatalf("unmarshal archive objects payload: %v", err)
	}
	if len(archiveItems.Items) != 3 {
		t.Fatalf("expected 3 moved objects in archive, got %+v", archiveItems.Items)
	}

	shareLinksRequest := httptest.NewRequest(http.MethodGet, "/api/v1/share-links", nil)
	shareLinksRequest.AddCookie(cookie)
	shareLinksRecorder := httptest.NewRecorder()
	handler.ServeHTTP(shareLinksRecorder, shareLinksRequest)
	if shareLinksRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected share links status: %d body=%s", shareLinksRecorder.Code, shareLinksRecorder.Body.String())
	}

	shareLinksPayload := struct {
		Items []struct {
			Bucket string `json:"bucket"`
			Key    string `json:"key"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(shareLinksRecorder.Body.Bytes(), &shareLinksPayload); err != nil {
		t.Fatalf("unmarshal share links payload: %v", err)
	}
	if len(shareLinksPayload.Items) != 2 {
		t.Fatalf("expected 2 share links after move, got %+v", shareLinksPayload.Items)
	}
	keys := []string{shareLinksPayload.Items[0].Bucket + "/" + shareLinksPayload.Items[0].Key, shareLinksPayload.Items[1].Bucket + "/" + shareLinksPayload.Items[1].Key}
	if !containsString(keys, "archive/moved/readme.txt") || !containsString(keys, "archive/imports/folder/a.txt") {
		t.Fatalf("expected moved share links to follow objects, got %+v", keys)
	}
}

func TestDeleteObjectAlsoRemovesShareLinks(t *testing.T) {
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
		Bucket: "gallery",
		Key:    "notes/delete-me.txt",
		Body:   bytes.NewBufferString("delete me"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	handler := NewHandler(cfg, store, zap.NewNop())
	cookie := loginCookie(t, handler)

	shareBody, _ := json.Marshal(map[string]any{
		"bucket":          "gallery",
		"key":             "notes/delete-me.txt",
		"expires_seconds": 3600,
	})
	shareRequest := httptest.NewRequest(http.MethodPost, "/api/v1/share-links", bytes.NewReader(shareBody))
	shareRequest.Header.Set("Content-Type", "application/json")
	shareRequest.AddCookie(cookie)
	shareRecorder := httptest.NewRecorder()
	handler.ServeHTTP(shareRecorder, shareRequest)
	if shareRecorder.Code != http.StatusCreated {
		t.Fatalf("unexpected share link create status: %d body=%s", shareRecorder.Code, shareRecorder.Body.String())
	}

	deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/gallery/objects/notes/delete-me.txt", nil)
	deleteRequest.AddCookie(cookie)
	deleteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteRecorder, deleteRequest)
	if deleteRecorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected delete object status: %d body=%s", deleteRecorder.Code, deleteRecorder.Body.String())
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/share-links", nil)
	listRequest.AddCookie(cookie)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected share links status after delete: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	listed := struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("unmarshal share links after delete failed: %v", err)
	}
	if len(listed.Items) != 0 {
		t.Fatalf("expected deleted object share links to be removed, got %+v", listed.Items)
	}
}

func TestUpdateObjectMetadata(t *testing.T) {
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
		Key:         "notes/edit.txt",
		Body:        bytes.NewBufferString("edit me"),
		ContentType: "text/plain",
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	handler := NewHandler(cfg, store, zap.NewNop())
	cookie := loginCookie(t, handler)

	body, _ := json.Marshal(map[string]any{
		"content_type":        "text/markdown",
		"content_disposition": "inline",
		"cache_control":       "public, max-age=60",
		"user_metadata": map[string]string{
			"author": "bare",
		},
	})
	request := httptest.NewRequest(http.MethodPut, "/api/v1/buckets/gallery/metadata/notes/edit.txt", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(cookie)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected update metadata status: %d body=%s", recorder.Code, recorder.Body.String())
	}

	payload := struct {
		ContentType        string            `json:"content_type"`
		ContentDisposition string            `json:"content_disposition"`
		CacheControl       string            `json:"cache_control"`
		UserMetadata       map[string]string `json:"user_metadata"`
	}{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal metadata payload: %v", err)
	}
	if payload.ContentType != "text/markdown" || payload.ContentDisposition != "inline" || payload.CacheControl != "public, max-age=60" {
		t.Fatalf("unexpected updated metadata payload: %+v", payload)
	}
	if payload.UserMetadata["author"] != "bare" {
		t.Fatalf("unexpected updated user metadata: %+v", payload.UserMetadata)
	}
}

func TestDeletePrefixAlsoRemovesShareLinks(t *testing.T) {
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
	for _, key := range []string{"folder/a.txt", "folder/deep/b.txt"} {
		if _, err := store.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: key, Body: bytes.NewBufferString(key)}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	handler := NewHandler(cfg, store, zap.NewNop())
	cookie := loginCookie(t, handler)
	for _, key := range []string{"folder/a.txt", "folder/deep/b.txt"} {
		shareBody, _ := json.Marshal(map[string]any{"bucket": "gallery", "key": key, "expires_seconds": 3600})
		shareRequest := httptest.NewRequest(http.MethodPost, "/api/v1/share-links", bytes.NewReader(shareBody))
		shareRequest.Header.Set("Content-Type", "application/json")
		shareRequest.AddCookie(cookie)
		shareRecorder := httptest.NewRecorder()
		handler.ServeHTTP(shareRecorder, shareRequest)
		if shareRecorder.Code != http.StatusCreated {
			t.Fatalf("unexpected share link create status: %d body=%s", shareRecorder.Code, shareRecorder.Body.String())
		}
	}

	deleteBody, _ := json.Marshal(map[string]string{"kind": "prefix", "bucket": "gallery", "prefix": "folder/"})
	deleteRequest := httptest.NewRequest(http.MethodPost, "/api/v1/browser/delete", bytes.NewReader(deleteBody))
	deleteRequest.Header.Set("Content-Type", "application/json")
	deleteRequest.AddCookie(cookie)
	deleteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteRecorder, deleteRequest)
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected delete prefix status: %d body=%s", deleteRecorder.Code, deleteRecorder.Body.String())
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/share-links", nil)
	listRequest.AddCookie(cookie)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected share links status after prefix delete: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	listed := struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("unmarshal share links after prefix delete failed: %v", err)
	}
	if len(listed.Items) != 0 {
		t.Fatalf("expected deleted prefix share links to be removed, got %+v", listed.Items)
	}
}

func TestDeleteBucketAlsoRemovesShareLinks(t *testing.T) {
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
	links, err := sharelink.New(cfg.Paths.DataDir, zap.NewNop())
	if err != nil {
		t.Fatalf("sharelink.New failed: %v", err)
	}
	if _, err := links.Create(ctx, sharelink.CreateInput{
		Bucket:  "gallery",
		Key:     "stale/orphan.txt",
		Expires: time.Hour,
	}); err != nil {
		t.Fatalf("Create stale share link failed: %v", err)
	}

	handler := NewHandler(cfg, store, zap.NewNop())
	cookie := loginCookie(t, handler)

	deleteRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/buckets/gallery", nil)
	deleteRequest.AddCookie(cookie)
	deleteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteRecorder, deleteRequest)
	if deleteRecorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected delete bucket status: %d body=%s", deleteRecorder.Code, deleteRecorder.Body.String())
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/share-links", nil)
	listRequest.AddCookie(cookie)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected share links status after bucket delete: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	listed := struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("unmarshal share links after bucket delete failed: %v", err)
	}
	if len(listed.Items) != 0 {
		t.Fatalf("expected deleted bucket share links to be removed, got %+v", listed.Items)
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

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
