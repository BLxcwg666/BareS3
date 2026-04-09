package admin

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/s3creds"
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

	store := newStorageStoreForTest(t, cfg)
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

	handler := newAdminHandlerForTest(t, cfg, store, nil)

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

func TestProtectedMutationsRemainAllowedWhenSyncEnabled(t *testing.T) {
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
	cfg.Sync.Enabled = true

	store := newStorageStoreForTest(t, cfg)
	handler := newAdminHandlerForTest(t, cfg, store, nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	cookie := loginCookie(t, handler)

	body, _ := json.Marshal(map[string]any{"name": "gallery"})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/buckets", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(cookie)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusCreated {
		t.Fatalf("unexpected mutation status: %d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestLoginSetsSecureCookieForHTTPSRequests(t *testing.T) {
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

	handler := newAdminHandlerForTest(t, cfg, newStorageStoreForTest(t, cfg), nil)

	loginBody, _ := json.Marshal(map[string]string{"username": "admin", "password": "secret-password"})
	loginRequest := httptest.NewRequest(http.MethodPost, "https://bares3.test/api/v1/auth/login", bytes.NewReader(loginBody))
	loginRequest.TLS = &tls.ConnectionState{}
	loginRequest.Header.Set("Content-Type", "application/json")
	loginRecorder := httptest.NewRecorder()
	handler.ServeHTTP(loginRecorder, loginRequest)
	if loginRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected login status: %d body=%s", loginRecorder.Code, loginRecorder.Body.String())
	}
	if len(loginRecorder.Result().Cookies()) == 0 {
		t.Fatalf("expected session cookie after login")
	}
	if !loginRecorder.Result().Cookies()[0].Secure {
		t.Fatalf("expected HTTPS login to set a secure cookie")
	}
}

func TestReadinessAndMetricsEndpoints(t *testing.T) {
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

	handler := newAdminHandlerForTest(t, cfg, newStorageStoreForTest(t, cfg), nil)

	readyRequest := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyRecorder := httptest.NewRecorder()
	handler.ServeHTTP(readyRecorder, readyRequest)
	if readyRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected readiness status: %d body=%s", readyRecorder.Code, readyRecorder.Body.String())
	}
	readyPayload := struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}{}
	if err := json.Unmarshal(readyRecorder.Body.Bytes(), &readyPayload); err != nil {
		t.Fatalf("unmarshal readiness payload: %v", err)
	}
	if readyPayload.Status != "ok" {
		t.Fatalf("unexpected readiness payload: %+v", readyPayload)
	}
	if readyPayload.Checks["storage"] != "ok" || readyPayload.Checks["share_links"] != "ok" || readyPayload.Checks["s3_credentials"] != "ok" {
		t.Fatalf("unexpected readiness checks: %+v", readyPayload.Checks)
	}

	healthRequest := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	healthRecorder := httptest.NewRecorder()
	handler.ServeHTTP(healthRecorder, healthRequest)
	if healthRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected health status: %d body=%s", healthRecorder.Code, healthRecorder.Body.String())
	}

	metricsRequest := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	metricsRecorder := httptest.NewRecorder()
	handler.ServeHTTP(metricsRecorder, metricsRequest)
	if metricsRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected metrics status: %d body=%s", metricsRecorder.Code, metricsRecorder.Body.String())
	}
	body := metricsRecorder.Body.String()
	if !strings.Contains(body, "bares3_http_requests_total") {
		t.Fatalf("expected request totals in metrics output, got %s", body)
	}
	if !strings.Contains(body, "service=\"admin\"") {
		t.Fatalf("expected admin service metrics, got %s", body)
	}
	if !strings.Contains(body, "bares3_build_info") {
		t.Fatalf("expected build info in metrics output, got %s", body)
	}
}

func TestObjectListingSupportsPaginationAndSearch(t *testing.T) {
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

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	for _, fixture := range []struct {
		key         string
		contentType string
	}{
		{key: "docs/alpha.txt", contentType: "text/plain"},
		{key: "docs/beta.txt", contentType: "text/plain"},
		{key: "images/cover.png", contentType: "image/png"},
	} {
		if _, err := store.PutObject(ctx, storage.PutObjectInput{
			Bucket:      "gallery",
			Key:         fixture.key,
			Body:        bytes.NewBufferString(fixture.key),
			ContentType: fixture.contentType,
		}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", fixture.key, err)
		}
	}

	handler := newAdminHandlerForTest(t, cfg, store, nil)
	cookie := loginCookie(t, handler)

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/gallery/objects?query=text/plain&limit=1", nil)
	listRequest.AddCookie(cookie)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list objects status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	listPayload := struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
		HasMore    bool   `json:"has_more"`
		NextCursor string `json:"next_cursor"`
	}{}
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("unmarshal list objects payload: %v", err)
	}
	if len(listPayload.Items) != 1 || listPayload.Items[0].Key != "docs/alpha.txt" {
		t.Fatalf("unexpected first list page: %+v", listPayload)
	}
	if !listPayload.HasMore || listPayload.NextCursor != "docs/alpha.txt" {
		t.Fatalf("expected next cursor for paginated results, got %+v", listPayload)
	}

	nextRequest := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/gallery/objects?query=text/plain&limit=1&cursor=docs%2Falpha.txt", nil)
	nextRequest.AddCookie(cookie)
	nextRecorder := httptest.NewRecorder()
	handler.ServeHTTP(nextRecorder, nextRequest)
	if nextRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected second list status: %d body=%s", nextRecorder.Code, nextRecorder.Body.String())
	}
	nextPayload := struct {
		Items []struct {
			Key string `json:"key"`
		} `json:"items"`
		HasMore bool `json:"has_more"`
	}{}
	if err := json.Unmarshal(nextRecorder.Body.Bytes(), &nextPayload); err != nil {
		t.Fatalf("unmarshal second list payload: %v", err)
	}
	if len(nextPayload.Items) != 1 || nextPayload.Items[0].Key != "docs/beta.txt" || nextPayload.HasMore {
		t.Fatalf("unexpected second list page: %+v", nextPayload)
	}
}

func TestGlobalSearchEndpointReturnsBucketAndObjectHits(t *testing.T) {
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

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	for _, bucket := range []string{"gallery", "readables"} {
		if _, err := store.CreateBucket(ctx, bucket, 0); err != nil {
			t.Fatalf("CreateBucket(%s) failed: %v", bucket, err)
		}
	}
	if _, err := store.PutObject(ctx, storage.PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/readme.txt",
		Body:   bytes.NewBufferString("readme"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	handler := newAdminHandlerForTest(t, cfg, store, nil)
	cookie := loginCookie(t, handler)

	searchRequest := httptest.NewRequest(http.MethodGet, "/api/v1/search?query=read&limit=5", nil)
	searchRequest.AddCookie(cookie)
	searchRecorder := httptest.NewRecorder()
	handler.ServeHTTP(searchRecorder, searchRequest)
	if searchRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected search status: %d body=%s", searchRecorder.Code, searchRecorder.Body.String())
	}
	searchPayload := struct {
		Items []struct {
			Kind   string `json:"kind"`
			Bucket string `json:"bucket"`
			Key    string `json:"key"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(searchRecorder.Body.Bytes(), &searchPayload); err != nil {
		t.Fatalf("unmarshal search payload: %v", err)
	}
	foundBucket := false
	foundObject := false
	for _, item := range searchPayload.Items {
		if item.Kind == "bucket" && item.Bucket == "readables" {
			foundBucket = true
		}
		if item.Kind == "object" && item.Bucket == "gallery" && item.Key == "notes/readme.txt" {
			foundObject = true
		}
	}
	if !foundBucket || !foundObject {
		t.Fatalf("expected bucket and object search hits, got %+v", searchPayload.Items)
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

	store := newStorageStoreForTest(t, cfg)
	handler := newAdminHandlerForTest(t, cfg, store, nil)

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

func TestUpdateSyncSettingsPersistsToDB(t *testing.T) {
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

	store := newStorageStoreForTest(t, cfg)
	handler := newAdminHandlerForTest(t, cfg, store, nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	cookie := loginCookie(t, handler)

	body, _ := json.Marshal(map[string]any{
		"enabled": true,
	})
	request := httptest.NewRequest(http.MethodPut, "/api/v1/settings/sync", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(cookie)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected sync settings status: %d body=%s", recorder.Code, recorder.Body.String())
	}

	stored, err := store.SyncSettings(context.Background())
	if err != nil {
		t.Fatalf("SyncSettings failed: %v", err)
	}
	if !stored.Enabled {
		t.Fatalf("unexpected stored sync config: %+v", stored)
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/api/v1/settings/sync", nil)
	getRequest.AddCookie(cookie)
	getRecorder := httptest.NewRecorder()
	handler.ServeHTTP(getRecorder, getRequest)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected get sync settings status: %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
}

func TestReplicationTokenCreateListRemoteAndDeleteFlow(t *testing.T) {
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

	store := newStorageStoreForTest(t, cfg)
	handler := newAdminHandlerForTest(t, cfg, store, nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	cookie := loginCookie(t, handler)

	tokenBody, _ := json.Marshal(map[string]string{"label": "Peer A"})
	tokenRequest := httptest.NewRequest(http.MethodPost, "/api/v1/replication/tokens", bytes.NewReader(tokenBody))
	tokenRequest.Header.Set("Content-Type", "application/json")
	tokenRequest.AddCookie(cookie)
	tokenRecorder := httptest.NewRecorder()
	handler.ServeHTTP(tokenRecorder, tokenRequest)
	if tokenRecorder.Code != http.StatusCreated {
		t.Fatalf("unexpected token create status: %d body=%s", tokenRecorder.Code, tokenRecorder.Body.String())
	}
	token := struct {
		ID    string `json:"id"`
		Token string `json:"token"`
	}{}
	if err := json.Unmarshal(tokenRecorder.Body.Bytes(), &token); err != nil {
		t.Fatalf("unmarshal token failed: %v", err)
	}
	if token.ID == "" || token.Token == "" {
		t.Fatalf("expected token id and value, got %+v", token)
	}

	tokensRequest := httptest.NewRequest(http.MethodGet, "/api/v1/replication/tokens", nil)
	tokensRequest.AddCookie(cookie)
	tokensRecorder := httptest.NewRecorder()
	handler.ServeHTTP(tokensRecorder, tokensRequest)
	if tokensRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list token status: %d body=%s", tokensRecorder.Code, tokensRecorder.Body.String())
	}

	remoteBody, _ := json.Marshal(map[string]string{
		"display_name":   "Loopback source",
		"endpoint":       server.URL,
		"token":          token.Token,
		"bootstrap_mode": "full",
	})
	remoteRequest := httptest.NewRequest(http.MethodPost, "/api/v1/replication/remotes", bytes.NewReader(remoteBody))
	remoteRequest.Header.Set("Content-Type", "application/json")
	remoteRequest.AddCookie(cookie)
	remoteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(remoteRecorder, remoteRequest)
	if remoteRecorder.Code != http.StatusCreated {
		t.Fatalf("unexpected remote create status: %d body=%s", remoteRecorder.Code, remoteRecorder.Body.String())
	}
	remote := struct {
		ID            string `json:"id"`
		DisplayName   string `json:"display_name"`
		BootstrapMode string `json:"bootstrap_mode"`
	}{}
	if err := json.Unmarshal(remoteRecorder.Body.Bytes(), &remote); err != nil {
		t.Fatalf("unmarshal remote failed: %v", err)
	}
	if remote.ID == "" || remote.DisplayName != "Loopback source" || remote.BootstrapMode != "full" {
		t.Fatalf("unexpected remote payload: %+v", remote)
	}

	remotesRequest := httptest.NewRequest(http.MethodGet, "/api/v1/replication/remotes", nil)
	remotesRequest.AddCookie(cookie)
	remotesRecorder := httptest.NewRecorder()
	handler.ServeHTTP(remotesRecorder, remotesRequest)
	if remotesRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list remotes status: %d body=%s", remotesRecorder.Code, remotesRecorder.Body.String())
	}

	deleteRemoteRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/remotes/"+remote.ID, nil)
	deleteRemoteRequest.AddCookie(cookie)
	deleteRemoteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteRemoteRecorder, deleteRemoteRequest)
	if deleteRemoteRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected delete remote status: %d body=%s", deleteRemoteRecorder.Code, deleteRemoteRecorder.Body.String())
	}

	revokeTokenRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/tokens/"+token.ID, nil)
	revokeTokenRequest.AddCookie(cookie)
	revokeTokenRecorder := httptest.NewRecorder()
	handler.ServeHTTP(revokeTokenRecorder, revokeTokenRequest)
	if revokeTokenRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected revoke token status: %d body=%s", revokeTokenRecorder.Code, revokeTokenRecorder.Body.String())
	}

	deleteTokenRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/replication/tokens/"+token.ID+"/remove", nil)
	deleteTokenRequest.AddCookie(cookie)
	deleteTokenRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteTokenRecorder, deleteTokenRequest)
	if deleteTokenRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected delete token status: %d body=%s", deleteTokenRecorder.Code, deleteTokenRecorder.Body.String())
	}
}

func TestAddRemoteFromNowStartsAtCurrentCursor(t *testing.T) {
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

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: "notes/readme.txt", Body: bytes.NewBufferString("hello")}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	handler := newAdminHandlerForTest(t, cfg, store, nil)
	server := httptest.NewServer(handler)
	defer server.Close()
	cookie := loginCookie(t, handler)

	tokenBody, _ := json.Marshal(map[string]string{"label": "Peer B"})
	tokenRequest := httptest.NewRequest(http.MethodPost, "/api/v1/replication/tokens", bytes.NewReader(tokenBody))
	tokenRequest.Header.Set("Content-Type", "application/json")
	tokenRequest.AddCookie(cookie)
	tokenRecorder := httptest.NewRecorder()
	handler.ServeHTTP(tokenRecorder, tokenRequest)
	token := struct {
		Token string `json:"token"`
	}{}
	if err := json.Unmarshal(tokenRecorder.Body.Bytes(), &token); err != nil {
		t.Fatalf("unmarshal token failed: %v", err)
	}

	remoteBody, _ := json.Marshal(map[string]string{
		"display_name":   "Loopback source",
		"endpoint":       server.URL,
		"token":          token.Token,
		"bootstrap_mode": "from_now",
	})
	remoteRequest := httptest.NewRequest(http.MethodPost, "/api/v1/replication/remotes", bytes.NewReader(remoteBody))
	remoteRequest.Header.Set("Content-Type", "application/json")
	remoteRequest.AddCookie(cookie)
	remoteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(remoteRecorder, remoteRequest)
	if remoteRecorder.Code != http.StatusCreated {
		t.Fatalf("unexpected remote create status: %d body=%s", remoteRecorder.Code, remoteRecorder.Body.String())
	}
	remote := struct {
		Cursor int64 `json:"cursor"`
	}{}
	if err := json.Unmarshal(remoteRecorder.Body.Bytes(), &remote); err != nil {
		t.Fatalf("unmarshal remote failed: %v", err)
	}
	if remote.Cursor == 0 {
		t.Fatalf("expected from_now remote to start at current cursor")
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

	store := newStorageStoreForTest(t, cfg)
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

	handler := newAdminHandlerForTest(t, cfg, store, nil)
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

	store := newStorageStoreForTest(t, cfg)
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

	handler := newAdminHandlerForTest(t, cfg, store, nil)
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

	store := newStorageStoreForTest(t, cfg)
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

	handler := newAdminHandlerForTest(t, cfg, store, nil)
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

	store := newStorageStoreForTest(t, cfg)
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

	handler := newAdminHandlerForTest(t, cfg, store, nil)
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

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	for _, key := range []string{"folder/a.txt", "folder/deep/b.txt"} {
		if _, err := store.PutObject(ctx, storage.PutObjectInput{Bucket: "gallery", Key: key, Body: bytes.NewBufferString(key)}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	handler := newAdminHandlerForTest(t, cfg, store, nil)
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

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	links := newShareLinksForTest(t, cfg.Paths.DataDir)
	if _, err := links.Create(ctx, sharelink.CreateInput{
		Bucket:  "gallery",
		Key:     "stale/orphan.txt",
		Expires: time.Hour,
	}); err != nil {
		t.Fatalf("Create stale share link failed: %v", err)
	}

	handler := newAdminHandlerForTest(t, cfg, store, nil)
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

func TestUpdateBucketRenamesMetadataAndHistory(t *testing.T) {
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

	store := newStorageStoreForTest(t, cfg)
	ctx := context.Background()
	if _, err := store.CreateBucket(ctx, "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(ctx, storage.PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/readme.txt",
		Body:   bytes.NewBufferString("hello"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	links := newShareLinksForTest(t, cfg.Paths.DataDir)
	if _, err := links.Create(ctx, sharelink.CreateInput{Bucket: "gallery", Key: "notes/readme.txt", Expires: time.Hour}); err != nil {
		t.Fatalf("Create share link failed: %v", err)
	}

	handler := newAdminHandlerForTest(t, cfg, store, nil)
	cookie := loginCookie(t, handler)

	updateBody, _ := json.Marshal(map[string]any{
		"name":        "archive",
		"access_mode": "public",
		"quota_bytes": 1024,
		"tags":        []string{"media", "launch"},
		"note":        "Launch assets",
	})
	updateRequest := httptest.NewRequest(http.MethodPut, "/api/v1/buckets/gallery", bytes.NewReader(updateBody))
	updateRequest.Header.Set("Content-Type", "application/json")
	updateRequest.AddCookie(cookie)
	updateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(updateRecorder, updateRequest)
	if updateRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected update bucket status: %d body=%s", updateRecorder.Code, updateRecorder.Body.String())
	}
	updated := struct {
		Name       string   `json:"name"`
		AccessMode string   `json:"access_mode"`
		QuotaBytes int64    `json:"quota_bytes"`
		Tags       []string `json:"tags"`
		Note       string   `json:"note"`
	}{}
	if err := json.Unmarshal(updateRecorder.Body.Bytes(), &updated); err != nil {
		t.Fatalf("unmarshal update bucket payload failed: %v", err)
	}
	if updated.Name != "archive" || updated.AccessMode != storage.BucketAccessPublic || updated.QuotaBytes != 1024 || updated.Note != "Launch assets" {
		t.Fatalf("unexpected updated bucket payload: %+v", updated)
	}
	if len(updated.Tags) != 2 || updated.Tags[0] != "media" || updated.Tags[1] != "launch" {
		t.Fatalf("unexpected updated tags: %+v", updated.Tags)
	}

	historyRequest := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/archive/history?limit=10", nil)
	historyRequest.AddCookie(cookie)
	historyRecorder := httptest.NewRecorder()
	handler.ServeHTTP(historyRecorder, historyRequest)
	if historyRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected bucket history status: %d body=%s", historyRecorder.Code, historyRecorder.Body.String())
	}
	historyPayload := struct {
		Items []struct {
			UsedBytes   int64 `json:"used_bytes"`
			ObjectCount int   `json:"object_count"`
			QuotaBytes  int64 `json:"quota_bytes"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(historyRecorder.Body.Bytes(), &historyPayload); err != nil {
		t.Fatalf("unmarshal history payload failed: %v", err)
	}
	if len(historyPayload.Items) < 2 {
		t.Fatalf("expected usage history entries, got %+v", historyPayload.Items)
	}
	lastHistory := historyPayload.Items[len(historyPayload.Items)-1]
	if lastHistory.UsedBytes != int64(len("hello")) || lastHistory.ObjectCount != 1 || lastHistory.QuotaBytes != 1024 {
		t.Fatalf("unexpected latest history sample: %+v", lastHistory)
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/share-links", nil)
	listRequest.AddCookie(cookie)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected share links status after bucket rename: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	listed := struct {
		Items []struct {
			Bucket string `json:"bucket"`
			Key    string `json:"key"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listed); err != nil {
		t.Fatalf("unmarshal share links after bucket rename failed: %v", err)
	}
	if len(listed.Items) != 1 || listed.Items[0].Bucket != "archive" || listed.Items[0].Key != "notes/readme.txt" {
		t.Fatalf("expected share link bucket rename to propagate, got %+v", listed.Items)
	}
}

func TestBucketAccessPolicyCRUD(t *testing.T) {
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

	store := newStorageStoreForTest(t, cfg)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	handler := newAdminHandlerForTest(t, cfg, store, nil)
	cookie := loginCookie(t, handler)

	updateBody, _ := json.Marshal(map[string]any{
		"mode": "custom",
		"policy": map[string]any{
			"default_action": "authenticated",
			"rules": []map[string]any{
				{"prefix": "images/", "action": "public", "note": "Public media"},
				{"prefix": "secret/", "action": "deny", "note": "Blocked"},
			},
		},
	})
	updateRequest := httptest.NewRequest(http.MethodPut, "/api/v1/buckets/gallery/access", bytes.NewReader(updateBody))
	updateRequest.Header.Set("Content-Type", "application/json")
	updateRequest.AddCookie(cookie)
	updateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(updateRecorder, updateRequest)
	if updateRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected update access status: %d body=%s", updateRecorder.Code, updateRecorder.Body.String())
	}

	readRequest := httptest.NewRequest(http.MethodGet, "/api/v1/buckets/gallery/access", nil)
	readRequest.AddCookie(cookie)
	readRecorder := httptest.NewRecorder()
	handler.ServeHTTP(readRecorder, readRequest)
	if readRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected read access status: %d body=%s", readRecorder.Code, readRecorder.Body.String())
	}

	configPayload := struct {
		Mode   string `json:"mode"`
		Policy struct {
			DefaultAction string `json:"default_action"`
			Rules         []struct {
				Prefix string `json:"prefix"`
				Action string `json:"action"`
				Note   string `json:"note"`
			} `json:"rules"`
		} `json:"policy"`
	}{}
	if err := json.Unmarshal(readRecorder.Body.Bytes(), &configPayload); err != nil {
		t.Fatalf("unmarshal access payload failed: %v", err)
	}
	if configPayload.Mode != storage.BucketAccessCustom || configPayload.Policy.DefaultAction != storage.BucketAccessActionAuthenticated {
		t.Fatalf("unexpected access config payload: %+v", configPayload)
	}
	if len(configPayload.Policy.Rules) != 2 || configPayload.Policy.Rules[0].Prefix != "images/" || configPayload.Policy.Rules[1].Action != storage.BucketAccessActionDeny {
		t.Fatalf("unexpected access rules payload: %+v", configPayload.Policy.Rules)
	}
}

func TestS3CredentialCRUD(t *testing.T) {
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
	cfg.Auth.S3.AccessKeyID = ""
	cfg.Auth.S3.SecretAccessKey = ""

	store := newStorageStoreForTest(t, cfg)
	handler := newAdminHandlerForTest(t, cfg, store, nil)
	cookie := loginCookie(t, handler)

	createBody, _ := json.Marshal(map[string]any{"label": "CI automation", "permission": "read_only", "buckets": []string{"gallery", "archive"}})
	createRequest := httptest.NewRequest(http.MethodPost, "/api/v1/settings/s3/credentials", bytes.NewReader(createBody))
	createRequest.Header.Set("Content-Type", "application/json")
	createRequest.AddCookie(cookie)
	createRecorder := httptest.NewRecorder()
	handler.ServeHTTP(createRecorder, createRequest)
	if createRecorder.Code != http.StatusCreated {
		t.Fatalf("unexpected create credential status: %d body=%s", createRecorder.Code, createRecorder.Body.String())
	}
	created := struct {
		AccessKeyID     string   `json:"access_key_id"`
		SecretAccessKey string   `json:"secret_access_key"`
		Label           string   `json:"label"`
		Permission      string   `json:"permission"`
		Buckets         []string `json:"buckets"`
		Status          string   `json:"status"`
	}{}
	if err := json.Unmarshal(createRecorder.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created credential failed: %v", err)
	}
	if created.AccessKeyID == "" || created.SecretAccessKey == "" || created.Label != "CI automation" || created.Permission != s3creds.PermissionReadOnly || len(created.Buckets) != 2 || created.Status != "active" {
		t.Fatalf("unexpected created credential payload: %+v", created)
	}

	updateBody, _ := json.Marshal(map[string]any{"label": "Updated automation", "permission": "read_write", "buckets": []string{"gallery"}})
	updateRequest := httptest.NewRequest(http.MethodPut, "/api/v1/settings/s3/credentials/"+created.AccessKeyID, bytes.NewReader(updateBody))
	updateRequest.Header.Set("Content-Type", "application/json")
	updateRequest.AddCookie(cookie)
	updateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(updateRecorder, updateRequest)
	if updateRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected update credential status: %d body=%s", updateRecorder.Code, updateRecorder.Body.String())
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/api/v1/settings/s3/credentials", nil)
	listRequest.AddCookie(cookie)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list credential status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	listPayload := struct {
		Items []struct {
			AccessKeyID string `json:"access_key_id"`
			Permission  string `json:"permission"`
			LastUsedAt  string `json:"last_used_at"`
			Status      string `json:"status"`
		} `json:"items"`
	}{}
	if err := json.Unmarshal(listRecorder.Body.Bytes(), &listPayload); err != nil {
		t.Fatalf("unmarshal listed credentials failed: %v", err)
	}
	if len(listPayload.Items) != 1 || listPayload.Items[0].AccessKeyID != created.AccessKeyID || listPayload.Items[0].Permission != s3creds.PermissionReadWrite || listPayload.Items[0].Status != "active" {
		t.Fatalf("unexpected listed credential payload: %+v", listPayload.Items)
	}

	revokeRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/settings/s3/credentials/"+created.AccessKeyID, nil)
	revokeRequest.AddCookie(cookie)
	revokeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(revokeRecorder, revokeRequest)
	if revokeRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected revoke credential status: %d body=%s", revokeRecorder.Code, revokeRecorder.Body.String())
	}

	removeRequest := httptest.NewRequest(http.MethodDelete, "/api/v1/settings/s3/credentials/"+created.AccessKeyID+"/remove", nil)
	removeRequest.AddCookie(cookie)
	removeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(removeRecorder, removeRequest)
	if removeRecorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected remove credential status: %d body=%s", removeRecorder.Code, removeRecorder.Body.String())
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

func newCredentialsForTest(t *testing.T, cfg config.Config) *s3creds.Store {
	t.Helper()
	creds, err := s3creds.New(cfg.Paths.DataDir, s3creds.BootstrapCredential{
		AccessKeyID:     cfg.Auth.S3.AccessKeyID,
		SecretAccessKey: cfg.Auth.S3.SecretAccessKey,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("s3creds.New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = creds.Close()
	})
	return creds
}

func newAdminHandlerForTest(t *testing.T, cfg config.Config, store *storage.Store, credentials *s3creds.Store) http.Handler {
	t.Helper()
	return newHandler(cfg, store, newShareLinksForTest(t, cfg.Paths.DataDir), credentialsOrDefault(t, cfg, credentials), zap.NewNop())
}

func credentialsOrDefault(t *testing.T, cfg config.Config, credentials *s3creds.Store) *s3creds.Store {
	t.Helper()
	if credentials != nil {
		return credentials
	}
	return newCredentialsForTest(t, cfg)
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
