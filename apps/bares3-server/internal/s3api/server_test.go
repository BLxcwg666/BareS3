package s3api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/s3creds"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/sigv4"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
)

func TestBucketLifecycleAndList(t *testing.T) {
	t.Parallel()

	_, handler := newTestHandler(t)

	request := httptest.NewRequest(http.MethodPut, "/gallery", nil)
	signHeaderRequest(t, request, config.Default(), nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected create bucket status: %d body=%s", recorder.Code, recorder.Body.String())
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	signHeaderRequest(t, listRequest, config.Default(), nil)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list buckets status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}

	result := listBucketsResult{}
	if err := xml.Unmarshal(listRecorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal list buckets xml: %v", err)
	}
	if len(result.Buckets.Items) != 1 || result.Buckets.Items[0].Name != "gallery" {
		t.Fatalf("unexpected buckets payload: %+v", result.Buckets.Items)
	}
}

func TestPutGetHeadAndListObjectsV2(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	putRequest := httptest.NewRequest(http.MethodPut, "/gallery/2026/launch/mock-02.txt", bytes.NewBufferString("hello s3"))
	putRequest.Header.Set("Content-Type", "text/plain")
	putRequest.Header.Set("Cache-Control", "public, max-age=60")
	putRequest.Header.Set("X-Amz-Meta-Origin", "test")
	signHeaderRequest(t, putRequest, config.Default(), []byte("hello s3"))
	putRecorder := httptest.NewRecorder()
	handler.ServeHTTP(putRecorder, putRequest)
	if putRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected put object status: %d body=%s", putRecorder.Code, putRecorder.Body.String())
	}
	if etag := putRecorder.Header().Get("ETag"); etag == "" {
		t.Fatalf("expected etag header")
	}

	headRequest := httptest.NewRequest(http.MethodHead, "/gallery/2026/launch/mock-02.txt", nil)
	signHeaderRequest(t, headRequest, config.Default(), nil)
	headRecorder := httptest.NewRecorder()
	handler.ServeHTTP(headRecorder, headRequest)
	if headRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected head object status: %d", headRecorder.Code)
	}
	if got := headRecorder.Header().Get("X-Amz-Meta-origin"); got != "test" {
		t.Fatalf("unexpected user metadata header: %q", got)
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/gallery/2026/launch/mock-02.txt", nil)
	signHeaderRequest(t, getRequest, config.Default(), nil)
	getRecorder := httptest.NewRecorder()
	handler.ServeHTTP(getRecorder, getRequest)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected get object status: %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	if body := strings.TrimSpace(getRecorder.Body.String()); body != "hello s3" {
		t.Fatalf("unexpected object body: %q", body)
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/gallery?list-type=2&prefix=2026/launch/", nil)
	signHeaderRequest(t, listRequest, config.Default(), nil)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list objects status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}

	result := listObjectsV2Result{}
	if err := xml.Unmarshal(listRecorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal list objects xml: %v", err)
	}
	if len(result.Contents) != 1 || result.Contents[0].Key != "2026/launch/mock-02.txt" {
		t.Fatalf("unexpected list objects payload: %+v", result.Contents)
	}
}

func TestObjectConditionalRequests(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	object, err := store.PutObject(context.Background(), storage.PutObjectInput{
		Bucket:      "gallery",
		Key:         "notes/conditional.txt",
		Body:        bytes.NewBufferString("hello conditional"),
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	notModifiedRequest := httptest.NewRequest(http.MethodGet, "/gallery/notes/conditional.txt", nil)
	notModifiedRequest.Header.Set("If-None-Match", `"`+object.ETag+`"`)
	signHeaderRequest(t, notModifiedRequest, config.Default(), nil)
	notModifiedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(notModifiedRecorder, notModifiedRequest)
	if notModifiedRecorder.Code != http.StatusNotModified {
		t.Fatalf("unexpected if-none-match status: %d body=%s", notModifiedRecorder.Code, notModifiedRecorder.Body.String())
	}

	ifMatchFailedRequest := httptest.NewRequest(http.MethodGet, "/gallery/notes/conditional.txt", nil)
	ifMatchFailedRequest.Header.Set("If-Match", `"different"`)
	signHeaderRequest(t, ifMatchFailedRequest, config.Default(), nil)
	ifMatchFailedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(ifMatchFailedRecorder, ifMatchFailedRequest)
	if ifMatchFailedRecorder.Code != http.StatusPreconditionFailed {
		t.Fatalf("unexpected if-match failure status: %d body=%s", ifMatchFailedRecorder.Code, ifMatchFailedRecorder.Body.String())
	}

	modifiedSinceRequest := httptest.NewRequest(http.MethodGet, "/gallery/notes/conditional.txt", nil)
	modifiedSinceRequest.Header.Set("If-Modified-Since", object.LastModified.Add(time.Hour).UTC().Format(http.TimeFormat))
	signHeaderRequest(t, modifiedSinceRequest, config.Default(), nil)
	modifiedSinceRecorder := httptest.NewRecorder()
	handler.ServeHTTP(modifiedSinceRecorder, modifiedSinceRequest)
	if modifiedSinceRecorder.Code != http.StatusNotModified {
		t.Fatalf("unexpected if-modified-since status: %d body=%s", modifiedSinceRecorder.Code, modifiedSinceRecorder.Body.String())
	}

	unmodifiedSinceRequest := httptest.NewRequest(http.MethodHead, "/gallery/notes/conditional.txt", nil)
	unmodifiedSinceRequest.Header.Set("If-Unmodified-Since", object.LastModified.Add(-time.Hour).UTC().Format(http.TimeFormat))
	signHeaderRequest(t, unmodifiedSinceRequest, config.Default(), nil)
	unmodifiedSinceRecorder := httptest.NewRecorder()
	handler.ServeHTTP(unmodifiedSinceRecorder, unmodifiedSinceRequest)
	if unmodifiedSinceRecorder.Code != http.StatusPreconditionFailed {
		t.Fatalf("unexpected if-unmodified-since status: %d", unmodifiedSinceRecorder.Code)
	}
}

func TestListObjectsV1WithoutListTypeParameter(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: "2026/launch/mock-02.txt", Body: bytes.NewBufferString("hello s3")}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/gallery?prefix=2026/launch/", nil)
	signHeaderRequest(t, listRequest, config.Default(), nil)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list objects v1 status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}

	result := listObjectsResult{}
	if err := xml.Unmarshal(listRecorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal list objects v1 xml: %v", err)
	}
	if result.Name != "gallery" || result.Prefix != "2026/launch/" {
		t.Fatalf("unexpected list objects v1 metadata: %+v", result)
	}
	if len(result.Contents) != 1 || result.Contents[0].Key != "2026/launch/mock-02.txt" {
		t.Fatalf("unexpected list objects v1 payload: %+v", result.Contents)
	}
}

func TestUnsignedRequestIsRejected(t *testing.T) {
	t.Parallel()

	_, handler := newTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	assertS3Error(t, recorder, http.StatusForbidden, "AccessDenied", config.Default().Storage.Region, "")
}

func TestManagedS3CredentialWorksWithoutConfigKey(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Auth.S3.AccessKeyID = ""
	cfg.Auth.S3.SecretAccessKey = ""

	store := storage.New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	creds, err := s3creds.New(cfg.Paths.DataDir, s3creds.BootstrapCredential{}, zap.NewNop())
	if err != nil {
		t.Fatalf("s3creds.New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = creds.Close()
	})
	created, err := creds.Create(context.Background(), s3creds.CreateInput{Label: "CI key"})
	if err != nil {
		t.Fatalf("Create credential failed: %v", err)
	}
	handler := newHandler(cfg, store, newShareLinksForTest(t, cfg.Paths.DataDir), creds, zap.NewNop())

	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	putBody := []byte("hello managed key")
	putRequest := httptest.NewRequest(http.MethodPut, "/gallery/managed.txt", bytes.NewReader(putBody))
	signedCfg := cfg
	signedCfg.Auth.S3.AccessKeyID = created.AccessKeyID
	signedCfg.Auth.S3.SecretAccessKey = created.SecretAccessKey
	signHeaderRequest(t, putRequest, signedCfg, putBody)
	putRecorder := httptest.NewRecorder()
	handler.ServeHTTP(putRecorder, putRequest)
	if putRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected managed put status: %d body=%s", putRecorder.Code, putRecorder.Body.String())
	}
	active, err := creds.GetActive(context.Background(), created.AccessKeyID)
	if err != nil {
		t.Fatalf("GetActive failed: %v", err)
	}
	if active.LastUsedAt == nil {
		t.Fatalf("expected managed credential last_used_at to be recorded")
	}
}

func TestScopedReadOnlyCredentialRestrictsBucketsAndWrites(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Auth.S3.AccessKeyID = ""
	cfg.Auth.S3.SecretAccessKey = ""

	store := storage.New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket gallery failed: %v", err)
	}
	if _, err := store.CreateBucket(context.Background(), "archive", 0); err != nil {
		t.Fatalf("CreateBucket archive failed: %v", err)
	}
	if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: "notes/readme.txt", Body: bytes.NewBufferString("hello")}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	creds, err := s3creds.New(cfg.Paths.DataDir, s3creds.BootstrapCredential{}, zap.NewNop())
	if err != nil {
		t.Fatalf("s3creds.New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = creds.Close()
	})
	created, err := creds.Create(context.Background(), s3creds.CreateInput{Label: "reader", Permission: s3creds.PermissionReadOnly, Buckets: []string{"gallery"}})
	if err != nil {
		t.Fatalf("Create credential failed: %v", err)
	}
	handler := newHandler(cfg, store, newShareLinksForTest(t, cfg.Paths.DataDir), creds, zap.NewNop())
	signedCfg := cfg
	signedCfg.Auth.S3.AccessKeyID = created.AccessKeyID
	signedCfg.Auth.S3.SecretAccessKey = created.SecretAccessKey

	allowedRequest := httptest.NewRequest(http.MethodGet, "/gallery/notes/readme.txt", nil)
	signHeaderRequest(t, allowedRequest, signedCfg, nil)
	allowedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(allowedRecorder, allowedRequest)
	if allowedRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected allowed scoped read status: %d body=%s", allowedRecorder.Code, allowedRecorder.Body.String())
	}

	blockedBucketRequest := httptest.NewRequest(http.MethodGet, "/archive?list-type=2", nil)
	signHeaderRequest(t, blockedBucketRequest, signedCfg, nil)
	blockedBucketRecorder := httptest.NewRecorder()
	handler.ServeHTTP(blockedBucketRecorder, blockedBucketRequest)
	assertS3Error(t, blockedBucketRecorder, http.StatusForbidden, "AccessDenied", cfg.Storage.Region, "archive")

	writeBody := []byte("nope")
	blockedWriteRequest := httptest.NewRequest(http.MethodPut, "/gallery/new.txt", bytes.NewReader(writeBody))
	signHeaderRequest(t, blockedWriteRequest, signedCfg, writeBody)
	blockedWriteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(blockedWriteRecorder, blockedWriteRequest)
	assertS3Error(t, blockedWriteRecorder, http.StatusForbidden, "AccessDenied", cfg.Storage.Region, "gallery")
}

func TestPresignedGetObject(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(context.Background(), storage.PutObjectInput{
		Bucket:       "gallery",
		Key:          "notes/presigned.txt",
		Body:         bytes.NewBufferString("presigned data"),
		ContentType:  "text/plain",
		CacheControl: "public, max-age=30",
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	request := httptest.NewRequest(http.MethodGet, "/gallery/notes/presigned.txt", nil)
	signPresignedRequest(t, request, config.Default(), 5*time.Minute)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected status for presigned get: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if body := strings.TrimSpace(recorder.Body.String()); body != "presigned data" {
		t.Fatalf("unexpected body for presigned get: %q", body)
	}
}

func TestCustomBucketAccessCanDenySignedReads(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	for _, fixture := range []struct {
		key  string
		body string
	}{
		{key: "notes/readme.txt", body: "allowed"},
		{key: "secret/plan.txt", body: "denied"},
	} {
		if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: fixture.key, Body: bytes.NewBufferString(fixture.body)}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", fixture.key, err)
		}
	}
	if _, err := store.UpdateBucketAccess(context.Background(), storage.UpdateBucketAccessInput{
		Name: "gallery",
		Mode: storage.BucketAccessCustom,
		Policy: storage.BucketAccessPolicy{
			DefaultAction: storage.BucketAccessActionAuthenticated,
			Rules: []storage.BucketAccessRule{
				{Prefix: "secret/", Action: storage.BucketAccessActionDeny},
			},
		},
	}); err != nil {
		t.Fatalf("UpdateBucketAccess failed: %v", err)
	}

	allowedRequest := httptest.NewRequest(http.MethodGet, "/gallery/notes/readme.txt", nil)
	signHeaderRequest(t, allowedRequest, config.Default(), nil)
	allowedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(allowedRecorder, allowedRequest)
	if allowedRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected allowed status: %d body=%s", allowedRecorder.Code, allowedRecorder.Body.String())
	}

	deniedRequest := httptest.NewRequest(http.MethodGet, "/gallery/secret/plan.txt", nil)
	signHeaderRequest(t, deniedRequest, config.Default(), nil)
	deniedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deniedRecorder, deniedRequest)
	assertS3Error(t, deniedRecorder, http.StatusForbidden, "AccessDenied", config.Default().Storage.Region, "gallery")
}

func TestDeleteObjectAndBucket(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	if _, err := store.PutObject(context.Background(), storage.PutObjectInput{
		Bucket: "gallery",
		Key:    "notes/delete-me.txt",
		Body:   bytes.NewBufferString("delete me"),
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	deleteObject := httptest.NewRequest(http.MethodDelete, "/gallery/notes/delete-me.txt", nil)
	signHeaderRequest(t, deleteObject, config.Default(), nil)
	deleteObjectRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteObjectRecorder, deleteObject)
	if deleteObjectRecorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected delete object status: %d body=%s", deleteObjectRecorder.Code, deleteObjectRecorder.Body.String())
	}

	deleteBucket := httptest.NewRequest(http.MethodDelete, "/gallery", nil)
	signHeaderRequest(t, deleteBucket, config.Default(), nil)
	deleteBucketRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteBucketRecorder, deleteBucket)
	if deleteBucketRecorder.Code != http.StatusNoContent {
		t.Fatalf("unexpected delete bucket status: %d body=%s", deleteBucketRecorder.Code, deleteBucketRecorder.Body.String())
	}
}

func TestGetBucketLocationAndDeleteObjects(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	for _, fixture := range []struct {
		key  string
		body string
	}{
		{key: "notes/a.txt", body: "a"},
		{key: "notes/b.txt", body: "b"},
	} {
		if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: fixture.key, Body: bytes.NewBufferString(fixture.body)}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", fixture.key, err)
		}
	}

	locationRequest := httptest.NewRequest(http.MethodGet, "/gallery?location", nil)
	signHeaderRequest(t, locationRequest, config.Default(), nil)
	locationRecorder := httptest.NewRecorder()
	handler.ServeHTTP(locationRecorder, locationRequest)
	if locationRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected bucket location status: %d body=%s", locationRecorder.Code, locationRecorder.Body.String())
	}
	locationResult := bucketLocationConstraint{}
	if err := xml.Unmarshal(locationRecorder.Body.Bytes(), &locationResult); err != nil {
		t.Fatalf("unmarshal location result: %v", err)
	}
	if locationResult.Value != config.Default().Storage.Region {
		t.Fatalf("unexpected location constraint: %q", locationResult.Value)
	}

	deleteBody := []byte(`<Delete><Object><Key>notes/a.txt</Key></Object><Object><Key>notes/missing.txt</Key></Object></Delete>`)
	deleteRequest := httptest.NewRequest(http.MethodPost, "/gallery?delete", bytes.NewReader(deleteBody))
	deleteRequest.Header.Set("Content-Type", "application/xml")
	signHeaderRequest(t, deleteRequest, config.Default(), deleteBody)
	deleteRecorder := httptest.NewRecorder()
	handler.ServeHTTP(deleteRecorder, deleteRequest)
	if deleteRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected delete objects status: %d body=%s", deleteRecorder.Code, deleteRecorder.Body.String())
	}
	deleteResult := deleteObjectsResult{}
	if err := xml.Unmarshal(deleteRecorder.Body.Bytes(), &deleteResult); err != nil {
		t.Fatalf("unmarshal delete objects result: %v", err)
	}
	deletedKeys := make([]string, 0, len(deleteResult.Deleted))
	for _, item := range deleteResult.Deleted {
		deletedKeys = append(deletedKeys, item.Key)
	}
	sort.Strings(deletedKeys)
	if len(deleteResult.Errors) != 0 {
		t.Fatalf("unexpected delete errors: %+v", deleteResult.Errors)
	}
	if len(deletedKeys) != 2 || deletedKeys[0] != "notes/a.txt" || deletedKeys[1] != "notes/missing.txt" {
		t.Fatalf("unexpected deleted keys: %+v", deletedKeys)
	}
	if _, err := store.StatObject(context.Background(), "gallery", "notes/a.txt"); err == nil {
		t.Fatalf("expected deleted object to be removed")
	}
	if _, err := store.StatObject(context.Background(), "gallery", "notes/b.txt"); err != nil {
		t.Fatalf("expected remaining object to stay available, got %v", err)
	}
	if got := deleteRecorder.Header().Get("X-Amz-Bucket-Region"); got != config.Default().Storage.Region {
		t.Fatalf("unexpected region header after delete objects: %q", got)
	}
}

func TestCopyObjectPreservesBodyAndSupportsMetadataReplace(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	for _, bucket := range []string{"gallery", "archive"} {
		if _, err := store.CreateBucket(context.Background(), bucket, 0); err != nil {
			t.Fatalf("CreateBucket(%s) failed: %v", bucket, err)
		}
	}
	if _, err := store.PutObject(context.Background(), storage.PutObjectInput{
		Bucket:             "gallery",
		Key:                "notes/source.txt",
		Body:               bytes.NewBufferString("copy me"),
		ContentType:        "text/plain",
		CacheControl:       "public, max-age=60",
		ContentDisposition: `inline; filename="source.txt"`,
		UserMetadata:       map[string]string{"origin": "source"},
	}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	copyRequest := httptest.NewRequest(http.MethodPut, "/archive/copied.txt", nil)
	copyRequest.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source.txt")
	signHeaderRequest(t, copyRequest, config.Default(), nil)
	copyRecorder := httptest.NewRecorder()
	handler.ServeHTTP(copyRecorder, copyRequest)
	if copyRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected copy object status: %d body=%s", copyRecorder.Code, copyRecorder.Body.String())
	}
	copyResult := copyObjectResult{}
	if err := xml.Unmarshal(copyRecorder.Body.Bytes(), &copyResult); err != nil {
		t.Fatalf("unmarshal copy object result: %v", err)
	}
	if strings.TrimSpace(copyResult.ETag) == "" || strings.TrimSpace(copyResult.LastModified) == "" {
		t.Fatalf("unexpected copy object result: %+v", copyResult)
	}

	getCopiedRequest := httptest.NewRequest(http.MethodGet, "/archive/copied.txt", nil)
	signHeaderRequest(t, getCopiedRequest, config.Default(), nil)
	getCopiedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(getCopiedRecorder, getCopiedRequest)
	if getCopiedRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected copied get status: %d body=%s", getCopiedRecorder.Code, getCopiedRecorder.Body.String())
	}
	if body := strings.TrimSpace(getCopiedRecorder.Body.String()); body != "copy me" {
		t.Fatalf("unexpected copied body: %q", body)
	}
	if got := getCopiedRecorder.Header().Get("Content-Type"); got != "text/plain" {
		t.Fatalf("unexpected copied content type: %q", got)
	}
	if got := getCopiedRecorder.Header().Get("Cache-Control"); got != "public, max-age=60" {
		t.Fatalf("unexpected copied cache control: %q", got)
	}
	if got := getCopiedRecorder.Header().Get("Content-Disposition"); got != `inline; filename="source.txt"` {
		t.Fatalf("unexpected copied content disposition: %q", got)
	}
	if got := getCopiedRecorder.Header().Get("X-Amz-Meta-origin"); got != "source" {
		t.Fatalf("unexpected copied metadata: %q", got)
	}

	replaceRequest := httptest.NewRequest(http.MethodPut, "/archive/replaced.txt", nil)
	replaceRequest.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source.txt")
	replaceRequest.Header.Set("X-Amz-Metadata-Directive", "REPLACE")
	replaceRequest.Header.Set("Content-Type", "application/json")
	replaceRequest.Header.Set("Cache-Control", "no-store")
	replaceRequest.Header.Set("Content-Disposition", `attachment; filename="replaced.txt"`)
	replaceRequest.Header.Set("X-Amz-Meta-origin", "replaced")
	replaceRequest.Header.Set("X-Amz-Meta-owner", "qa")
	signHeaderRequest(t, replaceRequest, config.Default(), nil)
	replaceRecorder := httptest.NewRecorder()
	handler.ServeHTTP(replaceRecorder, replaceRequest)
	if replaceRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected replace copy status: %d body=%s", replaceRecorder.Code, replaceRecorder.Body.String())
	}

	headReplacedRequest := httptest.NewRequest(http.MethodHead, "/archive/replaced.txt", nil)
	signHeaderRequest(t, headReplacedRequest, config.Default(), nil)
	headReplacedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(headReplacedRecorder, headReplacedRequest)
	if headReplacedRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected replaced head status: %d", headReplacedRecorder.Code)
	}
	if got := headReplacedRecorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("unexpected replaced content type: %q", got)
	}
	if got := headReplacedRecorder.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("unexpected replaced cache control: %q", got)
	}
	if got := headReplacedRecorder.Header().Get("Content-Disposition"); got != `attachment; filename="replaced.txt"` {
		t.Fatalf("unexpected replaced content disposition: %q", got)
	}
	if got := headReplacedRecorder.Header().Get("X-Amz-Meta-origin"); got != "replaced" {
		t.Fatalf("unexpected replaced origin metadata: %q", got)
	}
	if got := headReplacedRecorder.Header().Get("X-Amz-Meta-owner"); got != "qa" {
		t.Fatalf("unexpected replaced owner metadata: %q", got)
	}

	getReplacedRequest := httptest.NewRequest(http.MethodGet, "/archive/replaced.txt", nil)
	signHeaderRequest(t, getReplacedRequest, config.Default(), nil)
	getReplacedRecorder := httptest.NewRecorder()
	handler.ServeHTTP(getReplacedRecorder, getReplacedRequest)
	if getReplacedRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected replaced get status: %d body=%s", getReplacedRecorder.Code, getReplacedRecorder.Body.String())
	}
	if body := strings.TrimSpace(getReplacedRecorder.Body.String()); body != "copy me" {
		t.Fatalf("unexpected replaced body: %q", body)
	}
}

func TestCopyObjectRequiresSourceBucketAccess(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Auth.S3.AccessKeyID = ""
	cfg.Auth.S3.SecretAccessKey = ""

	store := storage.New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	for _, bucket := range []string{"gallery", "archive"} {
		if _, err := store.CreateBucket(context.Background(), bucket, 0); err != nil {
			t.Fatalf("CreateBucket(%s) failed: %v", bucket, err)
		}
	}
	if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: "notes/source.txt", Body: bytes.NewBufferString("copy me")}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	creds, err := s3creds.New(cfg.Paths.DataDir, s3creds.BootstrapCredential{}, zap.NewNop())
	if err != nil {
		t.Fatalf("s3creds.New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = creds.Close()
	})
	created, err := creds.Create(context.Background(), s3creds.CreateInput{Label: "archive-only", Permission: s3creds.PermissionReadWrite, Buckets: []string{"archive"}})
	if err != nil {
		t.Fatalf("Create credential failed: %v", err)
	}
	handler := newHandler(cfg, store, newShareLinksForTest(t, cfg.Paths.DataDir), creds, zap.NewNop())
	signedCfg := cfg
	signedCfg.Auth.S3.AccessKeyID = created.AccessKeyID
	signedCfg.Auth.S3.SecretAccessKey = created.SecretAccessKey

	request := httptest.NewRequest(http.MethodPut, "/archive/copied.txt", nil)
	request.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source.txt")
	signHeaderRequest(t, request, signedCfg, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	assertS3Error(t, recorder, http.StatusForbidden, "AccessDenied", cfg.Storage.Region, "archive")
	if _, err := store.StatObject(context.Background(), "archive", "copied.txt"); err == nil {
		t.Fatalf("expected denied copy to leave destination untouched")
	}
}

func TestCopyObjectSourceConditionalHeaders(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	for _, bucket := range []string{"gallery", "archive"} {
		if _, err := store.CreateBucket(context.Background(), bucket, 0); err != nil {
			t.Fatalf("CreateBucket(%s) failed: %v", bucket, err)
		}
	}
	sourceObject, err := store.PutObject(context.Background(), storage.PutObjectInput{
		Bucket:      "gallery",
		Key:         "notes/source-conditional.txt",
		Body:        bytes.NewBufferString("copy me conditionally"),
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	ifMatchOverridesRequest := httptest.NewRequest(http.MethodPut, "/archive/copied-if-match.txt", nil)
	ifMatchOverridesRequest.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source-conditional.txt")
	ifMatchOverridesRequest.Header.Set("X-Amz-Copy-Source-If-Match", `"`+sourceObject.ETag+`"`)
	ifMatchOverridesRequest.Header.Set("X-Amz-Copy-Source-If-Unmodified-Since", sourceObject.LastModified.Add(-time.Hour).UTC().Format(http.TimeFormat))
	signHeaderRequest(t, ifMatchOverridesRequest, config.Default(), nil)
	ifMatchOverridesRecorder := httptest.NewRecorder()
	handler.ServeHTTP(ifMatchOverridesRecorder, ifMatchOverridesRequest)
	if ifMatchOverridesRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected copy if-match override status: %d body=%s", ifMatchOverridesRecorder.Code, ifMatchOverridesRecorder.Body.String())
	}

	ifNoneMatchRequest := httptest.NewRequest(http.MethodPut, "/archive/copied-if-none-match.txt", nil)
	ifNoneMatchRequest.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source-conditional.txt")
	ifNoneMatchRequest.Header.Set("X-Amz-Copy-Source-If-None-Match", `"`+sourceObject.ETag+`"`)
	signHeaderRequest(t, ifNoneMatchRequest, config.Default(), nil)
	ifNoneMatchRecorder := httptest.NewRecorder()
	handler.ServeHTTP(ifNoneMatchRecorder, ifNoneMatchRequest)
	assertS3Error(t, ifNoneMatchRecorder, http.StatusPreconditionFailed, "PreconditionFailed", config.Default().Storage.Region, "archive")

	ifModifiedSinceRequest := httptest.NewRequest(http.MethodPut, "/archive/copied-if-modified-since.txt", nil)
	ifModifiedSinceRequest.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source-conditional.txt")
	ifModifiedSinceRequest.Header.Set("X-Amz-Copy-Source-If-Modified-Since", sourceObject.LastModified.Add(time.Hour).UTC().Format(http.TimeFormat))
	signHeaderRequest(t, ifModifiedSinceRequest, config.Default(), nil)
	ifModifiedSinceRecorder := httptest.NewRecorder()
	handler.ServeHTTP(ifModifiedSinceRecorder, ifModifiedSinceRequest)
	assertS3Error(t, ifModifiedSinceRecorder, http.StatusPreconditionFailed, "PreconditionFailed", config.Default().Storage.Region, "archive")

	invalidTimeRequest := httptest.NewRequest(http.MethodPut, "/archive/copied-invalid-time.txt", nil)
	invalidTimeRequest.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source-conditional.txt")
	invalidTimeRequest.Header.Set("X-Amz-Copy-Source-If-Modified-Since", "not-a-time")
	signHeaderRequest(t, invalidTimeRequest, config.Default(), nil)
	invalidTimeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(invalidTimeRecorder, invalidTimeRequest)
	assertS3Error(t, invalidTimeRecorder, http.StatusBadRequest, "InvalidArgument", config.Default().Storage.Region, "archive")
}

func TestS3ReadinessEndpoint(t *testing.T) {
	t.Parallel()

	_, handler := newTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("unexpected readiness status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	payload := struct {
		Status string            `json:"status"`
		Checks map[string]string `json:"checks"`
	}{}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("unmarshal readiness payload: %v", err)
	}
	if payload.Status != "ok" {
		t.Fatalf("unexpected readiness payload: %+v", payload)
	}
	if payload.Checks["storage"] != "ok" || payload.Checks["share_links"] != "ok" || payload.Checks["s3_credentials"] != "ok" {
		t.Fatalf("unexpected readiness checks: %+v", payload.Checks)
	}
}

func TestListObjectsV2DelimiterAndContinuationToken(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	fixtures := []string{
		"photos/2024/a.jpg",
		"photos/2024/b.jpg",
		"photos/raw.txt",
		"videos/clip.mp4",
	}
	for _, key := range fixtures {
		if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: key, Body: bytes.NewBufferString(key)}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	firstRequest := httptest.NewRequest(http.MethodGet, "/gallery?list-type=2&prefix=photos/&delimiter=/&max-keys=1", nil)
	signHeaderRequest(t, firstRequest, config.Default(), nil)
	firstRecorder := httptest.NewRecorder()
	handler.ServeHTTP(firstRecorder, firstRequest)
	if firstRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected first list status: %d body=%s", firstRecorder.Code, firstRecorder.Body.String())
	}

	firstResult := listObjectsV2Result{}
	if err := xml.Unmarshal(firstRecorder.Body.Bytes(), &firstResult); err != nil {
		t.Fatalf("unmarshal first list result: %v", err)
	}
	if !firstResult.IsTruncated || firstResult.NextContinuationToken == "" {
		t.Fatalf("expected truncated first page, got %+v", firstResult)
	}
	if len(firstResult.CommonPrefixes) != 1 || firstResult.CommonPrefixes[0].Prefix != "photos/2024/" {
		t.Fatalf("unexpected common prefixes: %+v", firstResult.CommonPrefixes)
	}

	secondRequest := httptest.NewRequest(http.MethodGet, "/gallery?list-type=2&prefix=photos/&delimiter=/&max-keys=1&continuation-token="+firstResult.NextContinuationToken, nil)
	signHeaderRequest(t, secondRequest, config.Default(), nil)
	secondRecorder := httptest.NewRecorder()
	handler.ServeHTTP(secondRecorder, secondRequest)
	if secondRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected second list status: %d body=%s", secondRecorder.Code, secondRecorder.Body.String())
	}

	secondResult := listObjectsV2Result{}
	if err := xml.Unmarshal(secondRecorder.Body.Bytes(), &secondResult); err != nil {
		t.Fatalf("unmarshal second list result: %v", err)
	}
	if len(secondResult.Contents) != 1 || secondResult.Contents[0].Key != "photos/raw.txt" {
		t.Fatalf("unexpected second page contents: %+v", secondResult.Contents)
	}
}

func TestListObjectsV1DelimiterAndMarker(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	fixtures := []string{
		"photos/2024/a.jpg",
		"photos/2024/b.jpg",
		"photos/raw.txt",
		"videos/clip.mp4",
	}
	for _, key := range fixtures {
		if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: key, Body: bytes.NewBufferString(key)}); err != nil {
			t.Fatalf("PutObject(%s) failed: %v", key, err)
		}
	}

	firstRequest := httptest.NewRequest(http.MethodGet, "/gallery?prefix=photos/&delimiter=/&max-keys=1", nil)
	signHeaderRequest(t, firstRequest, config.Default(), nil)
	firstRecorder := httptest.NewRecorder()
	handler.ServeHTTP(firstRecorder, firstRequest)
	if firstRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected first list v1 status: %d body=%s", firstRecorder.Code, firstRecorder.Body.String())
	}

	firstResult := listObjectsResult{}
	if err := xml.Unmarshal(firstRecorder.Body.Bytes(), &firstResult); err != nil {
		t.Fatalf("unmarshal first list v1 result: %v", err)
	}
	if !firstResult.IsTruncated || firstResult.NextMarker == "" {
		t.Fatalf("expected truncated first v1 page, got %+v", firstResult)
	}
	if len(firstResult.CommonPrefixes) != 1 || firstResult.CommonPrefixes[0].Prefix != "photos/2024/" {
		t.Fatalf("unexpected first v1 common prefixes: %+v", firstResult.CommonPrefixes)
	}
	if firstResult.NextMarker != "photos/2024/" {
		t.Fatalf("unexpected first v1 next marker: %q", firstResult.NextMarker)
	}

	secondRequest := httptest.NewRequest(http.MethodGet, "/gallery?prefix=photos/&delimiter=/&max-keys=1&marker="+url.QueryEscape(firstResult.NextMarker), nil)
	signHeaderRequest(t, secondRequest, config.Default(), nil)
	secondRecorder := httptest.NewRecorder()
	handler.ServeHTTP(secondRecorder, secondRequest)
	if secondRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected second list v1 status: %d body=%s", secondRecorder.Code, secondRecorder.Body.String())
	}

	secondResult := listObjectsResult{}
	if err := xml.Unmarshal(secondRecorder.Body.Bytes(), &secondResult); err != nil {
		t.Fatalf("unmarshal second list v1 result: %v", err)
	}
	if len(secondResult.Contents) != 1 || secondResult.Contents[0].Key != "photos/raw.txt" {
		t.Fatalf("unexpected second v1 page contents: %+v", secondResult.Contents)
	}
	if secondResult.Marker != "photos/2024/" {
		t.Fatalf("unexpected echoed marker: %q", secondResult.Marker)
	}
}

func TestMultipartUploadLifecycle(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	initiateRequest := httptest.NewRequest(http.MethodPost, "/gallery/archive/big.txt?uploads", nil)
	initiateRequest.Header.Set("Content-Type", "text/plain")
	signHeaderRequest(t, initiateRequest, config.Default(), nil)
	initiateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(initiateRecorder, initiateRequest)
	if initiateRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected initiate status: %d body=%s", initiateRecorder.Code, initiateRecorder.Body.String())
	}

	initiateResult := initiateMultipartUploadResult{}
	if err := xml.Unmarshal(initiateRecorder.Body.Bytes(), &initiateResult); err != nil {
		t.Fatalf("unmarshal initiate result: %v", err)
	}
	if initiateResult.UploadID == "" {
		t.Fatalf("expected upload id in initiate response")
	}

	partOneBody := []byte("hello ")
	partOneRequest := httptest.NewRequest(http.MethodPut, "/gallery/archive/big.txt?partNumber=1&uploadId="+initiateResult.UploadID, bytes.NewReader(partOneBody))
	signHeaderRequest(t, partOneRequest, config.Default(), partOneBody)
	partOneRecorder := httptest.NewRecorder()
	handler.ServeHTTP(partOneRecorder, partOneRequest)
	if partOneRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected upload part one status: %d body=%s", partOneRecorder.Code, partOneRecorder.Body.String())
	}
	partOneETag := partOneRecorder.Header().Get("ETag")

	partTwoBody := []byte("world")
	partTwoRequest := httptest.NewRequest(http.MethodPut, "/gallery/archive/big.txt?partNumber=2&uploadId="+initiateResult.UploadID, bytes.NewReader(partTwoBody))
	signHeaderRequest(t, partTwoRequest, config.Default(), partTwoBody)
	partTwoRecorder := httptest.NewRecorder()
	handler.ServeHTTP(partTwoRecorder, partTwoRequest)
	if partTwoRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected upload part two status: %d body=%s", partTwoRecorder.Code, partTwoRecorder.Body.String())
	}
	partTwoETag := partTwoRecorder.Header().Get("ETag")

	listPartsRequest := httptest.NewRequest(http.MethodGet, "/gallery/archive/big.txt?uploadId="+initiateResult.UploadID, nil)
	signHeaderRequest(t, listPartsRequest, config.Default(), nil)
	listPartsRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listPartsRecorder, listPartsRequest)
	if listPartsRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list parts status: %d body=%s", listPartsRecorder.Code, listPartsRecorder.Body.String())
	}
	partsResult := listPartsResult{}
	if err := xml.Unmarshal(listPartsRecorder.Body.Bytes(), &partsResult); err != nil {
		t.Fatalf("unmarshal list parts result: %v", err)
	}
	if len(partsResult.Parts) != 2 {
		t.Fatalf("expected 2 listed parts, got %+v", partsResult.Parts)
	}

	completeBody := []byte("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + partOneETag + "</ETag></Part><Part><PartNumber>2</PartNumber><ETag>" + partTwoETag + "</ETag></Part></CompleteMultipartUpload>")
	completeRequest := httptest.NewRequest(http.MethodPost, "/gallery/archive/big.txt?uploadId="+initiateResult.UploadID, bytes.NewReader(completeBody))
	completeRequest.Header.Set("Content-Type", "application/xml")
	signHeaderRequest(t, completeRequest, config.Default(), completeBody)
	completeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(completeRecorder, completeRequest)
	if completeRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected complete status: %d body=%s", completeRecorder.Code, completeRecorder.Body.String())
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/gallery/archive/big.txt", nil)
	signHeaderRequest(t, getRequest, config.Default(), nil)
	getRecorder := httptest.NewRecorder()
	handler.ServeHTTP(getRecorder, getRequest)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected get multipart object status: %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	if body := strings.TrimSpace(getRecorder.Body.String()); body != "hello world" {
		t.Fatalf("unexpected multipart object body: %q", body)
	}
}

func TestSyncEnabledDoesNotShortCircuitS3Writes(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Sync.Enabled = true

	store := storage.New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	handler := newHandler(cfg, store, newShareLinksForTest(t, cfg.Paths.DataDir), newCredentialsForTest(t, cfg), zap.NewNop())

	body := []byte("hello")
	request := httptest.NewRequest(http.MethodPut, "/gallery/notes/readme.txt", bytes.NewReader(body))
	signHeaderRequest(t, request, cfg, body)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	assertS3Error(t, recorder, http.StatusNotFound, "NoSuchBucket", cfg.Storage.Region, "gallery")
}

func TestUploadPartCopyLifecycle(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	for _, bucket := range []string{"gallery", "archive"} {
		if _, err := store.CreateBucket(context.Background(), bucket, 0); err != nil {
			t.Fatalf("CreateBucket(%s) failed: %v", bucket, err)
		}
	}
	source, err := store.PutObject(context.Background(), storage.PutObjectInput{
		Bucket:      "gallery",
		Key:         "notes/source.txt",
		Body:        bytes.NewBufferString("hello world"),
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	initiateRequest := httptest.NewRequest(http.MethodPost, "/archive/copied.txt?uploads", nil)
	initiateRequest.Header.Set("Content-Type", "text/plain")
	signHeaderRequest(t, initiateRequest, config.Default(), nil)
	initiateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(initiateRecorder, initiateRequest)
	if initiateRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected initiate status: %d body=%s", initiateRecorder.Code, initiateRecorder.Body.String())
	}
	initiateResult := initiateMultipartUploadResult{}
	if err := xml.Unmarshal(initiateRecorder.Body.Bytes(), &initiateResult); err != nil {
		t.Fatalf("unmarshal initiate result: %v", err)
	}

	partOneRequest := httptest.NewRequest(http.MethodPut, "/archive/copied.txt?partNumber=1&uploadId="+initiateResult.UploadID, nil)
	partOneRequest.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source.txt")
	partOneRequest.Header.Set("X-Amz-Copy-Source-Range", "bytes=0-5")
	signHeaderRequest(t, partOneRequest, config.Default(), nil)
	partOneRecorder := httptest.NewRecorder()
	handler.ServeHTTP(partOneRecorder, partOneRequest)
	if partOneRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected upload part copy one status: %d body=%s", partOneRecorder.Code, partOneRecorder.Body.String())
	}
	partOneResult := copyPartResult{}
	if err := xml.Unmarshal(partOneRecorder.Body.Bytes(), &partOneResult); err != nil {
		t.Fatalf("unmarshal copy part one result: %v", err)
	}

	partTwoRequest := httptest.NewRequest(http.MethodPut, "/archive/copied.txt?partNumber=2&uploadId="+initiateResult.UploadID, nil)
	partTwoRequest.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source.txt")
	partTwoRequest.Header.Set("X-Amz-Copy-Source-Range", "bytes=6-10")
	partTwoRequest.Header.Set("X-Amz-Copy-Source-If-Match", `"`+source.ETag+`"`)
	signHeaderRequest(t, partTwoRequest, config.Default(), nil)
	partTwoRecorder := httptest.NewRecorder()
	handler.ServeHTTP(partTwoRecorder, partTwoRequest)
	if partTwoRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected upload part copy two status: %d body=%s", partTwoRecorder.Code, partTwoRecorder.Body.String())
	}
	partTwoResult := copyPartResult{}
	if err := xml.Unmarshal(partTwoRecorder.Body.Bytes(), &partTwoResult); err != nil {
		t.Fatalf("unmarshal copy part two result: %v", err)
	}
	if strings.TrimSpace(partOneResult.ETag) == "" || strings.TrimSpace(partTwoResult.ETag) == "" {
		t.Fatalf("expected copy part etags, got %+v %+v", partOneResult, partTwoResult)
	}

	completeBody := []byte("<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" + partOneResult.ETag + "</ETag></Part><Part><PartNumber>2</PartNumber><ETag>" + partTwoResult.ETag + "</ETag></Part></CompleteMultipartUpload>")
	completeRequest := httptest.NewRequest(http.MethodPost, "/archive/copied.txt?uploadId="+initiateResult.UploadID, bytes.NewReader(completeBody))
	completeRequest.Header.Set("Content-Type", "application/xml")
	signHeaderRequest(t, completeRequest, config.Default(), completeBody)
	completeRecorder := httptest.NewRecorder()
	handler.ServeHTTP(completeRecorder, completeRequest)
	if completeRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected complete copied multipart status: %d body=%s", completeRecorder.Code, completeRecorder.Body.String())
	}

	getRequest := httptest.NewRequest(http.MethodGet, "/archive/copied.txt", nil)
	signHeaderRequest(t, getRequest, config.Default(), nil)
	getRecorder := httptest.NewRecorder()
	handler.ServeHTTP(getRecorder, getRequest)
	if getRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected get copied multipart object status: %d body=%s", getRecorder.Code, getRecorder.Body.String())
	}
	if body := strings.TrimSpace(getRecorder.Body.String()); body != "hello world" {
		t.Fatalf("unexpected copied multipart object body: %q", body)
	}
}

func TestUploadPartCopyRejectsInvalidRange(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	for _, bucket := range []string{"gallery", "archive"} {
		if _, err := store.CreateBucket(context.Background(), bucket, 0); err != nil {
			t.Fatalf("CreateBucket(%s) failed: %v", bucket, err)
		}
	}
	if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: "notes/source.txt", Body: bytes.NewBufferString("hello")}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	initiateRequest := httptest.NewRequest(http.MethodPost, "/archive/copied.txt?uploads", nil)
	signHeaderRequest(t, initiateRequest, config.Default(), nil)
	initiateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(initiateRecorder, initiateRequest)
	if initiateRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected initiate status: %d body=%s", initiateRecorder.Code, initiateRecorder.Body.String())
	}
	initiateResult := initiateMultipartUploadResult{}
	if err := xml.Unmarshal(initiateRecorder.Body.Bytes(), &initiateResult); err != nil {
		t.Fatalf("unmarshal initiate result: %v", err)
	}

	request := httptest.NewRequest(http.MethodPut, "/archive/copied.txt?partNumber=1&uploadId="+initiateResult.UploadID, nil)
	request.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source.txt")
	request.Header.Set("X-Amz-Copy-Source-Range", "bytes=0-99")
	signHeaderRequest(t, request, config.Default(), nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	assertS3Error(t, recorder, http.StatusBadRequest, "InvalidArgument", config.Default().Storage.Region, "archive")
}

func TestUploadPartCopyRequiresSourceBucketAccess(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")
	cfg.Auth.S3.AccessKeyID = ""
	cfg.Auth.S3.SecretAccessKey = ""

	store := storage.New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	for _, bucket := range []string{"gallery", "archive"} {
		if _, err := store.CreateBucket(context.Background(), bucket, 0); err != nil {
			t.Fatalf("CreateBucket(%s) failed: %v", bucket, err)
		}
	}
	if _, err := store.PutObject(context.Background(), storage.PutObjectInput{Bucket: "gallery", Key: "notes/source.txt", Body: bytes.NewBufferString("hello world")}); err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	creds, err := s3creds.New(cfg.Paths.DataDir, s3creds.BootstrapCredential{}, zap.NewNop())
	if err != nil {
		t.Fatalf("s3creds.New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = creds.Close()
	})
	created, err := creds.Create(context.Background(), s3creds.CreateInput{Label: "archive-only", Permission: s3creds.PermissionReadWrite, Buckets: []string{"archive"}})
	if err != nil {
		t.Fatalf("Create credential failed: %v", err)
	}
	handler := newHandler(cfg, store, newShareLinksForTest(t, cfg.Paths.DataDir), creds, zap.NewNop())
	signedCfg := cfg
	signedCfg.Auth.S3.AccessKeyID = created.AccessKeyID
	signedCfg.Auth.S3.SecretAccessKey = created.SecretAccessKey

	initiateRequest := httptest.NewRequest(http.MethodPost, "/archive/copied.txt?uploads", nil)
	signHeaderRequest(t, initiateRequest, signedCfg, nil)
	initiateRecorder := httptest.NewRecorder()
	handler.ServeHTTP(initiateRecorder, initiateRequest)
	if initiateRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected scoped initiate status: %d body=%s", initiateRecorder.Code, initiateRecorder.Body.String())
	}
	initiateResult := initiateMultipartUploadResult{}
	if err := xml.Unmarshal(initiateRecorder.Body.Bytes(), &initiateResult); err != nil {
		t.Fatalf("unmarshal initiate result: %v", err)
	}

	request := httptest.NewRequest(http.MethodPut, "/archive/copied.txt?partNumber=1&uploadId="+initiateResult.UploadID, nil)
	request.Header.Set("X-Amz-Copy-Source", "/gallery/notes/source.txt")
	signHeaderRequest(t, request, signedCfg, nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	assertS3Error(t, recorder, http.StatusForbidden, "AccessDenied", cfg.Storage.Region, "archive")
}

func TestListMultipartUploadsSupportsMarkers(t *testing.T) {
	t.Parallel()

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}

	initiate := func(key string) initiateMultipartUploadResult {
		request := httptest.NewRequest(http.MethodPost, "/gallery/"+key+"?uploads", nil)
		signHeaderRequest(t, request, config.Default(), nil)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("unexpected initiate status for %s: %d body=%s", key, recorder.Code, recorder.Body.String())
		}
		result := initiateMultipartUploadResult{}
		if err := xml.Unmarshal(recorder.Body.Bytes(), &result); err != nil {
			t.Fatalf("unmarshal initiate result for %s: %v", key, err)
		}
		return result
	}

	first := initiate("alpha.txt")
	second := initiate("alpha.txt")
	third := initiate("zeta.txt")

	listRequest := httptest.NewRequest(http.MethodGet, "/gallery?uploads&max-uploads=2", nil)
	signHeaderRequest(t, listRequest, config.Default(), nil)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected list multipart uploads status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	firstPage := listMultipartUploadsResult{}
	if err := xml.Unmarshal(listRecorder.Body.Bytes(), &firstPage); err != nil {
		t.Fatalf("unmarshal first multipart upload page: %v", err)
	}
	if !firstPage.IsTruncated {
		t.Fatalf("expected truncated first multipart page: %+v", firstPage)
	}
	if len(firstPage.Uploads) != 2 || firstPage.Uploads[0].Key != "alpha.txt" || firstPage.Uploads[1].Key != "alpha.txt" {
		t.Fatalf("unexpected first multipart page uploads: %+v", firstPage.Uploads)
	}
	if firstPage.NextKeyMarker != "alpha.txt" {
		t.Fatalf("unexpected next key marker: %q", firstPage.NextKeyMarker)
	}
	if firstPage.NextUploadIDMarker == "" {
		t.Fatalf("expected next upload id marker in first page")
	}
	if firstPage.Uploads[0].UploadID != first.UploadID || firstPage.Uploads[1].UploadID != second.UploadID {
		t.Fatalf("unexpected first page upload ids: %+v", firstPage.Uploads)
	}

	nextRequest := httptest.NewRequest(http.MethodGet, "/gallery?uploads&max-uploads=2&key-marker="+url.QueryEscape(firstPage.NextKeyMarker)+"&upload-id-marker="+url.QueryEscape(firstPage.NextUploadIDMarker), nil)
	signHeaderRequest(t, nextRequest, config.Default(), nil)
	nextRecorder := httptest.NewRecorder()
	handler.ServeHTTP(nextRecorder, nextRequest)
	if nextRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected second list multipart uploads status: %d body=%s", nextRecorder.Code, nextRecorder.Body.String())
	}
	secondPage := listMultipartUploadsResult{}
	if err := xml.Unmarshal(nextRecorder.Body.Bytes(), &secondPage); err != nil {
		t.Fatalf("unmarshal second multipart upload page: %v", err)
	}
	if len(secondPage.Uploads) != 1 || secondPage.Uploads[0].Key != "zeta.txt" || secondPage.Uploads[0].UploadID != third.UploadID {
		t.Fatalf("unexpected second multipart page uploads: %+v", secondPage.Uploads)
	}
	if secondPage.IsTruncated {
		t.Fatalf("expected second multipart page to be final: %+v", secondPage)
	}
}

func TestListMultipartUploadsSupportsDelimiter(t *testing.T) {
	t.Parallel()

	requestKeys := []string{"sample.txt", "photos/2026/a.jpg", "videos/clip.mp4"}

	store, handler := newTestHandler(t)
	if _, err := store.CreateBucket(context.Background(), "gallery", 0); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	for _, key := range requestKeys {
		request := httptest.NewRequest(http.MethodPost, "/gallery/"+key+"?uploads", nil)
		signHeaderRequest(t, request, config.Default(), nil)
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("unexpected initiate status for %s: %d body=%s", key, recorder.Code, recorder.Body.String())
		}
	}

	listRequest := httptest.NewRequest(http.MethodGet, "/gallery?uploads&delimiter=/", nil)
	signHeaderRequest(t, listRequest, config.Default(), nil)
	listRecorder := httptest.NewRecorder()
	handler.ServeHTTP(listRecorder, listRequest)
	if listRecorder.Code != http.StatusOK {
		t.Fatalf("unexpected delimiter list multipart uploads status: %d body=%s", listRecorder.Code, listRecorder.Body.String())
	}
	result := listMultipartUploadsResult{}
	if err := xml.Unmarshal(listRecorder.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal delimiter multipart upload result: %v", err)
	}
	if len(result.Uploads) != 1 || result.Uploads[0].Key != "sample.txt" {
		t.Fatalf("unexpected root uploads: %+v", result.Uploads)
	}
	prefixes := make([]string, 0, len(result.CommonPrefixes))
	for _, item := range result.CommonPrefixes {
		prefixes = append(prefixes, item.Prefix)
	}
	sort.Strings(prefixes)
	if len(prefixes) != 2 || prefixes[0] != "photos/" || prefixes[1] != "videos/" {
		t.Fatalf("unexpected multipart common prefixes: %+v", prefixes)
	}
}

func newTestHandler(t *testing.T) (*storage.Store, http.Handler) {
	t.Helper()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")

	store := storage.New(cfg, zap.NewNop())
	t.Cleanup(func() {
		_ = store.Close()
	})
	handler := newHandler(cfg, store, newShareLinksForTest(t, cfg.Paths.DataDir), newCredentialsForTest(t, cfg), zap.NewNop())

	return store, handler
}

func assertS3Error(t *testing.T, recorder *httptest.ResponseRecorder, wantStatus int, wantCode, wantRegion, wantBucket string) {
	t.Helper()

	if recorder.Code != wantStatus {
		t.Fatalf("unexpected status: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if contentType := recorder.Header().Get("Content-Type"); !strings.Contains(contentType, "application/xml") {
		t.Fatalf("unexpected content type: %q", contentType)
	}
	if got := recorder.Header().Get("X-Amz-Bucket-Region"); got != wantRegion {
		t.Fatalf("unexpected region header: %q", got)
	}
	if requestID := recorder.Header().Get("X-Amz-Request-Id"); strings.TrimSpace(requestID) == "" {
		t.Fatalf("expected X-Amz-Request-Id header")
	}
	if body := recorder.Body.String(); !strings.Contains(body, "<Code>"+wantCode+"</Code>") {
		t.Fatalf("unexpected body: %s", body)
	}
	if wantRegion != "" {
		if body := recorder.Body.String(); !strings.Contains(body, "<Region>"+wantRegion+"</Region>") {
			t.Fatalf("unexpected body region: %s", body)
		}
	}
	if wantBucket != "" {
		if body := recorder.Body.String(); !strings.Contains(body, "<BucketName>"+wantBucket+"</BucketName>") {
			t.Fatalf("unexpected body bucket: %s", body)
		}
	}
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

func signHeaderRequest(t *testing.T, request *http.Request, cfg config.Config, body []byte) {
	t.Helper()
	timestamp := time.Now().UTC()
	request.Header.Set("X-Amz-Date", timestamp.Format("20060102T150405Z"))
	payloadHash := hashHex(body)
	request.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signedHeaders := collectSignedHeaders(request)
	canonicalRequest := buildCanonicalRequest(t, request, signedHeaders, payloadHash, true)
	stringToSign := buildStringToSign(timestamp, cfg.Storage.Region, canonicalRequest)
	signature := signSignature(cfg.Auth.S3.SecretAccessKey, timestamp, cfg.Storage.Region, stringToSign)
	request.Header.Set("Authorization", strings.Join([]string{
		sigv4.Algorithm + " Credential=" + cfg.Auth.S3.AccessKeyID + "/" + timestamp.Format("20060102") + "/" + cfg.Storage.Region + "/s3/aws4_request",
		"SignedHeaders=" + strings.Join(signedHeaders, ";"),
		"Signature=" + signature,
	}, ", "))
}

func signPresignedRequest(t *testing.T, request *http.Request, cfg config.Config, expires time.Duration) {
	t.Helper()
	timestamp := time.Now().UTC()
	query := request.URL.Query()
	query.Set("X-Amz-Algorithm", sigv4.Algorithm)
	query.Set("X-Amz-Credential", cfg.Auth.S3.AccessKeyID+"/"+timestamp.Format("20060102")+"/"+cfg.Storage.Region+"/s3/aws4_request")
	query.Set("X-Amz-Date", timestamp.Format("20060102T150405Z"))
	query.Set("X-Amz-Expires", strconv.Itoa(int(expires.Seconds())))
	query.Set("X-Amz-SignedHeaders", "host")
	request.URL.RawQuery = query.Encode()

	canonicalRequest := buildCanonicalRequest(t, request, []string{"host"}, "UNSIGNED-PAYLOAD", false)
	stringToSign := buildStringToSign(timestamp, cfg.Storage.Region, canonicalRequest)
	query.Set("X-Amz-Signature", signSignature(cfg.Auth.S3.SecretAccessKey, timestamp, cfg.Storage.Region, stringToSign))
	request.URL.RawQuery = query.Encode()
}

func buildCanonicalRequest(t *testing.T, request *http.Request, signedHeaders []string, payloadHash string, includeSignatureQuery bool) string {
	t.Helper()
	query := request.URL.Query()
	if !includeSignatureQuery {
		query.Del("X-Amz-Signature")
	}

	headers := make([]string, 0, len(signedHeaders))
	for _, name := range signedHeaders {
		headers = append(headers, name+":"+canonicalHeaderValue(request, name)+"\n")
	}

	return strings.Join([]string{
		request.Method,
		canonicalURIForTest(request.URL.Path),
		canonicalQueryForTest(query),
		strings.Join(headers, ""),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")
}

func collectSignedHeaders(request *http.Request) []string {
	set := map[string]struct{}{"host": {}}
	for name := range request.Header {
		set[strings.ToLower(name)] = struct{}{}
	}
	items := make([]string, 0, len(set))
	for name := range set {
		items = append(items, name)
	}
	sort.Strings(items)
	return items
}

func canonicalHeaderValue(request *http.Request, name string) string {
	if name == "host" {
		return strings.ToLower(request.Host)
	}
	values := request.Header.Values(http.CanonicalHeaderKey(name))
	normalized := make([]string, 0, len(values))
	for _, value := range values {
		normalized = append(normalized, strings.Join(strings.Fields(strings.TrimSpace(value)), " "))
	}
	return strings.Join(normalized, ",")
}

func canonicalURIForTest(value string) string {
	if value == "" {
		return "/"
	}
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b == '/' {
			builder.WriteByte('/')
			continue
		}
		if isUnreservedTest(b) {
			builder.WriteByte(b)
			continue
		}
		builder.WriteString(percentEncodeTest(b))
	}
	return builder.String()
}

func canonicalQueryForTest(values map[string][]string) string {
	type pair struct{ key, value string }
	pairs := make([]pair, 0)
	for key, list := range values {
		if len(list) == 0 {
			pairs = append(pairs, pair{key: awsEncodeTest(key), value: ""})
			continue
		}
		for _, value := range list {
			pairs = append(pairs, pair{key: awsEncodeTest(key), value: awsEncodeTest(value)})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	parts := make([]string, 0, len(pairs))
	for _, item := range pairs {
		parts = append(parts, item.key+"="+item.value)
	}
	return strings.Join(parts, "&")
}

func buildStringToSign(timestamp time.Time, region, canonicalRequest string) string {
	hash := sha256.Sum256([]byte(canonicalRequest))
	return strings.Join([]string{
		sigv4.Algorithm,
		timestamp.Format("20060102T150405Z"),
		timestamp.Format("20060102") + "/" + region + "/s3/aws4_request",
		hex.EncodeToString(hash[:]),
	}, "\n")
}

func signSignature(secret string, timestamp time.Time, region, stringToSign string) string {
	kDate := hmacSHA256([]byte("AWS4"+secret), timestamp.Format("20060102"))
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	return hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
}

func hmacSHA256(key []byte, message string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(message))
	return h.Sum(nil)
}

func hashHex(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func awsEncodeTest(value string) string {
	var builder strings.Builder
	for i := 0; i < len(value); i++ {
		b := value[i]
		if isUnreservedTest(b) {
			builder.WriteByte(b)
			continue
		}
		builder.WriteString(percentEncodeTest(b))
	}
	return builder.String()
}

func isUnreservedTest(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '-' || b == '_' || b == '.' || b == '~'
}

func percentEncodeTest(b byte) string {
	const hexChars = "0123456789ABCDEF"
	return "%" + string([]byte{hexChars[b>>4], hexChars[b&0x0F]})
}
