package s3api

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"bares3-server/internal/config"
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

func TestUnsignedRequestIsRejected(t *testing.T) {
	t.Parallel()

	_, handler := newTestHandler(t)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("unexpected status for unsigned request: %d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "AccessDenied") {
		t.Fatalf("expected AccessDenied response, got %s", recorder.Body.String())
	}
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
	if deniedRecorder.Code != http.StatusForbidden {
		t.Fatalf("unexpected denied status: %d body=%s", deniedRecorder.Code, deniedRecorder.Body.String())
	}
	if !strings.Contains(deniedRecorder.Body.String(), "AccessDenied") {
		t.Fatalf("expected AccessDenied body, got %s", deniedRecorder.Body.String())
	}
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

func newTestHandler(t *testing.T) (*storage.Store, http.Handler) {
	t.Helper()

	root := t.TempDir()
	cfg := config.Default()
	cfg.Paths.DataDir = filepath.Join(root, "data")
	cfg.Paths.LogDir = filepath.Join(root, "logs")
	cfg.Storage.TmpDir = filepath.Join(root, "tmp")

	store := storage.New(cfg, zap.NewNop())
	handler := NewHandler(cfg, store, zap.NewNop())

	return store, handler
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
