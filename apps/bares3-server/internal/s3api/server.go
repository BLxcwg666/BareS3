package s3api

import (
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/httpx"
	"bares3-server/internal/sigv4"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

type listBucketsResult struct {
	XMLName xml.Name     `xml:"ListAllMyBucketsResult"`
	Xmlns   string       `xml:"xmlns,attr"`
	Owner   ownerInfo    `xml:"Owner"`
	Buckets bucketsBlock `xml:"Buckets"`
}

type bucketsBlock struct {
	Items []bucketEntry `xml:"Bucket"`
}

type bucketEntry struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`
}

type ownerInfo struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

type listObjectsV2Result struct {
	XMLName               xml.Name            `xml:"ListBucketResult"`
	Xmlns                 string              `xml:"xmlns,attr"`
	Name                  string              `xml:"Name"`
	Prefix                string              `xml:"Prefix"`
	KeyCount              int                 `xml:"KeyCount"`
	MaxKeys               int                 `xml:"MaxKeys"`
	IsTruncated           bool                `xml:"IsTruncated"`
	Contents              []listObjectEntry   `xml:"Contents"`
	CommonPrefixes        []commonPrefixEntry `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     string              `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string              `xml:"NextContinuationToken,omitempty"`
	StartAfter            string              `xml:"StartAfter,omitempty"`
	EncodingType          string              `xml:"EncodingType,omitempty"`
	Delimiter             string              `xml:"Delimiter,omitempty"`
}

type listObjectEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag,omitempty"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type commonPrefixEntry struct {
	Prefix string `xml:"Prefix"`
}

type initiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

type completeMultipartUploadRequest struct {
	XMLName xml.Name                   `xml:"CompleteMultipartUpload"`
	Parts   []completeMultipartPartXML `xml:"Part"`
}

type completeMultipartPartXML struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type completeMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr"`
	Location string   `xml:"Location,omitempty"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag,omitempty"`
}

type listPartsResult struct {
	XMLName              xml.Name        `xml:"ListPartsResult"`
	Xmlns                string          `xml:"xmlns,attr"`
	Bucket               string          `xml:"Bucket"`
	Key                  string          `xml:"Key"`
	UploadID             string          `xml:"UploadId"`
	PartNumberMarker     int             `xml:"PartNumberMarker"`
	NextPartNumberMarker int             `xml:"NextPartNumberMarker,omitempty"`
	MaxParts             int             `xml:"MaxParts"`
	IsTruncated          bool            `xml:"IsTruncated"`
	Parts                []listPartEntry `xml:"Part"`
}

type listPartEntry struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag,omitempty"`
	Size         int64  `xml:"Size"`
}

type listResponseItem struct {
	Kind   string
	Value  string
	Object storage.ObjectInfo
}

func NewHandler(cfg config.Config, store *storage.Store, logger *zap.Logger) http.Handler {
	verifier := sigv4.NewVerifier(cfg.Auth.S3.AccessKeyID, cfg.Auth.S3.SecretAccessKey, cfg.Storage.Region, "s3")

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "s3",
			"region":  cfg.Storage.Region,
			"version": buildinfo.Current(),
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleS3Request(w, r, cfg, store, verifier)
	})

	return httpx.RequestLogger(logger, "s3")(mux)
}

func handleS3Request(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, verifier *sigv4.Verifier) {
	if _, err := verifier.Authenticate(r); err != nil {
		writeAuthError(w, r, err)
		return
	}

	if r.URL.Path == "/" || strings.TrimSpace(r.URL.Path) == "" {
		handleServiceRoot(w, r, store)
		return
	}

	bucket, key := splitPath(r.URL.Path)
	if bucket == "" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidURI", "bucket name is required")
		return
	}

	w.Header().Set("X-Amz-Bucket-Region", cfg.Storage.Region)

	if key == "" {
		handleBucketRequest(w, r, store, bucket)
		return
	}

	handleObjectRequest(w, r, cfg, store, bucket, key)
}

func handleServiceRoot(w http.ResponseWriter, r *http.Request, store *storage.Store) {
	if r.Method != http.MethodGet {
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported on the service root")
		return
	}

	buckets, err := store.ListBuckets(r.Context())
	if err != nil {
		writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	items := make([]bucketEntry, 0, len(buckets))
	for _, bucket := range buckets {
		items = append(items, bucketEntry{
			Name:         bucket.Name,
			CreationDate: formatS3Time(bucket.CreatedAt),
		})
	}

	writeS3XML(w, http.StatusOK, listBucketsResult{
		Xmlns:   s3Namespace,
		Owner:   ownerInfo{ID: "bares3-local", DisplayName: config.ProductName},
		Buckets: bucketsBlock{Items: items},
	})
}

func handleBucketRequest(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	switch r.Method {
	case http.MethodHead:
		if _, err := store.GetBucket(r.Context(), bucket); err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPut:
		if _, err := store.CreateBucket(r.Context(), bucket, 0); err != nil {
			if errors.Is(err, storage.ErrBucketExists) {
				writeS3Error(w, r, http.StatusConflict, "BucketAlreadyOwnedByYou", err.Error())
				return
			}
			writeStorageAsS3Error(w, r, err)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		if r.URL.Query().Get("list-type") == "2" {
			handleListObjectsV2(w, r, store, bucket)
			return
		}
		writeS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "only ListObjectsV2 is currently supported")
	case http.MethodDelete:
		if err := store.DeleteBucket(r.Context(), bucket); err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported for bucket requests")
	}
}

func handleListObjectsV2(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	maxKeys := 1000
	if raw := strings.TrimSpace(r.URL.Query().Get("max-keys")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "max-keys must be a non-negative integer")
			return
		}
		maxKeys = parsed
	}
	if maxKeys > 1000 {
		maxKeys = 1000
	}

	delimiter := r.URL.Query().Get("delimiter")
	continuationToken := r.URL.Query().Get("continuation-token")
	startAfter := r.URL.Query().Get("start-after")
	encodingType := strings.TrimSpace(r.URL.Query().Get("encoding-type"))
	if encodingType != "" && encodingType != "url" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "encoding-type must be url")
		return
	}

	items, err := store.ListObjects(r.Context(), bucket, storage.ListObjectsOptions{
		Prefix: r.URL.Query().Get("prefix"),
	})
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	result, err := buildListObjectsV2Result(bucket, items, r.URL.Query().Get("prefix"), delimiter, continuationToken, startAfter, encodingType, maxKeys)
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	writeS3XML(w, http.StatusOK, result)
}

func handleObjectRequest(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, bucket, key string) {
	if hasQueryValue(r, "uploadId") {
		handleMultipartUploadRequest(w, r, cfg, store, bucket, key)
		return
	}
	if hasQueryValue(r, "uploads") {
		if r.Method != http.MethodPost {
			writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "multipart initiation requires POST")
			return
		}
		handleInitiateMultipartUpload(w, r, store, bucket, key)
		return
	}

	switch r.Method {
	case http.MethodPut:
		object, err := store.PutObject(r.Context(), storage.PutObjectInput{
			Bucket:             bucket,
			Key:                key,
			Body:               r.Body,
			ContentType:        strings.TrimSpace(r.Header.Get("Content-Type")),
			CacheControl:       strings.TrimSpace(r.Header.Get("Cache-Control")),
			ContentDisposition: strings.TrimSpace(r.Header.Get("Content-Disposition")),
			UserMetadata:       extractUserMetadata(r.Header),
		})
		if err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		if object.ETag != "" {
			w.Header().Set("ETag", `"`+object.ETag+`"`)
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodGet, http.MethodHead:
		file, object, err := store.OpenObject(r.Context(), bucket, key)
		if err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		defer func() {
			_ = file.Close()
		}()
		applyObjectHeaders(w, object)
		http.ServeContent(w, r, path.Base(object.Key), object.LastModified, file)
	case http.MethodDelete:
		if err := store.DeleteObject(r.Context(), bucket, key); err != nil && !errors.Is(err, storage.ErrObjectNotFound) {
			writeStorageAsS3Error(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported for object requests")
	}
}

func applyObjectHeaders(w http.ResponseWriter, object storage.ObjectInfo) {
	if object.ContentType != "" {
		w.Header().Set("Content-Type", object.ContentType)
	}
	if object.CacheControl != "" {
		w.Header().Set("Cache-Control", object.CacheControl)
	}
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	if object.ETag != "" {
		w.Header().Set("ETag", `"`+object.ETag+`"`)
	}
	for key, value := range object.UserMetadata {
		w.Header().Set("X-Amz-Meta-"+key, value)
	}
}

func extractUserMetadata(header http.Header) map[string]string {
	metadata := make(map[string]string)
	for key, values := range header {
		lower := strings.ToLower(key)
		if !strings.HasPrefix(lower, "x-amz-meta-") || len(values) == 0 {
			continue
		}
		metadata[strings.TrimPrefix(lower, "x-amz-meta-")] = values[0]
	}
	if len(metadata) == 0 {
		return nil
	}
	return metadata
}

func splitPath(requestPath string) (bucket string, key string) {
	trimmed := strings.TrimPrefix(requestPath, "/")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		return "", ""
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = parts[0]
	if len(parts) == 2 {
		key = parts[1]
	}
	return bucket, key
}

func handleInitiateMultipartUpload(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key string) {
	upload, err := store.InitiateMultipartUpload(r.Context(), storage.InitiateMultipartUploadInput{
		Bucket:             bucket,
		Key:                key,
		ContentType:        strings.TrimSpace(r.Header.Get("Content-Type")),
		CacheControl:       strings.TrimSpace(r.Header.Get("Cache-Control")),
		ContentDisposition: strings.TrimSpace(r.Header.Get("Content-Disposition")),
		UserMetadata:       extractUserMetadata(r.Header),
	})
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	writeS3XML(w, http.StatusOK, initiateMultipartUploadResult{
		Xmlns:    s3Namespace,
		Bucket:   upload.Bucket,
		Key:      upload.Key,
		UploadID: upload.UploadID,
	})
}

func handleMultipartUploadRequest(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, bucket, key string) {
	uploadID := strings.TrimSpace(r.URL.Query().Get("uploadId"))
	if uploadID == "" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "uploadId is required")
		return
	}

	switch r.Method {
	case http.MethodPut:
		rawPartNumber := strings.TrimSpace(r.URL.Query().Get("partNumber"))
		partNumber, err := strconv.Atoi(rawPartNumber)
		if err != nil {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "partNumber must be a positive integer")
			return
		}
		part, err := store.UploadPart(r.Context(), storage.UploadPartInput{
			Bucket:     bucket,
			Key:        key,
			UploadID:   uploadID,
			PartNumber: partNumber,
			Body:       r.Body,
		})
		if err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		w.Header().Set("ETag", `"`+part.ETag+`"`)
		w.WriteHeader(http.StatusOK)
	case http.MethodGet:
		handleListParts(w, r, store, bucket, key, uploadID)
	case http.MethodPost:
		handleCompleteMultipartUpload(w, r, cfg, store, bucket, key, uploadID)
	case http.MethodDelete:
		if err := store.AbortMultipartUpload(r.Context(), bucket, key, uploadID); err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported for multipart requests")
	}
}

func handleListParts(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key, uploadID string) {
	maxParts := 1000
	if raw := strings.TrimSpace(r.URL.Query().Get("max-parts")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "max-parts must be a non-negative integer")
			return
		}
		maxParts = parsed
	}
	if maxParts > 1000 {
		maxParts = 1000
	}

	partNumberMarker := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("part-number-marker")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "part-number-marker must be a non-negative integer")
			return
		}
		partNumberMarker = parsed
	}

	parts, err := store.ListParts(r.Context(), bucket, key, uploadID)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	visible := make([]storage.MultipartPartInfo, 0, len(parts))
	for _, part := range parts {
		if part.PartNumber > partNumberMarker {
			visible = append(visible, part)
		}
	}

	isTruncated := maxParts >= 0 && len(visible) > maxParts
	if isTruncated {
		visible = visible[:maxParts]
	}

	entries := make([]listPartEntry, 0, len(visible))
	for _, part := range visible {
		entries = append(entries, listPartEntry{
			PartNumber:   part.PartNumber,
			LastModified: formatS3Time(part.LastModified),
			ETag:         `"` + part.ETag + `"`,
			Size:         part.Size,
		})
	}

	nextMarker := 0
	if isTruncated && len(visible) > 0 {
		nextMarker = visible[len(visible)-1].PartNumber
	}

	writeS3XML(w, http.StatusOK, listPartsResult{
		Xmlns:                s3Namespace,
		Bucket:               bucket,
		Key:                  key,
		UploadID:             uploadID,
		PartNumberMarker:     partNumberMarker,
		NextPartNumberMarker: nextMarker,
		MaxParts:             maxParts,
		IsTruncated:          isTruncated,
		Parts:                entries,
	})
}

func handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, bucket, key, uploadID string) {
	requestPayload := completeMultipartUploadRequest{}
	if err := xml.NewDecoder(r.Body).Decode(&requestPayload); err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "MalformedXML", "complete multipart upload body is invalid")
		return
	}

	parts := make([]storage.CompletedPart, 0, len(requestPayload.Parts))
	for _, part := range requestPayload.Parts {
		parts = append(parts, storage.CompletedPart{PartNumber: part.PartNumber, ETag: part.ETag})
	}

	object, err := store.CompleteMultipartUpload(r.Context(), bucket, key, uploadID, parts)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	location := cfg.Storage.S3BaseURL
	if parsed, err := url.Parse(cfg.Storage.S3BaseURL); err == nil {
		parsed.Path = joinURLPath(parsed.Path, bucket, key)
		location = parsed.String()
	}

	writeS3XML(w, http.StatusOK, completeMultipartUploadResult{
		Xmlns:    s3Namespace,
		Location: location,
		Bucket:   bucket,
		Key:      key,
		ETag:     `"` + object.ETag + `"`,
	})
}

func buildListObjectsV2Result(bucket string, objects []storage.ObjectInfo, prefix, delimiter, continuationToken, startAfter, encodingType string, maxKeys int) (listObjectsV2Result, error) {
	entries, err := buildListItems(objects, prefix, delimiter)
	if err != nil {
		return listObjectsV2Result{}, err
	}

	startIndex := 0
	if continuationToken != "" {
		marker, err := decodeContinuationToken(continuationToken)
		if err != nil {
			return listObjectsV2Result{}, err
		}
		for startIndex < len(entries) && compareListItems(entries[startIndex], marker) <= 0 {
			startIndex++
		}
	} else if startAfter != "" {
		for startIndex < len(entries) && entries[startIndex].Value <= startAfter {
			startIndex++
		}
	}

	remaining := entries[startIndex:]
	emitted := remaining
	isTruncated := false
	if maxKeys >= 0 && len(emitted) > maxKeys {
		emitted = emitted[:maxKeys]
		isTruncated = true
	}

	result := listObjectsV2Result{
		Xmlns:             s3Namespace,
		Name:              bucket,
		Prefix:            encodeListValue(prefix, encodingType),
		MaxKeys:           maxKeys,
		IsTruncated:       isTruncated,
		ContinuationToken: continuationToken,
		StartAfter:        encodeListValue(startAfter, encodingType),
		EncodingType:      encodingType,
		Delimiter:         encodeListValue(delimiter, encodingType),
	}

	for _, entry := range emitted {
		switch entry.Kind {
		case "prefix":
			result.CommonPrefixes = append(result.CommonPrefixes, commonPrefixEntry{Prefix: encodeListValue(entry.Value, encodingType)})
		case "content":
			item := listObjectEntry{
				Key:          encodeListValue(entry.Object.Key, encodingType),
				LastModified: formatS3Time(entry.Object.LastModified),
				Size:         entry.Object.Size,
				StorageClass: "STANDARD",
			}
			if entry.Object.ETag != "" {
				item.ETag = `"` + entry.Object.ETag + `"`
			}
			result.Contents = append(result.Contents, item)
		}
	}

	result.KeyCount = len(result.Contents) + len(result.CommonPrefixes)
	if isTruncated && len(emitted) > 0 {
		result.NextContinuationToken = encodeContinuationToken(emitted[len(emitted)-1])
	}

	return result, nil
}

func buildListItems(objects []storage.ObjectInfo, prefix, delimiter string) ([]listResponseItem, error) {
	items := make([]listResponseItem, 0, len(objects))
	seenPrefixes := make(map[string]struct{})
	for _, object := range objects {
		if prefix != "" && !strings.HasPrefix(object.Key, prefix) {
			continue
		}
		if delimiter != "" {
			remainder := strings.TrimPrefix(object.Key, prefix)
			if index := strings.Index(remainder, delimiter); index >= 0 {
				commonPrefix := prefix + remainder[:index+len(delimiter)]
				if _, ok := seenPrefixes[commonPrefix]; !ok {
					seenPrefixes[commonPrefix] = struct{}{}
					items = append(items, listResponseItem{Kind: "prefix", Value: commonPrefix})
				}
				continue
			}
		}
		items = append(items, listResponseItem{Kind: "content", Value: object.Key, Object: object})
	}

	sort.Slice(items, func(i, j int) bool {
		return compareListItems(items[i], items[j]) < 0
	})
	return items, nil
}

func compareListItems(left, right listResponseItem) int {
	if left.Value < right.Value {
		return -1
	}
	if left.Value > right.Value {
		return 1
	}
	if left.Kind < right.Kind {
		return -1
	}
	if left.Kind > right.Kind {
		return 1
	}
	return 0
}

func encodeContinuationToken(item listResponseItem) string {
	return base64.RawURLEncoding.EncodeToString([]byte(item.Kind + ":" + item.Value))
}

func decodeContinuationToken(value string) (listResponseItem, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(value))
	if err != nil {
		return listResponseItem{}, fmt.Errorf("continuation token is invalid")
	}
	kind, marker, ok := strings.Cut(string(decoded), ":")
	if !ok || (kind != "content" && kind != "prefix") {
		return listResponseItem{}, fmt.Errorf("continuation token is invalid")
	}
	return listResponseItem{Kind: kind, Value: marker}, nil
}

func encodeListValue(value, encodingType string) string {
	if value == "" || encodingType != "url" {
		return value
	}
	return url.PathEscape(value)
}

func joinURLPath(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.Trim(part, "/")
		if trimmed == "" {
			continue
		}
		cleaned = append(cleaned, trimmed)
	}
	if len(cleaned) == 0 {
		return "/"
	}
	return "/" + strings.Join(cleaned, "/")
}

func hasQueryValue(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}

func formatS3Time(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format("2006-01-02T15:04:05.000Z")
}

func writeS3XML(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(xml.Header))
	_ = xml.NewEncoder(w).Encode(payload)
}

func writeStorageAsS3Error(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, storage.ErrBucketNotFound):
		writeS3Error(w, r, http.StatusNotFound, "NoSuchBucket", err.Error())
	case errors.Is(err, storage.ErrObjectNotFound):
		writeS3Error(w, r, http.StatusNotFound, "NoSuchKey", err.Error())
	case errors.Is(err, storage.ErrInvalidBucketName):
		writeS3Error(w, r, http.StatusBadRequest, "InvalidBucketName", err.Error())
	case errors.Is(err, storage.ErrInvalidObjectKey):
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
	case errors.Is(err, storage.ErrUploadNotFound):
		writeS3Error(w, r, http.StatusNotFound, "NoSuchUpload", err.Error())
	case errors.Is(err, storage.ErrInvalidPart):
		writeS3Error(w, r, http.StatusBadRequest, "InvalidPart", err.Error())
	case errors.Is(err, storage.ErrInvalidPartOrder):
		writeS3Error(w, r, http.StatusBadRequest, "InvalidPartOrder", err.Error())
	case errors.Is(err, storage.ErrInvalidPartNumber):
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
	case errors.Is(err, storage.ErrBucketNotEmpty):
		writeS3Error(w, r, http.StatusConflict, "BucketNotEmpty", err.Error())
	case errors.Is(err, storage.ErrBucketQuotaExceeded), errors.Is(err, storage.ErrInstanceQuotaExceeded):
		writeS3Error(w, r, http.StatusConflict, "QuotaExceeded", err.Error())
	case errors.Is(err, storage.ErrInvalidQuota):
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
	default:
		writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

func writeAuthError(w http.ResponseWriter, r *http.Request, err error) {
	var authErr *sigv4.Error
	if errors.As(err, &authErr) {
		writeS3Error(w, r, authErr.Status, authErr.Code, authErr.Message)
		return
	}
	writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "request authentication failed")
}

func writeS3Error(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	requestID := fmt.Sprintf("req-%d", time.Now().UnixNano())
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("X-Amz-Request-Id", requestID)
	w.WriteHeader(status)

	if r.Method == http.MethodHead {
		return
	}

	_ = xml.NewEncoder(w).Encode(errorResponse{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestID: requestID,
	})
}
