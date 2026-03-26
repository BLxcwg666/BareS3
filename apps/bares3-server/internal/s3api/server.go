package s3api

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"path"
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
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Xmlns                 string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	KeyCount              int               `xml:"KeyCount"`
	MaxKeys               int               `xml:"MaxKeys"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Contents              []listObjectEntry `xml:"Contents"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
}

type listObjectEntry struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag,omitempty"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

func NewHandler(cfg config.Config, store *storage.Store, logger *zap.Logger) http.Handler {
	verifier := sigv4.NewVerifier(cfg.Auth.S3AccessKeyID, cfg.Auth.S3SecretAccessKey, cfg.Storage.Region, "s3")

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

	handleObjectRequest(w, r, store, bucket, key)
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
		if _, err := store.CreateBucket(r.Context(), bucket); err != nil {
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

	items, err := store.ListObjects(r.Context(), bucket, storage.ListObjectsOptions{
		Prefix: r.URL.Query().Get("prefix"),
		Limit:  maxKeys,
	})
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	contents := make([]listObjectEntry, 0, len(items))
	for _, item := range items {
		entry := listObjectEntry{
			Key:          item.Key,
			LastModified: formatS3Time(item.LastModified),
			Size:         item.Size,
			StorageClass: "STANDARD",
		}
		if item.ETag != "" {
			entry.ETag = `"` + item.ETag + `"`
		}
		contents = append(contents, entry)
	}

	writeS3XML(w, http.StatusOK, listObjectsV2Result{
		Xmlns:       s3Namespace,
		Name:        bucket,
		Prefix:      r.URL.Query().Get("prefix"),
		KeyCount:    len(contents),
		MaxKeys:     maxKeys,
		IsTruncated: false,
		Contents:    contents,
		StartAfter:  r.URL.Query().Get("start-after"),
	})
}

func handleObjectRequest(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key string) {
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
	case errors.Is(err, storage.ErrBucketNotEmpty):
		writeS3Error(w, r, http.StatusConflict, "BucketNotEmpty", err.Error())
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
