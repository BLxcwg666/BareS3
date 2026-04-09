package s3api

import (
	"context"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
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
	"bares3-server/internal/s3creds"
	"bares3-server/internal/s3xml"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/sigv4"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
)

const s3Namespace = "http://s3.amazonaws.com/doc/2006-03-01/"

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

type listObjectsResult struct {
	XMLName        xml.Name            `xml:"ListBucketResult"`
	Xmlns          string              `xml:"xmlns,attr"`
	Name           string              `xml:"Name"`
	Prefix         string              `xml:"Prefix"`
	Marker         string              `xml:"Marker,omitempty"`
	NextMarker     string              `xml:"NextMarker,omitempty"`
	MaxKeys        int                 `xml:"MaxKeys"`
	Delimiter      string              `xml:"Delimiter,omitempty"`
	IsTruncated    bool                `xml:"IsTruncated"`
	Contents       []listObjectEntry   `xml:"Contents"`
	CommonPrefixes []commonPrefixEntry `xml:"CommonPrefixes,omitempty"`
	EncodingType   string              `xml:"EncodingType,omitempty"`
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name               `xml:"ListMultipartUploadsResult"`
	Xmlns              string                 `xml:"xmlns,attr"`
	Bucket             string                 `xml:"Bucket"`
	KeyMarker          string                 `xml:"KeyMarker"`
	UploadIDMarker     string                 `xml:"UploadIdMarker"`
	NextKeyMarker      string                 `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string                 `xml:"NextUploadIdMarker,omitempty"`
	Prefix             string                 `xml:"Prefix"`
	Delimiter          string                 `xml:"Delimiter,omitempty"`
	MaxUploads         int                    `xml:"MaxUploads"`
	IsTruncated        bool                   `xml:"IsTruncated"`
	Uploads            []multipartUploadEntry `xml:"Upload,omitempty"`
	CommonPrefixes     []commonPrefixEntry    `xml:"CommonPrefixes,omitempty"`
	EncodingType       string                 `xml:"EncodingType,omitempty"`
}

type multipartUploadEntry struct {
	Key          string    `xml:"Key"`
	UploadID     string    `xml:"UploadId"`
	Initiator    ownerInfo `xml:"Initiator"`
	Owner        ownerInfo `xml:"Owner"`
	StorageClass string    `xml:"StorageClass"`
	Initiated    string    `xml:"Initiated"`
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

type bucketLocationConstraint struct {
	XMLName xml.Name `xml:"LocationConstraint"`
	Xmlns   string   `xml:"xmlns,attr,omitempty"`
	Value   string   `xml:",chardata"`
}

type deleteObjectsRequest struct {
	XMLName xml.Name                   `xml:"Delete"`
	Quiet   bool                       `xml:"Quiet"`
	Objects []deleteObjectsRequestItem `xml:"Object"`
}

type deleteObjectsRequestItem struct {
	Key string `xml:"Key"`
}

type deleteObjectsResult struct {
	XMLName xml.Name                 `xml:"DeleteResult"`
	Xmlns   string                   `xml:"xmlns,attr"`
	Deleted []deleteObjectsDeleted   `xml:"Deleted,omitempty"`
	Errors  []deleteObjectsErrorItem `xml:"Error,omitempty"`
}

type deleteObjectsDeleted struct {
	Key string `xml:"Key"`
}

type deleteObjectsErrorItem struct {
	Key     string `xml:"Key,omitempty"`
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	Xmlns        string   `xml:"xmlns,attr,omitempty"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag,omitempty"`
}

type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	Xmlns        string   `xml:"xmlns,attr,omitempty"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag,omitempty"`
}

type listResponseItem struct {
	Kind   string
	Value  string
	Object storage.ObjectInfo
}

type multipartListItem struct {
	Kind   string
	Value  string
	Upload storage.MultipartUploadInfo
}

func NewHandler(cfg config.Config, store *storage.Store, credentials *s3creds.Store, logger *zap.Logger) http.Handler {
	return newHandler(cfg, store, nil, credentials, logger)
}

func newHandler(cfg config.Config, store *storage.Store, shareLinks *sharelink.Store, credentials *s3creds.Store, logger *zap.Logger) http.Handler {
	if credentials == nil {
		var err error
		credentials, err = s3creds.New(cfg.Paths.DataDir, s3creds.BootstrapCredential{
			AccessKeyID:     cfg.Auth.S3.AccessKeyID,
			SecretAccessKey: cfg.Auth.S3.SecretAccessKey,
		}, logger.Named("s3creds"))
		if err != nil {
			panic(fmt.Sprintf("initialize s3 credential store: %v", err))
		}
	}
	verifier := sigv4.NewVerifierWithLookup(func(accessKeyID string) (string, bool) {
		secret, err := credentials.LookupSecret(context.Background(), accessKeyID)
		if err != nil {
			return "", false
		}
		return secret, true
	}, cfg.Auth.S3.AccessKeyID, cfg.Auth.S3.SecretAccessKey, cfg.Storage.Region, "s3")
	if shareLinks == nil {
		var err error
		shareLinks, err = sharelink.New(cfg.Paths.DataDir, logger.Named("sharelink"))
		if err != nil {
			panic(fmt.Sprintf("initialize share link store: %v", err))
		}
	}

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
	mux.Handle("/readyz", httpx.ReadyHandler("s3",
		httpx.ReadinessCheck{Name: "storage", Check: store.Check},
		httpx.ReadinessCheck{Name: "share_links", Check: shareLinks.Check},
		httpx.ReadinessCheck{Name: "s3_credentials", Check: credentials.Check},
	))

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		handleS3Request(w, r, cfg, store, shareLinks, credentials, verifier)
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSpace(cfg.Storage.Region) != "" {
			w.Header().Set("X-Amz-Bucket-Region", cfg.Storage.Region)
		}
		mux.ServeHTTP(w, r)
	})

	return httpx.RequestLogger(logger, "s3")(handler)
}

func handleS3Request(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, shareLinks *sharelink.Store, credentials *s3creds.Store, verifier *sigv4.Verifier) {
	identity, err := verifier.Authenticate(r)
	if err != nil {
		writeAuthError(w, r, err)
		return
	}
	credential, err := credentials.GetActive(r.Context(), identity.AccessKeyID)
	if err != nil {
		writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "request authentication failed")
		return
	}
	if err := credentials.Touch(r.Context(), identity.AccessKeyID); err != nil {
		writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	if r.URL.Path == "/" || strings.TrimSpace(r.URL.Path) == "" {
		handleServiceRoot(w, r, store, credential)
		return
	}

	bucket, key := splitPath(r.URL.Path)
	if bucket == "" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidURI", "bucket name is required")
		return
	}

	if !authorizeBucketRequest(w, r, credential, bucket, requestRequiresWrite(r, key)) {
		return
	}

	if key == "" {
		handleBucketRequest(w, r, store, shareLinks, bucket)
		return
	}

	handleObjectRequest(w, r, cfg, store, shareLinks, credential, bucket, key)
}

func handleServiceRoot(w http.ResponseWriter, r *http.Request, store *storage.Store, credential s3creds.Credential) {
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
		if !credential.AllowsBucket(bucket.Name) {
			continue
		}
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

func requestRequiresWrite(r *http.Request, key string) bool {
	if strings.TrimSpace(key) == "" {
		if hasQueryValue(r, "delete") && r.Method == http.MethodPost {
			return true
		}
		switch r.Method {
		case http.MethodPut, http.MethodDelete:
			return true
		default:
			return false
		}
	}
	if hasQueryValue(r, "uploadId") || hasQueryValue(r, "uploads") {
		return true
	}
	switch r.Method {
	case http.MethodPut, http.MethodPost, http.MethodDelete:
		return true
	default:
		return false
	}
}

func authorizeBucketRequest(w http.ResponseWriter, r *http.Request, credential s3creds.Credential, bucket string, write bool) bool {
	if !credential.AllowsBucket(bucket) {
		writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "bucket is outside this access key scope")
		return false
	}
	if !credential.AllowsOperation(write) {
		writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "access key is read-only")
		return false
	}
	return true
}

func handleBucketRequest(w http.ResponseWriter, r *http.Request, store *storage.Store, shareLinks *sharelink.Store, bucket string) {
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
		if hasQueryValue(r, "location") {
			handleGetBucketLocation(w, r, store, bucket)
			return
		}
		if hasQueryValue(r, "uploads") {
			handleListMultipartUploads(w, r, store, bucket)
			return
		}
		if r.URL.Query().Get("list-type") == "2" {
			handleListObjectsV2(w, r, store, bucket)
			return
		}
		handleListObjects(w, r, store, bucket)
	case http.MethodPost:
		if hasQueryValue(r, "delete") {
			handleDeleteObjects(w, r, store, shareLinks, bucket)
			return
		}
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported for bucket requests")
	case http.MethodDelete:
		if err := store.DeleteBucket(r.Context(), bucket); err != nil {
			writeStorageAsS3Error(w, r, err)
			return
		}
		if _, err := shareLinks.RemoveByBucket(r.Context(), bucket); err != nil {
			writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported for bucket requests")
	}
}

func handleGetBucketLocation(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	if _, err := store.GetBucket(r.Context(), bucket); err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}
	writeS3XML(w, http.StatusOK, bucketLocationConstraint{
		Xmlns: s3Namespace,
		Value: w.Header().Get("X-Amz-Bucket-Region"),
	})
}

func handleDeleteObjects(w http.ResponseWriter, r *http.Request, store *storage.Store, shareLinks *sharelink.Store, bucket string) {
	if _, err := store.GetBucket(r.Context(), bucket); err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	requestPayload := deleteObjectsRequest{}
	if err := xml.NewDecoder(r.Body).Decode(&requestPayload); err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "MalformedXML", "delete objects body is invalid")
		return
	}

	result := deleteObjectsResult{Xmlns: s3Namespace}
	for _, item := range requestPayload.Objects {
		key := strings.TrimSpace(item.Key)
		if key == "" {
			result.Errors = append(result.Errors, deleteObjectsErrorItem{
				Code:    "InvalidArgument",
				Message: "object key is required",
			})
			continue
		}

		err := store.DeleteObject(r.Context(), bucket, key)
		switch {
		case err == nil:
		case errors.Is(err, storage.ErrObjectNotFound):
		case errors.Is(err, storage.ErrInvalidObjectKey):
			result.Errors = append(result.Errors, deleteObjectsErrorItem{
				Key:     key,
				Code:    "InvalidArgument",
				Message: err.Error(),
			})
			continue
		default:
			writeStorageAsS3Error(w, r, err)
			return
		}

		if _, err := shareLinks.RemoveByObject(r.Context(), bucket, key); err != nil {
			writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		if !requestPayload.Quiet {
			result.Deleted = append(result.Deleted, deleteObjectsDeleted{Key: key})
		}
	}

	writeS3XML(w, http.StatusOK, result)
}

func handleListMultipartUploads(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	maxUploads, ok := parseBoundedIntQuery(w, r, "max-uploads", 1000, "max-uploads must be a non-negative integer")
	if !ok {
		return
	}

	delimiter := r.URL.Query().Get("delimiter")
	keyMarker := r.URL.Query().Get("key-marker")
	uploadIDMarker := r.URL.Query().Get("upload-id-marker")
	encodingType := strings.TrimSpace(r.URL.Query().Get("encoding-type"))
	if encodingType != "" && encodingType != "url" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "encoding-type must be url")
		return
	}

	uploads, err := store.ListMultipartUploads(r.Context(), bucket)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	result, err := buildListMultipartUploadsResult(bucket, uploads, r.URL.Query().Get("prefix"), delimiter, keyMarker, uploadIDMarker, encodingType, maxUploads)
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	writeS3XML(w, http.StatusOK, result)
}

func handleListObjects(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	maxKeys, ok := parseMaxKeys(w, r)
	if !ok {
		return
	}

	delimiter := r.URL.Query().Get("delimiter")
	marker := r.URL.Query().Get("marker")
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

	result, err := buildListObjectsResult(bucket, items, r.URL.Query().Get("prefix"), delimiter, marker, encodingType, maxKeys)
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	writeS3XML(w, http.StatusOK, result)
}

func handleListObjectsV2(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket string) {
	maxKeys, ok := parseMaxKeys(w, r)
	if !ok {
		return
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

func handleObjectRequest(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, shareLinks *sharelink.Store, credential s3creds.Credential, bucket, key string) {
	if hasQueryValue(r, "uploadId") {
		handleMultipartUploadRequest(w, r, cfg, store, credential, bucket, key)
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
		if strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source")) != "" {
			handleCopyObject(w, r, store, credential, bucket, key)
			return
		}
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
		if !authorizeObjectRead(w, r, store, bucket, key) {
			return
		}
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
		if _, err := shareLinks.RemoveByObject(r.Context(), bucket, key); err != nil {
			writeS3Error(w, r, http.StatusInternalServerError, "InternalError", err.Error())
			return
		}
		w.WriteHeader(http.StatusNoContent)
	default:
		writeS3Error(w, r, http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported for object requests")
	}
}

func handleCopyObject(w http.ResponseWriter, r *http.Request, store *storage.Store, credential s3creds.Credential, bucket, key string) {
	sourceBucket, sourceKey, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	if !authorizeBucketRequest(w, r, credential, sourceBucket, false) {
		return
	}
	if !authorizeObjectRead(w, r, store, sourceBucket, sourceKey) {
		return
	}

	sourceFile, sourceObject, err := store.OpenObject(r.Context(), sourceBucket, sourceKey)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}
	defer func() {
		_ = sourceFile.Close()
	}()

	if spec := evaluateCopySourcePreconditions(r, sourceObject); spec != nil {
		writeS3Error(w, r, spec.Status, spec.Code, spec.Message)
		return
	}

	metadataDirective := strings.ToUpper(strings.TrimSpace(r.Header.Get("X-Amz-Metadata-Directive")))
	if metadataDirective == "" {
		metadataDirective = "COPY"
	}
	if metadataDirective != "COPY" && metadataDirective != "REPLACE" {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", "x-amz-metadata-directive must be COPY or REPLACE")
		return
	}

	putInput := storage.PutObjectInput{
		Bucket: bucket,
		Key:    key,
		Body:   sourceFile,
	}
	if metadataDirective == "COPY" {
		putInput.ContentType = sourceObject.ContentType
		putInput.CacheControl = sourceObject.CacheControl
		putInput.ContentDisposition = sourceObject.ContentDisposition
		putInput.UserMetadata = cloneMetadataMap(sourceObject.UserMetadata)
	} else {
		putInput.ContentType = firstNonEmpty(strings.TrimSpace(r.Header.Get("Content-Type")), sourceObject.ContentType)
		putInput.CacheControl = firstNonEmpty(strings.TrimSpace(r.Header.Get("Cache-Control")), sourceObject.CacheControl)
		putInput.ContentDisposition = firstNonEmpty(strings.TrimSpace(r.Header.Get("Content-Disposition")), sourceObject.ContentDisposition)
		putInput.UserMetadata = mergeMetadataDirective(sourceObject.UserMetadata, extractUserMetadata(r.Header))
	}

	object, err := store.PutObject(r.Context(), putInput)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	writeS3XML(w, http.StatusOK, copyObjectResult{
		Xmlns:        s3Namespace,
		LastModified: formatS3Time(object.LastModified),
		ETag:         `"` + object.ETag + `"`,
	})
}

func authorizeObjectRead(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key string) bool {
	action, err := store.ResolveBucketObjectAccess(r.Context(), bucket, key)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return false
	}
	if action == storage.BucketAccessActionDeny {
		writeS3Error(w, r, http.StatusForbidden, "AccessDenied", "object access denied by bucket policy")
		return false
	}
	return true
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

func parseCopySource(value string) (bucket string, key string, err error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return "", "", fmt.Errorf("x-amz-copy-source is required")
	}
	trimmed = strings.TrimPrefix(trimmed, "/")
	pathValue, rawQuery, _ := strings.Cut(trimmed, "?")
	if rawQuery != "" {
		return "", "", fmt.Errorf("copy source subresources are not supported")
	}
	encodedBucket, encodedKey, ok := strings.Cut(pathValue, "/")
	if !ok || strings.TrimSpace(encodedBucket) == "" || strings.TrimSpace(encodedKey) == "" {
		return "", "", fmt.Errorf("x-amz-copy-source must be bucket/key")
	}
	bucket, err = url.PathUnescape(encodedBucket)
	if err != nil {
		return "", "", fmt.Errorf("x-amz-copy-source bucket is invalid")
	}
	key, err = url.PathUnescape(encodedKey)
	if err != nil {
		return "", "", fmt.Errorf("x-amz-copy-source key is invalid")
	}
	return bucket, key, nil
}

type s3ErrorSpec struct {
	Status  int
	Code    string
	Message string
}

type copyRangeSpec struct {
	Start  int64
	Length int64
}

func evaluateCopySourcePreconditions(r *http.Request, object storage.ObjectInfo) *s3ErrorSpec {
	if matchValue := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-If-Match")); matchValue != "" {
		if !etagListMatches(matchValue, object.ETag) {
			return &s3ErrorSpec{Status: http.StatusPreconditionFailed, Code: "PreconditionFailed", Message: "copy source precondition failed"}
		}
	} else if raw := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-If-Unmodified-Since")); raw != "" {
		timestamp, err := parseHTTPTimeHeader(raw)
		if err != nil {
			return &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: err.Error()}
		}
		if isModifiedSince(object.LastModified, timestamp) {
			return &s3ErrorSpec{Status: http.StatusPreconditionFailed, Code: "PreconditionFailed", Message: "copy source precondition failed"}
		}
	}

	if noneMatchValue := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-If-None-Match")); noneMatchValue != "" {
		if etagListMatches(noneMatchValue, object.ETag) {
			return &s3ErrorSpec{Status: http.StatusPreconditionFailed, Code: "PreconditionFailed", Message: "copy source precondition failed"}
		}
	} else if raw := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-If-Modified-Since")); raw != "" {
		timestamp, err := parseHTTPTimeHeader(raw)
		if err != nil {
			return &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: err.Error()}
		}
		if !isModifiedSince(object.LastModified, timestamp) {
			return &s3ErrorSpec{Status: http.StatusPreconditionFailed, Code: "PreconditionFailed", Message: "copy source precondition failed"}
		}
	}

	return nil
}

func parseCopySourceRange(value string, size int64) (copyRangeSpec, *s3ErrorSpec) {
	trimmed := strings.TrimSpace(value)
	if !strings.HasPrefix(trimmed, "bytes=") {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range must use bytes=start-end"}
	}
	startRaw, endRaw, ok := strings.Cut(strings.TrimPrefix(trimmed, "bytes="), "-")
	if !ok || strings.TrimSpace(startRaw) == "" || strings.TrimSpace(endRaw) == "" {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range must use bytes=start-end"}
	}
	start, err := strconv.ParseInt(strings.TrimSpace(startRaw), 10, 64)
	if err != nil || start < 0 {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range must use bytes=start-end"}
	}
	end, err := strconv.ParseInt(strings.TrimSpace(endRaw), 10, 64)
	if err != nil || end < start {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range must use bytes=start-end"}
	}
	if size <= 0 || start >= size || end >= size {
		return copyRangeSpec{}, &s3ErrorSpec{Status: http.StatusBadRequest, Code: "InvalidArgument", Message: "x-amz-copy-source-range is outside the source object"}
	}
	return copyRangeSpec{Start: start, Length: end - start + 1}, nil
}

func etagListMatches(headerValue, etag string) bool {
	normalizedETag := normalizeETag(etag)
	for _, candidate := range strings.Split(headerValue, ",") {
		normalizedCandidate := normalizeETag(candidate)
		if normalizedCandidate == "" {
			continue
		}
		if normalizedCandidate == "*" || normalizedCandidate == normalizedETag {
			return true
		}
	}
	return false
}

func normalizeETag(value string) string {
	trimmed := strings.TrimSpace(value)
	trimmed = strings.TrimPrefix(trimmed, "W/")
	trimmed = strings.TrimPrefix(trimmed, "w/")
	trimmed = strings.TrimSpace(trimmed)
	if trimmed == "*" {
		return trimmed
	}
	return strings.Trim(trimmed, `"`)
}

func parseHTTPTimeHeader(value string) (time.Time, error) {
	parsed, err := http.ParseTime(strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, fmt.Errorf("time header is invalid")
	}
	return parsed.UTC(), nil
}

func isModifiedSince(modifiedAt, timestamp time.Time) bool {
	if modifiedAt.IsZero() {
		return false
	}
	return modifiedAt.UTC().Truncate(time.Second).After(timestamp.UTC().Truncate(time.Second))
}

func cloneMetadataMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func mergeMetadataDirective(source, replacement map[string]string) map[string]string {
	_ = source
	if len(replacement) == 0 {
		return nil
	}
	return cloneMetadataMap(replacement)
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
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

func handleMultipartUploadRequest(w http.ResponseWriter, r *http.Request, cfg config.Config, store *storage.Store, credential s3creds.Credential, bucket, key string) {
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
		if strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source")) != "" {
			handleUploadPartCopy(w, r, store, credential, bucket, key, uploadID, partNumber)
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

func handleUploadPartCopy(w http.ResponseWriter, r *http.Request, store *storage.Store, credential s3creds.Credential, bucket, key, uploadID string, partNumber int) {
	sourceBucket, sourceKey, err := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if err != nil {
		writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", err.Error())
		return
	}
	if !authorizeBucketRequest(w, r, credential, sourceBucket, false) {
		return
	}
	if !authorizeObjectRead(w, r, store, sourceBucket, sourceKey) {
		return
	}

	sourceFile, sourceObject, err := store.OpenObject(r.Context(), sourceBucket, sourceKey)
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}
	defer func() {
		_ = sourceFile.Close()
	}()

	if spec := evaluateCopySourcePreconditions(r, sourceObject); spec != nil {
		writeS3Error(w, r, spec.Status, spec.Code, spec.Message)
		return
	}

	body := io.Reader(sourceFile)
	if rawRange := strings.TrimSpace(r.Header.Get("X-Amz-Copy-Source-Range")); rawRange != "" {
		rangeSpec, spec := parseCopySourceRange(rawRange, sourceObject.Size)
		if spec != nil {
			writeS3Error(w, r, spec.Status, spec.Code, spec.Message)
			return
		}
		body = io.NewSectionReader(sourceFile, rangeSpec.Start, rangeSpec.Length)
	}

	part, err := store.UploadPart(r.Context(), storage.UploadPartInput{
		Bucket:     bucket,
		Key:        key,
		UploadID:   uploadID,
		PartNumber: partNumber,
		Body:       body,
	})
	if err != nil {
		writeStorageAsS3Error(w, r, err)
		return
	}

	writeS3XML(w, http.StatusOK, copyPartResult{
		Xmlns:        s3Namespace,
		LastModified: formatS3Time(part.LastModified),
		ETag:         `"` + part.ETag + `"`,
	})
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

func buildListMultipartUploadsResult(bucket string, uploads []storage.MultipartUploadInfo, prefix, delimiter, keyMarker, uploadIDMarker, encodingType string, maxUploads int) (listMultipartUploadsResult, error) {
	items := buildMultipartUploadItems(uploads, prefix, delimiter)
	startIndex := findMultipartStartIndex(items, keyMarker, uploadIDMarker)
	remaining := items[startIndex:]
	emitted := remaining
	isTruncated := false
	if maxUploads >= 0 && len(emitted) > maxUploads {
		emitted = emitted[:maxUploads]
		isTruncated = true
	}

	result := listMultipartUploadsResult{
		Xmlns:          s3Namespace,
		Bucket:         bucket,
		KeyMarker:      encodeListValue(keyMarker, encodingType),
		UploadIDMarker: uploadIDMarker,
		Prefix:         encodeListValue(prefix, encodingType),
		Delimiter:      encodeListValue(delimiter, encodingType),
		MaxUploads:     maxUploads,
		IsTruncated:    isTruncated,
		EncodingType:   encodingType,
	}

	for _, item := range emitted {
		switch item.Kind {
		case "prefix":
			result.CommonPrefixes = append(result.CommonPrefixes, commonPrefixEntry{Prefix: encodeListValue(item.Value, encodingType)})
		case "upload":
			entry := multipartUploadEntry{
				Key:          encodeListValue(item.Upload.Key, encodingType),
				UploadID:     item.Upload.UploadID,
				Initiator:    ownerInfo{ID: "bares3", DisplayName: "BareS3"},
				Owner:        ownerInfo{ID: "bares3", DisplayName: "BareS3"},
				StorageClass: "STANDARD",
				Initiated:    formatS3Time(item.Upload.CreatedAt),
			}
			result.Uploads = append(result.Uploads, entry)
		}
	}

	if isTruncated && len(emitted) > 0 {
		last := emitted[len(emitted)-1]
		result.NextKeyMarker = encodeListValue(last.Value, encodingType)
		if last.Kind == "upload" {
			result.NextUploadIDMarker = last.Upload.UploadID
		}
	}

	return result, nil
}

func buildListObjectsResult(bucket string, objects []storage.ObjectInfo, prefix, delimiter, marker, encodingType string, maxKeys int) (listObjectsResult, error) {
	entries, err := buildListItems(objects, prefix, delimiter)
	if err != nil {
		return listObjectsResult{}, err
	}

	startIndex := 0
	if marker != "" {
		for startIndex < len(entries) && entries[startIndex].Value <= marker {
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

	result := listObjectsResult{
		Xmlns:        s3Namespace,
		Name:         bucket,
		Prefix:       encodeListValue(prefix, encodingType),
		Marker:       encodeListValue(marker, encodingType),
		MaxKeys:      maxKeys,
		Delimiter:    encodeListValue(delimiter, encodingType),
		IsTruncated:  isTruncated,
		EncodingType: encodingType,
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

	if isTruncated && delimiter != "" && len(emitted) > 0 {
		result.NextMarker = encodeListValue(emitted[len(emitted)-1].Value, encodingType)
	}

	return result, nil
}

func buildMultipartUploadItems(uploads []storage.MultipartUploadInfo, prefix, delimiter string) []multipartListItem {
	items := make([]multipartListItem, 0, len(uploads))
	seenPrefixes := make(map[string]struct{})
	for _, upload := range uploads {
		if prefix != "" && !strings.HasPrefix(upload.Key, prefix) {
			continue
		}
		if delimiter != "" {
			remainder := strings.TrimPrefix(upload.Key, prefix)
			if index := strings.Index(remainder, delimiter); index >= 0 {
				commonPrefix := prefix + remainder[:index+len(delimiter)]
				if _, ok := seenPrefixes[commonPrefix]; !ok {
					seenPrefixes[commonPrefix] = struct{}{}
					items = append(items, multipartListItem{Kind: "prefix", Value: commonPrefix})
				}
				continue
			}
		}
		items = append(items, multipartListItem{Kind: "upload", Value: upload.Key, Upload: upload})
	}

	sort.Slice(items, func(i, j int) bool {
		return compareMultipartListItems(items[i], items[j]) < 0
	})
	return items
}

func findMultipartStartIndex(items []multipartListItem, keyMarker, uploadIDMarker string) int {
	if strings.TrimSpace(keyMarker) == "" {
		return 0
	}
	startIndex := 0
	markerMatched := strings.TrimSpace(uploadIDMarker) == ""
	for startIndex < len(items) {
		item := items[startIndex]
		if item.Value < keyMarker {
			startIndex++
			continue
		}
		if item.Value > keyMarker {
			break
		}
		if item.Kind == "prefix" {
			startIndex++
			continue
		}
		if markerMatched {
			startIndex++
			continue
		}
		if item.Upload.UploadID == uploadIDMarker {
			markerMatched = true
			startIndex++
			continue
		}
		if item.Upload.UploadID <= uploadIDMarker {
			startIndex++
			continue
		}
		break
	}
	return startIndex
}

func compareMultipartListItems(left, right multipartListItem) int {
	if left.Value != right.Value {
		return strings.Compare(left.Value, right.Value)
	}
	if left.Kind != right.Kind {
		return strings.Compare(left.Kind, right.Kind)
	}
	if left.Kind != "upload" {
		return 0
	}
	if !left.Upload.CreatedAt.Equal(right.Upload.CreatedAt) {
		if left.Upload.CreatedAt.Before(right.Upload.CreatedAt) {
			return -1
		}
		return 1
	}
	return strings.Compare(left.Upload.UploadID, right.Upload.UploadID)
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

func parseBoundedIntQuery(w http.ResponseWriter, r *http.Request, name string, upperBound int, invalidMessage string) (int, bool) {
	value := upperBound
	if raw := strings.TrimSpace(r.URL.Query().Get(name)); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			writeS3Error(w, r, http.StatusBadRequest, "InvalidArgument", invalidMessage)
			return 0, false
		}
		value = parsed
	}
	if value > upperBound {
		value = upperBound
	}
	return value, true
}

func parseMaxKeys(w http.ResponseWriter, r *http.Request) (int, bool) {
	return parseBoundedIntQuery(w, r, "max-keys", 1000, "max-keys must be a non-negative integer")
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
	s3xml.Write(w, status, payload)
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
	bucket, _ := splitPath(r.URL.Path)
	s3xml.WriteError(w, r, status, s3xml.ErrorOptions{
		Code:       code,
		Message:    message,
		Region:     w.Header().Get("X-Amz-Bucket-Region"),
		BucketName: bucket,
	})
}
