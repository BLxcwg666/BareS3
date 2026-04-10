package s3api

import (
	"errors"
	"net/http"
	"strings"

	"bares3-server/internal/config"
	"bares3-server/internal/s3creds"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/sigv4"
	"bares3-server/internal/storage"
)

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
