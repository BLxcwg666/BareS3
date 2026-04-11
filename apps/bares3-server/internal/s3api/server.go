package s3api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
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

func NewHandler(cfg config.Config, store *storage.Store, credentials *s3creds.Store, logger *zap.Logger) http.Handler {
	return newHandler(cfg, store, nil, credentials, logger)
}

func newHandler(cfg config.Config, store *storage.Store, shareLinks *sharelink.Store, credentials *s3creds.Store, logger *zap.Logger) http.Handler {
	if credentials == nil {
		var err error
		credentials, err = s3creds.New(cfg.Paths.DataDir, logger.Named("s3creds"))
		if err != nil {
			panic(fmt.Sprintf("initialize s3 credential store: %v", err))
		}
	}
	lookupSecret := func(accessKeyID string) (string, bool) {
		secret, err := credentials.LookupSecret(context.Background(), accessKeyID)
		if err != nil {
			return "", false
		}
		return secret, true
	}
	if shareLinks == nil {
		var err error
		shareLinks, err = sharelink.New(cfg.Paths.DataDir, logger.Named("sharelink"))
		if err != nil {
			panic(fmt.Sprintf("initialize share link store: %v", err))
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "s3",
			"region":  runtimeSettings.Region,
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
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		verifier := sigv4.NewVerifierWithLookup(lookupSecret, runtimeSettings.Region, "s3")
		handleS3Request(w, r, cfg, store, shareLinks, credentials, verifier)
	})

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err == nil && strings.TrimSpace(runtimeSettings.Region) != "" {
			w.Header().Set("X-Amz-Bucket-Region", runtimeSettings.Region)
		}
		mux.ServeHTTP(w, r)
	})

	return httpx.RequestLogger(logger, "s3")(handler)
}

func writeS3XML(w http.ResponseWriter, status int, payload any) {
	s3xml.Write(w, status, payload)
}

func writeStorageAsS3Error(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, storage.ErrBucketNotFound):
		writeS3Error(w, r, http.StatusNotFound, "NoSuchBucket", err.Error())
	case errors.Is(err, storage.ErrObjectSyncing):
		writeS3Error(w, r, http.StatusServiceUnavailable, "ServiceUnavailable", err.Error())
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
