package fileserve

import (
	"errors"
	"fmt"
	"mime"
	"net/http"
	"path"
	"time"

	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/httpx"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

func NewHandler(cfg config.Config, store *storage.Store, logger *zap.Logger) http.Handler {
	shareLinks, err := sharelink.New(cfg.Paths.DataDir, logger.Named("sharelink"))
	if err != nil {
		panic(fmt.Sprintf("initialize share link store: %v", err))
	}

	router := chi.NewRouter()
	router.Use(chiMiddleware.RequestID)
	router.Use(chiMiddleware.RealIP)
	router.Use(chiMiddleware.Recoverer)
	router.Use(httpx.RequestLogger(logger, "file"))

	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteText(w, http.StatusOK, fmt.Sprintf("%s file service\nversion: %s\npublic base URL: %s\n", config.ProductName, buildinfo.Current().Version, cfg.Storage.PublicBaseURL))
	})

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "file",
			"version": buildinfo.Current(),
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	router.Route("/pub", func(r chi.Router) {
		r.Get("/{bucket}/*", func(w http.ResponseWriter, r *http.Request) {
			serveRouteObject(w, r, store)
		})
		r.Head("/{bucket}/*", func(w http.ResponseWriter, r *http.Request) {
			serveRouteObject(w, r, store)
		})
	})
	router.Route("/dl", func(r chi.Router) {
		r.Get("/{id}", func(w http.ResponseWriter, r *http.Request) {
			serveShareLinkObject(w, r, store, shareLinks, true)
		})
		r.Head("/{id}", func(w http.ResponseWriter, r *http.Request) {
			serveShareLinkObject(w, r, store, shareLinks, true)
		})
	})
	router.Route("/s", func(r chi.Router) {
		r.Get("/{id}", func(w http.ResponseWriter, r *http.Request) {
			serveShareLinkObject(w, r, store, shareLinks, false)
		})
		r.Head("/{id}", func(w http.ResponseWriter, r *http.Request) {
			serveShareLinkObject(w, r, store, shareLinks, false)
		})
	})

	return router
}

func serveRouteObject(w http.ResponseWriter, r *http.Request, store *storage.Store) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	bucketInfo, err := store.GetBucket(r.Context(), bucket)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, storage.ErrBucketNotFound):
			status = http.StatusNotFound
		case errors.Is(err, storage.ErrInvalidBucketName):
			status = http.StatusBadRequest
		}
		httpx.WriteJSON(w, status, map[string]any{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	if !storage.IsBucketPublicAccess(bucketInfo.AccessMode) {
		httpx.WriteJSON(w, http.StatusForbidden, map[string]any{
			"status":  "error",
			"message": "bucket does not allow public access",
		})
		return
	}
	serveObject(w, r, store, bucket, key, "")
}

func serveShareLinkObject(w http.ResponseWriter, r *http.Request, store *storage.Store, shareLinks *sharelink.Store, forceDownload bool) {
	link, err := shareLinks.GetActive(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, sharelink.ErrInvalidID):
			status = http.StatusBadRequest
		case errors.Is(err, sharelink.ErrNotFound):
			status = http.StatusNotFound
		case errors.Is(err, sharelink.ErrExpired), errors.Is(err, sharelink.ErrRevoked):
			status = http.StatusGone
		}
		httpx.WriteJSON(w, status, map[string]any{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}

	contentDisposition := ""
	if forceDownload {
		contentDisposition = mime.FormatMediaType("attachment", map[string]string{"filename": link.Filename})
		if contentDisposition == "" {
			contentDisposition = `attachment; filename="` + path.Base(link.Key) + `"`
		}
	}

	serveObject(w, r, store, link.Bucket, link.Key, contentDisposition)
}

func serveObject(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key, contentDisposition string) {
	if bucket == "" || key == "" {
		httpx.WriteJSON(w, http.StatusNotFound, map[string]any{
			"status":  "error",
			"message": "bucket and key are required",
		})
		return
	}

	file, object, err := store.OpenObject(r.Context(), bucket, key)
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, storage.ErrBucketNotFound), errors.Is(err, storage.ErrObjectNotFound):
			status = http.StatusNotFound
		case errors.Is(err, storage.ErrInvalidBucketName), errors.Is(err, storage.ErrInvalidObjectKey):
			status = http.StatusBadRequest
		}
		httpx.WriteJSON(w, status, map[string]any{
			"status":  "error",
			"message": err.Error(),
		})
		return
	}
	defer func() {
		_ = file.Close()
	}()

	if object.ContentType != "" {
		w.Header().Set("Content-Type", object.ContentType)
	}
	if object.CacheControl != "" {
		w.Header().Set("Cache-Control", object.CacheControl)
	}
	if contentDisposition != "" {
		w.Header().Set("Content-Disposition", contentDisposition)
	} else if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	if object.ETag != "" {
		w.Header().Set("ETag", `"`+object.ETag+`"`)
	}

	http.ServeContent(w, r, path.Base(object.Key), object.LastModified, file)
}
