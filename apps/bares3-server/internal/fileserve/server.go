package fileserve

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"

	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/httpx"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

func NewHandler(cfg config.Config, store *storage.Store, logger *zap.Logger) http.Handler {
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
			serveObject(w, r, store)
		})
	})
	router.Route("/dl", func(r chi.Router) {
		r.Get("/*", notImplemented("download aliases are not wired yet"))
	})
	router.Route("/s", func(r chi.Router) {
		r.Get("/*", notImplemented("signed links are not wired yet"))
	})

	return router
}

func serveObject(w http.ResponseWriter, r *http.Request, store *storage.Store) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
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
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	if object.ETag != "" {
		w.Header().Set("ETag", `"`+object.ETag+`"`)
	}

	http.ServeContent(w, r, path.Base(object.Key), object.LastModified, file)
}

func notImplemented(message string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusNotImplemented, map[string]any{
			"status":  "not_implemented",
			"message": message,
			"path":    r.URL.Path,
		})
	}
}
