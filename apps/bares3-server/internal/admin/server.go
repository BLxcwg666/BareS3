package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
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
	router.Use(httpx.RequestLogger(logger, "admin"))

	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteHTML(w, http.StatusOK, renderIndex(cfg))
	})

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "admin",
			"version": buildinfo.Current(),
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	router.Route("/api/v1", func(api chi.Router) {
		api.Get("/health", func(w http.ResponseWriter, r *http.Request) {
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"status":  "ok",
				"service": "admin-api",
				"version": buildinfo.Current(),
				"time":    time.Now().UTC().Format(time.RFC3339),
			})
		})

		api.Get("/runtime", func(w http.ResponseWriter, r *http.Request) {
			buckets, err := store.ListBuckets(r.Context())
			if err != nil {
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
					"status":  "error",
					"message": err.Error(),
				})
				return
			}

			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"app": map[string]any{
					"name": config.ProductName,
					"env":  cfg.App.Env,
				},
				"version": buildinfo.Current(),
				"config": map[string]any{
					"path": cfg.Runtime.ConfigPath,
					"used": cfg.Runtime.ConfigUsed,
					"base": cfg.Runtime.BaseDir,
				},
				"paths": map[string]any{
					"data_dir": cfg.Paths.DataDir,
					"log_dir":  cfg.Paths.LogDir,
					"tmp_dir":  cfg.Storage.TmpDir,
				},
				"listen": map[string]any{
					"admin": cfg.Listen.Admin,
					"s3":    cfg.Listen.S3,
					"file":  cfg.Listen.File,
				},
				"storage": map[string]any{
					"region":          cfg.Storage.Region,
					"public_base_url": cfg.Storage.PublicBaseURL,
					"metadata_layout": cfg.Storage.MetadataLayout,
					"bucket_count":    len(buckets),
				},
			})
		})

		api.Get("/buckets", func(w http.ResponseWriter, r *http.Request) {
			buckets, err := store.ListBuckets(r.Context())
			if err != nil {
				httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
					"status":  "error",
					"message": err.Error(),
				})
				return
			}
			httpx.WriteJSON(w, http.StatusOK, map[string]any{
				"items": buckets,
			})
		})

		api.Post("/buckets", func(w http.ResponseWriter, r *http.Request) {
			payload := struct {
				Name string `json:"name"`
			}{}

			decoder := json.NewDecoder(r.Body)
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&payload); err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
					"status":  "error",
					"message": "invalid request body",
				})
				return
			}

			bucket, err := store.CreateBucket(r.Context(), payload.Name)
			if err != nil {
				status := http.StatusInternalServerError
				switch {
				case errors.Is(err, storage.ErrInvalidBucketName):
					status = http.StatusBadRequest
				case errors.Is(err, storage.ErrBucketExists):
					status = http.StatusConflict
				}
				httpx.WriteJSON(w, status, map[string]any{
					"status":  "error",
					"message": err.Error(),
				})
				return
			}

			httpx.WriteJSON(w, http.StatusCreated, bucket)
		})
	})

	return router
}

func renderIndex(cfg config.Config) string {
	info := buildinfo.Current()

	configPath := cfg.Runtime.ConfigPath
	if strings.TrimSpace(configPath) == "" {
		configPath = "(using built-in defaults; no config.yml found beside the executable)"
	}

	return fmt.Sprintf(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>%s admin</title>
    <style>
      body { font-family: "Segoe UI", sans-serif; margin: 40px; color: #1f231f; background: #f3f0e9; }
      main { max-width: 880px; }
      h1 { margin-bottom: 8px; }
      code { background: #faf7f0; padding: 2px 6px; border-radius: 6px; }
      ul { line-height: 1.7; }
      .box { padding: 16px 18px; background: #faf7f0; border: 1px solid #d8d3c8; border-radius: 8px; }
    </style>
  </head>
  <body>
    <main>
      <h1>%s admin</h1>
      <p>Backend skeleton is running. The polished frontend can be wired into this port next.</p>
      <div class="box">
        <ul>
          <li>version: <code>%s</code></li>
          <li>config: <code>%s</code></li>
          <li>data dir: <code>%s</code></li>
          <li>log dir: <code>%s</code></li>
          <li>admin listen: <code>%s</code></li>
          <li>s3 listen: <code>%s</code></li>
          <li>file listen: <code>%s</code></li>
        </ul>
      </div>
      <p>Useful endpoints: <code>/healthz</code>, <code>/api/v1/health</code>, <code>/api/v1/runtime</code></p>
    </main>
  </body>
</html>`, config.ProductName, config.ProductName, info.String(), configPath, cfg.Paths.DataDir, cfg.Paths.LogDir, cfg.Listen.Admin, cfg.Listen.S3, cfg.Listen.File)
}
