package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/httpx"
	"bares3-server/internal/sigv4"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

func NewHandler(cfg config.Config, store *storage.Store, logger *zap.Logger) http.Handler {
	manager, err := consoleauth.NewManager(consoleauth.Options{
		Username:      cfg.Auth.Console.Username,
		PasswordHash:  cfg.Auth.Console.PasswordHash,
		SessionSecret: cfg.Auth.Console.SessionSecret,
		TTL:           time.Duration(cfg.Auth.Console.SessionTTLMinutes) * time.Minute,
	})
	if err != nil {
		panic(fmt.Sprintf("initialize console auth manager: %v", err))
	}

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

		api.Route("/auth", func(auth chi.Router) {
			auth.Post("/login", func(w http.ResponseWriter, r *http.Request) {
				payload := struct {
					Username string `json:"username"`
					Password string `json:"password"`
				}{}

				decoder := json.NewDecoder(r.Body)
				decoder.DisallowUnknownFields()
				if err := decoder.Decode(&payload); err != nil {
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": "invalid request body"})
					return
				}

				session, err := manager.Authenticate(strings.TrimSpace(payload.Username), payload.Password)
				if err != nil {
					httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{"status": "error", "message": "invalid credentials"})
					return
				}

				cookie, err := manager.IssueCookie(session)
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": "failed to issue session"})
					return
				}
				http.SetCookie(w, cookie)
				httpx.WriteJSON(w, http.StatusOK, map[string]any{
					"username":   session.Username,
					"expires_at": session.ExpiresAt,
				})
			})

			logoutHandler := func(w http.ResponseWriter, r *http.Request) {
				http.SetCookie(w, manager.ClearCookie())
				w.WriteHeader(http.StatusNoContent)
			}
			auth.Post("/logout", logoutHandler)
			auth.Get("/logout", logoutHandler)

			auth.Get("/me", func(w http.ResponseWriter, r *http.Request) {
				session, err := manager.SessionFromRequest(r)
				if err != nil {
					httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{"status": "error", "message": "not authenticated"})
					return
				}
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"username": session.Username, "expires_at": session.ExpiresAt})
			})
		})

		api.Group(func(protected chi.Router) {
			protected.Use(requireSession(manager))

			protected.Get("/runtime", func(w http.ResponseWriter, r *http.Request) {
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
						"s3_base_url":     cfg.Storage.S3BaseURL,
						"metadata_layout": cfg.Storage.MetadataLayout,
						"bucket_count":    len(buckets),
					},
				})
			})

			protected.Get("/buckets", func(w http.ResponseWriter, r *http.Request) {
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

			protected.Post("/buckets", func(w http.ResponseWriter, r *http.Request) {
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
					writeStorageError(w, err)
					return
				}

				httpx.WriteJSON(w, http.StatusCreated, bucket)
			})

			protected.Delete("/buckets/{bucket}", func(w http.ResponseWriter, r *http.Request) {
				if err := store.DeleteBucket(r.Context(), chi.URLParam(r, "bucket")); err != nil {
					writeStorageError(w, err)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})

			protected.Get("/buckets/{bucket}", func(w http.ResponseWriter, r *http.Request) {
				bucket, err := store.GetBucket(r.Context(), chi.URLParam(r, "bucket"))
				if err != nil {
					writeStorageError(w, err)
					return
				}
				httpx.WriteJSON(w, http.StatusOK, bucket)
			})

			protected.Get("/buckets/{bucket}/objects", func(w http.ResponseWriter, r *http.Request) {
				limit := 0
				if rawLimit := strings.TrimSpace(r.URL.Query().Get("limit")); rawLimit != "" {
					parsed, err := strconv.Atoi(rawLimit)
					if err != nil || parsed < 0 {
						httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
							"status":  "error",
							"message": "limit must be a non-negative integer",
						})
						return
					}
					limit = parsed
				}

				items, err := store.ListObjects(r.Context(), chi.URLParam(r, "bucket"), storage.ListObjectsOptions{
					Prefix: r.URL.Query().Get("prefix"),
					Limit:  limit,
				})
				if err != nil {
					writeStorageError(w, err)
					return
				}
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
			})

			protected.Post("/buckets/{bucket}/objects", func(w http.ResponseWriter, r *http.Request) {
				if err := r.ParseMultipartForm(64 << 20); err != nil {
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
						"status":  "error",
						"message": "invalid multipart form",
					})
					return
				}

				file, header, err := r.FormFile("file")
				if err != nil {
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
						"status":  "error",
						"message": "file field is required",
					})
					return
				}
				defer func() {
					_ = file.Close()
				}()

				key := strings.TrimSpace(r.FormValue("key"))
				if key == "" && header != nil {
					key = header.Filename
				}

				object, err := store.PutObject(r.Context(), storage.PutObjectInput{
					Bucket:             chi.URLParam(r, "bucket"),
					Key:                key,
					Body:               file,
					ContentType:        resolveUploadContentType(r, header),
					CacheControl:       strings.TrimSpace(r.FormValue("cache_control")),
					ContentDisposition: strings.TrimSpace(r.FormValue("content_disposition")),
					UserMetadata:       collectMetadataFields(r.MultipartForm.Value),
				})
				if err != nil {
					writeStorageError(w, err)
					return
				}
				httpx.WriteJSON(w, http.StatusCreated, object)
			})

			protected.Delete("/buckets/{bucket}/objects/*", func(w http.ResponseWriter, r *http.Request) {
				if err := store.DeleteObject(r.Context(), chi.URLParam(r, "bucket"), chi.URLParam(r, "*")); err != nil {
					writeStorageError(w, err)
					return
				}
				w.WriteHeader(http.StatusNoContent)
			})

			protected.Get("/buckets/{bucket}/objects/*", func(w http.ResponseWriter, r *http.Request) {
				object, err := store.StatObject(r.Context(), chi.URLParam(r, "bucket"), chi.URLParam(r, "*"))
				if err != nil {
					writeStorageError(w, err)
					return
				}
				httpx.WriteJSON(w, http.StatusOK, object)
			})

			protected.Post("/presign/s3", func(w http.ResponseWriter, r *http.Request) {
				payload := struct {
					Method         string `json:"method"`
					Bucket         string `json:"bucket"`
					Key            string `json:"key"`
					ExpiresSeconds int    `json:"expires_seconds"`
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

				baseURL, err := url.Parse(cfg.Storage.S3BaseURL)
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": "invalid storage.s3_base_url configuration",
					})
					return
				}

				requestPath := "/" + strings.TrimPrefix(strings.TrimSpace(payload.Bucket), "/")
				if key := strings.TrimPrefix(strings.TrimSpace(payload.Key), "/"); key != "" {
					requestPath += "/" + key
				}
				baseURL.Path = requestPath

				verifier := sigv4.NewVerifier(cfg.Auth.S3.AccessKeyID, cfg.Auth.S3.SecretAccessKey, cfg.Storage.Region, "s3")
				result, err := verifier.Presign(sigv4.PresignInput{
					Method:  payload.Method,
					URL:     baseURL,
					Expires: time.Duration(payload.ExpiresSeconds) * time.Second,
				})
				if err != nil {
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}

				httpx.WriteJSON(w, http.StatusOK, result)
			})
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

func requireSession(manager *consoleauth.Manager) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, err := manager.SessionFromRequest(r); err != nil {
				httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{"status": "error", "message": "not authenticated"})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func writeStorageError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, storage.ErrInvalidBucketName), errors.Is(err, storage.ErrInvalidObjectKey):
		status = http.StatusBadRequest
	case errors.Is(err, storage.ErrBucketExists):
		status = http.StatusConflict
	case errors.Is(err, storage.ErrBucketNotEmpty):
		status = http.StatusConflict
	case errors.Is(err, storage.ErrBucketNotFound), errors.Is(err, storage.ErrObjectNotFound):
		status = http.StatusNotFound
	}

	httpx.WriteJSON(w, status, map[string]any{
		"status":  "error",
		"message": err.Error(),
	})
}

func resolveUploadContentType(r *http.Request, header *multipart.FileHeader) string {
	if value := strings.TrimSpace(r.FormValue("content_type")); value != "" {
		return value
	}
	if header == nil {
		return ""
	}
	return strings.TrimSpace(header.Header.Get("Content-Type"))
}

func collectMetadataFields(values map[string][]string) map[string]string {
	if len(values) == 0 {
		return nil
	}

	metadata := make(map[string]string)
	for key, entries := range values {
		if !strings.HasPrefix(key, "meta.") || len(entries) == 0 {
			continue
		}
		name := strings.TrimSpace(strings.TrimPrefix(key, "meta."))
		if name == "" {
			continue
		}
		metadata[name] = entries[0]
	}

	if len(metadata) == 0 {
		return nil
	}
	return metadata
}
