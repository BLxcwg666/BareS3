package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/httpx"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/sigv4"
	"bares3-server/internal/storage"
	"bares3-server/internal/webui"
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
	auditRecorder, err := auditlog.New(cfg.Paths.LogDir)
	if err != nil {
		panic(fmt.Sprintf("initialize audit recorder: %v", err))
	}
	shareLinks, err := sharelink.New(cfg.Paths.DataDir, logger.Named("sharelink"))
	if err != nil {
		panic(fmt.Sprintf("initialize share link store: %v", err))
	}

	router := chi.NewRouter()
	uiHandler := webui.NewHandler()
	router.Use(chiMiddleware.RequestID)
	router.Use(chiMiddleware.RealIP)
	router.Use(chiMiddleware.Recoverer)
	router.Use(httpx.RequestLogger(logger, "admin"))

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "admin",
			"version": buildinfo.Current(),
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	router.Route("/api/v1", func(api chi.Router) {
		api.NotFound(func(w http.ResponseWriter, r *http.Request) {
			httpx.WriteJSON(w, http.StatusNotFound, map[string]any{"status": "error", "message": "not found"})
		})

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
				username := strings.TrimSpace(payload.Username)

				session, err := manager.Authenticate(username, payload.Password)
				if err != nil {
					recordAudit(logger, auditRecorder, auditlog.Entry{
						Actor:  username,
						Action: "auth.login",
						Title:  fmt.Sprintf("Failed sign-in for %s", fallbackActor(username)),
						Detail: "Invalid credentials",
						Target: username,
						Remote: requestRemote(r),
						Status: "failed",
					})
					httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{"status": "error", "message": "invalid credentials"})
					return
				}

				cookie, err := manager.IssueCookie(session)
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": "failed to issue session"})
					return
				}
				http.SetCookie(w, cookie)
				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  session.Username,
					Action: "auth.login",
					Title:  "Signed in to console",
					Detail: fmt.Sprintf("Session active until %s", session.ExpiresAt.UTC().Format(time.RFC3339)),
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusOK, map[string]any{
					"username":   session.Username,
					"expires_at": session.ExpiresAt,
				})
			})

			logoutHandler := func(w http.ResponseWriter, r *http.Request) {
				if session, err := manager.SessionFromRequest(r); err == nil {
					recordAudit(logger, auditRecorder, auditlog.Entry{
						Actor:  session.Username,
						Action: "auth.logout",
						Title:  "Signed out of console",
						Remote: requestRemote(r),
						Status: "success",
					})
				}
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

			protected.Get("/audit/events", func(w http.ResponseWriter, r *http.Request) {
				limit := 10
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

				items, err := auditRecorder.Recent(limit)
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}

				httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
			})

			protected.Get("/runtime", func(w http.ResponseWriter, r *http.Request) {
				buckets, err := store.ListBuckets(r.Context())
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}

				usedBytes := int64(0)
				for _, bucket := range buckets {
					usedBytes += bucket.UsedBytes
				}
				activeLinkCount, err := shareLinks.ActiveCount(r.Context())
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
						"region":            cfg.Storage.Region,
						"public_base_url":   cfg.Storage.PublicBaseURL,
						"s3_base_url":       cfg.Storage.S3BaseURL,
						"metadata_layout":   cfg.Storage.MetadataLayout,
						"max_bytes":         store.InstanceQuotaBytes(),
						"used_bytes":        usedBytes,
						"bucket_count":      len(buckets),
						"active_link_count": activeLinkCount,
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
					Name       string `json:"name"`
					QuotaBytes int64  `json:"quota_bytes"`
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

				bucket, err := store.CreateBucket(r.Context(), payload.Name, payload.QuotaBytes)
				if err != nil {
					writeStorageError(w, err)
					return
				}
				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "bucket.create",
					Title:  fmt.Sprintf("Created bucket %s", bucket.Name),
					Detail: fmt.Sprintf("Quota %s", quotaLabel(bucket.QuotaBytes)),
					Target: bucket.Name,
					Remote: requestRemote(r),
					Status: "success",
				})

				httpx.WriteJSON(w, http.StatusCreated, bucket)
			})

			protected.Get("/settings/storage", func(w http.ResponseWriter, r *http.Request) {
				httpx.WriteJSON(w, http.StatusOK, map[string]any{
					"max_bytes": store.InstanceQuotaBytes(),
				})
			})

			protected.Put("/settings/storage", func(w http.ResponseWriter, r *http.Request) {
				payload := struct {
					MaxBytes int64 `json:"max_bytes"`
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
				nextConfig, path, _, err := config.LoadEditable(cfg.Runtime.ConfigPath)
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}
				if strings.TrimSpace(nextConfig.Storage.TmpDir) == "" {
					nextConfig.Storage.TmpDir = cfg.Storage.TmpDir
				}
				if strings.TrimSpace(nextConfig.Storage.PublicBaseURL) == "" {
					nextConfig.Storage.PublicBaseURL = cfg.Storage.PublicBaseURL
				}
				if strings.TrimSpace(nextConfig.Storage.S3BaseURL) == "" {
					nextConfig.Storage.S3BaseURL = cfg.Storage.S3BaseURL
				}
				nextConfig.Storage.MaxBytes = payload.MaxBytes
				if err := nextConfig.Validate(); err != nil {
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}
				if err := config.Save(path, nextConfig); err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}
				if err := store.SetInstanceQuotaBytes(payload.MaxBytes); err != nil {
					writeStorageError(w, err)
					return
				}

				cfg.Storage.MaxBytes = payload.MaxBytes
				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "settings.storage.update",
					Title:  "Updated instance storage limit",
					Detail: fmt.Sprintf("Limit set to %s", quotaLabel(payload.MaxBytes)),
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusOK, map[string]any{
					"max_bytes": payload.MaxBytes,
				})
			})

			protected.Delete("/buckets/{bucket}", func(w http.ResponseWriter, r *http.Request) {
				bucketName := chi.URLParam(r, "bucket")
				if err := store.DeleteBucket(r.Context(), bucketName); err != nil {
					writeStorageError(w, err)
					return
				}
				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "bucket.delete",
					Title:  fmt.Sprintf("Deleted bucket %s", bucketName),
					Target: bucketName,
					Remote: requestRemote(r),
					Status: "success",
				})
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
				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "object.upload",
					Title:  fmt.Sprintf("Uploaded %s/%s", object.Bucket, object.Key),
					Detail: fmt.Sprintf("%s · %s", formatBytes(object.Size), contentTypeLabel(object.ContentType)),
					Target: object.Bucket + "/" + object.Key,
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusCreated, object)
			})

			protected.Delete("/buckets/{bucket}/objects/*", func(w http.ResponseWriter, r *http.Request) {
				bucketName := chi.URLParam(r, "bucket")
				key := chi.URLParam(r, "*")
				if err := store.DeleteObject(r.Context(), bucketName, key); err != nil {
					writeStorageError(w, err)
					return
				}
				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "object.delete",
					Title:  fmt.Sprintf("Deleted %s/%s", bucketName, key),
					Target: bucketName + "/" + key,
					Remote: requestRemote(r),
					Status: "success",
				})
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

			protected.Get("/share-links", func(w http.ResponseWriter, r *http.Request) {
				items, err := shareLinks.List(r.Context())
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}

				response := make([]shareLinkResponse, 0, len(items))
				now := time.Now().UTC()
				for _, item := range items {
					response = append(response, makeShareLinkResponse(cfg.Storage.PublicBaseURL, item, now))
				}

				httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": response})
			})

			protected.Post("/share-links", func(w http.ResponseWriter, r *http.Request) {
				payload := struct {
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

				expires := 24 * time.Hour
				if payload.ExpiresSeconds > 0 {
					expires = time.Duration(payload.ExpiresSeconds) * time.Second
				}

				object, err := store.StatObject(r.Context(), strings.TrimSpace(payload.Bucket), strings.TrimSpace(payload.Key))
				if err != nil {
					writeStorageError(w, err)
					return
				}

				link, err := shareLinks.Create(r.Context(), sharelink.CreateInput{
					Bucket:      object.Bucket,
					Key:         object.Key,
					Filename:    path.Base(object.Key),
					ContentType: object.ContentType,
					Size:        object.Size,
					CreatedBy:   actorFromRequest(r),
					Expires:     expires,
				})
				if err != nil {
					writeShareLinkError(w, err)
					return
				}

				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "sharelink.create",
					Title:  fmt.Sprintf("Created share link for %s/%s", link.Bucket, link.Key),
					Detail: fmt.Sprintf("Expires at %s", link.ExpiresAt.UTC().Format(time.RFC3339)),
					Target: "/s/" + link.ID,
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusCreated, makeShareLinkResponse(cfg.Storage.PublicBaseURL, link, time.Now().UTC()))
			})

			protected.Delete("/share-links/{id}", func(w http.ResponseWriter, r *http.Request) {
				link, err := shareLinks.Revoke(r.Context(), chi.URLParam(r, "id"))
				if err != nil {
					writeShareLinkError(w, err)
					return
				}

				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "sharelink.revoke",
					Title:  fmt.Sprintf("Revoked share link for %s/%s", link.Bucket, link.Key),
					Target: "/s/" + link.ID,
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusOK, makeShareLinkResponse(cfg.Storage.PublicBaseURL, link, time.Now().UTC()))
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

				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "presign.s3",
					Title:  fmt.Sprintf("Generated presigned %s for %s/%s", strings.ToUpper(strings.TrimSpace(payload.Method)), strings.TrimSpace(payload.Bucket), strings.TrimSpace(payload.Key)),
					Detail: fmt.Sprintf("Expires in %ds", payload.ExpiresSeconds),
					Target: strings.TrimSpace(payload.Bucket) + "/" + strings.TrimSpace(payload.Key),
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusOK, result)
			})
		})
	})

	router.Get("/", uiHandler.ServeHTTP)
	router.NotFound(uiHandler.ServeHTTP)

	return router
}

func requireSession(manager *consoleauth.Manager) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			session, err := manager.SessionFromRequest(r)
			if err != nil {
				httpx.WriteJSON(w, http.StatusUnauthorized, map[string]any{"status": "error", "message": "not authenticated"})
				return
			}
			next.ServeHTTP(w, r.WithContext(sessionWithContext(r.Context(), session)))
		})
	}
}

func writeStorageError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, storage.ErrInvalidBucketName), errors.Is(err, storage.ErrInvalidObjectKey), errors.Is(err, storage.ErrInvalidQuota):
		status = http.StatusBadRequest
	case errors.Is(err, storage.ErrBucketExists):
		status = http.StatusConflict
	case errors.Is(err, storage.ErrBucketNotEmpty), errors.Is(err, storage.ErrBucketQuotaExceeded), errors.Is(err, storage.ErrInstanceQuotaExceeded):
		status = http.StatusConflict
	case errors.Is(err, storage.ErrBucketNotFound), errors.Is(err, storage.ErrObjectNotFound):
		status = http.StatusNotFound
	}

	httpx.WriteJSON(w, status, map[string]any{
		"status":  "error",
		"message": err.Error(),
	})
}

func writeShareLinkError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, sharelink.ErrInvalidID), errors.Is(err, sharelink.ErrInvalidExpiry):
		status = http.StatusBadRequest
	case errors.Is(err, sharelink.ErrNotFound):
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

type shareLinkResponse struct {
	ID          string     `json:"id"`
	Bucket      string     `json:"bucket"`
	Key         string     `json:"key"`
	Filename    string     `json:"filename"`
	ContentType string     `json:"content_type,omitempty"`
	Size        int64      `json:"size"`
	CreatedBy   string     `json:"created_by,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	ExpiresAt   time.Time  `json:"expires_at"`
	RevokedAt   *time.Time `json:"revoked_at,omitempty"`
	Status      string     `json:"status"`
	URL         string     `json:"url"`
	DownloadURL string     `json:"download_url"`
}

func makeShareLinkResponse(baseURL string, link sharelink.Link, now time.Time) shareLinkResponse {
	trimmedBaseURL := strings.TrimRight(strings.TrimSpace(baseURL), "/")
	return shareLinkResponse{
		ID:          link.ID,
		Bucket:      link.Bucket,
		Key:         link.Key,
		Filename:    link.Filename,
		ContentType: link.ContentType,
		Size:        link.Size,
		CreatedBy:   link.CreatedBy,
		CreatedAt:   link.CreatedAt,
		ExpiresAt:   link.ExpiresAt,
		RevokedAt:   link.RevokedAt,
		Status:      link.Status(now),
		URL:         trimmedBaseURL + "/s/" + link.ID,
		DownloadURL: trimmedBaseURL + "/dl/" + link.ID,
	}
}

type sessionContextKey struct{}

func sessionWithContext(ctx context.Context, session consoleauth.Session) context.Context {
	return context.WithValue(ctx, sessionContextKey{}, session)
}

func sessionFromContext(ctx context.Context) (consoleauth.Session, bool) {
	session, ok := ctx.Value(sessionContextKey{}).(consoleauth.Session)
	return session, ok
}

func actorFromRequest(r *http.Request) string {
	if r != nil {
		if session, ok := sessionFromContext(r.Context()); ok && strings.TrimSpace(session.Username) != "" {
			return session.Username
		}
	}
	return "system"
}

func requestRemote(r *http.Request) string {
	if r == nil {
		return ""
	}
	if host, _, err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr)); err == nil {
		return host
	}
	return strings.TrimSpace(r.RemoteAddr)
}

func fallbackActor(value string) string {
	if strings.TrimSpace(value) == "" {
		return "unknown user"
	}
	return strings.TrimSpace(value)
}

func quotaLabel(bytes int64) string {
	if bytes <= 0 {
		return "unlimited"
	}
	return formatBytes(bytes)
}

func contentTypeLabel(value string) string {
	if strings.TrimSpace(value) == "" {
		return "application/octet-stream"
	}
	return strings.TrimSpace(value)
}

func formatBytes(bytes int64) string {
	if bytes <= 0 {
		return "0 B"
	}
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	units := []string{"KB", "MB", "GB", "TB"}
	value := float64(bytes)
	index := -1
	for value >= 1024 && index < len(units)-1 {
		value /= 1024
		index += 1
	}
	if value >= 10 {
		return fmt.Sprintf("%.0f %s", value, units[index])
	}
	return fmt.Sprintf("%.1f %s", value, units[index])
}

func recordAudit(logger *zap.Logger, recorder *auditlog.Recorder, entry auditlog.Entry) {
	if recorder == nil {
		return
	}
	if err := recorder.Record(entry); err != nil {
		logger.Warn("record audit log", zap.Error(err), zap.String("action", entry.Action), zap.String("title", entry.Title))
	}
}
