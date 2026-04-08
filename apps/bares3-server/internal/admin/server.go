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

				cookie, err := manager.IssueCookie(session, consoleauth.SecureCookiesForRequest(r))
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
				http.SetCookie(w, manager.ClearCookie(consoleauth.SecureCookiesForRequest(r)))
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

			protected.Get("/search", func(w http.ResponseWriter, r *http.Request) {
				query := strings.TrimSpace(r.URL.Query().Get("query"))
				if query == "" {
					httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": []searchResultItem{}})
					return
				}

				limit := 12
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
				if limit == 0 {
					httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": []searchResultItem{}})
					return
				}
				if limit > 50 {
					limit = 50
				}

				buckets, err := store.ListBuckets(r.Context())
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}

				keyword := strings.ToLower(query)
				results := make([]searchResultItem, 0, limit)
				for _, bucket := range buckets {
					if strings.Contains(strings.ToLower(bucket.Name), keyword) {
						results = append(results, searchResultItem{Kind: "bucket", Bucket: bucket.Name})
						if len(results) >= limit {
							break
						}
					}
				}

				if len(results) < limit {
					for _, bucket := range buckets {
						page, err := store.ListObjectsPage(r.Context(), bucket.Name, storage.ListObjectsOptions{
							Query: query,
							Limit: limit - len(results),
						})
						if err != nil {
							writeStorageError(w, err)
							return
						}
						for _, item := range page.Items {
							results = append(results, searchResultItem{Kind: "object", Bucket: item.Bucket, Key: item.Key})
							if len(results) >= limit {
								break
							}
						}
						if len(results) >= limit {
							break
						}
					}
				}

				httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": results})
			})

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
					AccessMode string `json:"access_mode"`
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

				bucket, err := store.CreateBucketWithOptions(r.Context(), storage.CreateBucketInput{
					Name:       payload.Name,
					AccessMode: payload.AccessMode,
					QuotaBytes: payload.QuotaBytes,
				})
				if err != nil {
					writeStorageError(w, err)
					return
				}
				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "bucket.create",
					Title:  fmt.Sprintf("Created bucket %s", bucket.Name),
					Detail: fmt.Sprintf("Access %s · Quota %s", bucketAccessLabel(bucket.AccessMode), quotaLabel(bucket.QuotaBytes)),
					Target: bucket.Name,
					Remote: requestRemote(r),
					Status: "success",
				})

				httpx.WriteJSON(w, http.StatusCreated, bucket)
			})

			protected.Put("/buckets/{bucket}", func(w http.ResponseWriter, r *http.Request) {
				payload := struct {
					Name       string   `json:"name"`
					AccessMode string   `json:"access_mode"`
					QuotaBytes int64    `json:"quota_bytes"`
					Tags       []string `json:"tags"`
					Note       string   `json:"note"`
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

				bucketName := chi.URLParam(r, "bucket")
				updated, err := store.UpdateBucket(r.Context(), storage.UpdateBucketInput{
					Name:       bucketName,
					NewName:    payload.Name,
					AccessMode: payload.AccessMode,
					QuotaBytes: payload.QuotaBytes,
					Tags:       payload.Tags,
					Note:       payload.Note,
				})
				if err != nil {
					writeStorageError(w, err)
					return
				}
				if bucketName != updated.Name {
					if _, err := shareLinks.ReassignBucket(r.Context(), bucketName, updated.Name); err != nil {
						httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
							"status":  "error",
							"message": err.Error(),
						})
						return
					}
				}

				detailParts := []string{fmt.Sprintf("Access %s", bucketAccessLabel(updated.AccessMode)), fmt.Sprintf("Quota %s", quotaLabel(updated.QuotaBytes))}
				if bucketName != updated.Name {
					detailParts = append([]string{fmt.Sprintf("Renamed from %s", bucketName)}, detailParts...)
				}
				if len(updated.Tags) > 0 {
					detailParts = append(detailParts, fmt.Sprintf("Labels %s", strings.Join(updated.Tags, ", ")))
				}
				if updated.Note != "" {
					detailParts = append(detailParts, fmt.Sprintf("Note %s", updated.Note))
				}
				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "bucket.update",
					Title:  fmt.Sprintf("Updated bucket %s", updated.Name),
					Detail: strings.Join(detailParts, " · "),
					Target: updated.Name,
					Remote: requestRemote(r),
					Status: "success",
				})

				httpx.WriteJSON(w, http.StatusOK, updated)
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
				if _, err := shareLinks.RemoveByBucket(r.Context(), bucketName); err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
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

			protected.Get("/buckets/{bucket}/history", func(w http.ResponseWriter, r *http.Request) {
				limit := 24
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

				items, err := store.ListBucketUsageHistory(r.Context(), chi.URLParam(r, "bucket"), limit)
				if err != nil {
					writeStorageError(w, err)
					return
				}
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
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

				page, err := store.ListObjectsPage(r.Context(), chi.URLParam(r, "bucket"), storage.ListObjectsOptions{
					Prefix: r.URL.Query().Get("prefix"),
					Query:  r.URL.Query().Get("query"),
					After:  r.URL.Query().Get("cursor"),
					Limit:  limit,
				})
				if err != nil {
					writeStorageError(w, err)
					return
				}
				httpx.WriteJSON(w, http.StatusOK, map[string]any{
					"items":       page.Items,
					"has_more":    page.HasMore,
					"next_cursor": page.NextCursor,
				})
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
				if _, err := shareLinks.RemoveByObject(r.Context(), bucketName, key); err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
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

			protected.Put("/buckets/{bucket}/metadata/*", func(w http.ResponseWriter, r *http.Request) {
				payload := struct {
					ContentType        string            `json:"content_type"`
					ContentDisposition string            `json:"content_disposition"`
					CacheControl       string            `json:"cache_control"`
					UserMetadata       map[string]string `json:"user_metadata"`
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

				object, err := store.UpdateObjectMetadata(r.Context(), storage.UpdateObjectMetadataInput{
					Bucket:             chi.URLParam(r, "bucket"),
					Key:                chi.URLParam(r, "*"),
					ContentType:        payload.ContentType,
					ContentDisposition: payload.ContentDisposition,
					CacheControl:       payload.CacheControl,
					UserMetadata:       payload.UserMetadata,
				})
				if err != nil {
					writeStorageError(w, err)
					return
				}

				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "object.metadata.update",
					Title:  fmt.Sprintf("Updated metadata for %s/%s", object.Bucket, object.Key),
					Target: object.Bucket + "/" + object.Key,
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusOK, object)
			})

			protected.Post("/browser/delete", func(w http.ResponseWriter, r *http.Request) {
				payload := struct {
					Kind   string `json:"kind"`
					Bucket string `json:"bucket"`
					Prefix string `json:"prefix"`
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

				if strings.TrimSpace(payload.Kind) != "prefix" {
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
						"status":  "error",
						"message": "kind must be prefix",
					})
					return
				}

				deletedCount, err := store.DeletePrefix(r.Context(), payload.Bucket, payload.Prefix)
				if err != nil {
					writeStorageError(w, err)
					return
				}
				if _, err := shareLinks.RemoveByPrefix(r.Context(), payload.Bucket, payload.Prefix); err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}

				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "folder.delete",
					Title:  fmt.Sprintf("Deleted folder %s/%s", payload.Bucket, payload.Prefix),
					Detail: fmt.Sprintf("Removed %d item(s)", deletedCount),
					Target: payload.Bucket + "/" + payload.Prefix,
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"deleted_count": deletedCount})
			})

			protected.Post("/browser/move", func(w http.ResponseWriter, r *http.Request) {
				payload := struct {
					Kind              string `json:"kind"`
					SourceBucket      string `json:"source_bucket"`
					SourceKey         string `json:"source_key"`
					SourcePrefix      string `json:"source_prefix"`
					DestinationBucket string `json:"destination_bucket"`
					DestinationKey    string `json:"destination_key"`
					DestinationPrefix string `json:"destination_prefix"`
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

				kind := strings.TrimSpace(payload.Kind)
				switch kind {
				case "object":
					moved, err := store.MoveObject(r.Context(), storage.MoveObjectInput{
						SourceBucket:      payload.SourceBucket,
						SourceKey:         payload.SourceKey,
						DestinationBucket: payload.DestinationBucket,
						DestinationKey:    payload.DestinationKey,
					})
					if err != nil {
						writeStorageError(w, err)
						return
					}
					if _, err := shareLinks.ReassignObject(r.Context(), payload.SourceBucket, payload.SourceKey, moved.Bucket, moved.Key); err != nil {
						httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
							"status":  "error",
							"message": err.Error(),
						})
						return
					}

					result := storage.MoveResult{
						Kind:              "object",
						SourceBucket:      payload.SourceBucket,
						SourceKey:         payload.SourceKey,
						DestinationBucket: moved.Bucket,
						DestinationKey:    moved.Key,
						MovedCount:        1,
					}
					recordAudit(logger, auditRecorder, auditlog.Entry{
						Actor:  actorFromRequest(r),
						Action: "object.move",
						Title:  fmt.Sprintf("Moved %s/%s", payload.SourceBucket, payload.SourceKey),
						Detail: fmt.Sprintf("to %s/%s", moved.Bucket, moved.Key),
						Target: moved.Bucket + "/" + moved.Key,
						Remote: requestRemote(r),
						Status: "success",
					})
					httpx.WriteJSON(w, http.StatusOK, result)
				case "prefix":
					result, err := store.MovePrefix(r.Context(), storage.MovePrefixInput{
						SourceBucket:      payload.SourceBucket,
						SourcePrefix:      payload.SourcePrefix,
						DestinationBucket: payload.DestinationBucket,
						DestinationPrefix: payload.DestinationPrefix,
					})
					if err != nil {
						writeStorageError(w, err)
						return
					}
					if _, err := shareLinks.ReassignPrefix(
						r.Context(),
						result.SourceBucket,
						result.SourcePrefix,
						result.DestinationBucket,
						result.DestinationPrefix,
					); err != nil {
						httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
							"status":  "error",
							"message": err.Error(),
						})
						return
					}

					recordAudit(logger, auditRecorder, auditlog.Entry{
						Actor:  actorFromRequest(r),
						Action: "folder.move",
						Title:  fmt.Sprintf("Moved %s/%s", result.SourceBucket, result.SourcePrefix),
						Detail: fmt.Sprintf("to %s/%s · %d items", result.DestinationBucket, result.DestinationPrefix, result.MovedCount),
						Target: result.DestinationBucket + "/" + result.DestinationPrefix,
						Remote: requestRemote(r),
						Status: "success",
					})
					httpx.WriteJSON(w, http.StatusOK, result)
				default:
					httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
						"status":  "error",
						"message": "kind must be object or prefix",
					})
				}
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

			protected.Delete("/share-links/{id}/remove", func(w http.ResponseWriter, r *http.Request) {
				link, err := shareLinks.Remove(r.Context(), chi.URLParam(r, "id"))
				if err != nil {
					writeShareLinkError(w, err)
					return
				}

				recordAudit(logger, auditRecorder, auditlog.Entry{
					Actor:  actorFromRequest(r),
					Action: "sharelink.remove",
					Title:  fmt.Sprintf("Removed revoked share link for %s/%s", link.Bucket, link.Key),
					Target: "/s/" + link.ID,
					Remote: requestRemote(r),
					Status: "success",
				})
				w.WriteHeader(http.StatusNoContent)
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
	case errors.Is(err, storage.ErrInvalidBucketName), errors.Is(err, storage.ErrInvalidObjectKey), errors.Is(err, storage.ErrInvalidQuota), errors.Is(err, storage.ErrInvalidMove), errors.Is(err, storage.ErrInvalidMetadata), errors.Is(err, storage.ErrInvalidBucketAccess):
		status = http.StatusBadRequest
	case errors.Is(err, storage.ErrBucketExists), errors.Is(err, storage.ErrObjectExists):
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
	case errors.Is(err, sharelink.ErrInvalidID), errors.Is(err, sharelink.ErrInvalidExpiry), errors.Is(err, sharelink.ErrNotRevoked):
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

type searchResultItem struct {
	Kind   string `json:"kind"`
	Bucket string `json:"bucket"`
	Key    string `json:"key,omitempty"`
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

func bucketAccessLabel(value string) string {
	if storage.IsBucketPublicAccess(value) {
		return "public"
	}
	return "private"
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
