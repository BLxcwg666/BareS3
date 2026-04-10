package admin

import (
	"errors"
	"net/http"
	"os"
	"strconv"
	"strings"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/httpx"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
)

func RegisterObservabilityRoutes(protected chi.Router, cfg *config.Config, store *storage.Store, shareLinks *sharelink.Store, auditRecorder *auditlog.Recorder) {
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
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
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
		syncSettings, err := store.SyncSettings(r.Context())
		if errors.Is(err, os.ErrNotExist) {
			syncSettings = storage.DefaultSyncSettings()
		} else if err != nil {
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
				"tmp_dir":  cfg.Paths.TmpDir,
			},
			"listen": map[string]any{
				"admin": cfg.Listen.Admin,
				"s3":    cfg.Listen.S3,
				"file":  cfg.Listen.File,
			},
			"storage": map[string]any{
				"region":            runtimeSettings.Region,
				"public_base_url":   runtimeSettings.PublicBaseURL,
				"s3_base_url":       runtimeSettings.S3BaseURL,
				"metadata_layout":   runtimeSettings.MetadataLayout,
				"domain_bindings":   runtimeSettings.DomainBindings,
				"max_bytes":         runtimeSettings.MaxBytes,
				"used_bytes":        usedBytes,
				"bucket_count":      len(buckets),
				"active_link_count": activeLinkCount,
			},
			"sync": map[string]any{
				"enabled": syncSettings.Enabled,
			},
		})
	})
}
