package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/httpx"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func RegisterBucketRoutes(protected chi.Router, store *storage.Store, shareLinks *sharelink.Store, auditRecorder *auditlog.Recorder, logger *zap.Logger) {
	protected.Get("/buckets", func(w http.ResponseWriter, r *http.Request) {
		buckets, err := store.ListBuckets(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": buckets})
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

	protected.Get("/buckets/{bucket}/access", func(w http.ResponseWriter, r *http.Request) {
		config, err := store.GetBucketAccessConfig(r.Context(), chi.URLParam(r, "bucket"))
		if err != nil {
			writeStorageError(w, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, config)
	})

	protected.Put("/buckets/{bucket}/access", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Mode   string                     `json:"mode"`
			Policy storage.BucketAccessPolicy `json:"policy"`
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
		updated, err := store.UpdateBucketAccess(r.Context(), storage.UpdateBucketAccessInput{
			Name:   bucketName,
			Mode:   payload.Mode,
			Policy: payload.Policy,
		})
		if err != nil {
			writeStorageError(w, err)
			return
		}

		recordAudit(logger, auditRecorder, auditlog.Entry{
			Actor:  actorFromRequest(r),
			Action: "bucket.access.update",
			Title:  fmt.Sprintf("Updated access for bucket %s", bucketName),
			Detail: fmt.Sprintf("Mode %s · Default %s · %d rules", bucketAccessLabel(updated.Mode), bucketAccessRuleLabel(updated.Policy.DefaultAction), len(updated.Policy.Rules)),
			Target: bucketName,
			Remote: requestRemote(r),
			Status: "success",
		})

		httpx.WriteJSON(w, http.StatusOK, updated)
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
}
