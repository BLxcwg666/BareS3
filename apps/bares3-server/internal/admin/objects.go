package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/httpx"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func RegisterObjectRoutes(protected chi.Router, store *storage.Store, shareLinks *sharelink.Store, auditRecorder *auditlog.Recorder, logger *zap.Logger) {
	protected.Get("/buckets/{bucket}/objects", func(w http.ResponseWriter, r *http.Request) {
		limit := 0
		offset := 0
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
		if rawOffset := strings.TrimSpace(r.URL.Query().Get("offset")); rawOffset != "" {
			parsed, err := strconv.Atoi(rawOffset)
			if err != nil || parsed < 0 {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
					"status":  "error",
					"message": "offset must be a non-negative integer",
				})
				return
			}
			offset = parsed
		}

		page, err := store.ListObjectsPage(r.Context(), chi.URLParam(r, "bucket"), storage.ListObjectsOptions{
			Prefix:    r.URL.Query().Get("prefix"),
			Query:     r.URL.Query().Get("query"),
			After:     r.URL.Query().Get("cursor"),
			Offset:    offset,
			Limit:     limit,
			Delimiter: r.URL.Query().Get("delimiter"),
		})
		if err != nil {
			writeStorageError(w, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"items":       page.Items,
			"prefixes":    page.Prefixes,
			"total_count": page.TotalCount,
			"has_more":    page.HasMore,
			"next_cursor": page.NextCursor,
		})
	})

	protected.Post("/buckets/{bucket}/objects", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(64 << 20); err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
				"status":  "error",
				"message": normalizeMultipartFormError(err),
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

	protected.Post("/buckets/{bucket}/uploads", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Key                string            `json:"key"`
			ContentType        string            `json:"content_type"`
			CacheControl       string            `json:"cache_control"`
			ContentDisposition string            `json:"content_disposition"`
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

		upload, err := store.InitiateMultipartUpload(r.Context(), storage.InitiateMultipartUploadInput{
			Bucket:             chi.URLParam(r, "bucket"),
			Key:                payload.Key,
			ContentType:        payload.ContentType,
			CacheControl:       payload.CacheControl,
			ContentDisposition: payload.ContentDisposition,
			UserMetadata:       payload.UserMetadata,
		})
		if err != nil {
			writeStorageError(w, err)
			return
		}

		httpx.WriteJSON(w, http.StatusCreated, upload)
	})

	protected.Put("/buckets/{bucket}/uploads/{uploadID}/parts/{partNumber}", func(w http.ResponseWriter, r *http.Request) {
		partNumber, err := strconv.Atoi(strings.TrimSpace(chi.URLParam(r, "partNumber")))
		if err != nil || partNumber <= 0 {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
				"status":  "error",
				"message": "part number must be a positive integer",
			})
			return
		}

		part, err := store.UploadPart(r.Context(), storage.UploadPartInput{
			Bucket:     chi.URLParam(r, "bucket"),
			Key:        r.URL.Query().Get("key"),
			UploadID:   chi.URLParam(r, "uploadID"),
			PartNumber: partNumber,
			Body:       r.Body,
		})
		if err != nil {
			writeStorageError(w, err)
			return
		}

		httpx.WriteJSON(w, http.StatusOK, part)
	})

	protected.Post("/buckets/{bucket}/uploads/{uploadID}/complete", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Key   string `json:"key"`
			Parts []struct {
				PartNumber int    `json:"part_number"`
				ETag       string `json:"etag"`
			} `json:"parts"`
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

		parts := make([]storage.CompletedPart, 0, len(payload.Parts))
		for _, part := range payload.Parts {
			parts = append(parts, storage.CompletedPart{PartNumber: part.PartNumber, ETag: part.ETag})
		}

		object, err := store.CompleteMultipartUpload(r.Context(), chi.URLParam(r, "bucket"), payload.Key, chi.URLParam(r, "uploadID"), parts)
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

	protected.Delete("/buckets/{bucket}/uploads/{uploadID}", func(w http.ResponseWriter, r *http.Request) {
		if err := store.AbortMultipartUpload(r.Context(), chi.URLParam(r, "bucket"), r.URL.Query().Get("key"), chi.URLParam(r, "uploadID")); err != nil {
			writeStorageError(w, err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
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

	protected.Get("/buckets/{bucket}/preview/*", func(w http.ResponseWriter, r *http.Request) {
		file, object, err := store.OpenObject(r.Context(), chi.URLParam(r, "bucket"), chi.URLParam(r, "*"))
		if err != nil {
			writeStorageError(w, err)
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
		if object.ETag != "" {
			w.Header().Set("ETag", `"`+object.ETag+`"`)
		}
		w.Header().Set("Content-Disposition", `inline; filename="`+path.Base(object.Key)+`"`)

		http.ServeContent(w, r, path.Base(object.Key), object.LastModified, file)
	})
}
