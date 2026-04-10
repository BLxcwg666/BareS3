package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/httpx"
	"bares3-server/internal/s3creds"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/sigv4"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func RegisterShareLinkRoutes(protected chi.Router, store *storage.Store, shareLinks *sharelink.Store, credentials *s3creds.Store, auditRecorder *auditlog.Recorder, logger *zap.Logger) {
	protected.Get("/share-links", func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
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
			response = append(response, makeShareLinkResponse(runtimeSettings.PublicBaseURL, item, now))
		}

		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": response})
	})

	protected.Post("/share-links", func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
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
		httpx.WriteJSON(w, http.StatusCreated, makeShareLinkResponse(runtimeSettings.PublicBaseURL, link, time.Now().UTC()))
	})

	protected.Delete("/share-links/{id}", func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
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
		httpx.WriteJSON(w, http.StatusOK, makeShareLinkResponse(runtimeSettings.PublicBaseURL, link, time.Now().UTC()))
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

		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		baseURL, err := url.Parse(runtimeSettings.S3BaseURL)
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": "invalid settings.s3_base_url configuration",
			})
			return
		}

		requestPath := "/" + strings.TrimPrefix(strings.TrimSpace(payload.Bucket), "/")
		if key := strings.TrimPrefix(strings.TrimSpace(payload.Key), "/"); key != "" {
			requestPath += "/" + key
		}
		baseURL.Path = requestPath

		writeRequested := strings.EqualFold(strings.TrimSpace(payload.Method), http.MethodPut) || strings.EqualFold(strings.TrimSpace(payload.Method), http.MethodDelete) || strings.EqualFold(strings.TrimSpace(payload.Method), http.MethodPost)
		credential, err := credentials.FindForOperation(r.Context(), strings.TrimSpace(payload.Bucket), writeRequested)
		if err != nil {
			writeS3CredentialError(w, err)
			return
		}
		verifier := sigv4.NewVerifier(credential.AccessKeyID, credential.SecretAccessKey, runtimeSettings.Region, "s3")
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
			Detail: fmt.Sprintf("Expires in %ds · Access key %s", payload.ExpiresSeconds, credential.AccessKeyID),
			Target: strings.TrimSpace(payload.Bucket) + "/" + strings.TrimSpace(payload.Key),
			Remote: requestRemote(r),
			Status: "success",
		})
		httpx.WriteJSON(w, http.StatusOK, result)
	})
}
