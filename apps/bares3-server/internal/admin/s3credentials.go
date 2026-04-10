package admin

import (
	"encoding/json"
	"fmt"
	"net/http"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/httpx"
	"bares3-server/internal/s3creds"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func RegisterS3CredentialRoutes(protected chi.Router, credentials *s3creds.Store, auditRecorder *auditlog.Recorder, logger *zap.Logger) {
	protected.Get("/settings/s3/credentials", func(w http.ResponseWriter, r *http.Request) {
		items, err := credentials.List(r.Context())
		if err != nil {
			writeS3CredentialError(w, err)
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
	})

	protected.Post("/settings/s3/credentials", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Label      string   `json:"label"`
			Permission string   `json:"permission"`
			Buckets    []string `json:"buckets"`
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

		created, err := credentials.Create(r.Context(), s3creds.CreateInput{
			Label:      payload.Label,
			Permission: payload.Permission,
			Buckets:    payload.Buckets,
		})
		if err != nil {
			writeS3CredentialError(w, err)
			return
		}

		recordAudit(logger, auditRecorder, auditlog.Entry{
			Actor:  actorFromRequest(r),
			Action: "s3credential.create",
			Title:  fmt.Sprintf("Created S3 access key %s", created.AccessKeyID),
			Detail: fmt.Sprintf("Label %s · Permission %s · Buckets %s", nonEmptyLabel(created.Label, "(none)"), s3CredentialPermissionLabel(created.Permission), s3CredentialBucketLabel(created.Buckets)),
			Target: created.AccessKeyID,
			Remote: requestRemote(r),
			Status: "success",
		})

		httpx.WriteJSON(w, http.StatusCreated, map[string]any{
			"access_key_id":     created.AccessKeyID,
			"secret_access_key": created.SecretAccessKey,
			"label":             created.Label,
			"source":            created.Source,
			"permission":        created.Permission,
			"buckets":           created.Buckets,
			"created_at":        created.CreatedAt,
			"last_used_at":      created.LastUsedAt,
			"status":            created.Status(),
		})
	})

	protected.Put("/settings/s3/credentials/{accessKeyID}", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Label      string   `json:"label"`
			Permission string   `json:"permission"`
			Buckets    []string `json:"buckets"`
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

		accessKeyID := chi.URLParam(r, "accessKeyID")
		updated, err := credentials.Update(r.Context(), s3creds.UpdateInput{
			AccessKeyID: accessKeyID,
			Label:       payload.Label,
			Permission:  payload.Permission,
			Buckets:     payload.Buckets,
		})
		if err != nil {
			writeS3CredentialError(w, err)
			return
		}

		recordAudit(logger, auditRecorder, auditlog.Entry{
			Actor:  actorFromRequest(r),
			Action: "s3credential.update",
			Title:  fmt.Sprintf("Updated S3 access key %s", updated.AccessKeyID),
			Detail: fmt.Sprintf("Label %s · Permission %s · Buckets %s", nonEmptyLabel(updated.Label, "(none)"), s3CredentialPermissionLabel(updated.Permission), s3CredentialBucketLabel(updated.Buckets)),
			Target: updated.AccessKeyID,
			Remote: requestRemote(r),
			Status: "success",
		})

		httpx.WriteJSON(w, http.StatusOK, updated)
	})

	protected.Delete("/settings/s3/credentials/{accessKeyID}", func(w http.ResponseWriter, r *http.Request) {
		accessKeyID := chi.URLParam(r, "accessKeyID")
		revoked, err := credentials.Revoke(r.Context(), accessKeyID)
		if err != nil {
			writeS3CredentialError(w, err)
			return
		}

		recordAudit(logger, auditRecorder, auditlog.Entry{
			Actor:  actorFromRequest(r),
			Action: "s3credential.revoke",
			Title:  fmt.Sprintf("Revoked S3 access key %s", revoked.AccessKeyID),
			Detail: fmt.Sprintf("Label %s", nonEmptyLabel(revoked.Label, "(none)")),
			Target: revoked.AccessKeyID,
			Remote: requestRemote(r),
			Status: "success",
		})

		httpx.WriteJSON(w, http.StatusOK, revoked)
	})

	protected.Delete("/settings/s3/credentials/{accessKeyID}/remove", func(w http.ResponseWriter, r *http.Request) {
		accessKeyID := chi.URLParam(r, "accessKeyID")
		if err := credentials.Delete(r.Context(), accessKeyID); err != nil {
			writeS3CredentialError(w, err)
			return
		}

		recordAudit(logger, auditRecorder, auditlog.Entry{
			Actor:  actorFromRequest(r),
			Action: "s3credential.remove",
			Title:  fmt.Sprintf("Deleted revoked S3 access key %s", accessKeyID),
			Target: accessKeyID,
			Remote: requestRemote(r),
			Status: "success",
		})

		w.WriteHeader(http.StatusNoContent)
	})
}
