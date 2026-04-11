package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/config"
	"bares3-server/internal/httpx"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func RegisterSettingsRoutes(protected chi.Router, cfg *config.Config, store *storage.Store, auditRecorder *auditlog.Recorder, logger *zap.Logger) {
	protected.Get("/settings/storage", func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"max_bytes": runtimeSettings.MaxBytes,
		})
	})

	protected.Get("/settings/system", func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"public_base_url": runtimeSettings.PublicBaseURL,
			"s3_base_url":     runtimeSettings.S3BaseURL,
			"region":          runtimeSettings.Region,
			"metadata_layout": runtimeSettings.MetadataLayout,
			"tmp_dir":         cfg.Paths.TmpDir,
		})
	})

	protected.Get("/settings/domains", func(w http.ResponseWriter, r *http.Request) {
		bindings, err := store.PublicDomainBindings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": bindings})
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
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		runtimeSettings.MaxBytes = payload.MaxBytes
		if _, err := store.SetRuntimeSettings(r.Context(), runtimeSettings); err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
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

	protected.Put("/settings/system", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			PublicBaseURL  string `json:"public_base_url"`
			S3BaseURL      string `json:"s3_base_url"`
			Region         string `json:"region"`
			MetadataLayout string `json:"metadata_layout"`
			TmpDir         string `json:"tmp_dir"`
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
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		runtimeSettings.PublicBaseURL = payload.PublicBaseURL
		runtimeSettings.S3BaseURL = payload.S3BaseURL
		runtimeSettings.Region = payload.Region
		runtimeSettings.MetadataLayout = payload.MetadataLayout
		updated, err := store.SetRuntimeSettings(r.Context(), runtimeSettings)
		if err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
				"status":  "error",
				"message": err.Error(),
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
		nextConfig.Paths.TmpDir = strings.TrimSpace(payload.TmpDir)
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

		cfg.Paths.TmpDir = nextConfig.Paths.TmpDir
		recordAudit(logger, auditRecorder, auditlog.Entry{
			Actor:  actorFromRequest(r),
			Action: "settings.system.update",
			Title:  "Updated system settings",
			Detail: fmt.Sprintf("Region %s · Temp dir %s", updated.Region, nextConfig.Paths.TmpDir),
			Remote: requestRemote(r),
			Status: "success",
		})
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"public_base_url": updated.PublicBaseURL,
			"s3_base_url":     updated.S3BaseURL,
			"region":          updated.Region,
			"metadata_layout": updated.MetadataLayout,
			"tmp_dir":         nextConfig.Paths.TmpDir,
		})
	})

	protected.Put("/settings/domains", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Items []storage.PublicDomainBinding `json:"items"`
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

		updated, err := store.SetPublicDomainBindings(r.Context(), payload.Items)
		if err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}

		recordAudit(logger, auditRecorder, auditlog.Entry{
			Actor:  actorFromRequest(r),
			Action: "settings.domains.update",
			Title:  "Updated public domain bindings",
			Detail: fmt.Sprintf("%d domain binding(s)", len(updated)),
			Remote: requestRemote(r),
			Status: "success",
		})
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": updated})
	})
}
