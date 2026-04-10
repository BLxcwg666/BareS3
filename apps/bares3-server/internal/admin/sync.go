package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/httpx"
	"bares3-server/internal/remotes"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func RegisterSyncSettingsRoutes(protected chi.Router, store *storage.Store, remoteStore *remotes.Store, auditRecorder *auditlog.Recorder, logger *zap.Logger) {
	protected.Get("/settings/sync", func(w http.ResponseWriter, r *http.Request) {
		settings, err := store.SyncSettings(r.Context())
		if errors.Is(err, os.ErrNotExist) {
			settings = storage.DefaultSyncSettings()
		} else if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		currentCursor, err := store.CurrentSyncCursor(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		reconcileCounts, err := store.SyncStatusCounts(r.Context(), "")
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		reconcileSummary, err := store.SyncStatusSummary(r.Context(), "")
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		conflictItems, err := store.ConflictItems(r.Context(), "", 20)
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"enabled":           settings.Enabled,
			"leader_cursor":     currentCursor,
			"reconcile_counts":  reconcileCounts,
			"reconcile_summary": reconcileSummary,
			"conflict_items":    conflictItems,
		})
	})

	protected.Put("/settings/sync", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Enabled bool `json:"enabled"`
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

		updated, err := store.SetSyncSettings(r.Context(), storage.SyncSettings{Enabled: payload.Enabled})
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
				"status":  "error",
				"message": err.Error(),
			})
			return
		}

		recordAudit(logger, auditRecorder, auditlog.Entry{
			Actor:  actorFromRequest(r),
			Action: "settings.sync.update",
			Title:  "Updated sync settings",
			Detail: fmt.Sprintf("Sync enabled %t", updated.Enabled),
			Remote: requestRemote(r),
			Status: "success",
		})
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"enabled": updated.Enabled,
		})
	})

	protected.Post("/settings/sync/conflicts/resolve", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Bucket string `json:"bucket"`
			Key    string `json:"key"`
		}{}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&payload); err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": "invalid request body"})
			return
		}
		statusItem, err := store.GetObjectSyncStatus(r.Context(), payload.Bucket, payload.Key)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		if _, err := store.SyncSettings(r.Context()); errors.Is(err, os.ErrNotExist) {
			// Sync settings may be absent until first toggle; conflict resolution relies on remotes only.
		} else if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		if err := refetchObjectFromSource(r.Context(), store, remoteStore, statusItem, payload.Bucket, payload.Key); err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		recordAudit(logger, auditRecorder, auditlog.Entry{Actor: actorFromRequest(r), Action: "sync.conflict.resolve", Title: fmt.Sprintf("Resolved conflict for %s/%s", payload.Bucket, payload.Key), Target: payload.Bucket + "/" + payload.Key, Remote: requestRemote(r), Status: "success"})
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"status": "ok"})
	})
}
