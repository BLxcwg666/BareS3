package admin

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/httpx"
	"bares3-server/internal/remotes"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func RegisterReplicationRoutes(protected chi.Router, store *storage.Store, remoteStore *remotes.Store, auditRecorder *auditlog.Recorder, logger *zap.Logger) {
	protected.Get("/replication/tokens", func(w http.ResponseWriter, r *http.Request) {
		items, err := remoteStore.ListAccessTokens(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
	})

	protected.Post("/replication/tokens", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			Label string `json:"label"`
		}{}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&payload); err != nil && !errors.Is(err, io.EOF) {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": "invalid request body"})
			return
		}
		token, err := remoteStore.CreateAccessToken(r.Context(), remotes.CreateAccessTokenInput{Label: payload.Label, CreatedBy: actorFromRequest(r)})
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		recordAudit(logger, auditRecorder, auditlog.Entry{Actor: actorFromRequest(r), Action: "replication.token.create", Title: fmt.Sprintf("Created replication token %s", token.ID), Detail: fmt.Sprintf("Label %s", nonEmptyLabel(token.Label, "(none)")), Target: token.ID, Remote: requestRemote(r), Status: "success"})
		httpx.WriteJSON(w, http.StatusCreated, token)
	})

	protected.Delete("/replication/tokens/{id}", func(w http.ResponseWriter, r *http.Request) {
		token, err := remoteStore.RevokeAccessToken(r.Context(), chi.URLParam(r, "id"))
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, remotes.ErrTokenNotFound) {
				status = http.StatusNotFound
			}
			httpx.WriteJSON(w, status, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		recordAudit(logger, auditRecorder, auditlog.Entry{Actor: actorFromRequest(r), Action: "replication.token.revoke", Title: fmt.Sprintf("Revoked replication token %s", token.ID), Target: token.ID, Remote: requestRemote(r), Status: "success"})
		httpx.WriteJSON(w, http.StatusOK, token)
	})

	protected.Delete("/replication/tokens/{id}/remove", func(w http.ResponseWriter, r *http.Request) {
		token, err := remoteStore.DeleteAccessToken(r.Context(), chi.URLParam(r, "id"))
		if err != nil {
			status := http.StatusInternalServerError
			switch {
			case errors.Is(err, remotes.ErrTokenNotFound):
				status = http.StatusNotFound
			case errors.Is(err, remotes.ErrTokenActive):
				status = http.StatusConflict
			}
			httpx.WriteJSON(w, status, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		recordAudit(logger, auditRecorder, auditlog.Entry{Actor: actorFromRequest(r), Action: "replication.token.delete", Title: fmt.Sprintf("Deleted replication token %s", token.ID), Target: token.ID, Remote: requestRemote(r), Status: "success"})
		httpx.WriteJSON(w, http.StatusOK, token)
	})

	protected.Get("/replication/remotes", func(w http.ResponseWriter, r *http.Request) {
		items, err := remoteStore.ListRemotes(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		views, err := buildRemoteViews(r.Context(), store, items)
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": views})
	})

	protected.Post("/replication/remotes", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			DisplayName   string `json:"display_name"`
			Endpoint      string `json:"endpoint"`
			Token         string `json:"token"`
			Enabled       *bool  `json:"enabled"`
			FollowChanges *bool  `json:"follow_changes"`
			BootstrapMode string `json:"bootstrap_mode"`
		}{}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&payload); err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": "invalid request body"})
			return
		}
		status, err := fetchRemoteStatusForBootstrap(r.Context(), strings.TrimSpace(payload.Endpoint), strings.TrimSpace(payload.Token))
		if err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		cursor := int64(0)
		if remotes.NormalizeBootstrapMode(payload.BootstrapMode) == remotes.BootstrapModeFromNow {
			cursor = status.Cursor
		}
		followChanges := true
		if payload.FollowChanges != nil {
			followChanges = *payload.FollowChanges
		}
		remote, err := remoteStore.CreateRemote(r.Context(), remotes.CreateRemoteInput{DisplayName: payload.DisplayName, Endpoint: payload.Endpoint, Token: payload.Token, Enabled: payload.Enabled, FollowChanges: followChanges, BootstrapMode: payload.BootstrapMode, Cursor: cursor})
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, remotes.ErrInvalidBootstrapMode) {
				status = http.StatusBadRequest
			}
			httpx.WriteJSON(w, status, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		recordAudit(logger, auditRecorder, auditlog.Entry{Actor: actorFromRequest(r), Action: "replication.remote.create", Title: fmt.Sprintf("Added replication remote %s", remote.DisplayName), Detail: fmt.Sprintf("Endpoint %s · Mode %s", remote.Endpoint, remote.BootstrapMode), Target: remote.ID, Remote: requestRemote(r), Status: "success"})
		view, err := buildRemoteView(r.Context(), store, remote)
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		httpx.WriteJSON(w, http.StatusCreated, view)
	})

	protected.Patch("/replication/remotes/{id}", func(w http.ResponseWriter, r *http.Request) {
		payload := struct {
			DisplayName   *string `json:"display_name"`
			Endpoint      *string `json:"endpoint"`
			Token         *string `json:"token"`
			BootstrapMode *string `json:"bootstrap_mode"`
			Enabled       *bool   `json:"enabled"`
			FollowChanges *bool   `json:"follow_changes"`
		}{}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&payload); err != nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": "invalid request body"})
			return
		}
		if payload.DisplayName == nil && payload.Endpoint == nil && payload.Token == nil && payload.BootstrapMode == nil && payload.Enabled == nil && payload.FollowChanges == nil {
			httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": "at least one remote field is required"})
			return
		}
		remote, err := remoteStore.GetRemote(r.Context(), chi.URLParam(r, "id"))
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, remotes.ErrRemoteNotFound) {
				status = http.StatusNotFound
			}
			httpx.WriteJSON(w, status, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		effectiveEndpoint := remote.Endpoint
		if payload.Endpoint != nil {
			effectiveEndpoint = strings.TrimSpace(*payload.Endpoint)
		}
		effectiveToken := remote.Token
		if payload.Token != nil {
			effectiveToken = strings.TrimSpace(*payload.Token)
		}
		effectiveBootstrapMode := remote.BootstrapMode
		if payload.BootstrapMode != nil {
			effectiveBootstrapMode = remotes.NormalizeBootstrapMode(*payload.BootstrapMode)
			if effectiveBootstrapMode == "" {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": remotes.ErrInvalidBootstrapMode.Error()})
				return
			}
		}
		effectiveFollowChanges := remote.FollowChanges
		if payload.FollowChanges != nil {
			effectiveFollowChanges = *payload.FollowChanges
		}
		effectiveEnabled := remote.Enabled
		if payload.Enabled != nil {
			effectiveEnabled = *payload.Enabled
		}
		sourceChanged := effectiveEndpoint != remote.Endpoint || effectiveToken != remote.Token
		bootstrapChanged := effectiveBootstrapMode != remote.BootstrapMode
		snapshotReplayRequested := payload.Enabled != nil && *payload.Enabled && !remote.Enabled && !effectiveFollowChanges
		needsReset := sourceChanged || bootstrapChanged || snapshotReplayRequested

		updateInput := remotes.UpdateRemoteStateInput{ID: remote.ID, DisplayName: payload.DisplayName, Endpoint: payload.Endpoint, Token: payload.Token, BootstrapMode: payload.BootstrapMode, Enabled: payload.Enabled, FollowChanges: payload.FollowChanges}
		if sourceChanged || (bootstrapChanged && effectiveBootstrapMode == remotes.BootstrapModeFromNow) {
			status, err := fetchRemoteStatusForBootstrap(r.Context(), effectiveEndpoint, effectiveToken)
			if err != nil {
				httpx.WriteJSON(w, http.StatusBadRequest, map[string]any{"status": "error", "message": err.Error()})
				return
			}
			if needsReset && effectiveBootstrapMode == remotes.BootstrapModeFromNow {
				cursor := status.Cursor
				updateInput.Cursor = &cursor
			}
		}
		if needsReset {
			status := remotes.RemoteStatusPending
			disconnected := remotes.ConnectionStatusDisconnected
			updateInput.Status = &status
			updateInput.ConnectionStatus = &disconnected
			updateInput.ResetProgress = true
			updateInput.ResetLastSyncAt = true
			updateInput.ResetHeartbeat = true
			updateInput.ResetPeerStatus = true
			if effectiveBootstrapMode == remotes.BootstrapModeFull {
				updateInput.ResetSyncCursor = true
			}
		} else if payload.Enabled != nil && !effectiveEnabled {
			disconnected := remotes.ConnectionStatusDisconnected
			updateInput.ConnectionStatus = &disconnected
			updateInput.ResetHeartbeat = true
		}
		if err := remoteStore.UpdateRemoteState(r.Context(), updateInput); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, remotes.ErrRemoteNotFound) {
				status = http.StatusNotFound
			} else if errors.Is(err, remotes.ErrInvalidBootstrapMode) {
				status = http.StatusBadRequest
			} else if strings.Contains(strings.ToLower(strings.TrimSpace(err.Error())), "required") {
				status = http.StatusBadRequest
			}
			httpx.WriteJSON(w, status, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		updatedRemote, err := remoteStore.GetRemote(r.Context(), remote.ID)
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		view, err := buildRemoteView(r.Context(), store, updatedRemote)
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		detailParts := make([]string, 0, 6)
		if payload.DisplayName != nil {
			detailParts = append(detailParts, fmt.Sprintf("Name %s", updatedRemote.DisplayName))
		}
		if payload.Endpoint != nil {
			detailParts = append(detailParts, fmt.Sprintf("Endpoint %s", updatedRemote.Endpoint))
		}
		if payload.Token != nil {
			detailParts = append(detailParts, "Token rotated")
		}
		if payload.BootstrapMode != nil {
			detailParts = append(detailParts, fmt.Sprintf("Mode %s", updatedRemote.BootstrapMode))
		}
		if payload.Enabled != nil {
			detailParts = append(detailParts, fmt.Sprintf("Enabled %t", updatedRemote.Enabled))
		}
		if payload.FollowChanges != nil {
			detailParts = append(detailParts, fmt.Sprintf("Follow changes %t", updatedRemote.FollowChanges))
		}
		recordAudit(logger, auditRecorder, auditlog.Entry{Actor: actorFromRequest(r), Action: "replication.remote.update", Title: fmt.Sprintf("Updated replication remote %s", updatedRemote.DisplayName), Detail: strings.Join(detailParts, " · "), Target: updatedRemote.ID, Remote: requestRemote(r), Status: "success"})
		httpx.WriteJSON(w, http.StatusOK, view)
	})

	protected.Delete("/replication/remotes/{id}", func(w http.ResponseWriter, r *http.Request) {
		remote, err := remoteStore.GetRemote(r.Context(), chi.URLParam(r, "id"))
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, remotes.ErrRemoteNotFound) {
				status = http.StatusNotFound
			}
			httpx.WriteJSON(w, status, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		if err := remoteStore.DeleteRemote(r.Context(), remote.ID); err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, remotes.ErrRemoteNotFound) {
				status = http.StatusNotFound
			}
			httpx.WriteJSON(w, status, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		if err := store.DeleteSyncStatusesBySource(r.Context(), remote.ID); err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		recordAudit(logger, auditRecorder, auditlog.Entry{Actor: actorFromRequest(r), Action: "replication.remote.delete", Title: fmt.Sprintf("Removed replication remote %s", remote.DisplayName), Target: remote.ID, Remote: requestRemote(r), Status: "success"})
		view, err := buildRemoteView(r.Context(), store, remote)
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, view)
	})

	protected.Get("/replication/stream", func(w http.ResponseWriter, r *http.Request) {
		serveReplicationAdminStream(w, r, store, remoteStore, logger)
	})
}
