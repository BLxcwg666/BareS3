package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/httpx"
	"bares3-server/internal/remotes"
	"bares3-server/internal/replication"
	"bares3-server/internal/s3creds"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/sigv4"
	"bares3-server/internal/storage"
	"bares3-server/internal/webui"
	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

func NewHandler(cfg config.Config, store *storage.Store, credentials *s3creds.Store, logger *zap.Logger) http.Handler {
	return newHandler(cfg, store, nil, credentials, logger)
}

func newHandler(cfg config.Config, store *storage.Store, shareLinks *sharelink.Store, credentials *s3creds.Store, logger *zap.Logger) http.Handler {
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
	if shareLinks == nil {
		shareLinks, err = sharelink.New(cfg.Paths.DataDir, logger.Named("sharelink"))
		if err != nil {
			panic(fmt.Sprintf("initialize share link store: %v", err))
		}
	}
	if credentials == nil {
		credentials, err = s3creds.New(cfg.Paths.DataDir, s3creds.BootstrapCredential{
			AccessKeyID:     cfg.Auth.S3.AccessKeyID,
			SecretAccessKey: cfg.Auth.S3.SecretAccessKey,
		}, logger.Named("s3creds"))
		if err != nil {
			panic(fmt.Sprintf("initialize s3 credential store: %v", err))
		}
	}
	remoteStore, err := remotes.New(cfg.Paths.DataDir, logger.Named("remotes"))
	if err != nil {
		panic(fmt.Sprintf("initialize replication store: %v", err))
	}

	router := chi.NewRouter()
	uiHandler := webui.NewHandler()
	router.Use(chiMiddleware.RequestID)
	router.Use(chiMiddleware.RealIP)
	router.Use(httpx.RequestLogger(logger, "admin"))
	router.Use(chiMiddleware.Recoverer)

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "admin",
			"version": buildinfo.Current(),
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})
	router.Handle("/readyz", httpx.ReadyHandler("admin",
		httpx.ReadinessCheck{Name: "storage", Check: store.Check},
		httpx.ReadinessCheck{Name: "replication", Check: remoteStore.Check},
		httpx.ReadinessCheck{Name: "share_links", Check: shareLinks.Check},
		httpx.ReadinessCheck{Name: "s3_credentials", Check: credentials.Check},
	))
	router.Handle("/metrics", httpx.MetricsHandler())
	router.Mount("/internal/sync", replication.NewLeaderHandler(cfg, store, logger.Named("sync")))

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
			protected.Use(rejectFollowerMutations(store))

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

			protected.Get("/settings/s3/credentials", func(w http.ResponseWriter, r *http.Request) {
				items, err := credentials.List(r.Context())
				if err != nil {
					writeS3CredentialError(w, err)
					return
				}
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": items})
			})

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
				runtimeSettings, err := store.RuntimeSettings(r.Context())
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": runtimeSettings.DomainBindings})
			})

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

				runtimeSettings, err := store.RuntimeSettings(r.Context())
				if err != nil {
					httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{
						"status":  "error",
						"message": err.Error(),
					})
					return
				}
				runtimeSettings.DomainBindings = payload.Items
				updated, err := store.SetRuntimeSettings(r.Context(), runtimeSettings)
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
					Detail: fmt.Sprintf("%d domain binding(s)", len(updated.DomainBindings)),
					Remote: requestRemote(r),
					Status: "success",
				})
				httpx.WriteJSON(w, http.StatusOK, map[string]any{"items": updated.DomainBindings})
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

func rejectFollowerMutations(store *storage.Store) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r)
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
	case errors.Is(err, storage.ErrObjectSyncing):
		status = http.StatusServiceUnavailable
	case errors.Is(err, storage.ErrBucketNotFound), errors.Is(err, storage.ErrObjectNotFound):
		status = http.StatusNotFound
	}

	httpx.WriteJSON(w, status, map[string]any{
		"status":  "error",
		"message": err.Error(),
	})
}

func parseInt64OrZero(value string) int64 {
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0
	}
	return parsed
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

func writeS3CredentialError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, s3creds.ErrInvalidLabel), errors.Is(err, s3creds.ErrInvalidPermission):
		status = http.StatusBadRequest
	case errors.Is(err, s3creds.ErrCredentialNotFound):
		status = http.StatusNotFound
	case errors.Is(err, s3creds.ErrNoActiveCredential), errors.Is(err, s3creds.ErrCredentialActive):
		status = http.StatusConflict
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

func nonEmptyLabel(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}

func s3CredentialPermissionLabel(value string) string {
	if strings.TrimSpace(value) == s3creds.PermissionReadOnly {
		return "read_only"
	}
	return "read_write"
}

func s3CredentialBucketLabel(values []string) string {
	if len(values) == 0 {
		return "all"
	}
	return strings.Join(values, ",")
}

func quotaLabel(bytes int64) string {
	if bytes <= 0 {
		return "unlimited"
	}
	return formatBytes(bytes)
}

func bucketAccessLabel(value string) string {
	switch storage.NormalizeBucketAccessMode(value) {
	case storage.BucketAccessPublic:
		return "public"
	case storage.BucketAccessCustom:
		return "custom"
	default:
		return "private"
	}
}

func bucketAccessRuleLabel(value string) string {
	switch storage.NormalizeBucketAccessAction(value) {
	case storage.BucketAccessActionPublic:
		return "public"
	case storage.BucketAccessActionDeny:
		return "deny"
	default:
		return "authenticated"
	}
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
