package admin

import (
	"context"
	"fmt"
	"net/http"
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
	shareLinks.SetChangeHook(func(ctx context.Context, links []sharelink.Link) error {
		return store.RecordShareLinksSnapshotEvent(ctx, links)
	})
	if credentials == nil {
		credentials, err = s3creds.New(cfg.Paths.DataDir, logger.Named("s3creds"))
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
			RegisterAuthRoutes(auth, manager, auditRecorder, logger)
		})

		api.Group(func(protected chi.Router) {
			protected.Use(requireSession(manager))
			protected.Use(requireSameOriginMutations)
			protected.Use(rejectFollowerMutations(store))

			RegisterObservabilityRoutes(protected, &cfg, store, shareLinks, auditRecorder)

			RegisterS3CredentialRoutes(protected, credentials, auditRecorder, logger)
			RegisterReplicationRoutes(protected, store, remoteStore, auditRecorder, logger)
			RegisterBucketRoutes(protected, store, shareLinks, auditRecorder, logger)
			RegisterSettingsRoutes(protected, &cfg, store, auditRecorder, logger)
			RegisterSyncSettingsRoutes(protected, store, remoteStore, auditRecorder, logger)
			RegisterObjectRoutes(protected, store, shareLinks, auditRecorder, logger)
			RegisterShareLinkRoutes(protected, store, shareLinks, credentials, auditRecorder, logger)
		})
	})

	router.Get("/", uiHandler.ServeHTTP)
	router.NotFound(uiHandler.ServeHTTP)

	return router
}
