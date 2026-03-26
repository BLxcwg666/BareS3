package fileserve

import (
	"fmt"
	"net/http"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/httpx"
	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

func NewHandler(cfg config.Config, logger *zap.Logger) http.Handler {
	router := chi.NewRouter()
	router.Use(chiMiddleware.RequestID)
	router.Use(chiMiddleware.RealIP)
	router.Use(chiMiddleware.Recoverer)
	router.Use(httpx.RequestLogger(logger, "file"))

	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteText(w, http.StatusOK, fmt.Sprintf("%s file service\npublic base URL: %s\n", cfg.App.Name, cfg.Storage.PublicBaseURL))
	})

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "file",
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	router.Route("/pub", func(r chi.Router) {
		r.Get("/*", notImplemented("public file serving is not wired yet"))
	})
	router.Route("/dl", func(r chi.Router) {
		r.Get("/*", notImplemented("download aliases are not wired yet"))
	})
	router.Route("/s", func(r chi.Router) {
		r.Get("/*", notImplemented("signed links are not wired yet"))
	})

	return router
}

func notImplemented(message string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusNotImplemented, map[string]any{
			"status":  "not_implemented",
			"message": message,
			"path":    r.URL.Path,
		})
	}
}
