package admin

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/httpx"
	"github.com/go-chi/chi/v5"
	"go.uber.org/zap"
)

func RegisterAuthRoutes(auth chi.Router, manager *consoleauth.Manager, auditRecorder *auditlog.Recorder, logger *zap.Logger) {
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
		if err := validateSameOriginRequest(r); err != nil {
			httpx.WriteJSON(w, http.StatusForbidden, map[string]any{"status": "error", "message": err.Error()})
			return
		}
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
}
