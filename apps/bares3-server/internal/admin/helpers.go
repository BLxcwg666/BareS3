package admin

import (
	"context"
	"errors"
	"fmt"
	"mime/multipart"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/auditlog"
	"bares3-server/internal/consoleauth"
	"bares3-server/internal/httpx"
	"bares3-server/internal/s3creds"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
)

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

func requireSameOriginMutations(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !csrfProtectedMethod(r.Method) {
			next.ServeHTTP(w, r)
			return
		}
		if err := validateSameOriginRequest(r); err != nil {
			httpx.WriteJSON(w, http.StatusForbidden, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func csrfProtectedMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func validateSameOriginRequest(r *http.Request) error {
	if r == nil {
		return errors.New("forbidden cross-site request")
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		referer := strings.TrimSpace(r.Header.Get("Referer"))
		if referer == "" {
			return errors.New("origin or referer header is required")
		}
		parsedReferer, err := url.Parse(referer)
		if err != nil || parsedReferer.Scheme == "" || parsedReferer.Host == "" {
			return errors.New("referer header is invalid")
		}
		origin = parsedReferer.Scheme + "://" + parsedReferer.Host
	}
	parsedOrigin, err := url.Parse(origin)
	if err != nil || parsedOrigin.Scheme == "" || parsedOrigin.Host == "" {
		return errors.New("origin header is invalid")
	}
	if !sameOriginRequestHost(r, parsedOrigin) {
		return errors.New("forbidden cross-site request")
	}
	return nil
}

func sameOriginRequestHost(r *http.Request, origin *url.URL) bool {
	if r == nil || origin == nil {
		return false
	}
	requestScheme := requestScheme(r)
	requestHost := requestHost(r)
	if requestScheme == "" || requestHost == "" {
		return false
	}
	return strings.EqualFold(origin.Scheme, requestScheme) && strings.EqualFold(origin.Host, requestHost)
}

func requestScheme(r *http.Request) string {
	if consoleauth.SecureCookiesForRequest(r) {
		return "https"
	}
	return "http"
}

func requestHost(r *http.Request) string {
	if r == nil {
		return ""
	}
	if forwarded := firstHeaderToken(r.Header.Get("X-Forwarded-Host")); forwarded != "" {
		return strings.TrimSpace(forwarded)
	}
	if host := strings.TrimSpace(r.Host); host != "" {
		return host
	}
	return strings.TrimSpace(r.URL.Host)
}

func firstHeaderToken(value string) string {
	if value == "" {
		return ""
	}
	parts := strings.Split(value, ",")
	return strings.TrimSpace(parts[0])
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

func normalizeMultipartFormError(err error) string {
	if err == nil {
		return "invalid multipart form"
	}

	message := strings.TrimSpace(err.Error())
	if message == "" {
		return "invalid multipart form"
	}

	lower := strings.ToLower(message)
	switch {
	case strings.Contains(lower, "request body too large"):
		return "upload request body is too large"
	case strings.Contains(lower, "multipart: message too large"):
		return "multipart upload is too large"
	case strings.Contains(lower, "unexpected eof"), strings.Contains(lower, "unexpected end of json input"):
		return "upload body ended before the multipart form finished"
	case strings.Contains(lower, "connection reset"), strings.Contains(lower, "broken pipe"):
		return "upload connection was interrupted before the multipart form finished"
	default:
		return fmt.Sprintf("invalid multipart form: %s", message)
	}
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
