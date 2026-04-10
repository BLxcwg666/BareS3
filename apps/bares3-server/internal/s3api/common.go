package s3api

import (
	"net/http"
	"strings"
	"time"
)

func splitPath(requestPath string) (bucket string, key string) {
	trimmed := strings.TrimPrefix(requestPath, "/")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" {
		return "", ""
	}
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = parts[0]
	if len(parts) == 2 {
		key = parts[1]
	}
	return bucket, key
}

func joinURLPath(parts ...string) string {
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.Trim(part, "/")
		if trimmed == "" {
			continue
		}
		cleaned = append(cleaned, trimmed)
	}
	if len(cleaned) == 0 {
		return "/"
	}
	return "/" + strings.Join(cleaned, "/")
}

func hasQueryValue(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}

func formatS3Time(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format("2006-01-02T15:04:05.000Z")
}

func parseMaxKeys(w http.ResponseWriter, r *http.Request) (int, bool) {
	return parseBoundedIntQuery(w, r, "max-keys", 1000, "max-keys must be a non-negative integer")
}
