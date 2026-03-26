package s3api

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/httpx"
	"go.uber.org/zap"
)

type errorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

func NewHandler(cfg config.Config, logger *zap.Logger) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "s3",
			"region":  cfg.Storage.Region,
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeS3Error(w, r, http.StatusNotImplemented, "NotImplemented", "S3 API skeleton is online but no operations are wired yet")
	})

	return httpx.RequestLogger(logger, "s3")(mux)
}

func writeS3Error(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	requestID := fmt.Sprintf("req-%d", time.Now().UnixNano())
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("X-Amz-Request-Id", requestID)
	w.WriteHeader(status)

	if r.Method == http.MethodHead {
		return
	}

	_ = xml.NewEncoder(w).Encode(errorResponse{
		Code:      code,
		Message:   message,
		Resource:  r.URL.Path,
		RequestID: requestID,
	})
}
