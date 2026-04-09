package httpx

import (
	"context"
	"net/http"
	"time"
)

type ReadinessCheck struct {
	Name  string
	Check func(context.Context) error
}

func ReadyHandler(service string, checks ...ReadinessCheck) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		statusCode := http.StatusOK
		status := "ok"
		results := make(map[string]string, len(checks))
		for _, check := range checks {
			if check.Check == nil {
				results[check.Name] = "ok"
				continue
			}
			if err := check.Check(ctx); err != nil {
				statusCode = http.StatusServiceUnavailable
				status = "error"
				results[check.Name] = err.Error()
				continue
			}
			results[check.Name] = "ok"
		}

		WriteJSON(w, statusCode, map[string]any{
			"status":  status,
			"service": service,
			"checks":  results,
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})
}
