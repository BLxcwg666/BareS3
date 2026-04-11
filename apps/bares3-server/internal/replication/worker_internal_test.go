package replication

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWorkerHTTPClientAllowsSlowStreamingBody(t *testing.T) {
	t.Parallel()

	previousHeaderTimeout := replicationResponseHeaderTimeout
	replicationResponseHeaderTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		replicationResponseHeaderTimeout = previousHeaderTimeout
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(60 * time.Millisecond)
		_, _ = w.Write([]byte("payload"))
	}))
	defer server.Close()

	client := newWorkerHTTPClient()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext failed: %v", err)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do failed: %v", err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(body) != "payload" {
		t.Fatalf("unexpected body %q", string(body))
	}
	if client.Timeout != 0 {
		t.Fatalf("expected no total client timeout, got %s", client.Timeout)
	}
}

func TestWorkerHTTPClientTimesOutWaitingForHeaders(t *testing.T) {
	t.Parallel()

	previousHeaderTimeout := replicationResponseHeaderTimeout
	replicationResponseHeaderTimeout = 20 * time.Millisecond
	t.Cleanup(func() {
		replicationResponseHeaderTimeout = previousHeaderTimeout
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(60 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("payload"))
	}))
	defer server.Close()

	client := newWorkerHTTPClient()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequestWithContext failed: %v", err)
	}
	res, err := client.Do(req)
	if err == nil {
		res.Body.Close()
		t.Fatal("expected header timeout error")
	}
}
