package httpx

import (
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bares3-server/internal/buildinfo"
)

var requestDurationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

var processStartedAt = time.Now().UTC()

type requestMetricKey struct {
	Service string
	Method  string
	Status  string
}

type histogramMetric struct {
	Count   uint64
	Sum     float64
	Buckets []uint64
}

type metricsCollector struct {
	mu            sync.Mutex
	inFlight      map[string]int64
	requests      map[requestMetricKey]uint64
	responseBytes map[requestMetricKey]uint64
	durations     map[requestMetricKey]*histogramMetric
}

type metricsSnapshot struct {
	InFlight      map[string]int64
	Requests      map[requestMetricKey]uint64
	ResponseBytes map[requestMetricKey]uint64
	Durations     map[requestMetricKey]histogramMetric
}

var defaultMetricsCollector = &metricsCollector{
	inFlight:      make(map[string]int64),
	requests:      make(map[requestMetricKey]uint64),
	responseBytes: make(map[requestMetricKey]uint64),
	durations:     make(map[requestMetricKey]*histogramMetric),
}

func MetricsHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writePrometheusMetrics(w)
	})
}

func addInFlightRequest(service string, delta int64) {
	service = strings.TrimSpace(service)
	if service == "" {
		service = "unknown"
	}

	defaultMetricsCollector.mu.Lock()
	defer defaultMetricsCollector.mu.Unlock()
	defaultMetricsCollector.inFlight[service] += delta
	if defaultMetricsCollector.inFlight[service] <= 0 {
		delete(defaultMetricsCollector.inFlight, service)
	}
}

func observeRequest(service, method string, status, bytes int, duration time.Duration) {
	key := requestMetricKey{
		Service: normalizeMetricValue(service, "unknown"),
		Method:  normalizeMetricValue(strings.ToUpper(method), http.MethodGet),
		Status:  strconv.Itoa(status),
	}

	defaultMetricsCollector.mu.Lock()
	defer defaultMetricsCollector.mu.Unlock()

	defaultMetricsCollector.requests[key] += 1
	if bytes > 0 {
		defaultMetricsCollector.responseBytes[key] += uint64(bytes)
	}

	histogram, ok := defaultMetricsCollector.durations[key]
	if !ok {
		histogram = &histogramMetric{Buckets: make([]uint64, len(requestDurationBuckets))}
		defaultMetricsCollector.durations[key] = histogram
	}
	durationSeconds := duration.Seconds()
	histogram.Count += 1
	histogram.Sum += durationSeconds
	for index, bucket := range requestDurationBuckets {
		if durationSeconds <= bucket {
			histogram.Buckets[index] += 1
		}
	}
}

func writePrometheusMetrics(w http.ResponseWriter) {
	snapshot := defaultMetricsCollector.snapshot()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

	writeMetricHeader(w, "bares3_http_requests_in_flight", "gauge", "Current in-flight HTTP requests.")
	services := sortedServices(snapshot.InFlight)
	for _, service := range services {
		_, _ = fmt.Fprintf(w, "bares3_http_requests_in_flight%s %d\n", formatLabels(map[string]string{"service": service}), snapshot.InFlight[service])
	}

	writeMetricHeader(w, "bares3_http_requests_total", "counter", "Total HTTP requests handled by the current BareS3 process.")
	for _, key := range sortedRequestMetricKeys(snapshot.Requests) {
		_, _ = fmt.Fprintf(w, "bares3_http_requests_total%s %d\n", formatLabels(requestMetricLabels(key)), snapshot.Requests[key])
	}

	writeMetricHeader(w, "bares3_http_response_bytes_total", "counter", "Total response bytes written by the current BareS3 process.")
	for _, key := range sortedRequestMetricKeys(snapshot.ResponseBytes) {
		_, _ = fmt.Fprintf(w, "bares3_http_response_bytes_total%s %d\n", formatLabels(requestMetricLabels(key)), snapshot.ResponseBytes[key])
	}

	writeMetricHeader(w, "bares3_http_request_duration_seconds", "histogram", "HTTP request duration in seconds.")
	for _, key := range sortedRequestMetricKeysFromHistograms(snapshot.Durations) {
		histogram := snapshot.Durations[key]
		labels := requestMetricLabels(key)
		for index, bucket := range requestDurationBuckets {
			bucketLabels := cloneLabels(labels)
			bucketLabels["le"] = formatBucket(bucket)
			_, _ = fmt.Fprintf(w, "bares3_http_request_duration_seconds_bucket%s %d\n", formatLabels(bucketLabels), histogram.Buckets[index])
		}
		infLabels := cloneLabels(labels)
		infLabels["le"] = "+Inf"
		_, _ = fmt.Fprintf(w, "bares3_http_request_duration_seconds_bucket%s %d\n", formatLabels(infLabels), histogram.Count)
		_, _ = fmt.Fprintf(w, "bares3_http_request_duration_seconds_sum%s %s\n", formatLabels(labels), strconv.FormatFloat(histogram.Sum, 'f', -1, 64))
		_, _ = fmt.Fprintf(w, "bares3_http_request_duration_seconds_count%s %d\n", formatLabels(labels), histogram.Count)
	}

	build := buildinfo.Current()
	writeMetricHeader(w, "bares3_build_info", "gauge", "BareS3 build metadata.")
	_, _ = fmt.Fprintf(
		w,
		"bares3_build_info%s 1\n",
		formatLabels(map[string]string{
			"version":  normalizeMetricValue(build.Version, "dev"),
			"commit":   normalizeMetricValue(build.Commit, "unknown"),
			"built_at": normalizeMetricValue(build.BuiltAt, "unknown"),
		}),
	)

	writeMetricHeader(w, "bares3_process_start_time_seconds", "gauge", "BareS3 process start time in Unix seconds.")
	_, _ = fmt.Fprintf(w, "bares3_process_start_time_seconds %d\n", processStartedAt.Unix())

	memStats := runtime.MemStats{}
	runtime.ReadMemStats(&memStats)

	writeMetricHeader(w, "bares3_go_goroutines", "gauge", "Number of Go goroutines.")
	_, _ = fmt.Fprintf(w, "bares3_go_goroutines %d\n", runtime.NumGoroutine())

	writeMetricHeader(w, "bares3_go_memstats_alloc_bytes", "gauge", "Bytes of allocated heap objects.")
	_, _ = fmt.Fprintf(w, "bares3_go_memstats_alloc_bytes %d\n", memStats.Alloc)

	writeMetricHeader(w, "bares3_go_memstats_heap_alloc_bytes", "gauge", "Bytes of allocated heap memory.")
	_, _ = fmt.Fprintf(w, "bares3_go_memstats_heap_alloc_bytes %d\n", memStats.HeapAlloc)

	writeMetricHeader(w, "bares3_go_memstats_sys_bytes", "gauge", "Bytes of memory obtained from the OS.")
	_, _ = fmt.Fprintf(w, "bares3_go_memstats_sys_bytes %d\n", memStats.Sys)
}

func (c *metricsCollector) snapshot() metricsSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapshot := metricsSnapshot{
		InFlight:      make(map[string]int64, len(c.inFlight)),
		Requests:      make(map[requestMetricKey]uint64, len(c.requests)),
		ResponseBytes: make(map[requestMetricKey]uint64, len(c.responseBytes)),
		Durations:     make(map[requestMetricKey]histogramMetric, len(c.durations)),
	}
	for service, count := range c.inFlight {
		snapshot.InFlight[service] = count
	}
	for key, count := range c.requests {
		snapshot.Requests[key] = count
	}
	for key, count := range c.responseBytes {
		snapshot.ResponseBytes[key] = count
	}
	for key, histogram := range c.durations {
		copied := histogramMetric{Count: histogram.Count, Sum: histogram.Sum, Buckets: append([]uint64(nil), histogram.Buckets...)}
		snapshot.Durations[key] = copied
	}
	return snapshot
}

func writeMetricHeader(w http.ResponseWriter, name, metricType, help string) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	_, _ = fmt.Fprintf(w, "# TYPE %s %s\n", name, metricType)
}

func requestMetricLabels(key requestMetricKey) map[string]string {
	return map[string]string{
		"service": key.Service,
		"method":  key.Method,
		"status":  key.Status,
	}
}

func formatLabels(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"=\""+escapeLabelValue(labels[key])+"\"")
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func escapeLabelValue(value string) string {
	replacer := strings.NewReplacer("\\", "\\\\", "\n", "\\n", "\"", "\\\"")
	return replacer.Replace(value)
}

func normalizeMetricValue(value, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func sortedServices(values map[string]int64) []string {
	services := make([]string, 0, len(values))
	for service := range values {
		services = append(services, service)
	}
	sort.Strings(services)
	return services
}

func sortedRequestMetricKeys(values map[requestMetricKey]uint64) []requestMetricKey {
	keys := make([]requestMetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return compareRequestMetricKeys(keys[i], keys[j]) < 0
	})
	return keys
}

func sortedRequestMetricKeysFromHistograms(values map[requestMetricKey]histogramMetric) []requestMetricKey {
	keys := make([]requestMetricKey, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return compareRequestMetricKeys(keys[i], keys[j]) < 0
	})
	return keys
}

func compareRequestMetricKeys(left, right requestMetricKey) int {
	if left.Service != right.Service {
		return strings.Compare(left.Service, right.Service)
	}
	if left.Method != right.Method {
		return strings.Compare(left.Method, right.Method)
	}
	return strings.Compare(left.Status, right.Status)
}

func cloneLabels(labels map[string]string) map[string]string {
	cloned := make(map[string]string, len(labels))
	for key, value := range labels {
		cloned[key] = value
	}
	return cloned
}

func formatBucket(value float64) string {
	return strconv.FormatFloat(value, 'f', -1, 64)
}
