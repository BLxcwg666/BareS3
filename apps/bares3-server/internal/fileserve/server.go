package fileserve

import (
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"bares3-server/internal/buildinfo"
	"bares3-server/internal/config"
	"bares3-server/internal/httpx"
	"bares3-server/internal/s3xml"
	"bares3-server/internal/sharelink"
	"bares3-server/internal/storage"
	"github.com/go-chi/chi/v5"
	chiMiddleware "github.com/go-chi/chi/v5/middleware"
	"go.uber.org/zap"
)

func NewHandler(cfg config.Config, store *storage.Store, logger *zap.Logger) http.Handler {
	return newHandler(cfg, store, nil, logger)
}

func newHandler(cfg config.Config, store *storage.Store, shareLinks *sharelink.Store, logger *zap.Logger) http.Handler {
	if shareLinks == nil {
		var err error
		shareLinks, err = sharelink.New(cfg.Paths.DataDir, logger.Named("sharelink"))
		if err != nil {
			panic(fmt.Sprintf("initialize share link store: %v", err))
		}
	}

	router := chi.NewRouter()
	router.Use(chiMiddleware.RequestID)
	router.Use(chiMiddleware.RealIP)
	router.Use(httpx.RequestLogger(logger, "file"))
	router.Use(chiMiddleware.Recoverer)
	router.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			runtimeSettings, err := store.RuntimeSettings(r.Context())
			if err == nil && strings.TrimSpace(runtimeSettings.Region) != "" {
				w.Header().Set("X-Amz-Bucket-Region", runtimeSettings.Region)
			}
			if err == nil {
				domainBindings, domainErr := store.PublicDomainBindings(r.Context())
				if domainErr == nil {
					if binding, ok := matchPublicDomainBinding(domainBindings, r.Host); ok && !isReservedFileServicePath(r.URL.Path) {
						serveBoundDomainObject(w, r, store, binding)
						return
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	})
	router.NotFound(func(w http.ResponseWriter, r *http.Request) {
		writeS3Error(w, r, "", http.StatusNotFound, "NoSuchKey", "resource not found")
	})
	router.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		writeS3Error(w, r, "", http.StatusMethodNotAllowed, "MethodNotAllowed", "method is not supported")
	})

	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		httpx.WriteText(w, http.StatusOK, fmt.Sprintf("%s file service\nversion: %s\nregion: %s\npublic base URL: %s\n", config.ProductName, buildinfo.Current().Version, runtimeSettings.Region, runtimeSettings.PublicBaseURL))
	})

	router.Get("/healthz", func(w http.ResponseWriter, r *http.Request) {
		runtimeSettings, err := store.RuntimeSettings(r.Context())
		if err != nil {
			httpx.WriteJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ok",
			"service": "file",
			"region":  runtimeSettings.Region,
			"version": buildinfo.Current(),
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})
	router.Handle("/readyz", httpx.ReadyHandler("file",
		httpx.ReadinessCheck{Name: "storage", Check: store.Check},
		httpx.ReadinessCheck{Name: "share_links", Check: shareLinks.Check},
	))

	router.Route("/pub", func(r chi.Router) {
		r.Get("/{bucket}/*", func(w http.ResponseWriter, r *http.Request) {
			serveRouteObject(w, r, store)
		})
		r.Head("/{bucket}/*", func(w http.ResponseWriter, r *http.Request) {
			serveRouteObject(w, r, store)
		})
	})
	router.Route("/dl", func(r chi.Router) {
		r.Get("/{id}", func(w http.ResponseWriter, r *http.Request) {
			serveShareLinkObject(w, r, store, shareLinks, true)
		})
		r.Head("/{id}", func(w http.ResponseWriter, r *http.Request) {
			serveShareLinkObject(w, r, store, shareLinks, true)
		})
	})
	router.Route("/s", func(r chi.Router) {
		r.Get("/{id}", func(w http.ResponseWriter, r *http.Request) {
			serveShareLinkObject(w, r, store, shareLinks, false)
		})
		r.Head("/{id}", func(w http.ResponseWriter, r *http.Request) {
			serveShareLinkObject(w, r, store, shareLinks, false)
		})
	})

	return router
}

func serveRouteObject(w http.ResponseWriter, r *http.Request, store *storage.Store) {
	bucket := chi.URLParam(r, "bucket")
	key := chi.URLParam(r, "*")
	if !authorizeObjectRequest(w, r, store, bucket, key, false) {
		return
	}
	serveObject(w, r, store, bucket, key, "")
}

func serveBoundDomainObject(w http.ResponseWriter, r *http.Request, store *storage.Store, binding storage.PublicDomainBinding) {
	key := boundDomainObjectKey(binding, r.URL.Path)
	if !authorizeObjectRequest(w, r, store, binding.Bucket, key, false) {
		return
	}
	if serveDomainObject(w, r, store, binding, key) {
		return
	}
	if binding.SPAFallback {
		fallbackKey := boundDomainFallbackKey(binding)
		if fallbackKey != key && authorizeObjectRequest(w, r, store, binding.Bucket, fallbackKey, false) {
			if serveDomainObject(w, r, store, binding, fallbackKey) {
				return
			}
		}
	}
	writeStorageAsS3Error(w, r, binding.Bucket, storage.ErrObjectNotFound)
}

func serveShareLinkObject(w http.ResponseWriter, r *http.Request, store *storage.Store, shareLinks *sharelink.Store, forceDownload bool) {
	link, err := shareLinks.GetActive(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		status := http.StatusInternalServerError
		code := "InternalError"
		switch {
		case errors.Is(err, sharelink.ErrInvalidID):
			status = http.StatusBadRequest
			code = "InvalidArgument"
		case errors.Is(err, sharelink.ErrNotFound):
			status = http.StatusNotFound
			code = "NoSuchKey"
		case errors.Is(err, sharelink.ErrExpired), errors.Is(err, sharelink.ErrRevoked):
			status = http.StatusGone
			code = "AccessDenied"
		}
		writeS3Error(w, r, "", status, code, err.Error())
		return
	}

	if !authorizeObjectRequest(w, r, store, link.Bucket, link.Key, true) {
		return
	}

	contentDisposition := ""
	if forceDownload {
		contentDisposition = mime.FormatMediaType("attachment", map[string]string{"filename": link.Filename})
		if contentDisposition == "" {
			contentDisposition = `attachment; filename="` + path.Base(link.Key) + `"`
		}
	}

	serveObject(w, r, store, link.Bucket, link.Key, contentDisposition)
}

func authorizeObjectRequest(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key string, authenticated bool) bool {
	action, err := store.ResolveBucketObjectAccess(r.Context(), bucket, key)
	if err != nil {
		writeStorageAsS3Error(w, r, bucket, err)
		return false
	}

	switch action {
	case storage.BucketAccessActionPublic:
		return true
	case storage.BucketAccessActionAuthenticated:
		if authenticated {
			return true
		}
		writeS3Error(w, r, bucket, http.StatusForbidden, "AccessDenied", "object requires authentication")
		return false
	default:
		writeS3Error(w, r, bucket, http.StatusForbidden, "AccessDenied", "object access denied by bucket policy")
		return false
	}
}

func serveObject(w http.ResponseWriter, r *http.Request, store *storage.Store, bucket, key, contentDisposition string) {
	if bucket == "" || key == "" {
		writeS3Error(w, r, bucket, http.StatusBadRequest, "InvalidURI", "bucket and key are required")
		return
	}

	file, object, err := store.OpenObject(r.Context(), bucket, key)
	if err != nil {
		writeStorageAsS3Error(w, r, bucket, err)
		return
	}
	defer func() {
		_ = file.Close()
	}()

	if object.ContentType != "" {
		w.Header().Set("Content-Type", object.ContentType)
	}
	if object.CacheControl != "" {
		w.Header().Set("Cache-Control", object.CacheControl)
	}
	if contentDisposition != "" {
		w.Header().Set("Content-Disposition", contentDisposition)
	} else if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	if object.ETag != "" {
		w.Header().Set("ETag", `"`+object.ETag+`"`)
	}

	http.ServeContent(w, r, path.Base(object.Key), object.LastModified, file)
}

func serveDomainObject(w http.ResponseWriter, r *http.Request, store *storage.Store, binding storage.PublicDomainBinding, key string) bool {
	if key == "" {
		writeS3Error(w, r, binding.Bucket, http.StatusBadRequest, "InvalidURI", "bucket and key are required")
		return true
	}
	file, object, err := store.OpenObject(r.Context(), binding.Bucket, key)
	if errors.Is(err, storage.ErrObjectNotFound) {
		return false
	}
	if err != nil {
		writeStorageAsS3Error(w, r, binding.Bucket, err)
		return true
	}
	defer func() {
		_ = file.Close()
	}()

	if object.ContentType != "" {
		w.Header().Set("Content-Type", object.ContentType)
	}
	if object.CacheControl != "" {
		w.Header().Set("Cache-Control", object.CacheControl)
	}
	if object.ContentDisposition != "" {
		w.Header().Set("Content-Disposition", object.ContentDisposition)
	}
	if object.ETag != "" {
		w.Header().Set("ETag", `"`+object.ETag+`"`)
	}
	http.ServeContent(w, r, path.Base(object.Key), object.LastModified, file)
	return true
}

func matchPublicDomainBinding(bindings []storage.PublicDomainBinding, host string) (storage.PublicDomainBinding, bool) {
	normalizedHost := normalizeRequestHost(host)
	for _, binding := range bindings {
		if strings.EqualFold(binding.Host, normalizedHost) {
			return binding, true
		}
	}
	return storage.PublicDomainBinding{}, false
}

func normalizeRequestHost(host string) string {
	host = strings.ToLower(strings.TrimSpace(host))
	if parsedHost, _, err := net.SplitHostPort(host); err == nil {
		host = parsedHost
	}
	return strings.TrimSuffix(host, ".")
}

func isReservedFileServicePath(requestPath string) bool {
	switch requestPath {
	case "/healthz", "/readyz":
		return true
	default:
		return strings.HasPrefix(requestPath, "/pub/") || strings.HasPrefix(requestPath, "/dl/") || strings.HasPrefix(requestPath, "/s/")
	}
}

func boundDomainObjectKey(binding storage.PublicDomainBinding, requestPath string) string {
	trimmedPath := strings.TrimSpace(strings.TrimPrefix(requestPath, "/"))
	if trimmedPath == "" && binding.IndexDocument {
		trimmedPath = "index.html"
	}
	if prefix := strings.Trim(binding.Prefix, "/"); prefix == "" {
		return trimmedPath
	} else if trimmedPath == "" {
		return prefix
	} else {
		return prefix + "/" + trimmedPath
	}
}

func boundDomainFallbackKey(binding storage.PublicDomainBinding) string {
	if !binding.IndexDocument {
		return ""
	}
	prefix := strings.Trim(binding.Prefix, "/")
	if prefix == "" {
		return "index.html"
	}
	return prefix + "/index.html"
}

func writeStorageAsS3Error(w http.ResponseWriter, r *http.Request, bucket string, err error) {
	switch {
	case errors.Is(err, storage.ErrBucketNotFound):
		writeS3Error(w, r, bucket, http.StatusNotFound, "NoSuchBucket", err.Error())
	case errors.Is(err, storage.ErrObjectSyncing):
		writeS3Error(w, r, bucket, http.StatusServiceUnavailable, "ServiceUnavailable", err.Error())
	case errors.Is(err, storage.ErrObjectNotFound):
		writeS3Error(w, r, bucket, http.StatusNotFound, "NoSuchKey", err.Error())
	case errors.Is(err, storage.ErrInvalidBucketName):
		writeS3Error(w, r, bucket, http.StatusBadRequest, "InvalidBucketName", err.Error())
	case errors.Is(err, storage.ErrInvalidObjectKey):
		writeS3Error(w, r, bucket, http.StatusBadRequest, "InvalidArgument", err.Error())
	default:
		writeS3Error(w, r, bucket, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

func writeS3Error(w http.ResponseWriter, r *http.Request, bucket string, status int, code, message string) {
	s3xml.WriteError(w, r, status, s3xml.ErrorOptions{
		Code:       code,
		Message:    message,
		Region:     w.Header().Get("X-Amz-Bucket-Region"),
		BucketName: bucket,
	})
}
