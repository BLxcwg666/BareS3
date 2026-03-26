package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"path"
	"strings"
)

//go:embed all:dist
var embeddedFiles embed.FS

type handler struct {
	files      fs.FS
	indexHTML  []byte
	fileServer http.Handler
}

func NewHandler() http.Handler {
	sub, err := fs.Sub(embeddedFiles, "dist")
	if err != nil {
		panic(err)
	}

	indexHTML, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		panic(err)
	}

	return &handler{
		files:      sub,
		indexHTML:  indexHTML,
		fileServer: http.FileServer(http.FS(sub)),
	}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.NotFound(w, r)
		return
	}

	cleanedPath := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if cleanedPath == "." {
		cleanedPath = ""
	}

	if cleanedPath == "" {
		h.serveIndex(w, r)
		return
	}

	if fileExists(h.files, cleanedPath) {
		if strings.HasPrefix(cleanedPath, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "public, max-age=300")
		}
		h.fileServer.ServeHTTP(w, r)
		return
	}

	if shouldServeIndex(cleanedPath, r) {
		h.serveIndex(w, r)
		return
	}

	http.NotFound(w, r)
}

func (h *handler) serveIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(h.indexHTML)
}

func fileExists(files fs.FS, name string) bool {
	entry, err := fs.Stat(files, name)
	if err != nil {
		return false
	}
	return !entry.IsDir()
}

func shouldServeIndex(cleanedPath string, r *http.Request) bool {
	if !strings.Contains(path.Base(cleanedPath), ".") {
		return true
	}
	accept := r.Header.Get("Accept")
	return strings.Contains(accept, "text/html") && !strings.Contains(cleanedPath, ".")
}
