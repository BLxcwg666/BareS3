package auditlog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultFilePerm = 0o644
	defaultDirPerm  = 0o755
	defaultLimit    = 20
)

type Entry struct {
	Time   time.Time `json:"time"`
	Actor  string    `json:"actor"`
	Action string    `json:"action"`
	Title  string    `json:"title"`
	Detail string    `json:"detail,omitempty"`
	Target string    `json:"target,omitempty"`
	Remote string    `json:"remote,omitempty"`
	Status string    `json:"status,omitempty"`
}

type Recorder struct {
	dir string
	mu  sync.Mutex
}

func New(dir string) (*Recorder, error) {
	trimmed := strings.TrimSpace(dir)
	if trimmed == "" {
		return nil, fmt.Errorf("audit log dir is required")
	}
	if err := os.MkdirAll(trimmed, defaultDirPerm); err != nil {
		return nil, fmt.Errorf("create audit log dir: %w", err)
	}
	return &Recorder{dir: trimmed}, nil
}

func (r *Recorder) Record(entry Entry) error {
	if entry.Time.IsZero() {
		entry.Time = time.Now().UTC()
	} else {
		entry.Time = entry.Time.UTC()
	}
	entry.Actor = strings.TrimSpace(entry.Actor)
	if entry.Actor == "" {
		entry.Actor = "system"
	}
	entry.Action = strings.TrimSpace(entry.Action)
	entry.Title = strings.TrimSpace(entry.Title)
	entry.Detail = strings.TrimSpace(entry.Detail)
	entry.Target = strings.TrimSpace(entry.Target)
	entry.Remote = strings.TrimSpace(entry.Remote)
	entry.Status = strings.TrimSpace(entry.Status)

	r.mu.Lock()
	defer r.mu.Unlock()

	path := filepath.Join(r.dir, filenameFor(entry.Time))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, defaultFilePerm)
	if err != nil {
		return fmt.Errorf("open audit log: %w", err)
	}
	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	writeErr := encoder.Encode(entry)
	closeErr := file.Close()
	if writeErr != nil {
		return fmt.Errorf("write audit log: %w", writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close audit log: %w", closeErr)
	}
	return nil
}

func (r *Recorder) Recent(limit int) ([]Entry, error) {
	if limit < 0 {
		limit = defaultLimit
	}
	if limit == 0 {
		return []Entry{}, nil
	}

	paths, err := filepath.Glob(filepath.Join(r.dir, "audit_*.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("glob audit logs: %w", err)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(paths)))

	entries := make([]Entry, 0, limit)
	for _, path := range paths {
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read audit log %s: %w", path, err)
		}
		lines := bytes.Split(content, []byte{'\n'})
		for index := len(lines) - 1; index >= 0; index -= 1 {
			line := bytes.TrimSpace(lines[index])
			if len(line) == 0 {
				continue
			}
			entry := Entry{}
			if err := json.Unmarshal(line, &entry); err != nil {
				return nil, fmt.Errorf("decode audit log %s: %w", path, err)
			}
			entries = append(entries, entry)
			if len(entries) >= limit {
				return entries, nil
			}
		}
	}

	return entries, nil
}

func filenameFor(now time.Time) string {
	return "audit_" + now.UTC().Format("2006-01-02") + ".jsonl"
}
