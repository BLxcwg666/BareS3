package logx

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	defaultLogFilePerm = 0o644
	defaultLogDirPerm  = 0o755
	defaultSubBufSize  = 128
)

type Writer struct {
	mu            sync.Mutex
	dir           string
	rotateMaxSize int64
	rotateKeep    int
}

func NewWriter(dir string, rotateSizeMB, rotateKeep int) (*Writer, error) {
	if err := os.MkdirAll(dir, defaultLogDirPerm); err != nil {
		return nil, fmt.Errorf("create log dir: %w", err)
	}

	if rotateSizeMB <= 0 {
		rotateSizeMB = 16
	}

	return &Writer{
		dir:           dir,
		rotateMaxSize: int64(rotateSizeMB) * 1024 * 1024,
		rotateKeep:    rotateKeep,
	}, nil
}

func (w *Writer) Write(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	path := filepath.Join(w.dir, todayFilename(time.Now()))
	if err := w.rotateIfNeeded(path, int64(len(payload))); err != nil {
		return 0, err
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, defaultLogFilePerm)
	if err != nil {
		return 0, err
	}

	n, writeErr := file.Write(payload)
	closeErr := file.Close()
	if writeErr != nil {
		return n, writeErr
	}
	if closeErr != nil {
		return n, closeErr
	}

	if n > 0 {
		Publish(string(payload[:n]))
	}

	return n, nil
}

func (w *Writer) Sync() error {
	return nil
}

func todayFilename(now time.Time) string {
	return "bares3_" + now.Format("2006-01-02") + ".log"
}

func (w *Writer) rotateIfNeeded(path string, incoming int64) error {
	if w.rotateMaxSize <= 0 {
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	if info.Size()+incoming <= w.rotateMaxSize {
		return nil
	}

	return w.rotate(path)
}

func (w *Writer) rotate(path string) error {
	if w.rotateKeep <= 0 {
		file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, defaultLogFilePerm)
		if err != nil {
			return err
		}
		return file.Close()
	}

	oldest := rotatedPath(path, w.rotateKeep)
	if err := os.Remove(oldest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	for index := w.rotateKeep - 1; index >= 1; index-- {
		src := rotatedPath(path, index)
		dst := rotatedPath(path, index+1)
		if err := os.Rename(src, dst); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	if err := os.Rename(path, rotatedPath(path, 1)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}

	return nil
}

func rotatedPath(path string, index int) string {
	return fmt.Sprintf("%s.%d", path, index)
}

type streamHub struct {
	mu          sync.RWMutex
	nextID      int
	subscribers map[int]chan string
}

var globalStreamHub = &streamHub{subscribers: make(map[int]chan string)}

func Subscribe(buffer int) (int, <-chan string) {
	if buffer <= 0 {
		buffer = defaultSubBufSize
	}
	return globalStreamHub.subscribe(buffer)
}

func Unsubscribe(id int) {
	globalStreamHub.unsubscribe(id)
}

func Publish(message string) {
	if message == "" {
		return
	}
	globalStreamHub.publish(message)
}

func (h *streamHub) subscribe(buffer int) (int, <-chan string) {
	channel := make(chan string, buffer)

	h.mu.Lock()
	defer h.mu.Unlock()

	id := h.nextID
	h.nextID++
	h.subscribers[id] = channel

	return id, channel
}

func (h *streamHub) unsubscribe(id int) {
	h.mu.Lock()
	channel, ok := h.subscribers[id]
	if ok {
		delete(h.subscribers, id)
	}
	h.mu.Unlock()

	if ok {
		close(channel)
	}
}

func (h *streamHub) publish(message string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	for _, channel := range h.subscribers {
		select {
		case channel <- message:
		default:
		}
	}
}
