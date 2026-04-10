package replication

import (
	"context"
	"errors"
	"net/http"
	"time"

	"bares3-server/internal/storage"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"go.uber.org/zap"
)

const (
	StreamTypeHello     = "hello"
	StreamTypeHint      = "hint"
	StreamTypeHeartbeat = "heartbeat"

	streamHeartbeatInterval = 5 * time.Second
	streamWriteTimeout      = 5 * time.Second
	remoteManagerInterval   = 2 * time.Second
	remoteReconnectDelay    = 2 * time.Second
)

type StreamMessage struct {
	Type   string        `json:"type"`
	Cursor int64         `json:"cursor,omitempty"`
	Kind   string        `json:"kind,omitempty"`
	Bucket string        `json:"bucket,omitempty"`
	Key    string        `json:"key,omitempty"`
	Source *SourceStatus `json:"source,omitempty"`
	At     time.Time     `json:"at"`
}

func serveSyncStream(w http.ResponseWriter, r *http.Request, store *storage.Store, logger *zap.Logger) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
	}()

	ctx := r.Context()
	subscriptionID, events := store.SubscribeSyncEvents(128)
	defer store.UnsubscribeSyncEvents(subscriptionID)

	status, err := currentStreamSourceStatus(ctx, store)
	if err != nil {
		logger.Warn("build initial sync stream status failed", zap.Error(err))
		return
	}
	if err := writeStreamMessage(ctx, conn, StreamMessage{Type: StreamTypeHello, Cursor: status.Cursor, Source: &status, At: time.Now().UTC()}); err != nil {
		return
	}

	heartbeat := time.NewTicker(streamHeartbeatInterval)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-events:
			if !ok {
				return
			}
			if err := writeStreamMessage(ctx, conn, StreamMessage{Type: StreamTypeHint, Cursor: event.Cursor, Kind: event.Kind, Bucket: event.Bucket, Key: event.Key, At: time.Now().UTC()}); err != nil {
				return
			}
		case <-heartbeat.C:
			status, err := currentStreamSourceStatus(ctx, store)
			if err != nil {
				logger.Debug("build heartbeat sync stream status failed", zap.Error(err))
				continue
			}
			if err := writeStreamMessage(ctx, conn, StreamMessage{Type: StreamTypeHeartbeat, Cursor: status.Cursor, Source: &status, At: time.Now().UTC()}); err != nil {
				return
			}
		}
	}
}

func currentStreamSourceStatus(ctx context.Context, store *storage.Store) (SourceStatus, error) {
	usedBytes, objectCount, err := store.UsageSummary(ctx)
	if err != nil {
		return SourceStatus{}, err
	}
	buckets, err := store.ListBuckets(ctx)
	if err != nil {
		return SourceStatus{}, err
	}
	cursor, err := store.CurrentSyncCursor(ctx)
	if err != nil {
		return SourceStatus{}, err
	}
	return SourceStatus{Cursor: cursor, UsedBytes: usedBytes, BucketCount: len(buckets), ObjectCount: objectCount}, nil
}

func writeStreamMessage(ctx context.Context, conn *websocket.Conn, message StreamMessage) error {
	writeCtx, cancel := context.WithTimeout(ctx, streamWriteTimeout)
	defer cancel()
	if err := wsjson.Write(writeCtx, conn, message); err != nil {
		if errors.Is(err, context.Canceled) {
			return err
		}
		return err
	}
	return nil
}
