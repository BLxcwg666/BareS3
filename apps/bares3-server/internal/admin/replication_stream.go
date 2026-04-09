package admin

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"time"

	"bares3-server/internal/remotes"
	"bares3-server/internal/storage"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"go.uber.org/zap"
)

const replicationStreamInterval = time.Second

type replicationStreamSnapshot struct {
	Type     string               `json:"type"`
	Settings storage.SyncSettings `json:"settings"`
	Remotes  []remoteView         `json:"remotes"`
	At       time.Time            `json:"at"`
}

func serveReplicationAdminStream(w http.ResponseWriter, r *http.Request, store *storage.Store, remoteStore *remotes.Store, logger *zap.Logger) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer func() {
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
	}()

	ctx := r.Context()
	ticker := time.NewTicker(replicationStreamInterval)
	defer ticker.Stop()
	lastPayload := ""
	for {
		snapshot, payload, err := buildReplicationStreamSnapshot(ctx, store, remoteStore)
		if err != nil {
			logger.Debug("build replication stream snapshot failed", zap.Error(err))
			return
		}
		if payload != lastPayload {
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = wsjson.Write(writeCtx, conn, snapshot)
			cancel()
			if err != nil {
				return
			}
			lastPayload = payload
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func buildReplicationStreamSnapshot(ctx context.Context, store *storage.Store, remoteStore *remotes.Store) (replicationStreamSnapshot, string, error) {
	settings, err := store.SyncSettings(ctx)
	if errors.Is(err, os.ErrNotExist) {
		settings = storage.DefaultSyncSettings()
	} else if err != nil {
		return replicationStreamSnapshot{}, "", err
	}
	items, err := remoteStore.ListRemotes(ctx)
	if err != nil {
		return replicationStreamSnapshot{}, "", err
	}
	views, err := buildRemoteViews(ctx, store, items)
	if err != nil {
		return replicationStreamSnapshot{}, "", err
	}
	snapshot := replicationStreamSnapshot{Type: "snapshot", Settings: settings, Remotes: views, At: time.Now().UTC()}
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return replicationStreamSnapshot{}, "", err
	}
	return snapshot, string(encoded), nil
}
