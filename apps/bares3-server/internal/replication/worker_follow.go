package replication

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"bares3-server/internal/remotes"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"go.uber.org/zap"
)

type remoteRunner struct {
	signature string
	cancel    context.CancelFunc
	done      chan struct{}
}

func (w *Worker) reconcileRemoteRunners(ctx context.Context, runners map[string]*remoteRunner) error {
	for id, runner := range runners {
		select {
		case <-runner.done:
			delete(runners, id)
		default:
		}
	}

	settings, err := w.syncSettings(ctx)
	if err != nil {
		return err
	}
	if !settings.Enabled {
		for _, runner := range runners {
			runner.cancel()
		}
		return nil
	}

	items, err := w.remotes.ListRemotes(ctx)
	if err != nil {
		return err
	}
	desired := make(map[string]remotes.Remote, len(items))
	for _, item := range items {
		desired[item.ID] = item
	}
	for id, runner := range runners {
		remote, ok := desired[id]
		if !ok || remoteRunnerSignature(remote) != runner.signature || (!remote.FollowChanges && remote.LastSyncAt != nil) {
			runner.cancel()
			delete(runners, id)
		}
	}
	for _, remote := range items {
		signature := remoteRunnerSignature(remote)
		if _, ok := runners[remote.ID]; ok {
			continue
		}
		if !remote.FollowChanges && remote.LastSyncAt != nil {
			continue
		}
		runnerCtx, cancel := context.WithCancel(ctx)
		runner := &remoteRunner{signature: signature, cancel: cancel, done: make(chan struct{})}
		runners[remote.ID] = runner
		go func(remote remotes.Remote, runner *remoteRunner) {
			defer close(runner.done)
			if remote.FollowChanges {
				w.runRemoteFollower(runnerCtx, remote.ID)
				return
			}
			if err := w.syncRemoteByID(runnerCtx, remote.ID); err != nil && runnerCtx.Err() == nil {
				w.logger.Warn("snapshot sync failed", zap.String("remote_id", remote.ID), zap.Error(err))
			}
		}(remote, runner)
	}
	return nil
}

func remoteRunnerSignature(remote remotes.Remote) string {
	return strings.Join([]string{remote.Endpoint, remote.Token, remote.BootstrapMode, fmt.Sprintf("%t", remote.FollowChanges)}, "\n")
}

func (w *Worker) runRemoteFollower(ctx context.Context, remoteID string) {
	for {
		if ctx.Err() != nil {
			return
		}
		remote, err := w.remotes.GetRemote(ctx, remoteID)
		if err != nil {
			w.logger.Debug("load remote before stream failed", zap.String("remote_id", remoteID), zap.Error(err))
			return
		}
		connecting := remotes.ConnectionStatusConnecting
		_ = w.remotes.UpdateRemoteState(ctx, remotes.UpdateRemoteStateInput{ID: remote.ID, ConnectionStatus: &connecting})
		conn, err := w.connectRemoteStream(ctx, remote)
		if err != nil {
			w.logger.Debug("connect remote stream failed", zap.String("remote_id", remote.ID), zap.Error(err))
			disconnected := remotes.ConnectionStatusDisconnected
			_ = w.remotes.UpdateRemoteState(ctx, remotes.UpdateRemoteStateInput{ID: remote.ID, ConnectionStatus: &disconnected})
			if !sleepContext(ctx, remoteReconnectDelay) {
				return
			}
			continue
		}
		connected := remotes.ConnectionStatusConnected
		now := time.Now().UTC()
		_ = w.remotes.UpdateRemoteState(ctx, remotes.UpdateRemoteStateInput{ID: remote.ID, ConnectionStatus: &connected, LastHeartbeatAt: &now})
		if err := w.syncRemoteByID(ctx, remote.ID); err != nil && ctx.Err() == nil {
			w.logger.Warn("sync remote after connect failed", zap.String("remote_id", remote.ID), zap.Error(err))
		}
		if err := w.consumeRemoteStream(ctx, remote.ID, conn); err != nil && ctx.Err() == nil {
			w.logger.Debug("remote stream closed", zap.String("remote_id", remote.ID), zap.Error(err))
		}
		_ = conn.Close(websocket.StatusNormalClosure, "bye")
		disconnected := remotes.ConnectionStatusDisconnected
		_ = w.remotes.UpdateRemoteState(ctx, remotes.UpdateRemoteStateInput{ID: remote.ID, ConnectionStatus: &disconnected})
		if !sleepContext(ctx, remoteReconnectDelay) {
			return
		}
	}
}

func (w *Worker) syncRemoteByID(ctx context.Context, remoteID string) error {
	remote, err := w.remotes.GetRemote(ctx, remoteID)
	if err != nil {
		return err
	}
	return w.syncRemote(ctx, remote)
}

func (w *Worker) connectRemoteStream(ctx context.Context, remote remotes.Remote) (*websocket.Conn, error) {
	streamURL, err := remoteStreamURL(remote.Endpoint)
	if err != nil {
		return nil, err
	}
	header := http.Header{}
	header.Set(HeaderAccessToken, remote.Token)
	conn, _, err := websocket.Dial(ctx, streamURL, &websocket.DialOptions{HTTPHeader: header})
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (w *Worker) consumeRemoteStream(ctx context.Context, remoteID string, conn *websocket.Conn) error {
	for {
		message := StreamMessage{}
		if err := wsjson.Read(ctx, conn, &message); err != nil {
			return err
		}
		now := time.Now().UTC()
		connected := remotes.ConnectionStatusConnected
		state := remotes.UpdateRemoteStateInput{ID: remoteID, ConnectionStatus: &connected, LastHeartbeatAt: &now}
		if message.Source != nil {
			state.PeerCursor = &message.Source.Cursor
			peerUsedBytes := message.Source.UsedBytes
			peerBucketCount := int64(message.Source.BucketCount)
			peerObjectCount := int64(message.Source.ObjectCount)
			state.PeerUsedBytes = &peerUsedBytes
			state.PeerBucketCount = &peerBucketCount
			state.PeerObjectCount = &peerObjectCount
		}
		_ = w.remotes.UpdateRemoteState(ctx, state)
		switch message.Type {
		case StreamTypeHint:
			if err := w.syncRemoteByID(ctx, remoteID); err != nil && ctx.Err() == nil {
				w.logger.Warn("sync remote after hint failed", zap.String("remote_id", remoteID), zap.Error(err))
			}
		case StreamTypeHello:
			continue
		case StreamTypeHeartbeat:
			continue
		}
	}
}

func remoteStreamURL(endpoint string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(endpoint))
	if err != nil {
		return "", err
	}
	switch strings.ToLower(parsed.Scheme) {
	case "https":
		parsed.Scheme = "wss"
	default:
		parsed.Scheme = "ws"
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/internal/sync/stream"
	parsed.RawQuery = ""
	return parsed.String(), nil
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
