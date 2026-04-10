package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/remotes"
	"bares3-server/internal/replication"
	"bares3-server/internal/storage"
)

var replicationBootstrapHTTPClient = &http.Client{Timeout: 15 * time.Second}

type remoteView struct {
	remotes.Remote
	SyncCounts storage.SyncStatusCounts `json:"sync_counts"`
}

func fetchRemoteStatusForBootstrap(ctx context.Context, endpoint, token string) (replication.SourceStatus, error) {
	requestURL, err := joinReplicationURL(endpoint, "/internal/sync/status")
	if err != nil {
		return replication.SourceStatus{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return replication.SourceStatus{}, err
	}
	req.Header.Set(replication.HeaderAccessToken, strings.TrimSpace(token))
	res, err := replicationBootstrapHTTPClient.Do(req)
	if err != nil {
		return replication.SourceStatus{}, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		payload := struct {
			Message string `json:"message"`
		}{}
		_ = json.NewDecoder(res.Body).Decode(&payload)
		if strings.TrimSpace(payload.Message) == "" {
			payload.Message = fmt.Sprintf("bootstrap status probe failed with status %d", res.StatusCode)
		}
		return replication.SourceStatus{}, fmt.Errorf("%s", payload.Message)
	}
	status := replication.SourceStatus{}
	if err := json.NewDecoder(res.Body).Decode(&status); err != nil {
		return replication.SourceStatus{}, err
	}
	return status, nil
}

func refetchObjectFromSource(ctx context.Context, store *storage.Store, remoteStore *remotes.Store, statusItem storage.SyncObjectStatus, bucket, key string) error {
	sourceID := strings.TrimSpace(statusItem.Source)
	if sourceID == "" {
		return fmt.Errorf("sync status does not identify a replication source")
	}
	remote, err := remoteStore.GetRemote(ctx, sourceID)
	if err != nil {
		return err
	}
	return refetchObjectFromEndpoint(ctx, store, remote.Endpoint, replication.HeaderAccessToken, remote.Token, remote.ID, remote.DisplayName, bucket, key)
}

func refetchObjectFromEndpoint(ctx context.Context, store *storage.Store, endpoint, headerName, headerValue, sourceID, label, bucket, key string) error {
	requestURL, err := replication.JoinLeaderObjectURLForReplica(endpoint, bucket, key)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set(headerName, headerValue)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("fetch replica object: unexpected status %d", res.StatusCode)
	}
	revision, _ := strconv.ParseInt(strings.TrimSpace(res.Header.Get(replication.HeaderRevision)), 10, 64)
	lastModified := time.Time{}
	if header := strings.TrimSpace(res.Header.Get("Last-Modified")); header != "" {
		lastModified, _ = time.Parse(http.TimeFormat, header)
	}
	_, err = store.ApplyReplicaObject(ctx, storage.ReplicaObjectInput{
		Bucket:         bucket,
		Key:            key,
		Body:           res.Body,
		Size:           parseInt64OrZero(res.Header.Get("Content-Length")),
		ETag:           res.Header.Get(replication.HeaderETag),
		ChecksumSHA256: res.Header.Get(replication.HeaderChecksumSHA256),
		Revision:       revision,
		OriginNodeID:   res.Header.Get(replication.HeaderOriginNodeID),
		LastChangeID:   res.Header.Get(replication.HeaderLastChangeID),
		LastModified:   lastModified,
		Force:          true,
	})
	if err != nil {
		return err
	}
	return store.SetObjectSyncStatus(ctx, storage.SyncObjectStatus{Bucket: bucket, Key: key, Status: storage.SyncStatusReady, ExpectedChecksumSHA256: res.Header.Get(replication.HeaderChecksumSHA256), Source: sourceID, BaselineNodeID: label, UpdatedAt: time.Now().UTC()})
}

func joinReplicationURL(baseURL, endpointPath string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("parse replication url: %w", err)
	}
	parsed.Path = path.Join(strings.TrimSuffix(parsed.Path, "/"), endpointPath)
	parsed.RawQuery = ""
	return parsed.String(), nil
}

func buildRemoteView(ctx context.Context, store *storage.Store, remote remotes.Remote) (remoteView, error) {
	counts, err := store.SyncStatusCounts(ctx, remote.ID)
	if err != nil {
		return remoteView{}, err
	}
	return remoteView{Remote: remote, SyncCounts: counts}, nil
}

func buildRemoteViews(ctx context.Context, store *storage.Store, items []remotes.Remote) ([]remoteView, error) {
	views := make([]remoteView, 0, len(items))
	for _, item := range items {
		view, err := buildRemoteView(ctx, store, item)
		if err != nil {
			return nil, err
		}
		views = append(views, view)
	}
	return views, nil
}
