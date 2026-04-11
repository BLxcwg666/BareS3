package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"reflect"
	"strings"
	"sync"
	"time"

	"bares3-server/internal/config"
	"bares3-server/internal/remotes"
	"bares3-server/internal/storage"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const defaultObjectSyncParallelism = 4

type Worker struct {
	store       *storage.Store
	remotes     *remotes.Store
	client      *http.Client
	logger      *zap.Logger
	parallelism int
}

type syncTarget struct {
	ID           string
	Label        string
	Endpoint     string
	Cursor       int64
	AuthHeader   string
	AuthValue    string
	DeleteExtras bool
	StatusSource string
}

type syncRunTracker struct {
	startedAt time.Time

	mu               sync.Mutex
	totalObjects     int64
	completedObjects int64
	totalBytes       int64
	completedBytes   int64
	downloadedBytes  int64
	uploadedBytes    int64
}

func NewWorker(cfg config.Config, store *storage.Store, logger *zap.Logger) *Worker {
	if logger == nil {
		logger = zap.NewNop()
	}
	remoteStore, err := remotes.New(cfg.Paths.DataDir, logger.Named("remotes"))
	if err != nil {
		panic(fmt.Sprintf("initialize replication remote store: %v", err))
	}
	return &Worker{
		store:   store,
		remotes: remoteStore,
		client: &http.Client{
			Timeout: 2 * time.Minute,
		},
		logger:      logger,
		parallelism: defaultObjectSyncParallelism,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	ticker := time.NewTicker(remoteManagerInterval)
	defer ticker.Stop()
	runners := make(map[string]*remoteRunner)
	for {
		if err := w.reconcileRemoteRunners(ctx, runners); err != nil && ctx.Err() == nil {
			w.logger.Warn("reconcile remote runners failed", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			for _, runner := range runners {
				runner.cancel()
			}
			for _, runner := range runners {
				<-runner.done
			}
			return nil
		case <-ticker.C:
		}
	}
}

func (w *Worker) SyncOnce(ctx context.Context) error {
	settings, err := w.syncSettings(ctx)
	if err != nil {
		return err
	}
	if !settings.Enabled {
		return nil
	}
	configuredRemotes, err := w.remotes.ListRemotes(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, remote := range configuredRemotes {
		if !remote.Enabled {
			continue
		}
		if err := w.syncRemote(ctx, remote); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (w *Worker) syncRemote(ctx context.Context, remote remotes.Remote) error {
	tracker := newSyncRunTracker(time.Now().UTC())
	if err := w.remotes.UpdateRemoteState(ctx, tracker.startState(remote.ID)); err != nil {
		return err
	}
	stopReporter := w.startRemoteProgressReporter(ctx, remote.ID, tracker)
	defer stopReporter()
	target := syncTarget{
		ID:           remote.ID,
		Label:        remote.DisplayName,
		Endpoint:     remote.Endpoint,
		Cursor:       remote.Cursor,
		AuthHeader:   HeaderAccessToken,
		AuthValue:    remote.Token,
		DeleteExtras: false,
		StatusSource: remote.ID,
	}
	nextCursor, err := w.syncTarget(ctx, target, tracker)
	if err != nil {
		_ = w.remotes.UpdateRemoteState(ctx, tracker.errorState(remote.ID, err.Error()))
		return err
	}
	syncedAt := time.Now().UTC()
	if err := w.remotes.UpdateRemoteState(ctx, tracker.successState(remote.ID, nextCursor, syncedAt)); err != nil {
		return err
	}
	return nil
}
func (w *Worker) syncTarget(ctx context.Context, target syncTarget, tracker *syncRunTracker) (int64, error) {
	sourceStatus, err := w.fetchSourceStatus(ctx, target, tracker)
	if err != nil {
		return target.Cursor, err
	}
	goalCursor := sourceStatus.Cursor
	for {
		manifest, err := w.fetchManifest(ctx, target, tracker)
		if err != nil {
			return target.Cursor, err
		}
		nextCursor, err := w.applyManifest(ctx, target, manifest, tracker)
		if err != nil {
			return target.Cursor, err
		}
		if nextCursor <= target.Cursor {
			return nextCursor, nil
		}
		target.Cursor = nextCursor
		if manifest.Full || !manifest.HasMore || target.Cursor >= goalCursor {
			return target.Cursor, nil
		}
	}
}

func (w *Worker) applyManifest(ctx context.Context, target syncTarget, manifest Manifest, tracker *syncRunTracker) (int64, error) {
	if err := w.reconcileBuckets(ctx, manifest.Buckets); err != nil {
		return target.Cursor, err
	}
	if manifest.Full || manifest.DomainsChanged {
		if err := w.reconcileDomainBindings(ctx, manifest.Domains); err != nil {
			return target.Cursor, err
		}
	}
	if err := w.reconcileObjects(ctx, target, manifest.Objects, tracker); err != nil {
		return target.Cursor, err
	}
	if manifest.Full {
		if target.DeleteExtras {
			if err := w.deleteExtraObjects(ctx, manifest.Objects); err != nil {
				return target.Cursor, err
			}
			if err := w.deleteExtraBuckets(ctx, manifest.Buckets); err != nil {
				return target.Cursor, err
			}
		}
		return manifest.Cursor, nil
	}
	if err := w.deleteObjects(ctx, manifest.DeletedObjects); err != nil {
		return target.Cursor, err
	}
	if err := w.deleteBuckets(ctx, manifest.DeletedBuckets); err != nil {
		return target.Cursor, err
	}
	return manifest.Cursor, nil
}

func (w *Worker) fetchSourceStatus(ctx context.Context, target syncTarget, tracker *syncRunTracker) (SourceStatus, error) {
	requestURL, err := joinLeaderURL(target.Endpoint, "/internal/sync/status")
	if err != nil {
		return SourceStatus{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return SourceStatus{}, fmt.Errorf("build source status request: %w", err)
	}
	req.Header.Set(target.AuthHeader, target.AuthValue)
	tracker.addUpload(estimateRequestBytes(req))
	res, err := w.client.Do(req)
	if err != nil {
		return SourceStatus{}, fmt.Errorf("fetch source status: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return SourceStatus{}, fmt.Errorf("fetch source status: unexpected status %d", res.StatusCode)
	}
	status := SourceStatus{}
	if err := json.NewDecoder(newCountingReadCloser(res.Body, func(n int64) {
		tracker.addControlDownload(n)
	})).Decode(&status); err != nil {
		return SourceStatus{}, fmt.Errorf("decode source status: %w", err)
	}
	return status, nil
}

func (w *Worker) fetchManifest(ctx context.Context, target syncTarget, tracker *syncRunTracker) (Manifest, error) {
	requestURL, err := joinLeaderURL(target.Endpoint, "/internal/sync/manifest")
	if err != nil {
		return Manifest{}, err
	}
	if target.Cursor > 0 {
		parsed, err := url.Parse(requestURL)
		if err != nil {
			return Manifest{}, fmt.Errorf("parse manifest url: %w", err)
		}
		query := parsed.Query()
		query.Set("cursor", fmt.Sprintf("%d", target.Cursor))
		parsed.RawQuery = query.Encode()
		requestURL = parsed.String()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		return Manifest{}, fmt.Errorf("build manifest request: %w", err)
	}
	req.Header.Set(target.AuthHeader, target.AuthValue)
	tracker.addUpload(estimateRequestBytes(req))
	res, err := w.client.Do(req)
	if err != nil {
		return Manifest{}, fmt.Errorf("fetch manifest: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return Manifest{}, fmt.Errorf("fetch manifest: unexpected status %d", res.StatusCode)
	}
	manifest := Manifest{}
	if err := json.NewDecoder(newCountingReadCloser(res.Body, func(n int64) {
		tracker.addControlDownload(n)
	})).Decode(&manifest); err != nil {
		return Manifest{}, fmt.Errorf("decode manifest: %w", err)
	}
	return manifest, nil
}

func (w *Worker) reconcileBuckets(ctx context.Context, remote []BucketManifest) error {
	localBuckets, err := w.store.ListBuckets(ctx)
	if err != nil {
		return err
	}
	localByName := make(map[string]storage.BucketInfo, len(localBuckets))
	localAccessByName := make(map[string]storage.BucketAccessConfig, len(localBuckets))
	for _, bucket := range localBuckets {
		localByName[bucket.Name] = bucket
		access, err := w.store.GetBucketAccessConfig(ctx, bucket.Name)
		if err != nil {
			return err
		}
		localAccessByName[bucket.Name] = access
	}

	for _, bucket := range remote {
		local, exists := localByName[bucket.Name]
		if exists && !bucketChanged(local, localAccessByName[bucket.Name], bucket) {
			continue
		}
		if _, err := w.store.ApplyReplicaBucket(ctx, storage.ReplicaBucketInput{
			Name:           bucket.Name,
			CreatedAt:      bucket.CreatedAt,
			MetadataLayout: bucket.MetadataLayout,
			AccessMode:     bucket.AccessMode,
			AccessPolicy:   bucket.AccessPolicy,
			QuotaBytes:     bucket.QuotaBytes,
			Tags:           bucket.Tags,
			Note:           bucket.Note,
		}); err != nil {
			return fmt.Errorf("apply bucket %s: %w", bucket.Name, err)
		}
	}
	return nil
}

func (w *Worker) reconcileDomainBindings(ctx context.Context, remote []storage.PublicDomainBinding) error {
	_, err := w.store.ApplyReplicaDomainBindings(ctx, remote)
	return err
}

func (w *Worker) reconcileObjects(ctx context.Context, target syncTarget, remote []ObjectManifest, tracker *syncRunTracker) error {
	localBuckets, err := w.store.ListBuckets(ctx)
	if err != nil {
		return err
	}
	localObjects := make(map[string]storage.ObjectInfo)
	for _, bucket := range localBuckets {
		objects, err := w.store.ListObjects(ctx, bucket.Name, storage.ListObjectsOptions{})
		if err != nil {
			return err
		}
		for _, object := range objects {
			localObjects[objectID(object.Bucket, object.Key)] = object
		}
	}
	toSync := make([]ObjectManifest, 0)
	for _, object := range remote {
		id := objectID(object.Bucket, object.Key)
		if local, exists := localObjects[id]; exists && !objectChanged(local, object) {
			if err := w.setObjectStatus(ctx, target, object, storage.SyncStatusReady, ""); err != nil {
				return err
			}
			continue
		}
		if err := w.setObjectStatus(ctx, target, object, storage.SyncStatusPending, ""); err != nil {
			return err
		}
		toSync = append(toSync, object)
	}
	var totalBytes int64
	for _, object := range toSync {
		if object.Size > 0 {
			totalBytes += object.Size
		}
	}
	tracker.setPlan(int64(len(toSync)), totalBytes)
	if len(toSync) == 0 {
		return nil
	}
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(w.parallelism)
	for _, object := range toSync {
		object := object
		group.Go(func() error {
			return w.syncObject(groupCtx, target, object, tracker)
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return nil
}

func (w *Worker) deleteExtraObjects(ctx context.Context, remote []ObjectManifest) error {
	remoteIDs := make(map[string]struct{}, len(remote))
	for _, object := range remote {
		remoteIDs[objectID(object.Bucket, object.Key)] = struct{}{}
	}
	localBuckets, err := w.store.ListBuckets(ctx)
	if err != nil {
		return err
	}
	for _, bucket := range localBuckets {
		objects, err := w.store.ListObjects(ctx, bucket.Name, storage.ListObjectsOptions{})
		if err != nil {
			return err
		}
		for _, object := range objects {
			if _, ok := remoteIDs[objectID(object.Bucket, object.Key)]; ok {
				continue
			}
			if err := w.deleteObject(ctx, object.Bucket, object.Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Worker) deleteExtraBuckets(ctx context.Context, remote []BucketManifest) error {
	remoteNames := make(map[string]struct{}, len(remote))
	for _, bucket := range remote {
		remoteNames[bucket.Name] = struct{}{}
	}
	localBuckets, err := w.store.ListBuckets(ctx)
	if err != nil {
		return err
	}
	for _, bucket := range localBuckets {
		if _, ok := remoteNames[bucket.Name]; ok {
			continue
		}
		if err := w.store.DeleteBucket(ctx, bucket.Name); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
			return fmt.Errorf("delete stale bucket %s: %w", bucket.Name, err)
		}
	}
	return nil
}

func (w *Worker) deleteBuckets(ctx context.Context, names []string) error {
	for _, name := range names {
		objects, err := w.store.ListObjects(ctx, name, storage.ListObjectsOptions{})
		switch {
		case err == nil:
			for _, object := range objects {
				if err := w.deleteObject(ctx, object.Bucket, object.Key); err != nil {
					return err
				}
			}
		case errors.Is(err, storage.ErrBucketNotFound):
			continue
		default:
			return fmt.Errorf("list replicated bucket %s before delete: %w", name, err)
		}
		if err := w.store.DeleteBucket(ctx, name); err != nil && !errors.Is(err, storage.ErrBucketNotFound) {
			return fmt.Errorf("delete replicated bucket %s: %w", name, err)
		}
	}
	return nil
}

func (w *Worker) deleteObjects(ctx context.Context, objects []DeletedObjectManifest) error {
	for _, object := range objects {
		if err := w.deleteObject(ctx, object.Bucket, object.Key); err != nil {
			return err
		}
	}
	return nil
}

func (w *Worker) syncObject(ctx context.Context, target syncTarget, object ObjectManifest, tracker *syncRunTracker) error {
	if err := w.setObjectStatus(ctx, target, object, storage.SyncStatusDownloading, ""); err != nil {
		return err
	}
	requestURL, err := joinLeaderObjectURL(target.Endpoint, object.Bucket, object.Key)
	if err != nil {
		_ = w.setObjectStatus(ctx, target, object, storage.SyncStatusError, err.Error())
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		_ = w.setObjectStatus(ctx, target, object, storage.SyncStatusError, err.Error())
		return fmt.Errorf("build object request for %s/%s: %w", object.Bucket, object.Key, err)
	}
	req.Header.Set(target.AuthHeader, target.AuthValue)
	tracker.addUpload(estimateRequestBytes(req))
	res, err := w.client.Do(req)
	if err != nil {
		_ = w.setObjectStatus(ctx, target, object, storage.SyncStatusError, err.Error())
		return fmt.Errorf("fetch object %s/%s: %w", object.Bucket, object.Key, err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		_ = w.setObjectStatus(ctx, target, object, storage.SyncStatusError, fmt.Sprintf("unexpected status %d", res.StatusCode))
		return fmt.Errorf("fetch object %s/%s: unexpected status %d", object.Bucket, object.Key, res.StatusCode)
	}
	if checksum := strings.TrimSpace(res.Header.Get(HeaderChecksumSHA256)); checksum != "" && !strings.EqualFold(checksum, object.ChecksumSHA256) {
		_ = w.setObjectStatus(ctx, target, object, storage.SyncStatusError, "response checksum mismatch")
		return fmt.Errorf("fetch object %s/%s: manifest checksum %s does not match response checksum %s", object.Bucket, object.Key, object.ChecksumSHA256, checksum)
	}
	body := newCountingReadCloser(res.Body, func(n int64) {
		tracker.addPayloadDownload(n)
	})

	_, err = w.store.ApplyReplicaObject(ctx, storage.ReplicaObjectInput{
		Bucket:             object.Bucket,
		Key:                object.Key,
		Body:               body,
		Size:               object.Size,
		ETag:               object.ETag,
		ChecksumSHA256:     object.ChecksumSHA256,
		Revision:           object.Revision,
		OriginNodeID:       object.OriginNodeID,
		LastChangeID:       object.LastChangeID,
		ContentType:        object.ContentType,
		CacheControl:       object.CacheControl,
		ContentDisposition: object.ContentDisposition,
		UserMetadata:       object.UserMetadata,
		LastModified:       object.LastModified,
	})
	if err != nil {
		status := storage.SyncStatusError
		if errors.Is(err, storage.ErrObjectConflict) {
			status = storage.SyncStatusConflict
		}
		_ = w.setObjectStatus(ctx, target, object, status, err.Error())
		return fmt.Errorf("apply object %s/%s: %w", object.Bucket, object.Key, err)
	}
	if err := w.setObjectStatus(ctx, target, object, storage.SyncStatusReady, ""); err != nil {
		return err
	}
	tracker.completeObject()
	return nil
}

func (w *Worker) setObjectStatus(ctx context.Context, target syncTarget, object ObjectManifest, status, lastError string) error {
	return w.store.SetObjectSyncStatus(ctx, storage.SyncObjectStatus{
		Bucket:                 object.Bucket,
		Key:                    object.Key,
		Status:                 status,
		ExpectedChecksumSHA256: object.ChecksumSHA256,
		LastError:              lastError,
		Source:                 target.StatusSource,
		BaselineNodeID:         target.Label,
		UpdatedAt:              time.Now().UTC(),
	})
}

func (w *Worker) deleteObject(ctx context.Context, bucket, key string) error {
	if err := w.store.DeleteObject(ctx, bucket, key); err != nil && !errors.Is(err, storage.ErrObjectNotFound) && !errors.Is(err, storage.ErrBucketNotFound) {
		return fmt.Errorf("delete stale object %s/%s: %w", bucket, key, err)
	}
	if err := w.store.DeleteObjectSyncStatus(ctx, bucket, key); err != nil {
		return fmt.Errorf("delete sync status for %s/%s: %w", bucket, key, err)
	}
	return nil
}

func (w *Worker) syncSettings(ctx context.Context) (storage.SyncSettings, error) {
	settings, err := w.store.SyncSettings(ctx)
	if errors.Is(err, os.ErrNotExist) {
		return storage.DefaultSyncSettings(), nil
	}
	if err != nil {
		return storage.SyncSettings{}, err
	}
	return settings, nil
}

func joinLeaderURL(baseURL, endpointPath string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(baseURL))
	if err != nil {
		return "", fmt.Errorf("parse leader url: %w", err)
	}
	parsed.Path = path.Join(strings.TrimSuffix(parsed.Path, "/"), endpointPath)
	parsed.RawQuery = ""
	return parsed.String(), nil
}

func joinLeaderObjectURL(baseURL, bucket, key string) (string, error) {
	joined, err := joinLeaderURL(baseURL, "/internal/sync/object")
	if err != nil {
		return "", err
	}
	parsed, err := url.Parse(joined)
	if err != nil {
		return "", fmt.Errorf("parse object sync url: %w", err)
	}
	query := parsed.Query()
	query.Set("bucket", bucket)
	query.Set("key", key)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

func JoinLeaderObjectURLForReplica(baseURL, bucket, key string) (string, error) {
	return joinLeaderObjectURL(baseURL, bucket, key)
}

func bucketChanged(local storage.BucketInfo, localAccess storage.BucketAccessConfig, remote BucketManifest) bool {
	if !local.CreatedAt.Equal(remote.CreatedAt) {
		return true
	}
	if local.MetadataLayout != remote.MetadataLayout || local.AccessMode != remote.AccessMode || local.QuotaBytes != remote.QuotaBytes || local.Note != remote.Note {
		return true
	}
	if !reflect.DeepEqual(local.Tags, remote.Tags) {
		return true
	}
	if localAccess.Mode != remote.AccessMode {
		return true
	}
	return !reflect.DeepEqual(localAccess.Policy, remote.AccessPolicy)
}

func objectChanged(local storage.ObjectInfo, remote ObjectManifest) bool {
	if local.Size != remote.Size || local.ETag != remote.ETag || local.ChecksumSHA256 != remote.ChecksumSHA256 {
		return true
	}
	if local.ContentType != remote.ContentType || local.CacheControl != remote.CacheControl || local.ContentDisposition != remote.ContentDisposition {
		return true
	}
	if !local.LastModified.Equal(remote.LastModified) {
		return true
	}
	return !reflect.DeepEqual(local.UserMetadata, remote.UserMetadata)
}

func objectID(bucket, key string) string {
	return bucket + "\n" + key
}

func newSyncRunTracker(startedAt time.Time) *syncRunTracker {
	return &syncRunTracker{startedAt: startedAt.UTC()}
}

func (t *syncRunTracker) setPlan(totalObjects, totalBytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.totalObjects = totalObjects
	t.totalBytes = totalBytes
	t.completedObjects = 0
	t.completedBytes = 0
	t.downloadedBytes = 0
	t.uploadedBytes = 0
}

func (t *syncRunTracker) addPayloadDownload(bytes int64) {
	if bytes <= 0 {
		return
	}
	t.mu.Lock()
	t.completedBytes += bytes
	t.downloadedBytes += bytes
	t.mu.Unlock()
}

func (t *syncRunTracker) addControlDownload(bytes int64) {
	if bytes <= 0 {
		return
	}
	t.mu.Lock()
	t.downloadedBytes += bytes
	t.mu.Unlock()
}

func (t *syncRunTracker) addUpload(bytes int64) {
	if bytes <= 0 {
		return
	}
	t.mu.Lock()
	t.uploadedBytes += bytes
	t.mu.Unlock()
}

func (t *syncRunTracker) completeObject() {
	t.mu.Lock()
	t.completedObjects += 1
	t.mu.Unlock()
}

func (t *syncRunTracker) snapshot() (int64, int64, int64, int64, int64, int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.totalObjects, t.completedObjects, t.totalBytes, t.completedBytes, t.downloadedBytes, t.uploadedBytes
}

func (t *syncRunTracker) startState(remoteID string) remotes.UpdateRemoteStateInput {
	zero := int64(0)
	status := remotes.RemoteStatusSyncing
	clear := ""
	return remotes.UpdateRemoteStateInput{ID: remoteID, Status: &status, LastError: &clear, LastSyncStartedAt: &t.startedAt, ObjectsTotal: &zero, ObjectsCompleted: &zero, BytesTotal: &zero, BytesCompleted: &zero, DownloadRateBps: &zero, UploadRateBps: &zero}
}

func (t *syncRunTracker) runningState(remoteID string) remotes.UpdateRemoteStateInput {
	totalObjects, completedObjects, totalBytes, completedBytes, downloadedBytes, uploadedBytes := t.snapshot()
	downloadRate := averageRateBps(downloadedBytes, t.startedAt)
	uploadRate := averageRateBps(uploadedBytes, t.startedAt)
	status := remotes.RemoteStatusSyncing
	clear := ""
	return remotes.UpdateRemoteStateInput{ID: remoteID, Status: &status, LastError: &clear, LastSyncStartedAt: &t.startedAt, ObjectsTotal: &totalObjects, ObjectsCompleted: &completedObjects, BytesTotal: &totalBytes, BytesCompleted: &completedBytes, DownloadRateBps: &downloadRate, UploadRateBps: &uploadRate}
}

func (t *syncRunTracker) errorState(remoteID string, message string) remotes.UpdateRemoteStateInput {
	state := t.runningState(remoteID)
	status := remotes.RemoteStatusError
	errorText := strings.TrimSpace(message)
	state.Status = &status
	state.LastError = &errorText
	return state
}

func (t *syncRunTracker) successState(remoteID string, cursor int64, syncedAt time.Time) remotes.UpdateRemoteStateInput {
	state := t.runningState(remoteID)
	status := remotes.RemoteStatusIdle
	clear := ""
	state.Status = &status
	state.Cursor = &cursor
	state.LastError = &clear
	state.LastSyncAt = &syncedAt
	return state
}

func (w *Worker) startRemoteProgressReporter(ctx context.Context, remoteID string, tracker *syncRunTracker) func() {
	reportCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-reportCtx.Done():
				return
			case <-ticker.C:
				if err := w.remotes.UpdateRemoteState(context.Background(), tracker.runningState(remoteID)); err != nil {
					w.logger.Debug("update remote progress", zap.String("remote_id", remoteID), zap.Error(err))
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func averageRateBps(bytes int64, startedAt time.Time) int64 {
	if bytes <= 0 {
		return 0
	}
	elapsed := time.Since(startedAt)
	if elapsed <= 0 {
		return 0
	}
	return int64(float64(bytes) / elapsed.Seconds())
}

type countingReadCloser struct {
	reader io.ReadCloser
	onRead func(int64)
}

func newCountingReadCloser(reader io.ReadCloser, onRead func(int64)) *countingReadCloser {
	return &countingReadCloser{reader: reader, onRead: onRead}
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.reader.Read(p)
	if n > 0 && c.onRead != nil {
		c.onRead(int64(n))
	}
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.reader.Close()
}

func estimateRequestBytes(req *http.Request) int64 {
	if req == nil || req.URL == nil {
		return 0
	}
	total := int64(len(req.Method) + 1 + len(req.URL.RequestURI()) + 1 + len(req.Proto) + 2)
	total += int64(len("Host") + len(req.Host) + 4)
	for key, values := range req.Header {
		for _, value := range values {
			total += int64(len(key) + len(value) + 4)
		}
	}
	if req.ContentLength > 0 {
		total += req.ContentLength
	}
	return total + 2
}
