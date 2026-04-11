package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

func (s *Store) GetObjectSyncStatus(ctx context.Context, bucket, key string) (SyncObjectStatus, error) {
	if err := ctx.Err(); err != nil {
		return SyncObjectStatus{}, err
	}
	return s.metadata.getSyncObjectStatus(bucket, key)
}

func (s *Store) SetObjectSyncStatus(ctx context.Context, status SyncObjectStatus) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if status.UpdatedAt.IsZero() {
		status.UpdatedAt = time.Now().UTC()
	}
	return s.metadata.upsertSyncObjectStatus(status)
}

func (s *Store) DeleteObjectSyncStatus(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.metadata.deleteSyncObjectStatus(bucket, key)
}

func (s *Store) DeleteSyncStatusesBySource(ctx context.Context, source string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return s.metadata.deleteSyncObjectStatusesBySource(source)
}

func (s *Store) CurrentSyncCursor(ctx context.Context) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return s.metadata.currentSyncCursor()
}

func (s *Store) ListSyncEvents(ctx context.Context, after int64, limit int) ([]SyncEvent, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if after < 0 {
		return nil, fmt.Errorf("sync event cursor must not be negative")
	}
	return s.metadata.listSyncEvents(after, limit)
}

func (s *Store) objectUnavailableError(ctx context.Context, bucket, key string, notFound error) error {
	status, err := s.GetObjectSyncStatus(ctx, bucket, key)
	if err == nil && NormalizeSyncStatus(status.Status) != SyncStatusReady {
		return fmt.Errorf("%w: %s/%s", ErrObjectSyncing, bucket, key)
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return notFound
}

func (s *Store) recordBucketUpsertEvent(meta bucketMetadata) error {
	event := SyncEvent{
		Kind:       SyncEventBucketUpsert,
		Bucket:     meta.Name,
		BucketData: ptr(replicaBucketInputFromMetadata(meta)),
		CreatedAt:  time.Now().UTC(),
	}
	cursor, err := s.metadata.appendSyncEvent(event)
	if err != nil {
		return err
	}
	event.Cursor = cursor
	s.syncEvents.publish(event)
	return nil
}

func (s *Store) recordBucketDeleteEvent(name string) error {
	event := SyncEvent{
		Kind:      SyncEventBucketDelete,
		Bucket:    name,
		CreatedAt: time.Now().UTC(),
	}
	cursor, err := s.metadata.appendSyncEvent(event)
	if err != nil {
		return err
	}
	event.Cursor = cursor
	s.syncEvents.publish(event)
	return nil
}

func (s *Store) recordObjectUpsertEvent(meta objectMetadata) error {
	event := SyncEvent{
		Kind:       SyncEventObjectUpsert,
		Bucket:     meta.Bucket,
		Key:        meta.Key,
		ObjectData: ptr(replicaObjectMetadataFromObjectMeta(meta)),
		CreatedAt:  time.Now().UTC(),
	}
	cursor, err := s.metadata.appendSyncEvent(event)
	if err != nil {
		return err
	}
	event.Cursor = cursor
	s.syncEvents.publish(event)
	return nil
}

func (s *Store) recordObjectDeleteEvent(bucket, key string) error {
	event := SyncEvent{
		Kind:      SyncEventObjectDelete,
		Bucket:    bucket,
		Key:       key,
		CreatedAt: time.Now().UTC(),
	}
	cursor, err := s.metadata.appendSyncEvent(event)
	if err != nil {
		return err
	}
	event.Cursor = cursor
	s.syncEvents.publish(event)
	return nil
}

func (s *Store) recordDomainUpdateEvent(bindings []PublicDomainBinding) error {
	event := SyncEvent{
		Kind:       SyncEventDomainUpdate,
		DomainData: NormalizePublicDomainBindings(bindings),
		CreatedAt:  time.Now().UTC(),
	}
	cursor, err := s.metadata.appendSyncEvent(event)
	if err != nil {
		return err
	}
	event.Cursor = cursor
	s.syncEvents.publish(event)
	return nil
}

func ptr[T any](value T) *T {
	return &value
}

func (s *Store) MarkAllObjectsSyncStatus(ctx context.Context, status, source, baselineNodeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, bucket := range buckets {
		objects, err := s.ListObjects(ctx, bucket.Name, ListObjectsOptions{})
		if err != nil {
			return err
		}
		for _, object := range objects {
			if err := s.SetObjectSyncStatus(ctx, SyncObjectStatus{
				Bucket:                 object.Bucket,
				Key:                    object.Key,
				Status:                 status,
				ExpectedChecksumSHA256: object.ChecksumSHA256,
				Source:                 source,
				BaselineNodeID:         baselineNodeID,
				UpdatedAt:              now,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) SyncSettings(ctx context.Context) (SyncSettings, error) {
	if err := ctx.Err(); err != nil {
		return SyncSettings{}, err
	}
	raw, err := s.metadata.getSyncState(syncSettingsStateName)
	if errors.Is(err, os.ErrNotExist) {
		return SyncSettings{}, os.ErrNotExist
	}
	if err != nil {
		return SyncSettings{}, err
	}
	settings := SyncSettings{}
	if err := json.Unmarshal([]byte(raw), &settings); err != nil {
		return SyncSettings{}, fmt.Errorf("decode sync settings: %w", err)
	}
	return settings, nil
}

func (s *Store) SetSyncSettings(ctx context.Context, settings SyncSettings) (SyncSettings, error) {
	if err := ctx.Err(); err != nil {
		return SyncSettings{}, err
	}
	normalized := SyncSettings{
		Enabled:   settings.Enabled,
		UpdatedAt: time.Now().UTC(),
	}
	encoded, err := json.Marshal(normalized)
	if err != nil {
		return SyncSettings{}, fmt.Errorf("encode sync settings: %w", err)
	}
	if err := s.metadata.upsertSyncState(syncSettingsStateName, string(encoded)); err != nil {
		return SyncSettings{}, err
	}
	s.syncSettings.publish(normalized)
	return normalized, nil
}

func DefaultSyncSettings() SyncSettings {
	return SyncSettings{
		Enabled: false,
	}
}

func (s *Store) SyncStatusCounts(ctx context.Context, source string) (SyncStatusCounts, error) {
	if err := ctx.Err(); err != nil {
		return SyncStatusCounts{}, err
	}
	return s.metadata.syncStatusCountsBySource(source)
}

func (s *Store) SyncStatusSummary(ctx context.Context, source string) (SyncStatusSummary, error) {
	if err := ctx.Err(); err != nil {
		return SyncStatusSummary{}, err
	}
	return s.metadata.syncStatusSummaryBySource(source)
}

func (s *Store) ConflictItems(ctx context.Context, source string, limit int) ([]SyncConflictItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return s.metadata.listConflictItems(source, limit)
}
