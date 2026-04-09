package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/statedb"
)

var storageSyncMigrations = []statedb.Migration{
	{
		Name: "storage_sync_objects_v1",
		Statements: []string{
			`
			CREATE TABLE IF NOT EXISTS storage_sync_objects (
				bucket TEXT NOT NULL,
				key TEXT NOT NULL,
				status TEXT NOT NULL DEFAULT '',
				expected_checksum_sha256 TEXT NOT NULL DEFAULT '',
				last_error TEXT NOT NULL DEFAULT '',
				updated_at TEXT NOT NULL,
				PRIMARY KEY (bucket, key),
				FOREIGN KEY (bucket) REFERENCES storage_buckets(name) ON UPDATE CASCADE ON DELETE CASCADE
			)
			`,
			`CREATE INDEX IF NOT EXISTS storage_sync_objects_status_idx ON storage_sync_objects (status, updated_at, bucket, key)`,
		},
	},
	{
		Name: "storage_sync_objects_source_v1",
		Statements: []string{
			`ALTER TABLE storage_sync_objects ADD COLUMN source TEXT NOT NULL DEFAULT ''`,
			`ALTER TABLE storage_sync_objects ADD COLUMN baseline_node_id TEXT NOT NULL DEFAULT ''`,
		},
	},
	{
		Name: "storage_sync_events_v1",
		Statements: []string{
			`
			CREATE TABLE IF NOT EXISTS storage_sync_events (
				cursor INTEGER PRIMARY KEY AUTOINCREMENT,
				kind TEXT NOT NULL,
				bucket TEXT NOT NULL,
				key TEXT NOT NULL DEFAULT '',
				payload_json TEXT NOT NULL DEFAULT '',
				created_at TEXT NOT NULL
			)
			`,
		},
	},
	{
		Name: "storage_sync_state_v1",
		Statements: []string{
			`
			CREATE TABLE IF NOT EXISTS storage_sync_state (
				name TEXT PRIMARY KEY,
				value TEXT NOT NULL DEFAULT '',
				updated_at TEXT NOT NULL
			)
			`,
		},
	},
}

func (s *metadataStore) getSyncObjectStatus(bucket, key string) (SyncObjectStatus, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return SyncObjectStatus{}, err
	}
	defer func() {
		_ = db.Close()
	}()

	record := new(storageSyncObjectStatusRecord)
	err = db.NewSelect().Model(record).
		Where("bucket = ?", strings.TrimSpace(bucket)).
		Where("key = ?", strings.TrimSpace(key)).
		Limit(1).
		Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return SyncObjectStatus{}, os.ErrNotExist
	}
	if err != nil {
		return SyncObjectStatus{}, fmt.Errorf("read sync object status: %w", err)
	}
	return record.SyncObjectStatus()
}

func (s *metadataStore) upsertSyncObjectStatus(status SyncObjectStatus) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()

	if status.UpdatedAt.IsZero() {
		status.UpdatedAt = time.Now().UTC()
	}
	record := newStorageSyncObjectStatusRecord(status)
	_, err = db.NewInsert().Model(&record).
		On("CONFLICT (bucket, key) DO UPDATE").
		Set("status = EXCLUDED.status").
		Set("expected_checksum_sha256 = EXCLUDED.expected_checksum_sha256").
		Set("last_error = EXCLUDED.last_error").
		Set("source = EXCLUDED.source").
		Set("baseline_node_id = EXCLUDED.baseline_node_id").
		Set("updated_at = EXCLUDED.updated_at").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("upsert sync object status: %w", err)
	}
	return nil
}

func (s *metadataStore) deleteSyncObjectStatus(bucket, key string) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	if _, err := db.NewDelete().Model((*storageSyncObjectStatusRecord)(nil)).
		Where("bucket = ?", strings.TrimSpace(bucket)).
		Where("key = ?", strings.TrimSpace(key)).
		Exec(ctx); err != nil {
		return fmt.Errorf("delete sync object status: %w", err)
	}
	return nil
}

func (s *metadataStore) deleteSyncObjectStatusesBySource(source string) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	if _, err := db.NewDelete().Model((*storageSyncObjectStatusRecord)(nil)).Where("source = ?", strings.TrimSpace(source)).Exec(ctx); err != nil {
		return fmt.Errorf("delete sync object statuses by source: %w", err)
	}
	return nil
}

func (s *metadataStore) appendSyncEvent(event SyncEvent) (int64, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()
	if event.CreatedAt.IsZero() {
		event.CreatedAt = time.Now().UTC()
	}
	record, err := newStorageSyncEventRecord(event)
	if err != nil {
		return 0, err
	}
	if _, err := db.NewInsert().Model(&record).Exec(ctx); err != nil {
		return 0, fmt.Errorf("append sync event: %w", err)
	}
	value := struct {
		Cursor int64 `bun:"cursor"`
	}{}
	if err := db.NewSelect().Model((*storageSyncEventRecord)(nil)).ColumnExpr("COALESCE(MAX(cursor), 0) AS cursor").Scan(ctx, &value); err != nil {
		return 0, fmt.Errorf("inspect sync event cursor: %w", err)
	}
	return value.Cursor, nil
}

func (s *metadataStore) listSyncEvents(after int64, limit int) ([]SyncEvent, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()

	query := db.NewSelect().Model((*storageSyncEventRecord)(nil)).Where("cursor > ?", after).OrderExpr("cursor ASC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	records := make([]storageSyncEventRecord, 0)
	if err := query.Scan(ctx, &records); err != nil {
		return nil, fmt.Errorf("list sync events: %w", err)
	}

	events := make([]SyncEvent, 0, len(records))
	for _, record := range records {
		event, err := record.SyncEvent()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, nil
}

func (s *metadataStore) currentSyncCursor() (int64, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = db.Close()
	}()

	value := struct {
		Cursor int64 `bun:"cursor"`
	}{}
	if err := db.NewSelect().Model((*storageSyncEventRecord)(nil)).ColumnExpr("COALESCE(MAX(cursor), 0) AS cursor").Scan(ctx, &value); err != nil {
		return 0, fmt.Errorf("read current sync cursor: %w", err)
	}
	return value.Cursor, nil
}

func (s *metadataStore) getSyncState(name string) (string, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return "", err
	}
	defer func() {
		_ = db.Close()
	}()
	record := new(storageSyncStateRecord)
	err = db.NewSelect().Model(record).Where("name = ?", strings.TrimSpace(name)).Limit(1).Scan(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return "", os.ErrNotExist
	}
	if err != nil {
		return "", fmt.Errorf("read sync state: %w", err)
	}
	return record.Value, nil
}

func (s *metadataStore) upsertSyncState(name, value string) error {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() {
		_ = db.Close()
	}()
	record := storageSyncStateRecord{
		Name:      strings.TrimSpace(name),
		Value:     strings.TrimSpace(value),
		UpdatedAt: formatMetadataTime(time.Now().UTC()),
	}
	_, err = db.NewInsert().Model(&record).
		On("CONFLICT (name) DO UPDATE").
		Set("value = EXCLUDED.value").
		Set("updated_at = EXCLUDED.updated_at").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("upsert sync state: %w", err)
	}
	return nil
}

func parseSyncCursor(raw string) (int64, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse sync cursor: %w", err)
	}
	if value < 0 {
		return 0, fmt.Errorf("parse sync cursor: cursor must not be negative")
	}
	return value, nil
}

func (s *metadataStore) syncStatusCountsBySource(source string) (SyncStatusCounts, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return SyncStatusCounts{}, err
	}
	defer func() {
		_ = db.Close()
	}()
	rows := []struct {
		Status string `bun:"status"`
		Count  int    `bun:"count"`
	}{}
	query := db.NewSelect().TableExpr("storage_sync_objects").Column("status").ColumnExpr("COUNT(*) AS count")
	if strings.TrimSpace(source) != "" {
		query = query.Where("source = ?", strings.TrimSpace(source))
	}
	query = query.Group("status")
	if err := query.Scan(ctx, &rows); err != nil {
		return SyncStatusCounts{}, fmt.Errorf("count sync statuses: %w", err)
	}
	counts := SyncStatusCounts{}
	for _, row := range rows {
		switch NormalizeSyncStatus(strings.TrimSpace(row.Status)) {
		case SyncStatusPending:
			counts.Pending = row.Count
		case SyncStatusVerifying:
			counts.Verifying = row.Count
		case SyncStatusDownloading:
			counts.Downloading = row.Count
		case SyncStatusReady:
			counts.Ready = row.Count
		case SyncStatusError:
			counts.Error = row.Count
		case SyncStatusConflict:
			counts.Conflict = row.Count
		}
	}
	return counts, nil
}

func (s *metadataStore) syncStatusSummaryBySource(source string) (SyncStatusSummary, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return SyncStatusSummary{}, err
	}
	defer func() {
		_ = db.Close()
	}()
	summary := SyncStatusSummary{}
	baseline := struct {
		BaselineNodeID string `bun:"baseline_node_id"`
	}{}
	query := db.NewSelect().TableExpr("storage_sync_objects").Column("baseline_node_id")
	if strings.TrimSpace(source) != "" {
		query = query.Where("source = ?", strings.TrimSpace(source))
	}
	if err := query.Where("baseline_node_id <> ''").OrderExpr("updated_at DESC").Limit(1).Scan(ctx, &baseline); err == nil {
		summary.BaselineNodeID = baseline.BaselineNodeID
	} else if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return SyncStatusSummary{}, fmt.Errorf("read sync baseline summary: %w", err)
	}
	lastError := struct {
		LastError string `bun:"last_error"`
	}{}
	errorQuery := db.NewSelect().TableExpr("storage_sync_objects").Column("last_error")
	if strings.TrimSpace(source) != "" {
		errorQuery = errorQuery.Where("source = ?", strings.TrimSpace(source))
	}
	if scanErr := errorQuery.Where("last_error <> ''").OrderExpr("updated_at DESC").Limit(1).Scan(ctx, &lastError); scanErr == nil {
		summary.LastError = lastError.LastError
	} else if scanErr != nil && !errors.Is(scanErr, sql.ErrNoRows) {
		return SyncStatusSummary{}, fmt.Errorf("read sync error summary: %w", scanErr)
	}
	return summary, nil
}

func (s *metadataStore) listConflictItems(source string, limit int) ([]SyncConflictItem, error) {
	ctx := context.Background()
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = db.Close()
	}()
	records := make([]storageSyncObjectStatusRecord, 0)
	query := db.NewSelect().Model(&records).Where("status = ?", SyncStatusConflict).OrderExpr("updated_at DESC, bucket ASC, key ASC")
	if strings.TrimSpace(source) != "" {
		query = query.Where("source = ?", strings.TrimSpace(source))
	}
	if limit > 0 {
		query = query.Limit(limit)
	}
	if err := query.Scan(ctx); err != nil {
		return nil, fmt.Errorf("list conflict sync items: %w", err)
	}
	items := make([]SyncConflictItem, 0, len(records))
	for _, record := range records {
		status, err := record.SyncObjectStatus()
		if err != nil {
			return nil, err
		}
		items = append(items, SyncConflictItem{Bucket: status.Bucket, Key: status.Key, Source: status.Source, BaselineNodeID: status.BaselineNodeID, LastError: status.LastError, UpdatedAt: status.UpdatedAt})
	}
	return items, nil
}
