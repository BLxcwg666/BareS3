package storage

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/uptrace/bun"
)

type storageSyncObjectStatusRecord struct {
	bun.BaseModel `bun:"table:storage_sync_objects"`

	Bucket                 string `bun:"bucket,pk"`
	Key                    string `bun:"key,pk"`
	Status                 string `bun:"status"`
	ExpectedChecksumSHA256 string `bun:"expected_checksum_sha256"`
	LastError              string `bun:"last_error"`
	Source                 string `bun:"source"`
	BaselineNodeID         string `bun:"baseline_node_id"`
	UpdatedAt              string `bun:"updated_at"`
}

type storageSyncEventRecord struct {
	bun.BaseModel `bun:"table:storage_sync_events"`

	Cursor      int64  `bun:"cursor,pk,autoincrement"`
	Kind        string `bun:"kind"`
	Bucket      string `bun:"bucket"`
	Key         string `bun:"key"`
	PayloadJSON string `bun:"payload_json"`
	CreatedAt   string `bun:"created_at"`
}

type storageSyncStateRecord struct {
	bun.BaseModel `bun:"table:storage_sync_state"`

	Name      string `bun:"name,pk"`
	Value     string `bun:"value"`
	UpdatedAt string `bun:"updated_at"`
}

func newStorageSyncObjectStatusRecord(status SyncObjectStatus) storageSyncObjectStatusRecord {
	return storageSyncObjectStatusRecord{
		Bucket:                 strings.TrimSpace(status.Bucket),
		Key:                    strings.TrimSpace(status.Key),
		Status:                 NormalizeSyncStatus(strings.TrimSpace(status.Status)),
		ExpectedChecksumSHA256: strings.TrimSpace(status.ExpectedChecksumSHA256),
		LastError:              strings.TrimSpace(status.LastError),
		Source:                 strings.TrimSpace(status.Source),
		BaselineNodeID:         strings.TrimSpace(status.BaselineNodeID),
		UpdatedAt:              formatMetadataTime(status.UpdatedAt),
	}
}

func (r storageSyncObjectStatusRecord) SyncObjectStatus() (SyncObjectStatus, error) {
	updatedAt, err := parseMetadataTime(r.UpdatedAt)
	if err != nil {
		return SyncObjectStatus{}, err
	}
	return SyncObjectStatus{
		Bucket:                 strings.TrimSpace(r.Bucket),
		Key:                    strings.TrimSpace(r.Key),
		Status:                 NormalizeSyncStatus(strings.TrimSpace(r.Status)),
		ExpectedChecksumSHA256: strings.TrimSpace(r.ExpectedChecksumSHA256),
		LastError:              strings.TrimSpace(r.LastError),
		Source:                 strings.TrimSpace(r.Source),
		BaselineNodeID:         strings.TrimSpace(r.BaselineNodeID),
		UpdatedAt:              updatedAt,
	}, nil
}

func newStorageSyncEventRecord(event SyncEvent) (storageSyncEventRecord, error) {
	payload := ""
	switch event.Kind {
	case SyncEventBucketUpsert:
		if event.BucketData == nil {
			return storageSyncEventRecord{}, fmt.Errorf("sync event bucket payload is required")
		}
		encoded, err := jsonMarshal(event.BucketData)
		if err != nil {
			return storageSyncEventRecord{}, fmt.Errorf("encode bucket sync event payload: %w", err)
		}
		payload = encoded
	case SyncEventObjectUpsert:
		if event.ObjectData == nil {
			return storageSyncEventRecord{}, fmt.Errorf("sync event object payload is required")
		}
		encoded, err := jsonMarshal(event.ObjectData)
		if err != nil {
			return storageSyncEventRecord{}, fmt.Errorf("encode object sync event payload: %w", err)
		}
		payload = encoded
	case SyncEventDomainUpdate:
		encoded, err := jsonMarshal(NormalizePublicDomainBindings(event.DomainData))
		if err != nil {
			return storageSyncEventRecord{}, fmt.Errorf("encode domain sync event payload: %w", err)
		}
		payload = encoded
	case SyncEventBucketDelete, SyncEventObjectDelete:
	default:
		return storageSyncEventRecord{}, fmt.Errorf("unknown sync event kind %q", event.Kind)
	}
	return storageSyncEventRecord{
		Kind:        event.Kind,
		Bucket:      strings.TrimSpace(event.Bucket),
		Key:         strings.TrimSpace(event.Key),
		PayloadJSON: payload,
		CreatedAt:   formatMetadataTime(event.CreatedAt),
	}, nil
}

func (r storageSyncEventRecord) SyncEvent() (SyncEvent, error) {
	createdAt, err := parseMetadataTime(r.CreatedAt)
	if err != nil {
		return SyncEvent{}, err
	}
	event := SyncEvent{
		Cursor:    r.Cursor,
		Kind:      strings.TrimSpace(r.Kind),
		Bucket:    strings.TrimSpace(r.Bucket),
		Key:       strings.TrimSpace(r.Key),
		CreatedAt: createdAt,
	}
	switch event.Kind {
	case SyncEventBucketUpsert:
		payload := ReplicaBucketInput{}
		if err := decodeJSONField(r.PayloadJSON, &payload, ReplicaBucketInput{}); err != nil {
			return SyncEvent{}, fmt.Errorf("decode bucket sync event payload: %w", err)
		}
		event.BucketData = &payload
	case SyncEventObjectUpsert:
		payload := ReplicaObjectMetadata{}
		if err := decodeJSONField(r.PayloadJSON, &payload, ReplicaObjectMetadata{}); err != nil {
			return SyncEvent{}, fmt.Errorf("decode object sync event payload: %w", err)
		}
		event.ObjectData = &payload
	case SyncEventDomainUpdate:
		payload := []PublicDomainBinding{}
		if err := decodeJSONField(r.PayloadJSON, &payload, []PublicDomainBinding{}); err != nil {
			return SyncEvent{}, fmt.Errorf("decode domain sync event payload: %w", err)
		}
		event.DomainData = NormalizePublicDomainBindings(payload)
	case SyncEventBucketDelete, SyncEventObjectDelete:
	default:
		return SyncEvent{}, fmt.Errorf("unknown sync event kind %q", event.Kind)
	}
	return event, nil
}

func jsonMarshal(value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}
