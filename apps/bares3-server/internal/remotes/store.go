package remotes

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"bares3-server/internal/statedb"
	"github.com/uptrace/bun"
	"go.uber.org/zap"
)

const (
	idBytes = 16

	TokenStatusActive  = "active"
	TokenStatusRevoked = "revoked"

	BootstrapModeFull    = "full"
	BootstrapModeFromNow = "from_now"

	RemoteStatusPending = "pending"
	RemoteStatusSyncing = "syncing"
	RemoteStatusIdle    = "idle"
	RemoteStatusError   = "error"

	ConnectionStatusDisconnected = "disconnected"
	ConnectionStatusConnecting   = "connecting"
	ConnectionStatusConnected    = "connected"
)

var (
	ErrTokenNotFound        = errors.New("replication token not found")
	ErrTokenRevoked         = errors.New("replication token is revoked")
	ErrTokenActive          = errors.New("replication token is still active")
	ErrRemoteNotFound       = errors.New("replication remote not found")
	ErrInvalidBootstrapMode = errors.New("invalid replication bootstrap mode")

	storeMigrations = []statedb.Migration{
		{
			Name: "replication_access_tokens_v1",
			Statements: []string{`
				CREATE TABLE IF NOT EXISTS replication_access_tokens (
					id TEXT PRIMARY KEY,
					token TEXT NOT NULL UNIQUE,
					label TEXT NOT NULL DEFAULT '',
					created_by TEXT NOT NULL DEFAULT '',
					created_at TEXT NOT NULL,
					status TEXT NOT NULL,
					revoked_at TEXT
				)
			`, `CREATE INDEX IF NOT EXISTS replication_access_tokens_status_idx ON replication_access_tokens (status, created_at DESC)`},
		},
		{
			Name: "replication_remotes_v1",
			Statements: []string{`
				CREATE TABLE IF NOT EXISTS replication_remotes (
					id TEXT PRIMARY KEY,
					display_name TEXT NOT NULL,
					endpoint TEXT NOT NULL,
					token TEXT NOT NULL,
					bootstrap_mode TEXT NOT NULL,
					cursor TEXT NOT NULL DEFAULT '0',
					last_error TEXT NOT NULL DEFAULT '',
					last_sync_at TEXT,
					created_at TEXT NOT NULL,
					updated_at TEXT NOT NULL
				)
			`, `CREATE INDEX IF NOT EXISTS replication_remotes_updated_idx ON replication_remotes (updated_at DESC, id ASC)`},
		},
		{
			Name: "replication_remotes_runtime_v2",
			Statements: []string{
				`ALTER TABLE replication_remotes ADD COLUMN status TEXT NOT NULL DEFAULT 'pending'`,
				`ALTER TABLE replication_remotes ADD COLUMN last_sync_started_at TEXT`,
				`ALTER TABLE replication_remotes ADD COLUMN objects_total INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE replication_remotes ADD COLUMN objects_completed INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE replication_remotes ADD COLUMN bytes_total INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE replication_remotes ADD COLUMN bytes_completed INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE replication_remotes ADD COLUMN download_rate_bps INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE replication_remotes ADD COLUMN upload_rate_bps INTEGER NOT NULL DEFAULT 0`,
			},
		},
		{
			Name: "replication_remotes_connection_v3",
			Statements: []string{
				`ALTER TABLE replication_remotes ADD COLUMN follow_changes INTEGER NOT NULL DEFAULT 1`,
				`ALTER TABLE replication_remotes ADD COLUMN connection_status TEXT NOT NULL DEFAULT 'disconnected'`,
				`ALTER TABLE replication_remotes ADD COLUMN last_heartbeat_at TEXT`,
			},
		},
		{
			Name: "replication_remotes_peer_status_v4",
			Statements: []string{
				`ALTER TABLE replication_remotes ADD COLUMN peer_cursor INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE replication_remotes ADD COLUMN peer_used_bytes INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE replication_remotes ADD COLUMN peer_bucket_count INTEGER NOT NULL DEFAULT 0`,
				`ALTER TABLE replication_remotes ADD COLUMN peer_object_count INTEGER NOT NULL DEFAULT 0`,
			},
		},
	}
)

type Store struct {
	dataDir string
	logger  *zap.Logger
	now     func() time.Time
}

type AccessToken struct {
	ID        string     `json:"id"`
	Token     string     `json:"token"`
	Label     string     `json:"label,omitempty"`
	CreatedBy string     `json:"created_by,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	Status    string     `json:"status"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

type Remote struct {
	ID                string     `json:"id"`
	DisplayName       string     `json:"display_name"`
	Endpoint          string     `json:"endpoint"`
	Token             string     `json:"-"`
	FollowChanges     bool       `json:"follow_changes"`
	Status            string     `json:"status"`
	ConnectionStatus  string     `json:"connection_status"`
	BootstrapMode     string     `json:"bootstrap_mode"`
	Cursor            int64      `json:"cursor"`
	LastError         string     `json:"last_error,omitempty"`
	LastSyncStartedAt *time.Time `json:"last_sync_started_at,omitempty"`
	LastHeartbeatAt   *time.Time `json:"last_heartbeat_at,omitempty"`
	LastSyncAt        *time.Time `json:"last_sync_at,omitempty"`
	PeerCursor        int64      `json:"peer_cursor"`
	PeerUsedBytes     int64      `json:"peer_used_bytes"`
	PeerBucketCount   int64      `json:"peer_bucket_count"`
	PeerObjectCount   int64      `json:"peer_object_count"`
	ObjectsTotal      int64      `json:"objects_total"`
	ObjectsCompleted  int64      `json:"objects_completed"`
	BytesTotal        int64      `json:"bytes_total"`
	BytesCompleted    int64      `json:"bytes_completed"`
	DownloadRateBps   int64      `json:"download_rate_bps"`
	UploadRateBps     int64      `json:"upload_rate_bps"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

type CreateAccessTokenInput struct {
	Label     string
	CreatedBy string
}

type CreateRemoteInput struct {
	DisplayName   string
	Endpoint      string
	Token         string
	FollowChanges bool
	BootstrapMode string
	Cursor        int64
}

type UpdateRemoteStateInput struct {
	ID                string
	FollowChanges     *bool
	ConnectionStatus  *string
	Status            *string
	Cursor            *int64
	LastError         *string
	LastSyncStartedAt *time.Time
	LastHeartbeatAt   *time.Time
	LastSyncAt        *time.Time
	PeerCursor        *int64
	PeerUsedBytes     *int64
	PeerBucketCount   *int64
	PeerObjectCount   *int64
	ObjectsTotal      *int64
	ObjectsCompleted  *int64
	BytesTotal        *int64
	BytesCompleted    *int64
	DownloadRateBps   *int64
	UploadRateBps     *int64
}

type accessTokenRecord struct {
	bun.BaseModel `bun:"table:replication_access_tokens"`
	ID            string         `bun:"id,pk"`
	Token         string         `bun:"token"`
	Label         string         `bun:"label"`
	CreatedBy     string         `bun:"created_by"`
	CreatedAt     string         `bun:"created_at"`
	Status        string         `bun:"status"`
	RevokedAt     sql.NullString `bun:"revoked_at"`
}

type remoteRecord struct {
	bun.BaseModel     `bun:"table:replication_remotes"`
	ID                string         `bun:"id,pk"`
	DisplayName       string         `bun:"display_name"`
	Endpoint          string         `bun:"endpoint"`
	Token             string         `bun:"token"`
	FollowChanges     int64          `bun:"follow_changes"`
	Status            string         `bun:"status"`
	ConnectionStatus  string         `bun:"connection_status"`
	BootstrapMode     string         `bun:"bootstrap_mode"`
	Cursor            string         `bun:"cursor"`
	LastError         string         `bun:"last_error"`
	LastSyncStartedAt sql.NullString `bun:"last_sync_started_at"`
	LastHeartbeatAt   sql.NullString `bun:"last_heartbeat_at"`
	LastSyncAt        sql.NullString `bun:"last_sync_at"`
	PeerCursor        int64          `bun:"peer_cursor"`
	PeerUsedBytes     int64          `bun:"peer_used_bytes"`
	PeerBucketCount   int64          `bun:"peer_bucket_count"`
	PeerObjectCount   int64          `bun:"peer_object_count"`
	ObjectsTotal      int64          `bun:"objects_total"`
	ObjectsCompleted  int64          `bun:"objects_completed"`
	BytesTotal        int64          `bun:"bytes_total"`
	BytesCompleted    int64          `bun:"bytes_completed"`
	DownloadRateBps   int64          `bun:"download_rate_bps"`
	UploadRateBps     int64          `bun:"upload_rate_bps"`
	CreatedAt         string         `bun:"created_at"`
	UpdatedAt         string         `bun:"updated_at"`
}

func New(dataDir string, logger *zap.Logger) (*Store, error) {
	trimmed := strings.TrimSpace(dataDir)
	if trimmed == "" {
		return nil, fmt.Errorf("replication data dir is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	sqlDB, err := statedb.Open(trimmed)
	if err != nil {
		return nil, err
	}
	bunDB := statedb.Wrap(sqlDB)
	if err := statedb.EnsureMigrations(sqlDB, storeMigrations); err != nil {
		_ = bunDB.Close()
		return nil, fmt.Errorf("initialize replication schema: %w", err)
	}
	if err := bunDB.Close(); err != nil {
		return nil, fmt.Errorf("close replication bootstrap db: %w", err)
	}
	return &Store{dataDir: trimmed, logger: logger, now: time.Now}, nil
}

func (s *Store) Close() error {
	if s == nil {
		return nil
	}
	return nil
}

func (s *Store) Check(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	if _, err := db.NewRaw("SELECT 1").Exec(ctx); err != nil {
		return fmt.Errorf("check replication db: %w", err)
	}
	return nil
}

func (s *Store) CreateAccessToken(ctx context.Context, input CreateAccessTokenInput) (AccessToken, error) {
	if err := ctx.Err(); err != nil {
		return AccessToken{}, err
	}
	db, err := s.openDB()
	if err != nil {
		return AccessToken{}, err
	}
	defer func() { _ = db.Close() }()
	for range 16 {
		id, err := randomToken(idBytes)
		if err != nil {
			return AccessToken{}, err
		}
		token, err := randomToken(24)
		if err != nil {
			return AccessToken{}, err
		}
		now := s.now().UTC()
		item := AccessToken{ID: id, Token: token, Label: strings.TrimSpace(input.Label), CreatedBy: strings.TrimSpace(input.CreatedBy), CreatedAt: now, Status: TokenStatusActive}
		if _, err := db.NewInsert().Model(newAccessTokenRecord(item)).Exec(ctx); err == nil {
			return item, nil
		} else if !isUniqueConstraint(err) {
			return AccessToken{}, fmt.Errorf("insert replication access token: %w", err)
		}
	}
	return AccessToken{}, fmt.Errorf("generate replication access token: exhausted retries")
}

func (s *Store) ListAccessTokens(ctx context.Context) ([]AccessToken, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	records := make([]accessTokenRecord, 0)
	if err := db.NewSelect().Model(&records).OrderExpr("created_at DESC, id ASC").Scan(ctx); err != nil {
		return nil, fmt.Errorf("list replication access tokens: %w", err)
	}
	items := make([]AccessToken, 0, len(records))
	for _, record := range records {
		item, err := record.AccessToken()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Store) RevokeAccessToken(ctx context.Context, id string) (AccessToken, error) {
	if err := ctx.Err(); err != nil {
		return AccessToken{}, err
	}
	db, err := s.openDB()
	if err != nil {
		return AccessToken{}, err
	}
	defer func() { _ = db.Close() }()
	record, err := s.readAccessTokenByID(ctx, db, id)
	if err != nil {
		return AccessToken{}, err
	}
	now := formatTime(s.now().UTC())
	if _, err := db.NewUpdate().Model((*accessTokenRecord)(nil)).Set("status = ?", TokenStatusRevoked).Set("revoked_at = ?", now).Where("id = ?", record.ID).Exec(ctx); err != nil {
		return AccessToken{}, fmt.Errorf("revoke replication access token: %w", err)
	}
	updated, err := s.readAccessTokenByID(ctx, db, id)
	if err != nil {
		return AccessToken{}, err
	}
	return updated.AccessToken()
}

func (s *Store) DeleteAccessToken(ctx context.Context, id string) (AccessToken, error) {
	if err := ctx.Err(); err != nil {
		return AccessToken{}, err
	}
	db, err := s.openDB()
	if err != nil {
		return AccessToken{}, err
	}
	defer func() { _ = db.Close() }()
	record, err := s.readAccessTokenByID(ctx, db, id)
	if err != nil {
		return AccessToken{}, err
	}
	if record.Status != TokenStatusRevoked {
		return AccessToken{}, ErrTokenActive
	}
	item, err := record.AccessToken()
	if err != nil {
		return AccessToken{}, err
	}
	result, err := db.NewDelete().Model((*accessTokenRecord)(nil)).Where("id = ?", record.ID).Exec(ctx)
	if err != nil {
		return AccessToken{}, fmt.Errorf("delete replication access token: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return AccessToken{}, fmt.Errorf("inspect deleted replication access token: %w", err)
	}
	if affected == 0 {
		return AccessToken{}, ErrTokenNotFound
	}
	return item, nil
}

func (s *Store) AccessTokenByValue(ctx context.Context, token string) (AccessToken, error) {
	if err := ctx.Err(); err != nil {
		return AccessToken{}, err
	}
	db, err := s.openDB()
	if err != nil {
		return AccessToken{}, err
	}
	defer func() { _ = db.Close() }()
	record, err := s.readAccessTokenByValue(ctx, db, token)
	if err != nil {
		return AccessToken{}, err
	}
	if record.Status != TokenStatusActive {
		return AccessToken{}, ErrTokenRevoked
	}
	return record.AccessToken()
}

func (s *Store) CreateRemote(ctx context.Context, input CreateRemoteInput) (Remote, error) {
	if err := ctx.Err(); err != nil {
		return Remote{}, err
	}
	endpoint := strings.TrimRight(strings.TrimSpace(input.Endpoint), "/")
	token := strings.TrimSpace(input.Token)
	if endpoint == "" || token == "" {
		return Remote{}, fmt.Errorf("replication remote endpoint and token are required")
	}
	bootstrapMode := NormalizeBootstrapMode(input.BootstrapMode)
	if bootstrapMode == "" {
		return Remote{}, ErrInvalidBootstrapMode
	}
	cursor := input.Cursor
	if cursor < 0 {
		return Remote{}, fmt.Errorf("replication remote cursor must not be negative")
	}
	displayName := strings.TrimSpace(input.DisplayName)
	if displayName == "" {
		displayName = endpoint
	}
	db, err := s.openDB()
	if err != nil {
		return Remote{}, err
	}
	defer func() { _ = db.Close() }()
	for range 16 {
		id, err := randomToken(idBytes)
		if err != nil {
			return Remote{}, err
		}
		now := s.now().UTC()
		remote := Remote{ID: id, DisplayName: displayName, Endpoint: endpoint, Token: token, FollowChanges: input.FollowChanges, Status: RemoteStatusPending, ConnectionStatus: ConnectionStatusDisconnected, BootstrapMode: bootstrapMode, Cursor: cursor, CreatedAt: now, UpdatedAt: now}
		if _, err := db.NewInsert().Model(newRemoteRecord(remote)).Exec(ctx); err == nil {
			return remote, nil
		} else if !isUniqueConstraint(err) {
			return Remote{}, fmt.Errorf("insert replication remote: %w", err)
		}
	}
	return Remote{}, fmt.Errorf("generate replication remote id: exhausted retries")
}

func (s *Store) ListRemotes(ctx context.Context) ([]Remote, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	db, err := s.openDB()
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	records := make([]remoteRecord, 0)
	if err := db.NewSelect().Model(&records).OrderExpr("updated_at DESC, id ASC").Scan(ctx); err != nil {
		return nil, fmt.Errorf("list replication remotes: %w", err)
	}
	items := make([]Remote, 0, len(records))
	for _, record := range records {
		item, err := record.Remote()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func (s *Store) GetRemote(ctx context.Context, id string) (Remote, error) {
	if err := ctx.Err(); err != nil {
		return Remote{}, err
	}
	db, err := s.openDB()
	if err != nil {
		return Remote{}, err
	}
	defer func() { _ = db.Close() }()
	record, err := s.readRemoteByID(ctx, db, id)
	if err != nil {
		return Remote{}, err
	}
	return record.Remote()
}

func (s *Store) DeleteRemote(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	result, err := db.NewDelete().Model((*remoteRecord)(nil)).Where("id = ?", strings.TrimSpace(id)).Exec(ctx)
	if err != nil {
		return fmt.Errorf("delete replication remote: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect deleted replication remote: %w", err)
	}
	if affected == 0 {
		return ErrRemoteNotFound
	}
	return nil
}

func (s *Store) UpdateRemoteState(ctx context.Context, input UpdateRemoteStateInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if input.Cursor != nil && *input.Cursor < 0 {
		return fmt.Errorf("replication remote cursor must not be negative")
	}
	for _, value := range []*int64{input.PeerCursor, input.PeerUsedBytes, input.PeerBucketCount, input.PeerObjectCount, input.ObjectsTotal, input.ObjectsCompleted, input.BytesTotal, input.BytesCompleted, input.DownloadRateBps, input.UploadRateBps} {
		if value != nil && *value < 0 {
			return fmt.Errorf("replication remote metrics must not be negative")
		}
	}
	db, err := s.openDB()
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	query := db.NewUpdate().Model((*remoteRecord)(nil)).Set("updated_at = ?", formatTime(s.now().UTC())).Where("id = ?", strings.TrimSpace(input.ID))
	if input.FollowChanges != nil {
		query = query.Set("follow_changes = ?", boolToInt(*input.FollowChanges))
	}
	if input.ConnectionStatus != nil {
		query = query.Set("connection_status = ?", NormalizeConnectionStatus(*input.ConnectionStatus))
	}
	if input.Status != nil {
		query = query.Set("status = ?", NormalizeRemoteStatus(*input.Status))
	}
	if input.Cursor != nil {
		query = query.Set("cursor = ?", strconv.FormatInt(*input.Cursor, 10))
	}
	if input.LastError != nil {
		query = query.Set("last_error = ?", strings.TrimSpace(*input.LastError))
	}
	if input.LastSyncStartedAt != nil {
		query = query.Set("last_sync_started_at = ?", formatTime(input.LastSyncStartedAt.UTC()))
	}
	if input.LastHeartbeatAt != nil {
		query = query.Set("last_heartbeat_at = ?", formatTime(input.LastHeartbeatAt.UTC()))
	}
	if input.LastSyncAt != nil {
		query = query.Set("last_sync_at = ?", formatTime(input.LastSyncAt.UTC()))
	}
	if input.PeerCursor != nil {
		query = query.Set("peer_cursor = ?", *input.PeerCursor)
	}
	if input.PeerUsedBytes != nil {
		query = query.Set("peer_used_bytes = ?", *input.PeerUsedBytes)
	}
	if input.PeerBucketCount != nil {
		query = query.Set("peer_bucket_count = ?", *input.PeerBucketCount)
	}
	if input.PeerObjectCount != nil {
		query = query.Set("peer_object_count = ?", *input.PeerObjectCount)
	}
	if input.ObjectsTotal != nil {
		query = query.Set("objects_total = ?", *input.ObjectsTotal)
	}
	if input.ObjectsCompleted != nil {
		query = query.Set("objects_completed = ?", *input.ObjectsCompleted)
	}
	if input.BytesTotal != nil {
		query = query.Set("bytes_total = ?", *input.BytesTotal)
	}
	if input.BytesCompleted != nil {
		query = query.Set("bytes_completed = ?", *input.BytesCompleted)
	}
	if input.DownloadRateBps != nil {
		query = query.Set("download_rate_bps = ?", *input.DownloadRateBps)
	}
	if input.UploadRateBps != nil {
		query = query.Set("upload_rate_bps = ?", *input.UploadRateBps)
	}
	result, err := query.Exec(ctx)
	if err != nil {
		return fmt.Errorf("update replication remote state: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect updated replication remote state: %w", err)
	}
	if affected == 0 {
		return ErrRemoteNotFound
	}
	return nil
}

func NormalizeBootstrapMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", BootstrapModeFull:
		return BootstrapModeFull
	case BootstrapModeFromNow:
		return BootstrapModeFromNow
	default:
		return ""
	}
}

func NormalizeRemoteStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", RemoteStatusPending:
		return RemoteStatusPending
	case RemoteStatusSyncing:
		return RemoteStatusSyncing
	case RemoteStatusIdle:
		return RemoteStatusIdle
	case RemoteStatusError:
		return RemoteStatusError
	default:
		return RemoteStatusPending
	}
}

func NormalizeConnectionStatus(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case ConnectionStatusConnecting:
		return ConnectionStatusConnecting
	case ConnectionStatusConnected:
		return ConnectionStatusConnected
	case "", ConnectionStatusDisconnected:
		return ConnectionStatusDisconnected
	default:
		return ConnectionStatusDisconnected
	}
}

func (s *Store) openDB() (*bun.DB, error) {
	if s == nil || strings.TrimSpace(s.dataDir) == "" {
		return nil, fmt.Errorf("replication store is closed")
	}
	sqlDB, err := statedb.Open(s.dataDir)
	if err != nil {
		return nil, err
	}
	return statedb.Wrap(sqlDB), nil
}

func (s *Store) readAccessTokenByID(ctx context.Context, db *bun.DB, id string) (accessTokenRecord, error) {
	record := accessTokenRecord{}
	if err := db.NewSelect().Model(&record).Where("id = ?", strings.TrimSpace(id)).Limit(1).Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return accessTokenRecord{}, ErrTokenNotFound
	} else if err != nil {
		return accessTokenRecord{}, fmt.Errorf("read replication access token: %w", err)
	}
	return record, nil
}

func (s *Store) readAccessTokenByValue(ctx context.Context, db *bun.DB, token string) (accessTokenRecord, error) {
	record := accessTokenRecord{}
	if err := db.NewSelect().Model(&record).Where("token = ?", strings.TrimSpace(token)).Limit(1).Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return accessTokenRecord{}, ErrTokenNotFound
	} else if err != nil {
		return accessTokenRecord{}, fmt.Errorf("read replication access token by value: %w", err)
	}
	return record, nil
}

func (s *Store) readRemoteByID(ctx context.Context, db *bun.DB, id string) (remoteRecord, error) {
	record := remoteRecord{}
	if err := db.NewSelect().Model(&record).Where("id = ?", strings.TrimSpace(id)).Limit(1).Scan(ctx); errors.Is(err, sql.ErrNoRows) {
		return remoteRecord{}, ErrRemoteNotFound
	} else if err != nil {
		return remoteRecord{}, fmt.Errorf("read replication remote: %w", err)
	}
	return record, nil
}

func newAccessTokenRecord(item AccessToken) *accessTokenRecord {
	record := &accessTokenRecord{ID: item.ID, Token: item.Token, Label: item.Label, CreatedBy: item.CreatedBy, CreatedAt: formatTime(item.CreatedAt), Status: item.Status}
	if item.RevokedAt != nil {
		record.RevokedAt = sql.NullString{String: formatTime(*item.RevokedAt), Valid: true}
	}
	return record
}

func (r accessTokenRecord) AccessToken() (AccessToken, error) {
	createdAt, err := parseTime(r.CreatedAt)
	if err != nil {
		return AccessToken{}, err
	}
	var revokedAt *time.Time
	if r.RevokedAt.Valid {
		parsed, err := parseTime(r.RevokedAt.String)
		if err != nil {
			return AccessToken{}, err
		}
		revokedAt = &parsed
	}
	return AccessToken{ID: r.ID, Token: r.Token, Label: r.Label, CreatedBy: r.CreatedBy, CreatedAt: createdAt, Status: r.Status, RevokedAt: revokedAt}, nil
}

func newRemoteRecord(item Remote) *remoteRecord {
	record := &remoteRecord{ID: item.ID, DisplayName: item.DisplayName, Endpoint: item.Endpoint, Token: item.Token, FollowChanges: boolToInt(item.FollowChanges), Status: NormalizeRemoteStatus(item.Status), ConnectionStatus: NormalizeConnectionStatus(item.ConnectionStatus), BootstrapMode: item.BootstrapMode, Cursor: strconv.FormatInt(item.Cursor, 10), LastError: item.LastError, PeerCursor: item.PeerCursor, PeerUsedBytes: item.PeerUsedBytes, PeerBucketCount: item.PeerBucketCount, PeerObjectCount: item.PeerObjectCount, ObjectsTotal: item.ObjectsTotal, ObjectsCompleted: item.ObjectsCompleted, BytesTotal: item.BytesTotal, BytesCompleted: item.BytesCompleted, DownloadRateBps: item.DownloadRateBps, UploadRateBps: item.UploadRateBps, CreatedAt: formatTime(item.CreatedAt), UpdatedAt: formatTime(item.UpdatedAt)}
	if item.LastSyncStartedAt != nil {
		record.LastSyncStartedAt = sql.NullString{String: formatTime(*item.LastSyncStartedAt), Valid: true}
	}
	if item.LastHeartbeatAt != nil {
		record.LastHeartbeatAt = sql.NullString{String: formatTime(*item.LastHeartbeatAt), Valid: true}
	}
	if item.LastSyncAt != nil {
		record.LastSyncAt = sql.NullString{String: formatTime(*item.LastSyncAt), Valid: true}
	}
	return record
}

func (r remoteRecord) Remote() (Remote, error) {
	createdAt, err := parseTime(r.CreatedAt)
	if err != nil {
		return Remote{}, err
	}
	updatedAt, err := parseTime(r.UpdatedAt)
	if err != nil {
		return Remote{}, err
	}
	cursor, err := strconv.ParseInt(strings.TrimSpace(r.Cursor), 10, 64)
	if err != nil {
		return Remote{}, fmt.Errorf("parse replication remote cursor: %w", err)
	}
	var lastSyncStartedAt *time.Time
	if r.LastSyncStartedAt.Valid {
		parsed, err := parseTime(r.LastSyncStartedAt.String)
		if err != nil {
			return Remote{}, err
		}
		lastSyncStartedAt = &parsed
	}
	var lastHeartbeatAt *time.Time
	if r.LastHeartbeatAt.Valid {
		parsed, err := parseTime(r.LastHeartbeatAt.String)
		if err != nil {
			return Remote{}, err
		}
		lastHeartbeatAt = &parsed
	}
	var lastSyncAt *time.Time
	if r.LastSyncAt.Valid {
		parsed, err := parseTime(r.LastSyncAt.String)
		if err != nil {
			return Remote{}, err
		}
		lastSyncAt = &parsed
	}
	return Remote{ID: r.ID, DisplayName: r.DisplayName, Endpoint: r.Endpoint, Token: r.Token, FollowChanges: intToBool(r.FollowChanges), Status: NormalizeRemoteStatus(r.Status), ConnectionStatus: NormalizeConnectionStatus(r.ConnectionStatus), BootstrapMode: NormalizeBootstrapMode(r.BootstrapMode), Cursor: cursor, LastError: r.LastError, LastSyncStartedAt: lastSyncStartedAt, LastHeartbeatAt: lastHeartbeatAt, LastSyncAt: lastSyncAt, PeerCursor: r.PeerCursor, PeerUsedBytes: r.PeerUsedBytes, PeerBucketCount: r.PeerBucketCount, PeerObjectCount: r.PeerObjectCount, ObjectsTotal: r.ObjectsTotal, ObjectsCompleted: r.ObjectsCompleted, BytesTotal: r.BytesTotal, BytesCompleted: r.BytesCompleted, DownloadRateBps: r.DownloadRateBps, UploadRateBps: r.UploadRateBps, CreatedAt: createdAt, UpdatedAt: updatedAt}, nil
}

func boolToInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func intToBool(value int64) bool {
	return value != 0
}

func randomToken(size int) (string, error) {
	buffer := make([]byte, size)
	if _, err := rand.Read(buffer); err != nil {
		return "", fmt.Errorf("generate replication token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buffer), nil
}

func formatTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse replication time: %w", err)
	}
	return parsed.UTC(), nil
}

func isUniqueConstraint(err error) bool {
	message := strings.ToLower(strings.TrimSpace(err.Error()))
	return strings.Contains(message, "unique") || strings.Contains(message, "constraint failed")
}
