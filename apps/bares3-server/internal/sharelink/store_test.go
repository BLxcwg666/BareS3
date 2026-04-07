package sharelink

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestCreateListActiveCountAndRevoke(t *testing.T) {
	t.Parallel()

	store, now := newTestStore(t)

	created, err := store.Create(context.Background(), CreateInput{
		Bucket:    "gallery",
		Key:       "notes/readme.txt",
		Expires:   2 * time.Hour,
		Filename:  "readme.txt",
		Size:      42,
		CreatedBy: "admin",
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if created.ID == "" {
		t.Fatalf("expected share link id")
	}

	links, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(links) != 1 {
		t.Fatalf("expected 1 link, got %d", len(links))
	}
	if links[0].Status(*now) != "active" {
		t.Fatalf("expected active status, got %s", links[0].Status(*now))
	}

	count, err := store.ActiveCount(context.Background())
	if err != nil {
		t.Fatalf("ActiveCount failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected active count 1, got %d", count)
	}

	revoked, err := store.Revoke(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Revoke failed: %v", err)
	}
	if revoked.RevokedAt == nil {
		t.Fatalf("expected revoked timestamp")
	}

	count, err = store.ActiveCount(context.Background())
	if err != nil {
		t.Fatalf("ActiveCount after revoke failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected active count 0 after revoke, got %d", count)
	}

	if _, err := store.GetActive(context.Background(), created.ID); err == nil {
		t.Fatalf("expected GetActive to fail for revoked link")
	}

	removed, err := store.Remove(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Remove failed: %v", err)
	}
	if removed.ID != created.ID {
		t.Fatalf("unexpected removed link id: %s", removed.ID)
	}

	links, err = store.List(context.Background())
	if err != nil {
		t.Fatalf("List after remove failed: %v", err)
	}
	if len(links) != 0 {
		t.Fatalf("expected 0 links after remove, got %d", len(links))
	}
}

func TestGetActiveRejectsExpiredLinks(t *testing.T) {
	t.Parallel()

	store, now := newTestStore(t)
	created, err := store.Create(context.Background(), CreateInput{
		Bucket:  "gallery",
		Key:     "notes/expired.txt",
		Expires: time.Hour,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	*now = now.Add(2 * time.Hour)
	if _, err := store.GetActive(context.Background(), created.ID); err == nil {
		t.Fatalf("expected expired link error")
	}

	count, err := store.ActiveCount(context.Background())
	if err != nil {
		t.Fatalf("ActiveCount failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected active count 0 for expired link, got %d", count)
	}

	removed, err := store.Remove(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Remove expired link failed: %v", err)
	}
	if removed.ID != created.ID {
		t.Fatalf("unexpected removed expired link id: %s", removed.ID)
	}
}

func TestRemoveRejectsActiveLinks(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t)
	created, err := store.Create(context.Background(), CreateInput{
		Bucket:  "gallery",
		Key:     "notes/active.txt",
		Expires: time.Hour,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	if _, err := store.Remove(context.Background(), created.ID); err == nil {
		t.Fatalf("expected remove to fail for active link")
	}
}

func newTestStore(t *testing.T) (*Store, *time.Time) {
	t.Helper()

	root := t.TempDir()
	store, err := New(filepath.Join(root, "data"), zap.NewNop())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	now := time.Date(2026, time.April, 8, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	return store, &now
}
