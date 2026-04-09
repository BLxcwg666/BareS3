package sharelink

import (
	"context"
	"encoding/json"
	"os"
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

func TestReassignObjectUpdatesExistingLinks(t *testing.T) {
	t.Parallel()

	store, now := newTestStore(t)
	created, err := store.Create(context.Background(), CreateInput{
		Bucket:    "gallery",
		Key:       "notes/readme.txt",
		Filename:  "readme.txt",
		Expires:   time.Hour,
		CreatedBy: "admin",
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	updated, err := store.ReassignObject(context.Background(), "gallery", "notes/readme.txt", "archive", "moved/guide.txt")
	if err != nil {
		t.Fatalf("ReassignObject failed: %v", err)
	}
	if updated != 1 {
		t.Fatalf("expected 1 reassigned link, got %d", updated)
	}

	link, err := store.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if link.Bucket != "archive" || link.Key != "moved/guide.txt" || link.Filename != "guide.txt" {
		t.Fatalf("unexpected reassigned link: %+v", link)
	}
	if link.Status(*now) != "active" {
		t.Fatalf("expected link to stay active, got %s", link.Status(*now))
	}
}

func TestReassignBucketUpdatesExistingLinks(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t)
	created, err := store.Create(context.Background(), CreateInput{
		Bucket:  "gallery",
		Key:     "notes/readme.txt",
		Expires: time.Hour,
	})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	updated, err := store.ReassignBucket(context.Background(), "gallery", "archive")
	if err != nil {
		t.Fatalf("ReassignBucket failed: %v", err)
	}
	if updated != 1 {
		t.Fatalf("expected 1 reassigned link, got %d", updated)
	}

	link, err := store.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if link.Bucket != "archive" || link.Key != "notes/readme.txt" {
		t.Fatalf("unexpected reassigned bucket link: %+v", link)
	}
}

func TestReassignPrefixUpdatesMatchingLinksOnly(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t)
	first, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "folder/a.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create first failed: %v", err)
	}
	second, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "folder/deep/b.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create second failed: %v", err)
	}
	third, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "other/c.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create third failed: %v", err)
	}

	updated, err := store.ReassignPrefix(context.Background(), "gallery", "folder/", "archive", "imports/folder/")
	if err != nil {
		t.Fatalf("ReassignPrefix failed: %v", err)
	}
	if updated != 2 {
		t.Fatalf("expected 2 reassigned links, got %d", updated)
	}

	firstLink, err := store.Get(context.Background(), first.ID)
	if err != nil {
		t.Fatalf("Get first failed: %v", err)
	}
	if firstLink.Key != "imports/folder/a.txt" || firstLink.Bucket != "archive" {
		t.Fatalf("unexpected first reassigned link: %+v", firstLink)
	}

	secondLink, err := store.Get(context.Background(), second.ID)
	if err != nil {
		t.Fatalf("Get second failed: %v", err)
	}
	if secondLink.Key != "imports/folder/deep/b.txt" || secondLink.Bucket != "archive" {
		t.Fatalf("unexpected second reassigned link: %+v", secondLink)
	}

	thirdLink, err := store.Get(context.Background(), third.ID)
	if err != nil {
		t.Fatalf("Get third failed: %v", err)
	}
	if thirdLink.Key != "other/c.txt" || thirdLink.Bucket != "gallery" {
		t.Fatalf("expected unrelated link unchanged, got %+v", thirdLink)
	}
}

func TestRemoveByObjectDeletesAllMatchingLinks(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t)
	first, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "notes/readme.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create first failed: %v", err)
	}
	second, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "notes/readme.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create second failed: %v", err)
	}
	_, err = store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "notes/other.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create third failed: %v", err)
	}

	removed, err := store.RemoveByObject(context.Background(), "gallery", "notes/readme.txt")
	if err != nil {
		t.Fatalf("RemoveByObject failed: %v", err)
	}
	if removed != 2 {
		t.Fatalf("expected 2 removed links, got %d", removed)
	}
	if _, err := store.Get(context.Background(), first.ID); err == nil {
		t.Fatalf("expected first link to be removed")
	}
	if _, err := store.Get(context.Background(), second.ID); err == nil {
		t.Fatalf("expected second link to be removed")
	}
}

func TestRemoveByBucketDeletesAllMatchingLinks(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t)
	first, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "notes/a.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create first failed: %v", err)
	}
	second, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "notes/b.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create second failed: %v", err)
	}
	third, err := store.Create(context.Background(), CreateInput{Bucket: "archive", Key: "notes/c.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create third failed: %v", err)
	}

	removed, err := store.RemoveByBucket(context.Background(), "gallery")
	if err != nil {
		t.Fatalf("RemoveByBucket failed: %v", err)
	}
	if removed != 2 {
		t.Fatalf("expected 2 removed links, got %d", removed)
	}
	if _, err := store.Get(context.Background(), first.ID); err == nil {
		t.Fatalf("expected first link to be removed")
	}
	if _, err := store.Get(context.Background(), second.ID); err == nil {
		t.Fatalf("expected second link to be removed")
	}
	if _, err := store.Get(context.Background(), third.ID); err != nil {
		t.Fatalf("expected other bucket link to stay, got %v", err)
	}
}

func TestRemoveByPrefixDeletesMatchingLinks(t *testing.T) {
	t.Parallel()

	store, _ := newTestStore(t)
	first, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "folder/a.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create first failed: %v", err)
	}
	second, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "folder/deep/b.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create second failed: %v", err)
	}
	third, err := store.Create(context.Background(), CreateInput{Bucket: "gallery", Key: "other/c.txt", Expires: time.Hour})
	if err != nil {
		t.Fatalf("Create third failed: %v", err)
	}

	removed, err := store.RemoveByPrefix(context.Background(), "gallery", "folder/")
	if err != nil {
		t.Fatalf("RemoveByPrefix failed: %v", err)
	}
	if removed != 2 {
		t.Fatalf("expected 2 removed links, got %d", removed)
	}
	if _, err := store.Get(context.Background(), first.ID); err == nil {
		t.Fatalf("expected first link to be removed")
	}
	if _, err := store.Get(context.Background(), second.ID); err == nil {
		t.Fatalf("expected second link to be removed")
	}
	if _, err := store.Get(context.Background(), third.ID); err != nil {
		t.Fatalf("expected unrelated link to stay, got %v", err)
	}
}

func newTestStore(t *testing.T) (*Store, *time.Time) {
	t.Helper()

	root := t.TempDir()
	store, err := New(filepath.Join(root, "data"), zap.NewNop())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	now := time.Date(2026, time.April, 8, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	return store, &now
}

func TestNewIgnoresLegacyShareLinks(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	legacyDir := filepath.Join(dataDir, ".bares3", "sharelinks")
	if err := os.MkdirAll(legacyDir, 0o755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	createdAt := time.Date(2026, time.April, 9, 14, 0, 0, 0, time.UTC)
	content, err := json.Marshal(Link{
		ID:        "0123456789abcdef0123456789abcdef",
		Bucket:    "gallery",
		Key:       "notes/readme.txt",
		Filename:  "readme.txt",
		Size:      42,
		CreatedBy: "admin",
		CreatedAt: createdAt,
		ExpiresAt: createdAt.Add(2 * time.Hour),
	})
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if err := os.WriteFile(filepath.Join(legacyDir, "legacy.json"), content, 0o644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	store, err := New(dataDir, zap.NewNop())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})
	store.now = func() time.Time { return createdAt }

	links, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(links) != 0 {
		t.Fatalf("expected legacy share links to be ignored, got %+v", links)
	}
	count, err := store.ActiveCount(context.Background())
	if err != nil {
		t.Fatalf("ActiveCount failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected ignored legacy share links count 0, got %d", count)
	}
}
