package s3creds

import (
	"context"
	"testing"

	"go.uber.org/zap"
)

func TestBootstrapCreateAndRevoke(t *testing.T) {
	t.Parallel()

	store, err := New(t.TempDir(), BootstrapCredential{AccessKeyID: "legacy-key", SecretAccessKey: "legacy-secret"}, zap.NewNop())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	items, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List failed: %v", err)
	}
	if len(items) != 1 || items[0].AccessKeyID != "legacy-key" || items[0].Status != "active" {
		t.Fatalf("unexpected bootstrapped credentials: %+v", items)
	}

	created, err := store.Create(context.Background(), CreateInput{Label: "CI key", Permission: PermissionReadOnly, Buckets: []string{"gallery", "archive", "gallery"}})
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if created.AccessKeyID == "" || created.SecretAccessKey == "" {
		t.Fatalf("expected generated credentials, got %+v", created)
	}
	if created.Permission != PermissionReadOnly || len(created.Buckets) != 2 {
		t.Fatalf("unexpected created credential scope: %+v", created)
	}

	updated, err := store.Update(context.Background(), UpdateInput{AccessKeyID: created.AccessKeyID, Label: "Updated CI key", Permission: PermissionReadWrite, Buckets: []string{"gallery"}})
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updated.Label != "Updated CI key" || updated.Permission != PermissionReadWrite || len(updated.Buckets) != 1 || updated.Buckets[0] != "gallery" {
		t.Fatalf("unexpected updated credential: %+v", updated)
	}

	secret, err := store.LookupSecret(context.Background(), created.AccessKeyID)
	if err != nil {
		t.Fatalf("LookupSecret failed: %v", err)
	}
	if secret != created.SecretAccessKey {
		t.Fatalf("unexpected secret lookup result: %q", secret)
	}

	revoked, err := store.Revoke(context.Background(), created.AccessKeyID)
	if err != nil {
		t.Fatalf("Revoke failed: %v", err)
	}
	if revoked.Status != "revoked" || revoked.RevokedAt == nil {
		t.Fatalf("unexpected revoked credential: %+v", revoked)
	}

	if _, err := store.LookupSecret(context.Background(), created.AccessKeyID); err == nil {
		t.Fatalf("expected revoked credential lookup to fail")
	}
	if err := store.Delete(context.Background(), created.AccessKeyID); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
}
