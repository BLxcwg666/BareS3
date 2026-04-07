package auditlog

import (
	"path/filepath"
	"testing"
	"time"
)

func TestRecentReturnsNewestEntriesFirst(t *testing.T) {
	t.Parallel()

	recorder, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	entries := []Entry{
		{Time: time.Date(2026, 4, 7, 9, 0, 0, 0, time.UTC), Actor: "admin", Action: "bucket.create", Title: "Created bucket gallery"},
		{Time: time.Date(2026, 4, 8, 10, 0, 0, 0, time.UTC), Actor: "admin", Action: "object.upload", Title: "Uploaded gallery/mock-01.png"},
		{Time: time.Date(2026, 4, 8, 11, 0, 0, 0, time.UTC), Actor: "admin", Action: "settings.storage.update", Title: "Updated instance storage limit"},
	}
	for _, entry := range entries {
		if err := recorder.Record(entry); err != nil {
			t.Fatalf("Record(%s) failed: %v", entry.Action, err)
		}
	}

	recent, err := recorder.Recent(2)
	if err != nil {
		t.Fatalf("Recent failed: %v", err)
	}
	if len(recent) != 2 {
		t.Fatalf("expected 2 recent entries, got %d", len(recent))
	}
	if recent[0].Action != "settings.storage.update" {
		t.Fatalf("unexpected first action: %s", recent[0].Action)
	}
	if recent[1].Action != "object.upload" {
		t.Fatalf("unexpected second action: %s", recent[1].Action)
	}

	path := filepath.Join(recorder.dir, filenameFor(entries[2].Time))
	if path == "" {
		t.Fatalf("expected non-empty audit log path")
	}
}
