package statedb

import "testing"

func TestOpenAllowsConcurrentHandleDuringWriteTransaction(t *testing.T) {
	t.Parallel()

	dataDir := t.TempDir()
	db, err := Open(dataDir)
	if err != nil {
		t.Fatalf("Open first db failed: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_items (id INTEGER PRIMARY KEY, name TEXT NOT NULL)`); err != nil {
		t.Fatalf("create table failed: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if _, err := tx.Exec(`INSERT INTO test_items (name) VALUES (?)`, "alpha"); err != nil {
		t.Fatalf("insert in tx failed: %v", err)
	}

	second, err := Open(dataDir)
	if err != nil {
		t.Fatalf("Open second db during active tx failed: %v", err)
	}
	defer func() {
		_ = second.Close()
	}()
}
