package statedb

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite"
)

const (
	controlDirName   = ".bares3"
	databaseFileName = "state.db"
)

type Migration struct {
	Name       string
	Statements []string
}

type BunSession struct {
	*bun.DB
}

func Session(db *bun.DB) *BunSession {
	if db == nil {
		return nil
	}
	return &BunSession{DB: db}
}

func (s *BunSession) Close() error {
	return nil
}

type journalModeState struct {
	mu    sync.Mutex
	ready bool
}

var journalModeStates sync.Map

func Path(dataDir string) (string, error) {
	trimmed := strings.TrimSpace(dataDir)
	if trimmed == "" {
		return "", fmt.Errorf("state data dir is required")
	}
	return filepath.Join(trimmed, controlDirName, databaseFileName), nil
}

func Open(dataDir string) (*sql.DB, error) {
	path, err := Path(dataDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create state db dir: %w", err)
	}

	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open state db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	if err := configure(path, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func OpenBun(dataDir string) (*bun.DB, error) {
	sqlDB, err := Open(dataDir)
	if err != nil {
		return nil, err
	}
	return Wrap(sqlDB), nil
}

func Wrap(db *sql.DB) *bun.DB {
	return bun.NewDB(db, sqlitedialect.New())
}

func EnsureMigrations(db *sql.DB, migrations []Migration) error {
	if db == nil {
		return fmt.Errorf("state db is required")
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			name TEXT PRIMARY KEY,
			applied_at TEXT NOT NULL
		)
	`); err != nil {
		return fmt.Errorf("create schema migrations table: %w", err)
	}

	applied := make(map[string]struct{}, len(migrations))
	rows, err := db.Query(`SELECT name FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("list schema migrations: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan schema migration: %w", err)
		}
		applied[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate schema migrations: %w", err)
	}

	for _, migration := range migrations {
		if migration.Name == "" {
			return fmt.Errorf("migration name must not be empty")
		}
		if _, ok := applied[migration.Name]; ok {
			continue
		}
		if err := applyMigration(db, migration); err != nil {
			return err
		}
	}

	return nil
}

func applyMigration(db *sql.DB, migration Migration) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", migration.Name, err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, statement := range migration.Statements {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := tx.Exec(statement); err != nil {
			return fmt.Errorf("apply migration %s: %w", migration.Name, err)
		}
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations (name, applied_at) VALUES (?, ?)`, migration.Name, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("record migration %s: %w", migration.Name, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %s: %w", migration.Name, err)
	}
	return nil
}

func configure(path string, db *sql.DB) error {
	for _, statement := range []string{
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA foreign_keys = ON`,
		`PRAGMA synchronous = NORMAL`,
	} {
		if _, err := db.Exec(statement); err != nil {
			return fmt.Errorf("configure state db: %w", err)
		}
	}
	if err := ensureJournalMode(path, db); err != nil {
		return err
	}
	return nil
}

func ensureJournalMode(path string, db *sql.DB) error {
	stateValue, _ := journalModeStates.LoadOrStore(path, &journalModeState{})
	state := stateValue.(*journalModeState)

	state.mu.Lock()
	defer state.mu.Unlock()

	if state.ready {
		return nil
	}
	if _, err := db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		return fmt.Errorf("configure state db: %w", err)
	}
	state.ready = true
	return nil
}
