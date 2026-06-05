package db

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/leijurv/gb/config"
	sqlite3 "github.com/mattn/go-sqlite3"
)

// TestReadWriteSplit locks in the read-only/read-write pool split: DB must reject writes, and the
// read-then-write pattern that used to race into SQLITE_BUSY_SNAPSHOT must work on RWDB. Uses the
// real on-disk WAL SetupDatabase(), not the in-memory test mode (where DB==RWDB).
func TestReadWriteSplit(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")
	if err := os.WriteFile(dbPath, nil, 0644); err != nil {
		t.Fatal(err)
	}
	config.DatabaseLocation = dbPath
	defer func() { config.DatabaseLocation = "" }()

	SetupDatabase()
	defer ShutdownDatabase()

	if _, err := RWDB.Exec("INSERT INTO sizes (hash, size) VALUES (?, ?)", make([]byte, 32), 5); err != nil {
		t.Fatalf("RWDB should accept writes: %v", err)
	}

	// DB must reject writes with SQLITE_READONLY
	_, err := DB.Exec("INSERT INTO sizes (hash, size) VALUES (?, ?)", make([]byte, 32), 7)
	if err == nil {
		t.Fatal("db.DB accepted a write, but it must be read-only (query_only)")
	}
	var se sqlite3.Error
	if !errors.As(err, &se) || se.Code != sqlite3.ErrReadonly {
		t.Fatalf("expected SQLITE_READONLY from db.DB write, got: %v", err)
	}

	// read-then-write in one tx (the hasher.go pattern that used to race into SQLITE_BUSY_SNAPSHOT)
	tx, err := RWDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	var n int
	if err := tx.QueryRow("SELECT COUNT(*) FROM sizes").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("UPDATE sizes SET size = 6 WHERE size = 5"); err != nil {
		t.Fatalf("read-then-write on RWDB must not hit SQLITE_BUSY_SNAPSHOT, got: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
}
