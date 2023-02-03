package db

import (
	"database/sql"
	"errors"
	"log"
	"os"

	"github.com/leijurv/gb/config"

	_ "github.com/mattn/go-sqlite3"
)

// the below is from the faq for go-sqlite3, but with the foreign key part added
const databaseTestPath = "file::memory:?mode=memory&cache=shared&_foreign_keys=1"

var ErrNoRows = sql.ErrNoRows

var DB *sql.DB

func SetupDatabase() {
	var db string
	if config.DatabaseLocation != "" {
		db = config.DatabaseLocation
		if _, err := os.Stat(db); errors.Is(err, os.ErrNotExist) {
			panic(db + " does not exist")
		}
	} else {
		db = config.Config().DatabaseLocation
	}
	setupDatabase("file:"+db+"?_foreign_keys=1&_journal_mode=wal&_sync=1&_locking_mode=exclusive&_busy_timeout=20000", true)
}

func SetupDatabaseTestMode(setupSchema bool) {
	setupDatabase(databaseTestPath, setupSchema)
}

func setupDatabase(fullPath string, setupSchema bool) {
	//log.Println("Opening database file", fullPath)
	var err error
	DB, err = sql.Open("sqlite3", fullPath)
	if err != nil {
		panic(err)
	}
	_, err = DB.Exec("PRAGMA journal_size_limit = 100000000") // 100 megabytes
	if err != nil {
		panic(err)
	}
	//log.Println("Database connection created")
	//DB.SetMaxOpenConns(1) // 100x better to block for a few hundred ms than to panic with SQLITE_BUSY!!!!
	// commenting out until i actually hit a sqlite_busy
	if setupSchema {
		initialSetup()
	}
}

func ShutdownDatabase() {
	if DB == nil {
		log.Println("Attempting to shutdown a database that has never been setup??")
		return
	}
	DB.Close()
}
