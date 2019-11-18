package db

import (
	"database/sql"
	//"log"

	"github.com/leijurv/gb/config"

	_ "github.com/mattn/go-sqlite3"
)

var databaseFullPath = "file:" + config.Config().DatabaseLocation + "?_foreign_keys=1&_journal_mode=wal&_sync=1&_locking_mode=exclusive&_busy_timeout=5000"

// the below is from the faq for go-sqlite3, but with the foreign key part added
const databaseTestPath = "file::memory:?mode=memory&cache=shared&_foreign_keys=1"

var ErrNoRows = sql.ErrNoRows

var DB *sql.DB

func SetupDatabase() {
	setupDatabase(databaseFullPath)
}

func SetupDatabaseTestMode() {
	setupDatabase(databaseTestPath)
}

func setupDatabase(fullPath string) {
	//log.Println("Opening database file", fullPath)
	var err error
	DB, err = sql.Open("sqlite3", fullPath)
	if err != nil {
		panic(err)
	}
	//log.Println("Database connection created")
	//DB.SetMaxOpenConns(1) // 100x better to block for a few hundred ms than to panic with SQLITE_BUSY!!!!
	// commenting out until i actually hit a sqlite_busy
	initialSetup()
}

func ShutdownDatabase() {
	DB.Close()
}
