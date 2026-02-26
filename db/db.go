package db

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/leijurv/gb/config"

	_ "github.com/mattn/go-sqlite3"
)

// the below is from the faq for go-sqlite3, but with the foreign key part added
const databaseTestPath = "file::memory:?mode=memory&cache=shared&_foreign_keys=1"

var ErrNoRows = sql.ErrNoRows

var DB *sql.DB

// only to be used for sqlite errors
// sql errors in gb are 1. unrecoverable 2. not expected to ever be user-facing
// a panic is the right choice
func Must(err error) {
	if err != nil {
		panic(err)
	}
}

// see https://sqlite.org/forum/info/eabfcd13dcd71807
func StartsWithPattern(arg int32) string {
	return fmt.Sprintf(" BETWEEN (?%d) AND (?%d || x'ff') ", arg, arg)
	// this works because 0xff is higher than any byte of any valid utf8 string. technically x'f8' or higher would work but it's more obviously correct to put x'ff'
	// if you could have 0xff in a utf8 string then this wouldn't catch it (since `path || x'ff'` is less than `path || x'ff' || 'foo'`), but, that isn't valid utf8, and utils.WalkFiles checks for it and so does `gb paranoia db` so it's fine
}

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
	setupDatabase("file:"+db+"?_foreign_keys=1&_journal_mode=wal&_sync=1&_busy_timeout=20000", true)
}

func SetupDatabaseTestMode(setupSchema bool) {
	setupDatabase(databaseTestPath, setupSchema)
}

func setupDatabase(fullPath string, setupSchema bool) {
	//log.Println("Opening database file", fullPath)
	var err error
	DB, err = sql.Open("sqlite3", fullPath)
	Must(err)
	_, err = DB.Exec("PRAGMA journal_size_limit = 100000000") // 100 megabytes
	Must(err)
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
