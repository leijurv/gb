package main

import (
	"database/sql"
	"log"

	"github.com/leijurv/gb/config"

	_ "github.com/mattn/go-sqlite3"
)

var databaseFullPath = "file:" + config.Config().DatabaseLocation + "?_foreign_keys=1"

// the below is from the faq for go-sqlite3, but with the foreign key part added
const databaseTestPath = "file::memory:?mode=memory&cache=shared&_foreign_keys=1"

var ErrNoRows = sql.ErrNoRows

var db *sql.DB

func SetupDatabase() {
	setupDatabase(databaseFullPath)
}

func SetupDatabaseTestMode() {
	setupDatabase(databaseTestPath)
}

func setupDatabase(fullPath string) {
	log.Println("Opening database file", fullPath)
	var err error
	db, err = sql.Open("sqlite3", fullPath)
	if err != nil {
		panic(err)
	}
	log.Println("Database connection created")
	initialSetup()
}

func ShutdownDatabase() {
	db.Close()
}
