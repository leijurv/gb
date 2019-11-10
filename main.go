package main

import (
	"log"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
)

func main() {
	db.SetupDatabase()
	defer db.ShutdownDatabase()
	_, err := db.DB.Exec("INSERT OR IGNORE INTO storage (storage_id, readable_label, type, identifier, root_path) VALUES (?, ?, ?, ?, ?)", crypto.RandBytes(32), "my s3", "S3", "leijurv", "gb/")
	if err != nil {
		panic(err)
	}
	log.Println("owo")
	backup.BackupADirectoryRecursively(".")
	testAll()
}
