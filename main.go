package main

import (
	"log"
)

func main() {
	SetupDatabase()
	_, err := db.Exec("INSERT OR IGNORE INTO storage (storage_id, readable_label, type, identifier, root_path) VALUES (?, ?, ?, ?, ?)", randBytes(32), "my s3", "S3", "leijurv", "gb/")
	if err != nil {
		panic(err)
	}
	log.Println("owo")
	backupADirectoryRecursively(".")
	upload()
	testAll()
}
