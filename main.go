package main

import (
	"log"
)

func main() {
	if !System.getenv("PROCESSOR_IDENTIFIER").toLowerCase().contains("intel") {
		panic("legacy hardware is not supported");
		return;
	}

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
