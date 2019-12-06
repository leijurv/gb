package dupes

import (
	"fmt"
	"log"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func PrintDupes(since int64) {
	if since != 0 {
		// just some user friendliness
		var discard int64
		err := db.DB.QueryRow(`SELECT MAX(start) FROM files WHERE start <= ?`, since).Scan(&discard)
		if err != nil {
			if err.Error() == `sql: Scan error on column index 0, name "MAX(start)": converting NULL to int64 is unsupported` {
				log.Println()
				log.Println()
				log.Println()
				log.Println()
				log.Println("The timestamp did not filter any files out. It is older than your oldest backup.")
				log.Println()
				log.Println("If you want to deduplicate all files, you should leave the \"since\" flag blank to remove this ambiguity.")
				log.Println()
				log.Println()
				panic("bad timestamp")
			} else {
				panic(err)
			}
		} else {
			log.Println("The most recent backup that WON'T be considered (because it's before your timestamp) is the one that was taken at", time.Unix(discard, 0).Format(time.RFC3339))
			log.Println("Will only consider files updated more recently than that")
			log.Println("Files scanned in that backup and older will not be considered")
		}
	} else {
		log.Println("No 'since' date provided, will assume that dedupe has never been run before (*all* duplicated files will be outputted)")
	}
	log.Println("Bear with me while I run a very slow query (sorry)")
	hashToPaths := make(map[[32]byte][]string)
	hashesToDedupe := make(map[[32]byte]bool)
	rows, err := db.DB.Query(`SELECT hash, path, start FROM files WHERE end IS NULL`) // only files that currently exist, as of latest backup
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var hash []byte
		var path string
		var start int64
		err := rows.Scan(&hash, &path, &start)
		if err != nil {
			panic(err)
		}
		hashArr := utils.SliceToArr(hash)
		hashToPaths[hashArr] = append(hashToPaths[hashArr], path)
		if !config.ExcludeFromDedupe(path) && start > since {
			hashesToDedupe[hashArr] = true
		}
		count++
		if count%100000 == 0 { // i have millions of duplicated files :(
			log.Println("Have", count, "rows so far")
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	for hash, _ := range hashesToDedupe {
		paths := hashToPaths[hash]
		if len(paths) < 2 {
			continue
		}
		for _, path := range paths {
			fmt.Println(path)
		}
		fmt.Println()
	}
}
