package dupes

import (
	"fmt"
	"log"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func PrintDupes() {
	log.Println("Bear with me while I run a very slow query (sorry)")
	m := make(map[[32]byte][]string)
	rows, err := db.DB.Query(`SELECT hash, path FROM files WHERE end IS NULL`) // only files that currently exist, as of latest backup
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		var hash []byte
		var path string
		err := rows.Scan(&hash, &path)
		if err != nil {
			panic(err)
		}
		hashArr := utils.SliceToArr(hash)
		m[hashArr] = append(m[hashArr], path)
		count++
		if count%100000 == 0 { // i have millions of duplicated files :(
			log.Println("Have", count, "rows so far")
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	for _, v := range m {
		if len(v) < 2 {
			continue
		}
		for _, f := range v {
			fmt.Println(f)
		}
		fmt.Println()
	}
}
