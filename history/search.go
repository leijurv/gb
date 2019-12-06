package history

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/leijurv/gb/db"
)

func Search(path string) {
	query := "%" + path + "%"
	log.Println("Query is:", query)
	rows, err := db.DB.Query(`SELECT files.path, files.start, files.end, files.permissions, files.fs_modified, sizes.size, files.hash FROM files INNER JOIN sizes ON sizes.hash = files.hash WHERE files.path LIKE ?`, query)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	log.Println()
	log.Println("Path: revision start - revision end: permissions, filesystem modified, size, hash")
	for rows.Next() {
		var path string
		var start int64
		var end *int64
		var perms os.FileMode
		var fsModified int64
		var size int64
		var hash []byte
		err := rows.Scan(&path, &start, &end, &perms, &fsModified, &size, &hash)
		if err != nil {
			panic(err)
		}
		line := ""
		line += path
		line += ": "
		line += time.Unix(start, 0).Format(time.RFC3339)
		line += " - "
		if end == nil {
			line += "current"
		} else {
			line += time.Unix(*end, 0).Format(time.RFC3339)
		}
		line += ": "
		line += fmt.Sprint(perms)
		line += ", "
		line += time.Unix(fsModified, 0).Format(time.RFC3339)
		line += ", "
		line += fmt.Sprint(size)
		line += ", "
		line += hex.EncodeToString(hash)
		log.Println(line)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	log.Println("Done")
}
