package history

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/leijurv/gb/db"
)

func Search(input string) {
	query := `SELECT files.path, files.start, files.end, files.permissions, files.fs_modified, sizes.size, files.hash FROM files INNER JOIN sizes ON sizes.hash = files.hash WHERE `
	var arg any
	if hash, err := hex.DecodeString(input); err == nil && len(hash) == 32 {
		query += "files.hash = ?"
		arg = hash
		log.Println("Query by hash:", input)
	} else {
		query += "files.path LIKE ?"
		arg = "%" + input + "%"
		log.Println("Query is:", arg)
	}
	rows, err := db.DB.Query(query, arg)
	db.Must(err)
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
		db.Must(rows.Scan(&path, &start, &end, &perms, &fsModified, &size, &hash))
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
	db.Must(rows.Err())
	log.Println("Done")
}
