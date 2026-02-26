package history

import (
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leijurv/gb/db"
)

func FileHistory(path string) {
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	log.Println("Fetching history of", path)
	log.Println("This will only work on individual files. For directories, use \"ls\" instead of \"history\".")
	if strings.HasSuffix(path, "/") {
		log.Println("It is unlikely for a file to end in \"/\"...")
	}
	rows, err := db.DB.Query(`SELECT files.start, files.end, files.permissions, files.fs_modified, sizes.size, files.hash FROM files INNER JOIN sizes ON sizes.hash = files.hash WHERE files.path = ? ORDER BY files.start`, path)
	db.Must(err)
	defer rows.Close()
	log.Println()
	log.Println("Revision start - Revision end: permissions, filesystem modified, size, hash")
	for rows.Next() {
		var start int64
		var end *int64
		var perms os.FileMode
		var fsModified int64
		var size int64
		var hash []byte
		db.Must(rows.Scan(&start, &end, &perms, &fsModified, &size, &hash))
		line := ""
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
