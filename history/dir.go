package history

import (
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/leijurv/gb/db"
)

func DirHistory(path string) {
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	log.Println("Fetching history of", path)
	log.Println("This will only work on directories. For files, use \"history\" instead of \"ls\".")
	rows, err := db.DB.Query(`SELECT path, COUNT(*) AS num_revisions, MIN(start) AS first_backup, MAX(fs_modified) AS max_fs_modified, MIN(COALESCE(end, 0)) AS min_end FROM files WHERE path GLOB ? AND path NOT GLOB ? GROUP BY path`, path+"*", path+"*/*")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	log.Println()
	log.Println("Path: Number of revisions, timestamp of first backup, most recent backed up filesystem last modified time, currently exists")
	for rows.Next() {
		var path string
		var numRevs int64
		var firstBackup int64
		var maxFsModified int64
		var minEnd int64
		err := rows.Scan(&path, &numRevs, &firstBackup, &maxFsModified, &minEnd)
		if err != nil {
			panic(err)
		}
		// TODO use % formatting :(
		line := path
		line += ": "
		line += time.Unix(firstBackup, 0).Format(time.RFC3339)
		line += ", "
		line += time.Unix(maxFsModified, 0).Format(time.RFC3339)
		line += ", "
		if minEnd == 0 {
			line += "true"
		} else {
			line += "false"
		}
		log.Println(line)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	log.Println("Done")
}
