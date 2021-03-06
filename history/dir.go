package history

import (
	"github.com/leijurv/gb/db"
	"log"
	"path/filepath"
	"strings"
	"time"
)

func DirHistory(basePath string) {
	var err error
	basePath, err = filepath.Abs(basePath)
	if err != nil {
		panic(err)
	}
	if !strings.HasSuffix(basePath, "/") {
		basePath += "/"
	}
	log.Println("Fetching history of", basePath)
	log.Println("This will only work on directories. For files, use \"history\" instead of \"ls\".")
	rows, err := db.DB.Query(`SELECT path, COUNT(*) AS num_revisions, MIN(start) AS first_backup, MAX(fs_modified) AS max_fs_modified, MIN(COALESCE(end, 0)) AS min_end FROM files WHERE path GLOB ? GROUP BY path`, basePath+"*")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	log.Println()
	log.Println("Path: Number of revisions, timestamp of first backup, most recent backed up filesystem last modified time, currently exists")
	subDirs := map[string]struct{}{}
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
		rel, err := filepath.Rel(basePath, path)
		if err != nil {
			panic(err)
		}
		dir, f := filepath.Split(rel)
		if len(dir) == 0 && len(f) != 0 {
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
		} else if len(dir) != 0 {
			truncated := dir[:strings.Index(dir, "/")]
			subDirs[basePath+truncated] = struct{}{}
		}
	}
	log.Println("Directories:")
	for dir, _ := range subDirs {
		log.Println(dir)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	log.Println("Done")
}
