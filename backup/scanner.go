package backup

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
)

func scannerThread(path string) {
	defer close(hasherCh)
	defer wg.Done()
	defer func() {
		go func() {
			for {
				time.Sleep(1 * time.Second)
				bucketerCh <- Planned{}
			}
		}()
	}()
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		log.Println("Scanner committing to database (even though it's read only)")
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		log.Println("Committed")
	}()
	filesMap := make(map[string]os.FileInfo)
	log.Println("Beginning scan now!")
	err = filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("While traversing those files, I got this error:")
			log.Println(err)
			log.Println("while looking at this path:")
			log.Println(path)
			return err
		}
		if info.Mode()&os.ModeType != 0 { // **THIS IS WHAT SKIPS DIRECTORIES**
			// skip Weird Things such as directories, symlinks, pipes, sockets, block devices, etc
			return nil
		}
		if config.ExcludeFromBackup(path) {
			log.Println("EXCLUDING this path and pretending it doesn't exist, due to your exclude config:", path)
			return nil
		}
		filesMap[path] = info
		scanOneFile(File{path, info}, tx)
		return nil
	})
	if err != nil {
		// permission error while traversing
		// we should *not* continue, because that would mark all further files as "deleted"
		// aka, do not continue with a partially complete traversal of the directory lmao
		panic(err)
	}
	log.Println("Finally, handling deleted files!")
	// anything that was in this directory but is no longer can be deleted
	pruneDeletedFiles(path, filesMap)
}

func scanOneFile(file File, tx *sql.Tx) {
	var expectedLastModifiedTime int64
	var expectedHash []byte
	var expectedSize int64
	size := file.info.Size()
	err := tx.QueryRow("SELECT files.fs_modified, files.hash, sizes.size FROM files INNER JOIN sizes ON files.hash = sizes.hash WHERE files.path = ? AND files.end IS NULL", file.path).Scan(&expectedLastModifiedTime, &expectedHash, &expectedSize)
	if err == nil {
		if expectedLastModifiedTime == file.info.ModTime().Unix() && expectedSize == size { // only rescan on size change or modified change, NOT on permissions change lmao
			log.Println("UNMODIFIED:", file.path, "ModTime is still", expectedLastModifiedTime, "and size is still", expectedSize)
			return
		}
		log.Println("MODIFIED:", file.path, "Was previously stored, but I'm updating it since the last modified time has changed from", expectedLastModifiedTime, "to", file.info.ModTime().Unix(), "or the size has changed from", expectedSize, "to", size)
	} else {
		if err != db.ErrNoRows {
			panic(err) // unexpected error, maybe sql syntax error?
		}
		// ErrNoRows = file is brand new
	}

	// check if there is an existing file of this length
	var otherHash []byte
	err = tx.QueryRow("SELECT hash FROM sizes WHERE size = ?", size).Scan(&otherHash)
	if err != nil {
		if err != db.ErrNoRows {
			panic(err) // unexpected error, maybe sql syntax error?
		}
		// ErrNoRows = no existing files of this size stored in the db! we can do the bypass!
		if stakeSizeClaim(size) {
			// UwU we CAN do the bypass YAY
			log.Println("Staked size claim", size, "skipping hasher directly to bucketer epic style", file.path)
			wg.Add(1)
			bucketerCh <- Planned{file, nil, nil, &size}
			log.Println("wrote", file.path)
			return
		}
	}
	// no bypass :(
	log.Println("hasherCh write", file.path)
	hasherCh <- HashPlan{file, nil}
	log.Println("wrote", file.path)
}

// find files in the database for this path, that no longer exist on disk (i.e. they're DELETED LOL)
func pruneDeletedFiles(backupPath string, filesMap map[string]os.FileInfo) {
	if !strings.HasSuffix(backupPath, "/") {
		panic(backupPath) // sanity check, should have already been completed
	}
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		log.Println("Pruner committing to database")
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		log.Println("Committed")
	}()
	like := backupPath + "*"
	rows, err := tx.Query("SELECT path FROM files WHERE path GLOB ? AND end IS NULL", like)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var databasePath string
		err := rows.Scan(&databasePath)
		if err != nil {
			panic(err)
		}
		if !strings.HasPrefix(databasePath, backupPath) {
			// if there's a *, sqlite will match too many rows (not too few), which is at least better than the alternative i guess
			// but we do need to check it again like this
			log.Println("Having a * in your folder name is really a bad idea, good thing I thought of this!")
			continue
		}
		if _, ok := filesMap[databasePath]; !ok {
			log.Println(databasePath, "used to exist but does not any longer. Marking as ended.")
			_, err = tx.Exec("UPDATE files SET end = ? WHERE path = ? AND end IS NULL", now, databasePath)
			if err != nil {
				panic(err)
			}
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
}
