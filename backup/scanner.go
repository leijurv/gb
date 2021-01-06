package backup

import (
	"database/sql"
	"log"
	"os"
	"strings"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func scannerThread(path string, info os.FileInfo) {
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	if !info.IsDir() {
		scanFile(File{path, info}, tx)
		return
	}
	log.Println("Beginning scan now!")
	filesMap := make(map[string]os.FileInfo)
	utils.WalkFiles(path, func(path string, info os.FileInfo) {
		filesMap[path] = info
		scanFile(File{path, info}, tx)
	})
	log.Println("Scanner committing")
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	log.Println("Scanner committed")
	go func() {
		for {
			time.Sleep(1 * time.Second)
			bucketerCh <- Planned{}
		}
	}()
	close(hasherCh)
	wg.Wait()
	pruneDeletedFiles(path, filesMap)
}

func scanFile(file File, tx *sql.Tx) {
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
		log.Println("MODIFIED:", file.path, "was previously stored, but I'm updating it since the last modified time has changed from", expectedLastModifiedTime, "to", file.info.ModTime().Unix(), "and/or the size has changed from", expectedSize, "to", size)
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
		if stakeSizeClaim(size) { // no files of this size in the database yet... but check if there is already one in progress Right Now?
			// UwU we CAN do the bypass YAY
			log.Println("Staked size claim", size, "skipping hasher directly to bucketer epic style", file.path)
			wg.Add(1)
			bucketerCh <- Planned{file, nil, nil, &size}
			log.Println("wrote", file.path)
			return
		}
	}
	// no bypass :(
	// we know of a file with the exact same size (either in db, or currently being uploaded)
	// so we do actually need to check the hash of this file to determine if it's unique or not
	hasherCh <- HashPlan{file, nil}
}

// find files in the database for this path, that no longer exist on disk (i.e. they're DELETED LOL)
func pruneDeletedFiles(backupPath string, filesMap map[string]os.FileInfo) {
	// we cannot upgrade the long lived RO transaction to a RW transaction, it would conflict with the intermediary RW transactions, it seems
	// reusing the tx from scanner results in a sqlite busy panic, very consistently
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		log.Println("Pruner committing")
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		log.Println("Pruner committed")
	}()
	if !strings.HasSuffix(backupPath, "/") {
		panic(backupPath) // sanity check, should have already been completed
	}
	log.Println("Finally, handling deleted files!")
	// anything that was in this directory but is no longer can be deleted
	pattern := backupPath + "*"
	rows, err := tx.Query("SELECT path FROM files WHERE path GLOB ? AND end IS NULL", pattern)
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
