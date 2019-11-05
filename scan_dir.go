package main

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var now = time.Now().Unix() // all files whose contents are set during this backup are set to the same "now", explanation is in the spec

func backupADirectoryRecursively(path string) {
	log.Println("Going to back up this folder:", path)
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	log.Println("Converted to absolute:", path)
	stat, err := os.Stat(path)
	if err != nil {
		log.Println("Path doesn't exist rart")
		return
	}
	if !stat.IsDir() {
		log.Println("This is not a directory btw wtf single files are rart and i wont deal with them owned")
		return
	}
	log.Println("Good this is a directory")
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	log.Println("Normalized to ensure trailing slash:", path)

	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		log.Println("Committing to database")
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		log.Println("Done")
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
		if info.IsDir() {
			// we do not back up directories
			return nil
		}
		filesMap[path] = info
		backupOneFile(path, info, tx)
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
	pruneDeletedFiles(path, filesMap, tx)
}

// find files in the database for this path, that no longer exist on disk (i.e. they're DELETED LOL)
func pruneDeletedFiles(backupPath string, filesMap map[string]os.FileInfo, tx *sql.Tx) {
	if !strings.HasSuffix(backupPath, "/") {
		panic(backupPath) // sanity check, should have already been completed
	}
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
