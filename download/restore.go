package download

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leijurv/gb/utils"

	"github.com/leijurv/gb/db"
)

const (
	QUERY_BASE = "SELECT files.hash, files.path, files.fs_modified, files.permissions, files.start, sizes.size FROM files INNER JOIN sizes ON files.hash = sizes.hash WHERE (? > files.start AND (files.end > ? OR files.end IS NULL)) AND files.path "
)

// one path on disk we are going to write, and what should be written there
type Item struct {
	hash        []byte
	origPath    string
	fsModified  int64
	permissions os.FileMode
	start       int64
	size        int64

	destPath string
}

// one "thing" (hash) to restore (potentially to more than 1 destination on disk)
type Restoration struct {
	hash []byte
	size int64

	destinations []Item

	sourcesOnDisk []Source
}

// somewhere on disk where we expect to find some given data
type Source struct {
	path       string
	fsModified int64
}

func Restore(src string, dest string, timestamp int64) {
	// concept: restore a directory
	// src is where the directory was (is, in the database)
	// dest is where the directory should be
	if timestamp == -1 {
		timestamp = time.Now().Unix()
	}
	if dest == "" {
		dest = src
	}
	// src doesn't necessarily need to exist on the filesystem
	// that is, in fact, the entire point of restoring from a backup
	// lol
	// but this works even if it don't exist
	var err error
	src, err = filepath.Abs(src)
	if err != nil {
		panic(err)
	}
	dest, err = filepath.Abs(dest)
	if err != nil {
		panic(err)
	}
	// we should not consider what's on the filesystem at the source
	// this is a restore :)

	log.Println("src:", src)
	log.Println("dest:", dest)
	log.Println("timestamp:", timestamp)

	assumingFile := generatePlanAssumingFile(src, timestamp)
	if len(assumingFile) > 1 {
		panic("database should not allow this?")
	}
	assumingDir := generatePlanAssumingDir(src, timestamp)
	srcFile := len(assumingFile) > 0
	srcDir := len(assumingDir) > 0
	if !srcFile && !srcDir {
		panic(src + " did not exist in the database (as either a file or directory) as of that timestamp")
	}
	if srcFile && srcDir {
		panic("Unclear if you mean the file or the directory (i.e. should I restore one file, or many). This should never happen. You can add a trailing / to indicate you mean a directory. If it's just 1 file, restore it manually using history and cat lol")
	}
	items := append(assumingFile, assumingDir...) // only one will have entries, as we just showed
	for _, item := range items {                  // useless sanity check
		if item.destPath != "" {
			panic(item.destPath)
		}
	}
	destStat, err := os.Stat(dest)
	if err != nil {
		// dest does NOT exist
		if os.IsNotExist(err) && srcFile {
			log.Println("Destination path does not exist, BUT I will allow this since what you're restoring is a single file, and that's pretty reasonable")
			items[0].destPath = dest
		} else {
			log.Println("Destination must exist, sorry!")
			panic(err)
		}
	} else {
		// dest DOES exist
		if !destStat.IsDir() && !utils.NormalFile(destStat) {
			panic("dst must either be a directory or a normal file")
		}
		if srcFile {
			if destStat.IsDir() { // file to dir
				// if src is /a/b/c (a file) and dest is /d/e/f/ (a directory) we restore to /d/e/f/c
				items[0].destPath = filepath.Join(dest, filepath.Base(src))
			} else { // file to file
				// overwrite i guess?
				items[0].destPath = dest
			}
		} else { // dir to dir
			if utils.NormalFile(destStat) {
				panic("you cannot restore a directory to a file")
			}
			if !strings.HasSuffix(src, "/") {
				src += "/"
			}
			for i := range items {
				orig := items[i].origPath
				if !strings.HasPrefix(orig, src) { // useless sanity check
					panic("what")
				}
				items[i].destPath = filepath.Join(dest, orig[len(src):])
			}
		}
	}
	for _, item := range items { // useless sanity check
		if item.destPath == "" {
			panic("what")
		}
	}
	description := "Destination path & restored from, timestamp when backup of source was taken, filesystem last modified timestamp as of that revision, size, permissions, hash"
	log.Println()
	log.Println(description)
	cnt := 0
	for _, item := range items {
		line := ""

		if item.origPath == item.destPath {
			line += "Restore to the same place: "
			line += item.origPath
		} else {
			line += "Restore from a DIFFERENT path: "
			line += "restore to "
			line += item.destPath
			line += " the file originally at "
			line += item.origPath
			cnt++
		}
		line += ", "
		line += time.Unix(item.start, 0).Format(time.RFC3339)
		line += ", "
		line += time.Unix(item.fsModified, 0).Format(time.RFC3339)
		line += ", "
		line += fmt.Sprint(item.size)
		line += ", "
		line += fmt.Sprint(item.permissions)
		line += ", "
		line += hex.EncodeToString(item.hash)
		log.Println(line)
	}
	log.Println(description)
	log.Println()
	log.Println("^ There's a list of where paths would be restored from/to. Look good?")
	log.Println()
	if cnt > 0 {
		log.Println("NOTE:", cnt, "files are being restored to locations DIFFERENT from where they were originally backed up from")
		log.Println()
	}
	m := maxstart(items)
	log.Println("NOTE: I am restoring to timestamp", time.Unix(timestamp, 0).Format(time.RFC3339), "BUT the most recent gb backup in which this data had been updated was at", time.Unix(m, 0).Format(time.RFC3339))
	log.Println("NOTE: That disparity is", timestamp-m, "seconds")
	log.Println("Confirm? (yes: enter, no: ctrl+c) >")
	bufio.NewReader(os.Stdin).ReadString('\n')

	plan := make(map[[32]byte]*Restoration)
	for _, item := range items {
		key := utils.SliceToArr(item.hash)
		if _, ok := plan[key]; !ok {
			plan[key] = &Restoration{item.hash, item.size, nil, nil}
		}
		plan[key].destinations = append(plan[key].destinations, item)
	}
	//log.Println(plan)
	locateSourcesOnDisk(plan)
	//log.Println(plan)
	for _, r := range plan {
		if len(r.destinations) == 0 || len(r.hash) == 0 {
			panic("failed")
		}
		if len(r.sourcesOnDisk) == 0 {
			log.Println("WARNING: have no source on disk for", r.destinations[0].destPath)
		} else {
			log.Println("Have a source on disk for", r.destinations[0].destPath, ":", r.sourcesOnDisk[0].path)
		}
	}
	log.Println("TODO: now execute the plan")
}

func locateSourcesOnDisk(plan map[[32]byte]*Restoration) {
	// here be dragons
	// we cannot do a "AND hash IN (?, ?, ?...)" because sqlite only allows 999 of those ?s lmfao
	log.Println("Sorry, need to run some queries now that can be slow...")
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
	}()
	// use a prepared statement since we're going to do it MANY MANY MANY times in a row
	stmt, err := tx.Prepare("SELECT path, fs_modified FROM files WHERE end IS NULL AND hash = ?")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	for hash, rest := range plan {
		func() { // wrap in a closure so that rows.Close isn't all saved till the end
			rows, err := stmt.Query(hash[:])
			if err != nil {
				panic(err)
			}
			defer rows.Close()
			for rows.Next() {
				var source Source
				err = rows.Scan(&source.path, &source.fsModified)
				if err != nil {
					panic(err)
				}
				rest.sourcesOnDisk = append(rest.sourcesOnDisk, source)
			}
			err = rows.Err()
			if err != nil {
				panic(err)
			}
		}()
	}
	log.Println("Done with the slow queries lol")
}

func maxstart(items []Item) int64 {
	m := items[0].start
	for _, item := range items {
		if item.start > m {
			m = item.start
		}
	}
	return m
}

func generatePlanAssumingFile(path string, timestamp int64) []Item {
	return generatePlanUsingQuery(QUERY_BASE+" = ?", path, timestamp, path)
}

func generatePlanAssumingDir(path string, timestamp int64) []Item {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return generatePlanUsingQuery(QUERY_BASE+" GLOB ?", path+"*", timestamp, path)
}

func generatePlanUsingQuery(query string, path string, timestamp int64, prefixChk string) []Item {
	plan := make([]Item, 0)
	log.Println(query, path, timestamp)
	rows, err := db.DB.Query(query, timestamp, timestamp, path)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var item Item
		err = rows.Scan(&item.hash, &item.origPath, &item.fsModified, &item.permissions, &item.start, &item.size)
		if err != nil {
			panic(err)
		}
		if !strings.HasPrefix(item.origPath, prefixChk) {
			// handle the case where a directory actually has a * in its name smh
			continue
		}
		plan = append(plan, item)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return plan
}
