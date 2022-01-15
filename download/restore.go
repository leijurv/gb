package download

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

const (
	QUERY_BASE = "SELECT files.hash, files.path, files.fs_modified, files.permissions, files.start, sizes.size FROM files INNER JOIN sizes ON files.hash = sizes.hash WHERE (? >= files.start AND (files.end > ? OR files.end IS NULL)) AND files.path "
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

	destinations map[string]Item

	nominatedSource *string

	// somewhere on disk where we expect to find some given data
	sourcesOnDisk map[string]int64 // path to fsModified
}

func Restore(src string, dest string, timestamp int64) {
	// concept: restore a directory
	// src is where the directory was (is, in the database)
	// dest is where the directory should be
	if timestamp == 0 {
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
		if src == config.Config().DatabaseLocation {
			panic("The database can not be restored through gb")
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

		if utils.IsDatabaseFile(item.origPath) || utils.IsDatabaseFile(item.destPath) {
			continue
		}
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
		if utils.IsDatabaseFile(item.origPath) || utils.IsDatabaseFile(item.destPath) {
			continue
		}
		key := utils.SliceToArr(item.hash)

		if _, ok := plan[key]; !ok {
			plan[key] = &Restoration{
				hash:          item.hash,
				size:          item.size,
				destinations:  make(map[string]Item),
				sourcesOnDisk: make(map[string]int64),
			}
		}
		plan[key].destinations[item.destPath] = item
	}
	//log.Println(plan)
	locateSourcesOnDisk(plan)
	//log.Println(plan)
	for _, r := range plan {
		if len(r.destinations) == 0 || len(r.hash) == 0 {
			panic("failed")
		}
	}
	log.Println("Okay that was all database stuff, now I will stat your disk to see how much is already in place, how much I can pull from other files, and how much needs to be downloaded from storage")
	statSources(plan)
	cnt = 0
	cnt2 := 0
	for _, r := range plan {
		if len(r.destinations) == 0 {
			panic("failed")
		}
		if len(r.sourcesOnDisk) == 0 {
			for _, d := range r.destinations {
				log.Println("WARNING: have no source on disk for", d.destPath)
				cnt2++
			}
			log.Println("Size is", r.size)
			cnt++
		}
	}
	log.Println("Okay I've stat'd all the possible sources of these hashes, run the numbers, etc etc etc")
	log.Println("Obviously, who cares how much I need to mkdir and cp --reflink and modify mtimes to make this happen")
	log.Println("Local sources don't matter")
	log.Println("Let's get down to what really matters: how much of it is not local, aka files I need to pull from storage")
	var sum int64
	for _, r := range plan {
		if r.nominatedSource == nil {
			sum += r.size
		}
	}
	log.Println("The answer is", sum, "bytes across", cnt, "distinct hashes, to be written to", cnt2, "places on disk")
	log.Println("Confirm? (yes: enter, no: ctrl+c) >")
	bufio.NewReader(os.Stdin).ReadString('\n')
	for _, r := range plan {
		execute(*r)
	}
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func execute(rest Restoration) {
	paths := make([]string, 0)
	for path, _ := range rest.destinations {
		paths = append(paths, path)
	}
	// To avoid potentially exhausting the open file limit, write to up to 500 files at a time.
	// This will do multiple downloads but this is only likely to happen with the 0 byte file or a small file
	for i := 0; i < len(paths); i += 500 {
		chunk := paths[i:min(len(rest.destinations), i+500)]
		handles := make([]*os.File, 0)
		writers := make([]io.Writer, 0)
		for _, path := range chunk {
			dir := filepath.Dir(path)
			mode := rest.destinations[path].permissions

			// https://stackoverflow.com/a/31151508/2277831
			dirMode := mode               // start with perms of the file
			dirMode |= (mode >> 2) & 0111 // for group and other, allow execute (dir read) if they can read
			dirMode |= 0700               // we must have full access no matter what, otherwise this recursive mkdir won't work in the first place

			log.Println("mkdir", dir, "with original", mode, "overridden to", dirMode)
			err := os.MkdirAll(dir, dirMode)
			if err != nil {
				panic(err)
			}

			log.Println("open", path, "for write")
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
			if err != nil {
				panic(err)
			}
			handles = append(handles, f)
			writers = append(writers, f)
		}

		out := io.MultiWriter(writers...)

		hs := utils.NewSHA256HasherSizer()
		out = io.MultiWriter(out, &hs)

		var src io.Reader
		if rest.nominatedSource == nil {
			log.Println("Fetching from storage")
			src = CatEz(rest.hash)
		} else {
			log.Println("Reading locally, from", *rest.nominatedSource)
			f, err := os.Open(*rest.nominatedSource)
			if err != nil {
				panic(err)
			}
			defer f.Close()
			src = f
		}
		utils.Copy(out, src)
		log.Println("Expecting size and hash:", rest.size, hex.EncodeToString(rest.hash))
		hash, size := hs.HashAndSize()
		log.Println("Got size and hash:", size, hex.EncodeToString(hash))
		if size != rest.size || !bytes.Equal(hash, rest.hash) {
			panic("wrong")
		}
		log.Println("Success")

		for _, f := range handles {
			f.Close()
		}
	}
}

func statSources(plan map[[32]byte]*Restoration) {
	// it's impossible for one path to appear as a source in more than 1 restoration
	// > this is because the files table has a partial unique index on path where end is null
	// therefore, no caching is needed we can just stat them all in order
	// but perhaps, for disk locality, let's do them in lexicographic order
	destinations := make(map[string][32]byte)
	sources := make(map[string][32]byte)
	paths := make([]string, 0)
	sourceVerified := make(map[[32]byte]struct{})
	for hash, rest := range plan {
		for _, item := range rest.destinations {
			destinations[item.destPath] = hash
		}
		for path, _ := range rest.sourcesOnDisk {
			sources[path] = hash
			paths = append(paths, path)
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		_, iDest := destinations[paths[i]]
		_, jDest := destinations[paths[j]]
		if iDest && !jDest {
			return true
		}
		if jDest && !iDest {
			return false
		}
		return paths[i] < paths[j]
	})
	log.Println(paths)
	for _, path := range paths {
		key := sources[path]
		if _, ok := sourceVerified[key]; ok {
			if _, ok := destinations[path]; !ok {
				//log.Println("Skipping stat of", path, "since we already have a verified source and this is not a destination")
				continue
			}
		}
		restoration := plan[key]
		stat, err := os.Stat(path)
		if err == nil && utils.NormalFile(stat) && stat.Size() == restoration.size && stat.ModTime().Unix() == restoration.sourcesOnDisk[path] {
			sourceVerified[key] = struct{}{}
			tmp := path                        // CURSED: &path results in the same address the whole way through
			restoration.nominatedSource = &tmp // CURSED: &path results in the same address the whole way through
			if _, ok := restoration.destinations[path]; ok {
				// regardless of fsModified, this path has the contents we want it to, since the hash matches
				// TODO: leave it as a destination if the fsModified time is wrong? idk probably not
				log.Println("Therefore", path, "is DONE")
				// okay so basically this file is #done
				delete(restoration.destinations, path)
			}
			continue
		} else {
			// don't crash lol!
			// this just means that the user has deleted/edited a file after backing it up, and we didn't know until right this moment
			if err != nil {
				log.Println(path, "no longer exists, cannot use")
			} else {
				log.Println(path, "exists but has been modified")
			}
			delete(restoration.sourcesOnDisk, path)
		}
	}
	// at this point, there is no overlap between sourcesOndisk and destinations for any restoration
	for _, hash := range completedRestorationDestinations(plan) {
		delete(plan, hash)
	}
}

func completedRestorationDestinations(plan map[[32]byte]*Restoration) [][32]byte {
	ret := make([][32]byte, 0)
	for k, v := range plan {
		if len(v.destinations) == 0 {
			ret = append(ret, k)
		}
	}
	return ret
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
				var path string
				var fsModified int64
				err = rows.Scan(&path, &fsModified)
				if err != nil {
					panic(err)
				}
				rest.sourcesOnDisk[path] = fsModified
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
