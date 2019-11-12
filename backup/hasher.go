package backup

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"log"
	"os"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func hasherThread() {
	defer wg.Done()
	for hashPlan := range hasherCh {
		hashOneFile(hashPlan)
	}
	log.Println("hasherCh closed")
}

func fileHasKnownData(tx *sql.Tx, path string, info os.FileInfo, hash []byte) {
	// important to use the same "now" for both of these queries, so that the file's history is presented without "gaps" (that would be present if we called time.Now() twice in a row)
	_, err := tx.Exec("UPDATE files SET end = ? WHERE path = ? AND end IS NULL", now, path)
	if err != nil {
		panic(err)
	}
	_, err = tx.Exec("INSERT INTO files (path, hash, start, fs_modified, permissions) VALUES (?, ?, ?, ?, ?)", path, hash, now, info.ModTime().Unix(), info.Mode()&os.ModePerm)
	if err != nil {
		panic(err)
	}
}

func hashOneFile(plan HashPlan) {
	path := plan.path
	info := plan.info
	expectedHash := plan.expectedHash
	// now, it's time to hash the file to see if it needs to be backed up or if we've already got it
	log.Println("Beginning read for sha256 calc:", path)

	hash, size, err := hashAFile(path)
	if err != nil {
		for i := 0; i < 100; i++ {
			log.Println("not going to back up because i literally couldnt read it", path, err)
		}
		return
	}
	if size != info.Size() {
		// wtf
		log.Println("WARNING: path is changing very rapidly, maybe a log file? will try to back up anyway lol ", path)
	}

	log.Println("sha256 is", hex.EncodeToString(hash), "and length is", size)

	if bytes.Equal(hash, expectedHash) {
		log.Println("This hash is unchanged from last time, even though last modified is changed...?")
		log.Println("Updating fs_modifed in db so next time I don't reread this for no reason lol")
		// this is VERY uncommon, so it is NOT worth maintaining a db WRITE transaction for it sadly
		_, err := db.DB.Exec("UPDATE files SET fs_modified = ?, permissions = ? WHERE path = ? AND end IS NULL", info.ModTime().Unix(), info.Mode()&os.ModePerm, path)
		if err != nil {
			panic(err)
		}
		return
	}

	if expectedHash == nil {
		log.Println("NEW FILE:", path)
	} else {
		log.Println(path, "hash has changed from", hex.EncodeToString(expectedHash), "to", hex.EncodeToString(hash))
	}

	r := func() *Planned {
		hashLateMapLock.Lock() // YES, the database query MUST be within this lock (to make sure that the Commit happens before this defer!)
		defer hashLateMapLock.Unlock()
		var dbHash []byte
		err = db.DB.QueryRow("SELECT hash FROM blob_entries WHERE hash = ?", hash).Scan(&dbHash)
		if err == nil {
			// tx CANNOT include previous query because ro to rw upgrade is fucky with sqlite multithreaded
			tx, err := db.DB.Begin()
			if err != nil {
				panic(err)
			}
			defer func() {
				log.Println("Hasher committing to database")
				err = tx.Commit()
				if err != nil {
					panic(err)
				}
				log.Println("Committed")
			}()
			// yeah so we already have this hash backed up, so the train stops here. we just need to add this to files table, and we done!
			fileHasKnownData(tx, path, info, hash)
			return nil // done, no need to upload
		}
		if err != db.ErrNoRows {
			panic(err) // unexpected error, maybe sql syntax error?
		}
		hashArr := utils.SliceToArr(hash)
		late, ok := hashLateMap[hashArr]
		if ok {
			if late == nil || len(late) == 0 {
				panic("i am dummy and didnt lock properly somewhere")
			}
			// another thread is *currently* uploading a file that is confirmed to have *this same hash*
			// let's just let them do that
			// but let em know that once they're done they should put OUR file into files too
			hashLateMap[hashArr] = append(late, plan.File)
			return nil
		}
		// wow! we are the FIRST! how exciting! how exciting!
		hashLateMap[hashArr] = []File{plan.File}
		// we **cannot** write to bucketerCh here, since we're still holding hashLateMapLock and that would cause deadlock
		// so we return and let the other thing deal with it lmao!
		return &Planned{plan.File, hash, &size, nil}
	}

	w := func() {
		plan := r()
		if plan != nil {
			bucketerCh <- *plan
		} else {
			wg.Done() // decrement the false increment from earlier
		}
	}

	lock, ok := fetchContentionMutex(size)
	wg.Add(1)
	if ok {
		go func() {
			lock.Lock()   // this will block for a LONG time - until unstakeClaim is called, which is once the file upload is complete
			lock.Unlock() // we aren't staking a claim since that's no longer sensical (the file of that length is already uploaded), so instantly unlock once we've confirmed the first claim is over
			w()
		}()
	} else {
		w()
	}
}
