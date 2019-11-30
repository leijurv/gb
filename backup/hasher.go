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
	log.Println("Hasher thread exiting")
}

func fileHasKnownData(tx *sql.Tx, path string, info os.FileInfo, hash []byte) {
	// important to use the same "now" for both of these queries, so that the file's history is presented without "gaps" (that could be present if we called time.Now() twice in a row)
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
		// thought about this, and well
		// honestly I'd rather have the whole thing throw a big error when a file fails to be backed up
		// insetad of just continuing...
		panic(err)
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

	bucketWithKnownHash := func() *Planned {
		hashLateMapLock.Lock() // YES, the database query MUST be within this lock (to make sure that the Commit happens before this defer!)
		defer hashLateMapLock.Unlock()
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
		var dbHash []byte
		err = tx.QueryRow("SELECT hash FROM blob_entries WHERE hash = ?", hash).Scan(&dbHash)
		if err == nil {
			// yeah so we already have this hash backed up, so the train stops here. we just need to add this to files table, and we're done!
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
		// even though we want to, we **cannot** write to bucketerCh here, since we're still holding hashLateMapLock and that would cause deadlock
		// so we return and let the caller do it it lmao!
		return &Planned{plan.File, hash, &size, nil}
	}

	// split this up into two functions so that as above ^, we write the result after the defer unlock
	nextStepWrapper := func() {
		plan := bucketWithKnownHash()
		if plan != nil {
			bucketerCh <- *plan
		} else {
			// waitgroup should only be incremented for a real write to bucketerCh
			// so decrement if we aren't actually going to do that now
			wg.Done()
		}
	}

	// given that this can start a new goroutine and block on a size claim, we should add to the in progress wait group now, so that we don't forget about it
	// > this wouldn't be necessary if we always blockingly wrote to bucketerCh (i.e. called nextStepWrapper directly, not through a new goroutine)
	// >> reason being that wg can only be completed once all hasher threads exit, which couldn't happen if one of said threads was blocking on a channel write
	wg.Add(1)
	// we also can't do this only in the case where there is a preexisting size claim in contention, because it's entirely possible (downright likely, even) that the staked size claim may have actually been the same hash as this file, resulting in us not actually needing to write anything to bucketerCh.

	lock, ok := fetchContentionMutex(size)
	if ok {
		go func() {
			lock.Lock()   // this will block for a LONG time – until unstakeClaim is called, which is once the file upload in the other thread is complete
			lock.Unlock() // we aren't staking a claim since that's no longer sensical (the file of that length is already uploaded), so instantly unlock once we've confirmed the first claim is over
			nextStepWrapper()
		}()
	} else {
		nextStepWrapper()
	}
}
