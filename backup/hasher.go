package backup

import (
	"bytes"
	"encoding/hex"
	"log"
	"os"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func (s *BackupSession) hasherThread() {
	defer s.hasherWg.Done()
	for hashPlan := range s.hasherCh {
		s.hashOneFile(hashPlan)
	}
	log.Println("Hasher thread exiting")
}

func (s *BackupSession) hashOneFile(plan HashPlan) {
	path := plan.path
	info := plan.info
	expectedHash := plan.expectedHash
	// now, it's time to hash the file to see if it needs to be backed up or if we've already got it
	log.Println("Beginning read for sha256 calc:", path)

	hash, size, err := s.hashAFile(path)
	if err != nil {
		if config.Config().SkipHashFailures {
			log.Println("Skipping", path, "due to", err, "(maybe it was deleted?) because skip_hash_failures is true")
			return
		} else {
			log.Println(path, "couldn't be backed up due to", err, "and skip_hash_failures is false, so, panicking now")
			panic(err)
		}
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
		s.hashLateMapLock.Lock() // this lock ensures atomicity between the hashLateMap, and the database (blob_entries and files)
		defer s.hashLateMapLock.Unlock()
		tx, err := db.DB.Begin()
		if err != nil {
			panic(err)
		}
		defer tx.Rollback() // fileHasKnownData can panic
		var dbHash []byte
		err = tx.QueryRow("SELECT hash FROM blob_entries WHERE hash = ?", hash).Scan(&dbHash)
		if err == nil {
			// yeah so we already have this hash backed up, so the train stops here. we just need to add this to files table, and we're done!
			s.fileHasKnownData(tx, path, info, hash)
			if err := tx.Commit(); err != nil {
				panic(err)
			}
			return nil // done, no need to upload
		}
		if err != db.ErrNoRows {
			panic(err) // unexpected error, maybe sql syntax error?
		}
		hashArr := utils.SliceToArr(hash)
		late, ok := s.hashLateMap[hashArr]
		if ok {
			if late == nil || len(late) == 0 {
				panic("i am dummy and didnt lock properly somewhere")
			}
			// another thread is *currently* uploading a file that is confirmed to have *this same hash*
			// let's just let them do that
			// but let em know that once they're done they should put OUR file into files too
			s.hashLateMap[hashArr] = append(late, plan.File)
			return nil
		}
		// wow! we are the FIRST! how exciting! how exciting!
		s.hashLateMap[hashArr] = []File{plan.File}
		// even though we want to, we **cannot** write to bucketerCh here, since we're still holding hashLateMapLock and that would cause deadlock
		// so we return and let the caller do it it lmao!
		return &Planned{plan.File, hash, &size, nil}
	}

	// split this up into two functions so that as above ^, we write the result after the defer unlock
	nextStepWrapper := func() {
		plan := bucketWithKnownHash()
		if plan != nil {
			s.bucketerCh <- *plan
		} else {
			// waitgroup should only be incremented for a real write to bucketerCh
			// so decrement if we aren't actually going to do that now
			s.filesWg.Done()
		}
	}

	// Add to wait group now since the callback may be invoked later (after size claim is released).
	// We also can't do this only in the case where there is a preexisting size claim in contention,
	// because it's entirely possible (downright likely, even) that the staked size claim may have
	// actually been the same hash as this file, resulting in us not actually needing to write
	// anything to bucketerCh.
	s.filesWg.Add(1)

	// Register callback to be invoked when the size claim is released (or immediately if no claim).
	// This ensures FIFO ordering - callbacks are invoked in the order they were registered.
	s.registerSizeClaimCallback(size, nextStepWrapper)
}
