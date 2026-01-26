package backup

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/leijurv/gb/compression"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func (s *BackupSession) uploaderThread(service UploadService) {
	for plan := range s.uploaderCh {
		s.executeBlobUploadPlan(plan, service)
	}
}

func (s *BackupSession) executeBlobUploadPlan(plan BlobPlan, serv UploadService) {
	log.Println("Executing upload plan", plan)
	for _, f := range plan {
		defer s.filesWg.Done() // there's a wg.Add(1) for each and every entry in the plan
		if f.stakedClaim != nil {
			sz := *f.stakedClaim
			defer s.releaseAndUnstakeSizeClaim(sz)
			// NO MATTER HOW this function exits, the claim is over
			// whether it's successful upload, IO error, or size mismatch
		}
	}

	blobID := crypto.RandBytes(32)
	rawServOut := serv.Begin(blobID)
	txCommitted := false
	defer func() {
		if r := recover(); r != nil {
			if !txCommitted {
				log.Println("Upload aborted, cleaning up blobs...")
				serv.Cancel()
			}
			panic(r)
		}
	}()

	postEncInfo := utils.NewSHA256HasherSizer()
	postEncOut := io.MultiWriter(rawServOut, &postEncInfo)

	s.addUploadStats(&postEncInfo)

	type blobEntry struct {
		originalPlan        Planned
		hash                []byte
		key                 []byte
		offset              int64
		postCompressionSize int64
		preCompressionSize  int64
		compression         string
	}
	entries := make([]blobEntry, 0)

	for _, planned := range plan {
		log.Println("Adding", planned.File)
		startOffset := postEncInfo.Size()
		verify := utils.NewSHA256HasherSizer()

		f, err := s.FileOpener.Open(planned.path)
		if err != nil {
			log.Println("I can no longer read from it to back it up???", err, planned.path)
			// call this here since we will NOT be adding an entry to entries, so it won't be called later on lol
			func() {
				s.hashLateMapLock.Lock()
				defer s.hashLateMapLock.Unlock()
				s.uploadFailure(planned)
			}()
			continue
		}
		s.addCurrentlyUploading(planned.path, &verify)
		encryptedOut, key := crypto.EncryptBlob(postEncOut, startOffset)
		compAlg := compression.Compress(compression.SelectCompressionForPath(planned.path), encryptedOut, io.TeeReader(f, &verify), &verify)
		s.finishedUploading(planned.path)
		f.Close()
		realHash, realSize := verify.HashAndSize()
		if len(planned.hash) > 0 && !bytes.Equal(realHash, planned.hash) {
			log.Println("File copied successfully, but hash was", hex.EncodeToString(realHash), "when we expected", hex.EncodeToString(planned.hash))
		}
		length := postEncInfo.Size() - startOffset
		if compAlg == "" {
			log.Println("File length was", utils.FormatCommas(realSize), "and was not compressed")
		} else {
			log.Println("File length was", utils.FormatCommas(realSize), "but was compressed to", utils.FormatCommas(length), "change of", utils.FormatCommas(length-realSize))
		}
		entries = append(entries, blobEntry{
			originalPlan:        planned,
			hash:                realHash,
			key:                 key,
			offset:              startOffset,
			preCompressionSize:  realSize,
			postCompressionSize: length,
			compression:         compAlg,
		})
	}
	if len(entries) == 0 {
		log.Println("Exiting because nothing wrote; cancelling upload")
		serv.Cancel()
		return
	}
	paddingOffset := postEncInfo.Size()
	paddingOut, paddingKey := crypto.EncryptBlob(postEncOut, paddingOffset)
	_, err := paddingOut.Write(make([]byte, SamplePaddingLength(paddingOffset))) // padding with zeros is fine, it'll be indistinguishable from real data after AES
	if err != nil {
		panic(err)
	}
	hashPostEnc, sizePostEnc := postEncInfo.HashAndSize()
	totalSize := sizePostEnc
	log.Println("All bytes written")
	completeds := serv.End(hashPostEnc, totalSize)
	log.Println("All bytes flushed")

	s.hashLateMapLock.Lock() // YES, the database query MUST be within this lock (to make sure that the Commit happens before this defer!)
	defer s.hashLateMapLock.Unlock()
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	// **obviously** all this needs to be in a tx
	_, err = tx.Exec("INSERT INTO blobs (blob_id, padding_key, size, final_hash) VALUES (?, ?, ?, ?)", blobID, paddingKey, totalSize, hashPostEnc)
	if err != nil {
		panic(err)
	}
	now := time.Now().Unix()
	for _, completed := range completeds {
		if !bytes.Equal(completed.BlobID, blobID) {
			log.Println(completed.Path)
			log.Println(completed.BlobID)
			log.Println(blobID)
			panic("sanity check")
		}
		_, err = tx.Exec("INSERT INTO blob_storage (blob_id, storage_id, path, checksum, timestamp) VALUES (?, ?, ?, ?, ?)", blobID, completed.StorageID, completed.Path, completed.Checksum, now)
		if err != nil {
			panic(err)
		}
	}
	for _, entry := range entries {
		// do this first (before fileHasKnownData) because of that pesky foreign key
		_, err = tx.Exec("INSERT OR IGNORE INTO sizes (hash, size) VALUES (?, ?)", entry.hash, entry.preCompressionSize)
		if err != nil {
			panic(err)
		}
		if bytes.Equal(entry.originalPlan.hash, entry.hash) {
			// fetch ALL the files that hashed to this hash
			files := s.hashLateMap[utils.SliceToArr(entry.hash)]
			// time to add ALL of them to the files table, now that this hash is backed up :D
			if files[0] != entry.originalPlan.File {
				panic("something is profoundly broken")
			}
			for _, file := range files {
				s.fileHasKnownData(tx, file.path, file.info, entry.hash)
			}
			delete(s.hashLateMap, utils.SliceToArr(entry.hash))
		} else {
			// a dummy stupid file changed from underneath us, now we need to clean up that mess :(
			// it is possible that other files were relying on this thread to complete the upload for this hash
			// but we let them down :(
			// the file we uploaded was not of the hash we wanted
			s.uploadFailure(entry.originalPlan)

			// even if the contents of the file were not as expected, they are still the contents of the file, and we should still back up this file since we just uploaded it and it is a file lol
			// note: even though the file has demonstrably changed since our original os.Stat, we should NOT stat it again (to get an updated permissions / last modified time). reason: if we stat-then-hash, the last modified time will be less than or equal to the "correct" time for that hash. other way around, not so much. we don't want to end up in a scenario where the next time we scan this directory, we don't rehash this file because we incorrectly stored a last modified time that's potentially newer than the data we actually read and backed up!
			s.fileHasKnownData(tx, entry.originalPlan.path, entry.originalPlan.info, entry.hash)
		}
		// and either way, make a note of what hash is stored in this blob at this location
		_, err = tx.Exec("INSERT INTO blob_entries (hash, blob_id, encryption_key, final_size, offset, compression_alg) VALUES (?, ?, ?, ?, ?, ?)", entry.hash, blobID, entry.key, entry.postCompressionSize, entry.offset, entry.compression)
		if err != nil {
			panic(err)
		}
	}
	log.Println("Uploader done with blob", plan)
	log.Println("Uploader committing to database")
	txCommitted = true // err on the side of caution - if tx.Commit returns an err, very likely it did not actually commit, but, it's possible! so don't delete the blob if there's ANY chance that the db is expecting this blob to exist.
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
	log.Println("Committed uploaded blob")
}

func (s *BackupSession) fileHasKnownData(tx *sql.Tx, path string, info os.FileInfo, hash []byte) {
	// important to use the same "now" for both of these queries, so that the file's history is presented without "gaps" (that could be present if we called time.Now() twice in a row)
	_, err := tx.Exec("UPDATE files SET end = ? WHERE path = ? AND end IS NULL", s.now, path)
	if err != nil {
		panic(err)
	}
	modTime := info.ModTime().Unix()
	if modTime < 0 {
		panic(fmt.Sprintf("Invalid modification time for %s: %d", path, modTime))
	}
	_, err = tx.Exec("INSERT INTO files (path, hash, start, fs_modified, permissions) VALUES (?, ?, ?, ?, ?)", path, hash, s.now, modTime, info.Mode()&os.ModePerm)
	if err != nil {
		panic(err)
	}
}

// this function should be called if the uploader thread was intended to upload a backup of a certain hash, but failed to do so, for any reason
func (s *BackupSession) uploadFailure(planned Planned) {
	plannedHash := planned.hash
	// NOTE: this can be called on file open error on a staked size claim plan, without a confirmed hash size
	// therefore, we cannot assume that we even HAVE an expected hash lmao
	if plannedHash == nil {
		return
	}
	expected := utils.SliceToArr(plannedHash)
	late := s.hashLateMap[expected]
	if late[0] != planned.File {
		panic("somehow something isn't being synchronized :(")
	}
	late = late[1:] // we failed :(
	if len(late) > 0 {
		s.hashLateMap[expected] = late
		// confirmed, another file was relying on this
		newSource := late[0] // enqueue THAT file to be uploaded
		s.filesWg.Add(1)
		go func() {
			// obviously, only write ONE of the other files we know to have this hash, not all
			s.bucketerCh <- Planned{newSource, plannedHash, planned.confirmedSize, nil}
		}() // we will upload the next file on the list with the same hash so they don't get left stranded (hashed, planned, but not actually uploaded)
	} else {
		delete(s.hashLateMap, expected) // important! otherwise the ok / len(late) > 0 check would panic lmao
	}
}
