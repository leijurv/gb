package backup

import (
	"bytes"
	"encoding/hex"
	"io"
	"log"
	"os"
	"time"

	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"

	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func uploaderThread() {
	storage := storage.GetAll()
	for plan := range uploaderCh {
		executeOrder66(plan, storage)
	}
}

type BlobEntry struct {
	originalPlan        Planned
	hash                []byte
	offset              int64
	postCompressionSize int64
	preCompressionSize  int64
	compression         *string
}

func executeOrder66(plan BlobPlan, storageDests []storage_base.Storage) {
	log.Println("Executing upload plan", plan)
	for _, f := range plan {
		defer wg.Done() // there's a wg.Add(1) for each entry in the plan
		if f.stakedClaim != nil {
			sz := *f.stakedClaim
			defer releaseAndUnstakeSizeClaim(sz)
			// NO MATTER HOW this function exits, the claim is over
			// whether it's successful upload, IO error, or size mismatch
		}
	}
	blobID := crypto.RandBytes(32)

	uploads := make([]storage_base.StorageUpload, 0)
	for _, storage := range storageDests {
		uploads = append(uploads, storage.BeginBlobUpload(blobID))
	}
	writers := make([]io.Writer, 0)
	for _, upload := range uploads {
		writers = append(writers, upload.Begin())
	}

	out := io.MultiWriter(writers...)

	postEncInfo := utils.NewSHA256HasherSizer()
	out = io.MultiWriter(out, &postEncInfo)

	var key []byte
	out, key = crypto.EncryptBlob(out)

	preEncInfo := utils.NewSHA256HasherSizer()
	out = io.MultiWriter(out, &preEncInfo)

	stats.Add(&preEncInfo)

	entries := make([]BlobEntry, 0)

	for _, planned := range plan {
		log.Println("Adding", planned.File)
		startOffset := preEncInfo.Size()
		verify := utils.NewSHA256HasherSizer()
		tmpOut := out // TODO compressor(out)
		f, err := os.Open(planned.path)
		if err != nil {
			log.Println("I can no longer read from it to back it up???", err, planned.path)
			// call this here since we will NOT be adding an entry to entries, so it won't be called later on lol
			uploadFailure(planned)
			continue
		}
		err = func() error {
			defer f.Close() // yeah its kinda paranoid but i prefer to always defer in a closure than put a Close/Unlock manually afterwards
			_, err := io.Copy(io.MultiWriter(tmpOut, &verify), f)
			return err
		}()
		realHash, realSize := verify.HashAndSize()
		if err != nil {
			// TODO perhaps there could be some optimization, like, if we wrote 0 bytes, then it's no different from if we failed to open the file
			// however, that's tricky because I can imagine some compression algorithms that will "compress" 0 bytes into more than 0
			// so idk

			// it's tricky what to do here tbh
			// sadly i think we need to abandon the upload entirely?

			// idk
			panic("lol idk what to do")
		}
		// not sure what the error should be regarding confirmed size vs staked claims, or if there even should be an error.....
		/*if realSize != planned.size {
			log.Println("File copied successfully, but bytes read was", realSize, "when we expected", planned.size)
		}*/
		if len(planned.hash) > 0 && !bytes.Equal(realHash, planned.hash) {
			log.Println("File copied successfully, but hash was", hex.EncodeToString(realHash), "when we expected", hex.EncodeToString(planned.hash))
		}
		end := preEncInfo.Size()
		length := end - startOffset
		log.Println("File length was", realSize, "but was compressed to", length)
		entries = append(entries, BlobEntry{
			originalPlan:        planned,
			hash:                realHash,
			offset:              startOffset,
			preCompressionSize:  realSize,
			postCompressionSize: length,
			compression:         nil,
		})
	}
	out.Write(make([]byte, samplePaddingLength(postEncInfo.Size()))) // padding with zeros is fine, it'll be indistinguishable from real data after AES
	log.Println("All bytes written")
	completeds := make([]storage_base.CompletedUpload, 0)
	for _, upload := range uploads {
		completeds = append(completeds, upload.End())
	}
	log.Println("All bytes flushed")

	hashPreEnc, sizePreEnc := preEncInfo.HashAndSize()
	hashPostEnc, sizePostEnc := postEncInfo.HashAndSize()
	if sizePreEnc != sizePostEnc {
		panic("what??")
	}
	totalSize := sizePreEnc

	hashLateMapLock.Lock() // YES, the database query MUST be within this lock (to make sure that the Commit happens before this defer!)
	defer hashLateMapLock.Unlock()
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		log.Println("Uploader committing to database")
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		log.Println("Committed")
	}()
	// **obviously** all this needs to be in a tx
	_, err = tx.Exec("INSERT INTO blobs (blob_id, encryption_key, size, hash_pre_enc, hash_post_enc) VALUES (?, ?, ?, ?, ?)", blobID, key, totalSize, hashPreEnc, hashPostEnc)
	if err != nil {
		panic(err)
	}
	now := time.Now().Unix()
	for i, completed := range completeds {
		_, err = tx.Exec("INSERT INTO blob_storage (blob_id, storage_id, path, checksum, timestamp) VALUES (?, ?, ?, ?, ?)", blobID, storageDests[i].GetID(), completed.Path, completed.Checksum, now)
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
			files := hashLateMap[utils.SliceToArr(entry.hash)]
			// time to add ALL of them to the files table, now that this hash is backed up :D
			if files[0] != entry.originalPlan.File {
				panic("something is profoundly broken")
			}
			for _, file := range files {
				fileHasKnownData(tx, file.path, file.info, entry.hash)
			}
		} else {
			// a dummy stupid file changed from underneath us, now we need to clean up that mess :(
			// it is possible that other files were relying on this thread to complete the upload for this hash
			// but we let them down :(
			// the file we uploaded was not of the hash we wanted
			uploadFailure(entry.originalPlan)

			// even if the contents of the file were not as expected, they are still the contents of the file, and we should still back up this file since we just uploaded it and it is a file lol
			// note: even though the file has demonstrably changed since our original os.Stat, we should NOT stat it again (to get an updated permissions / last modified time). reason: if we stat-then-hash, the last modified time will be less than or equal to the "correct" time for that hash. other way around, not so much. we don't want to end up in a scenario where the next time we scan this directory, we don't rehash this file because we incorrectly stored a last modified time that's potentially newer than the data we actually read and backed up!
			fileHasKnownData(tx, entry.originalPlan.path, entry.originalPlan.info, entry.hash)
		}
		// and either way, make a note of what hash is stored in this blob at this location
		_, err = tx.Exec("INSERT INTO blob_entries (hash, blob_id, final_size, offset, compression_alg) VALUES (?, ?, ?, ?, ?)", entry.hash, blobID, entry.postCompressionSize, entry.offset, entry.compression)
		if err != nil {
			panic(err)
		}
	}
	log.Println("Uploader done")
}

// this function should be called if the uploader thread was intended to upload a backup of a certain hash, but failed to do so, for any reason
func uploadFailure(planned Planned) {
	plannedHash := planned.hash
	// NOTE: this can be called on file open error on a staked size claim plan, without a confirmed hash size
	// therefore, we cannot assume that we even HAVE an expected hash lmao
	if plannedHash == nil {
		return
	}
	expected := utils.SliceToArr(plannedHash)
	late := hashLateMap[expected]
	if late[0] != planned.File {
		panic("somehow something isn't being synchronized :(")
	}
	late = late[1:]
	if len(late) > 0 {
		hashLateMap[expected] = late
		// confirmed, another file was relying on this
		wg.Add(1)
		go func() {
			bucketerCh <- Planned{late[0], plannedHash, planned.confirmedSize, nil}
		}() // we will upload the next file on the list with the same hash so they don't get left stranded (hashed, planned, but not actually uploaded)
	} else {
		delete(hashLateMap, expected) // important! otherwise the ok / len(late) > 0 check would panic lmao
	}
}
