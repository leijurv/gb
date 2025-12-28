package repack

import (
	"bytes"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/compression"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/paranoia"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

// Entry represents a hash and its decompressed data
type Entry struct {
	Hash []byte
	Data []byte
}

// blobEntry tracks metadata for each entry in a new blob
type blobEntry struct {
	hash                []byte
	key                 []byte
	offset              int64
	postCompressionSize int64
	preCompressionSize  int64
	compression         string
}

// newBlobData holds all data for a new blob being created
type newBlobData struct {
	blobID      []byte
	paddingKey  []byte
	totalSize   int64
	hashPostEnc []byte
	completeds  []storage_base.UploadedBlob
	entries     []blobEntry
}

type RepackMode int

const (
	BlobIDsFromStdin RepackMode = iota
	Deduplicate
	UpgradeEncryption
)

func blobIDsFromStdin() [][]byte {
	stdin, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(stdin), "\n")
	blobIDs := make([][]byte, 0)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if len(line) != 64 {
			panic("Line length is not 64: " + line)
		}
		blobID, err := hex.DecodeString(line)
		if err != nil {
			panic(err)
		}
		blobIDs = append(blobIDs, blobID)
	}
	return blobIDs
}

func Repack(label string, mode RepackMode) {
	// Step 1: Storage Selection
	stor, ok := storage.StorageSelect(label)
	if !ok {
		return
	}

	if mode == Deduplicate {
		log.Println("Skipping paranoia db check because you presumably have duplicated blob_entries that I'm here to fix")
	} else {
		log.Println("Running paranoia db check...")
		paranoia.DBParanoia()
		log.Println("Paranoia checks passed")
	}

	var blobIDs [][]byte
	switch mode {
	case BlobIDsFromStdin:
		blobIDs = blobIDsFromStdin()
	case Deduplicate:
		rows, err := db.DB.Query(`
			SELECT DISTINCT blob_id FROM blob_entries
			WHERE hash IN (SELECT hash FROM blob_entries GROUP BY hash HAVING COUNT(*) > 1)
		`)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var blobID []byte
			err := rows.Scan(&blobID)
			if err != nil {
				panic(err)
			}
			blobIDs = append(blobIDs, blobID)
		}
		if err := rows.Err(); err != nil {
			panic(err)
		}
		rows.Close()
	case UpgradeEncryption:
		rows, err := db.DB.Query(`
			SELECT blob_id FROM blob_entries GROUP BY blob_id HAVING COUNT(DISTINCT encryption_key) = 1 AND COUNT(*) > 1
		`)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var blobID []byte
			err := rows.Scan(&blobID)
			if err != nil {
				panic(err)
			}
			blobIDs = append(blobIDs, blobID)
		}
		if err := rows.Err(); err != nil {
			panic(err)
		}
		rows.Close()
	}

	if len(blobIDs) == 0 {
		log.Println("No blob IDs provided")
		return
	}
	seenBlobIDs := make(map[[32]byte]bool)
	for _, blobID := range blobIDs {
		blobIDArr := utils.SliceToArr(blobID)
		if seenBlobIDs[blobIDArr] {
			panic("Duplicate blob ID in stdin: " + hex.EncodeToString(blobID))
		}
		seenBlobIDs[blobIDArr] = true
	}
	log.Println("Processing", len(blobIDs), "blobs")

	// Step 5: Verify Size Consistency and Global Uniqueness
	// Within each blob, either all entries >= MinBlobSize (skip) or all < MinBlobSize (use)
	// Also verify that any duplicate hashes are all within seenBlobIDs
	log.Println("Verifying size consistency and filtering blobs...")
	minBlobSize := config.Config().MinBlobSize
	blobsToProcess := make([][]byte, 0)
	hashDedupe := make(map[[32]byte]struct{}) // tracks hashes we've "claimed" (either large skipped or will process)
	blobsToDelete := make([][]byte, 0)        // large blobs that are duplicates and should just be deleted
	for _, blobID := range blobIDs {
		rows, err := db.DB.Query(`
			SELECT blob_entries.hash, sizes.size FROM blob_entries
			INNER JOIN sizes ON blob_entries.hash = sizes.hash
			WHERE blob_id = ?
		`, blobID)
		if err != nil {
			panic(err)
		}
		var hashes [][]byte
		var hasLarge bool
		for rows.Next() {
			var hash []byte
			var size int64
			err := rows.Scan(&hash, &size)
			if err != nil {
				panic(err)
			}
			hashes = append(hashes, hash)
			if size >= minBlobSize {
				hasLarge = true
			}
		}
		if err := rows.Err(); err != nil {
			panic(err)
		}
		rows.Close()

		// Check global uniqueness: for each hash, all blobs containing it must be in seenBlobIDs
		for _, hash := range hashes {
			rows, err := db.DB.Query(`SELECT blob_id FROM blob_entries WHERE hash = ?`, hash)
			if err != nil {
				panic(err)
			}
			for rows.Next() {
				var otherBlobID []byte
				err := rows.Scan(&otherBlobID)
				if err != nil {
					panic(err)
				}
				if !seenBlobIDs[utils.SliceToArr(otherBlobID)] {
					rows.Close()
					panic("Hash " + hex.EncodeToString(hash) + " in blob " + hex.EncodeToString(blobID) +
						" also appears in blob " + hex.EncodeToString(otherBlobID) + " which is not being repacked")
				}
			}
			if err := rows.Err(); err != nil {
				panic(err)
			}
			rows.Close()
		}

		if hasLarge {
			// Skipping this blob because all entries are large
			if len(hashes) != 1 {
				panic("Blob " + hex.EncodeToString(blobID) + " has multiple large entries - not supported. repack will respect your MinBlobSize config; increase it accordingly?")
			}
			hashArr := utils.SliceToArr(hashes[0])
			if _, exists := hashDedupe[hashArr]; exists {
				// This hash was already claimed by another blob, so this blob is a duplicate
				log.Println("Blob", hex.EncodeToString(blobID), "is a duplicate large blob - will be deleted")
				blobsToDelete = append(blobsToDelete, blobID)
			} else {
				// Claim this hash
				hashDedupe[hashArr] = struct{}{}
				log.Println("Skipping blob", hex.EncodeToString(blobID), "- all entries are >= MinBlobSize")
			}
			continue
		}
		blobsToProcess = append(blobsToProcess, blobID)
	}

	if len(blobsToProcess) == 0 && len(blobsToDelete) == 0 {
		log.Println("No blobs need repacking or deleting")
		return
	}
	log.Println("Will repack", len(blobsToProcess), "blobs")
	if len(blobsToDelete) > 0 {
		log.Println("Will delete", len(blobsToDelete), "duplicate large blobs")
	}

	// Collect "before" statistics
	var beforeEntries int64
	var beforeUncompressed int64
	var beforeCompressed int64
	var beforeFinalSize int64
	for _, blobID := range append(blobsToProcess, blobsToDelete...) {
		var blobSize int64
		err := db.DB.QueryRow("SELECT size FROM blobs WHERE blob_id = ?", blobID).Scan(&blobSize)
		if err != nil {
			panic(err)
		}
		beforeFinalSize += blobSize

		rows, err := db.DB.Query(`
			SELECT sizes.size, blob_entries.final_size
			FROM blob_entries
			INNER JOIN sizes ON blob_entries.hash = sizes.hash
			WHERE blob_id = ?
		`, blobID)
		if err != nil {
			panic(err)
		}
		for rows.Next() {
			var uncompSize, compSize int64
			err := rows.Scan(&uncompSize, &compSize)
			if err != nil {
				panic(err)
			}
			beforeEntries++
			beforeUncompressed += uncompSize
			beforeCompressed += compSize
		}
		if err := rows.Err(); err != nil {
			panic(err)
		}
		rows.Close()
	}

	// Step 6: Download and Extract
	entryCh := make(chan Entry, 10)

	// Producer goroutine - downloads blobs and sends entries to channel
	go func() {
		defer close(entryCh)
		var totalDownloaded int64
		for i, blobID := range blobsToProcess {
			log.Println("Downloading blob", i+1, "of", len(blobsToProcess), ":", hex.EncodeToString(blobID))
			// Collect entries in a local slice to avoid blocking during download
			// (blocking on channel send can cause Backblaze to close the connection)
			var blobEntries []Entry
			callback := func(hash []byte, data []byte) {
				// Make copies since the data might be reused
				hashCopy := make([]byte, len(hash))
				copy(hashCopy, hash)
				dataCopy := make([]byte, len(data))
				copy(dataCopy, data)
				blobEntries = append(blobEntries, Entry{Hash: hashCopy, Data: dataCopy})
			}
			totalDownloaded += paranoia.BlobReaderParanoiaWithCallback(
				paranoia.DownloadEntireBlob(blobID, stor),
				blobID,
				stor,
				callback,
			)
			// Now that the blob is fully downloaded, send entries to channel (blocking is OK here)
			for _, entry := range blobEntries {
				entryCh <- entry
			}
			log.Println("Downloaded", i+1, "blobs out of", len(blobsToProcess), "-", utils.FormatCommas(totalDownloaded), "bytes total")
		}
	}()

	// Step 7 & 8: Bucketing and Upload
	// Accumulate entries and upload as new blobs
	storages := storage.GetAll()
	uploadService := backup.BeginDirectUpload(storages)

	var accumulated []Entry
	var accumulatedSize int64
	var newBlobs []newBlobData

	for entry := range entryCh {
		// Dedupe: skip entries whose hash we've already seen (from large blobs or earlier in this loop)
		hashArr := utils.SliceToArr(entry.Hash)
		if _, exists := hashDedupe[hashArr]; exists {
			log.Println("Skipping duplicate hash", hex.EncodeToString(entry.Hash[:8]))
			continue
		}
		hashDedupe[hashArr] = struct{}{}

		accumulated = append(accumulated, entry)
		accumulatedSize += int64(len(entry.Data))

		// Flush when we have enough data or too many entries
		if accumulatedSize >= minBlobSize || len(accumulated) > 5000 {
			log.Println("Flushing", len(accumulated), "entries,", utils.FormatCommas(accumulatedSize), "bytes")
			newBlob := uploadEntries(accumulated, uploadService)
			newBlobs = append(newBlobs, newBlob)
			accumulated = nil
			accumulatedSize = 0
		}
	}

	// Flush remaining entries
	if len(accumulated) > 0 {
		log.Println("Flushing remaining", len(accumulated), "entries,", utils.FormatCommas(accumulatedSize), "bytes")
		newBlob := uploadEntries(accumulated, uploadService)
		newBlobs = append(newBlobs, newBlob)
	}

	log.Println("Created", len(newBlobs), "new blobs")

	// Step 9: Database Transaction
	log.Println("Beginning database transaction...")
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	now := time.Now().Unix()

	for _, blob := range newBlobs {
		// Insert blob record
		_, err = tx.Exec("INSERT INTO blobs (blob_id, padding_key, size, final_hash) VALUES (?, ?, ?, ?)",
			blob.blobID, blob.paddingKey, blob.totalSize, blob.hashPostEnc)
		if err != nil {
			panic(err)
		}

		// Insert blob_storage records
		for _, completed := range blob.completeds {
			if !bytes.Equal(completed.BlobID, blob.blobID) {
				panic("sanity check: blob ID mismatch")
			}
			_, err = tx.Exec("INSERT INTO blob_storage (blob_id, storage_id, path, checksum, timestamp) VALUES (?, ?, ?, ?, ?)",
				blob.blobID, completed.StorageID, completed.Path, completed.Checksum, now)
			if err != nil {
				panic(err)
			}
		}

		// Insert blob_entries records
		for _, entry := range blob.entries {
			_, err = tx.Exec("INSERT INTO blob_entries (hash, blob_id, encryption_key, final_size, offset, compression_alg) VALUES (?, ?, ?, ?, ?, ?)",
				entry.hash, blob.blobID, entry.key, entry.postCompressionSize, entry.offset, entry.compression)
			if err != nil {
				panic(err)
			}
		}
	}

	// Delete old blob data (must delete in correct order due to foreign keys)
	// This includes both repacked blobs and duplicate large blobs
	allBlobsToDelete := append(blobsToProcess, blobsToDelete...)
	log.Println("Deleting", len(allBlobsToDelete), "old blob records (", len(blobsToProcess), "repacked +", len(blobsToDelete), "duplicate large)...")
	for _, blobID := range allBlobsToDelete {
		// Delete blob_entries first (foreign key to blobs)
		_, err = tx.Exec("DELETE FROM blob_entries WHERE blob_id = ?", blobID)
		if err != nil {
			panic(err)
		}
		// Delete blob_storage (foreign key to blobs)
		_, err = tx.Exec("DELETE FROM blob_storage WHERE blob_id = ?", blobID)
		if err != nil {
			panic(err)
		}
		// Delete blobs
		_, err = tx.Exec("DELETE FROM blobs WHERE blob_id = ?", blobID)
		if err != nil {
			panic(err)
		}
	}
	log.Println("Deleted", len(allBlobsToDelete), "old blob records")

	// Run DB paranoia on the transaction before committing
	log.Println("Running DB paranoia on transaction...")
	paranoia.DBParanoiaTx(tx)

	log.Println("Committing transaction...")
	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	log.Println("Repack complete!")
	log.Println("Old blob files remain in storage - run `gb paranoia storage --delete-unknown-files` to clean them up.")

	blobCh := make(chan newBlobData, len(newBlobs))
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			for blob := range blobCh {
				paranoia.BlobReaderParanoia(paranoia.DownloadEntireBlob(blob.blobID, stor), blob.blobID, stor)
			}
			wg.Done()
		}()
	}

	for _, blob := range newBlobs {
		blobCh <- blob
	}
	close(blobCh)
	wg.Wait()

	// Backup the database itself
	backup.BackupDB()

	// Compute "after" statistics from newBlobs
	var afterEntries int64
	var afterUncompressed int64
	var afterCompressed int64
	var afterFinalSize int64
	for _, blob := range newBlobs {
		afterFinalSize += blob.totalSize
		for _, entry := range blob.entries {
			afterEntries++
			afterUncompressed += entry.preCompressionSize
			afterCompressed += entry.postCompressionSize
		}
	}

	// Print summary
	log.Println()
	log.Printf("Before: %d blobs, %d entries, %s uncompressed, %s compressed, %s final size with padding",
		len(blobsToProcess), beforeEntries,
		utils.FormatCommas(beforeUncompressed),
		utils.FormatCommas(beforeCompressed),
		utils.FormatCommas(beforeFinalSize))
	log.Printf("After: %d blobs, %d entries, %s uncompressed, %s compressed, %s final size with padding",
		len(newBlobs), afterEntries,
		utils.FormatCommas(afterUncompressed),
		utils.FormatCommas(afterCompressed),
		utils.FormatCommas(afterFinalSize))
	log.Println()

	// Print all new blob IDs
	log.Println("New blob IDs:")
	for _, blob := range newBlobs {
		log.Println(strings.ToUpper(hex.EncodeToString(blob.blobID)))
	}
}

// uploadEntries creates a new blob from the given entries and uploads it
func uploadEntries(entries []Entry, uploadService backup.UploadService) newBlobData {
	blobID := crypto.RandBytes(32)
	rawServOut := uploadService.Begin(blobID)

	postEncInfo := utils.NewSHA256HasherSizer()
	postEncOut := io.MultiWriter(rawServOut, &postEncInfo)

	blobEntries := make([]blobEntry, 0, len(entries))

	for _, entry := range entries {
		startOffset := postEncInfo.Size()

		// Look up a file path to determine compression
		var path string
		err := db.DB.QueryRow(`
			SELECT path FROM files WHERE hash = ?
			ORDER BY (
				path LIKE "%.jpg" COLLATE NOCASE OR
				path LIKE "%.jpeg" COLLATE NOCASE -- A given hash can appear in multiple places. I want lepton to compress all jpgs, even if they appeared as something else at some point. Therefore, yes this is weird, but it's just an "order by" to reduce arbitrariness and put JPGs first
			) DESC
			LIMIT 1
		`, entry.Hash).Scan(&path)
		if err != nil {
			panic(err)
		}

		// Encrypt
		encryptedOut, key := crypto.EncryptBlob(postEncOut, startOffset)

		// Compress with optimal algorithm based on file path
		verify := utils.NewSHA256HasherSizer()
		compAlg := compression.Compress(
			compression.SelectCompressionForPath(path),
			encryptedOut,
			io.TeeReader(bytes.NewReader(entry.Data), &verify),
			&verify,
		)

		realHash, realSize := verify.HashAndSize()
		if !bytes.Equal(realHash, entry.Hash) {
			panic("hash mismatch during recompression!")
		}

		length := postEncInfo.Size() - startOffset
		log.Println("Entry", hex.EncodeToString(entry.Hash[:8]), "size", utils.FormatCommas(realSize), "->", utils.FormatCommas(length), "compression:", compAlg, "from:", path)

		blobEntries = append(blobEntries, blobEntry{
			hash:                entry.Hash,
			key:                 key,
			offset:              startOffset,
			preCompressionSize:  realSize,
			postCompressionSize: length,
			compression:         compAlg,
		})
	}

	// Add padding
	paddingOffset := postEncInfo.Size()
	paddingOut, paddingKey := crypto.EncryptBlob(postEncOut, paddingOffset)
	_, err := paddingOut.Write(make([]byte, backup.SamplePaddingLength(paddingOffset)))
	if err != nil {
		panic(err)
	}

	hashPostEnc, sizePostEnc := postEncInfo.HashAndSize()
	log.Println("Blob", hex.EncodeToString(blobID[:8]), "total size:", utils.FormatCommas(sizePostEnc))

	completeds := uploadService.End(hashPostEnc, sizePostEnc)
	log.Println("Blob uploaded to", len(completeds), "storages")

	return newBlobData{
		blobID:      blobID,
		paddingKey:  paddingKey,
		totalSize:   sizePostEnc,
		hashPostEnc: hashPostEnc,
		completeds:  completeds,
		entries:     blobEntries,
	}
}
