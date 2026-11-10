package paranoia

import (
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/share"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

type storageAndPath struct { // can be used as a map key
	storageID [32]byte
	path      string
}

// unknown files uploaded more recently than this are never deleted, because they could belong to a backup or repack that is
// running right now and hasn't committed them to the database yet (repack only commits once it's uploaded everything)
const unknownFileMinAgeForDeletionDays = 7

func StorageParanoia(deleteUnknownFiles bool) bool {
	expected := fetchAllExpected()
	actual := fetchAllActual()
	log.Println("Comparing expected against actual")
	anyErrors := false
	for k, v := range expected {
		realBlob, ok := actual[k]
		if !ok {
			log.Println("MISSING!!!")
			log.Println("Storage:", storage.GetByID(k.storageID[:]))
			log.Println("Path:", k.path)
			log.Println("Expected: ", v)
			anyErrors = true
			continue
		}
		if realBlob.Checksum != v.Checksum || realBlob.Size != v.Size || realBlob.Path != v.Path {
			log.Println("INCORRECT METADATA!!")
			log.Println("Storage:", storage.GetByID(k.storageID[:]))
			log.Println("Actual:", realBlob)
			log.Println("Expected: ", v)
			anyErrors = true
		}
	}

	unknownFiles := make([]storageAndPath, 0)
	deletableFiles := make([]storageAndPath, 0)
	var deletableBytes int64
	unknownCountPerStorage := make(map[[32]byte]int64)
	unknownBytesPerStorage := make(map[[32]byte]int64)
	for k, v := range actual {
		_, ok := expected[k] // already checked keys that exist in both maps, so this is just keys that aren't present in expected
		if !ok {
			// Skip share files - they're verified separately by verifyShareJSONs
			if strings.HasPrefix(k.path, "share/") {
				continue
			}
			log.Println("UNKNOWN / UNEXPECTED FILE!!")
			log.Println("Storage:", storage.GetByID(k.storageID[:]))
			log.Println("Info:", v)
			log.Println("Blob ID:", hex.EncodeToString(v.BlobID))
			log.Println("Size (bytes):", utils.FormatCommas(v.Size))
			unknownFiles = append(unknownFiles, k)
			unknownCountPerStorage[k.storageID]++
			unknownBytesPerStorage[k.storageID] += v.Size
			if !v.Uploaded.IsZero() && time.Since(v.Uploaded) >= unknownFileMinAgeForDeletionDays*24*time.Hour {
				deletableFiles = append(deletableFiles, k)
				deletableBytes += v.Size
			}
		}
	}

	if len(unknownFiles) > 0 {
		log.Println("Summary of unknown / unexpected files per storage:")
		var totalBytes int64
		for _, stor := range storage.GetAll() {
			id := utils.SliceToArr(stor.GetID())
			count, ok := unknownCountPerStorage[id]
			if !ok {
				continue
			}
			bytes := unknownBytesPerStorage[id]
			totalBytes += bytes
			log.Printf("  %s: %s files, %s bytes (%s)", stor, utils.FormatCommas(count), utils.FormatCommas(bytes), utils.FormatBytes(bytes))
		}
		log.Printf("  Total: %s files, %s bytes (%s)", utils.FormatCommas(int64(len(unknownFiles))), utils.FormatCommas(totalBytes), utils.FormatBytes(totalBytes))
	}

	if deleteUnknownFiles && len(deletableFiles) < len(unknownFiles) {
		log.Printf("NOT deleting %d of those unknown files, because they were uploaded less than %d days ago (or the storage didn't say when). They could belong to a backup or repack that is still running. Run this again once they're older.", len(unknownFiles)-len(deletableFiles), unknownFileMinAgeForDeletionDays)
	}
	if deleteUnknownFiles && len(deletableFiles) > 0 {
		log.Printf("Are you sure you want to delete the %d unknown files that are older than %d days, totaling %d bytes? Type 'yes' to continue: ", len(deletableFiles), unknownFileMinAgeForDeletionDays, deletableBytes)
		var response string
		_, err := fmt.Scanln(&response)
		if err != nil || response != "yes" {
			log.Println("Deletion cancelled")
			return false
		}

		log.Println("Deleting", len(deletableFiles), "unknown files...")

		for _, k := range deletableFiles {
			stor := storage.GetByID(k.storageID[:])
			stor.DeleteBlob(k.path)
		}

		log.Printf("Deletion complete: %d files deleted", len(deletableFiles))
	}
	if anyErrors {
		panic("Storage paranoia found errors (see above)")
	}

	// Verify share JSONs
	if !verifyShareJSONs() {
		anyErrors = true
	}
	if len(unknownFiles) > 0 {
		log.Println("Storage paranoia found unknown files (see above)")
		return false
	}

	if anyErrors {
		panic("Storage paranoia found errors (see above)")
	}

	log.Println("Done")
	return true
}

func verifyShareJSONs() bool {
	log.Println("Verifying share JSONs...")
	allOk := true

	for _, stor := range storage.GetAll() {
		expected := share.ExpectedShareJSONs(stor)

		// Build map of expected files by derived filename (filename is HMAC of password)
		expectedByFilename := make(map[string]share.ExpectedShareFile)
		for _, e := range expected {
			// Extract derived filename from path "share/<derived_filename>"
			filename := strings.TrimPrefix(e.Path, "share/")
			expectedByFilename[filename] = e
		}

		// Get actual share files from storage
		actualFiles := stor.ListPrefix("share/")
		actualByFilename := make(map[string]storage_base.ListedFile)
		for _, f := range actualFiles {
			// Name is the filename without prefix, e.g., "abc123def456"
			actualByFilename[f.Name] = f
		}

		// Check expected files exist and have correct metadata
		for filename, exp := range expectedByFilename {
			actual, ok := actualByFilename[filename]
			if !ok {
				log.Printf("MISSING SHARE JSON: %s in %s", exp.Path, stor)
				allOk = false
				continue
			}

			// Get checksum from storage
			checksum, size := stor.Metadata(actual.Path)
			if size != exp.Size {
				log.Printf("SHARE JSON SIZE MISMATCH: %s in %s (expected %d, got %d)", exp.Path, stor, exp.Size, size)
				allOk = false
			}
			if checksum != "" && checksum != exp.Checksum {
				log.Printf("SHARE JSON CHECKSUM MISMATCH: %s in %s (expected %s, got %s)", exp.Path, stor, exp.Checksum, checksum)
				allOk = false
			}
		}

		// Check for unexpected share files
		for filename, actual := range actualByFilename {
			if _, ok := expectedByFilename[filename]; !ok {
				log.Printf("UNEXPECTED SHARE JSON: %s in %s", actual.Path, stor)
				allOk = false
			}
		}
	}

	if allOk {
		log.Println("Share JSONs verified OK")
	}
	return allOk
}

func fetchAllActual() map[storageAndPath]storage_base.UploadedBlob {
	result := make(map[storageAndPath]storage_base.UploadedBlob)
	for _, s := range storage.GetAll() {
		id := utils.SliceToArr(s.GetID())
		for _, file := range s.ListBlobs() {
			result[storageAndPath{id, file.Path}] = file
		}
	}
	return result
}

func fetchAllExpected() map[storageAndPath]storage_base.UploadedBlob {
	rows, err := db.DB.Query(`
			SELECT
				blob_storage.path,
				blob_storage.checksum,
				blobs.size,
				blobs.blob_id,
				blob_storage.storage_id
			FROM blob_storage
				INNER JOIN blobs ON blob_storage.blob_id = blobs.blob_id`)
	db.Must(err)
	defer rows.Close()
	result := make(map[storageAndPath]storage_base.UploadedBlob)
	for rows.Next() {
		var path string
		var checksum string
		var size int64
		var blobID []byte
		var storageID []byte
		db.Must(rows.Scan(&path, &checksum, &size, &blobID, &storageID))
		// the database has a unique constraint on storageID and path so this is safe
		result[storageAndPath{utils.SliceToArr(storageID), path}] = storage_base.UploadedBlob{
			Path:     path,
			Checksum: checksum,
			Size:     size,
			BlobID:   blobID,
		}
	}
	db.Must(rows.Err())
	return result
}
