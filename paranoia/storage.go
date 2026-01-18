package paranoia

import (
	"encoding/hex"
	"fmt"
	"log"
	"strings"

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
	var totalBytes int64
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
			totalBytes += v.Size
		}
	}

	if deleteUnknownFiles && len(unknownFiles) > 0 {
		log.Printf("Are you sure you want to delete those %d files totaling %d bytes? Type 'yes' to continue: ", len(unknownFiles), totalBytes)
		var response string
		_, err := fmt.Scanln(&response)
		if err != nil || response != "yes" {
			log.Println("Deletion cancelled")
			return false
		}

		log.Println("Deleting", len(unknownFiles), "unknown files...")

		for _, k := range unknownFiles {
			stor := storage.GetByID(k.storageID[:])
			stor.DeleteBlob(k.path)
		}

		log.Printf("Deletion complete: %d files deleted", len(unknownFiles))
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
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	result := make(map[storageAndPath]storage_base.UploadedBlob)
	for rows.Next() {
		var path string
		var checksum string
		var size int64
		var blobID []byte
		var storageID []byte
		err := rows.Scan(&path, &checksum, &size, &blobID, &storageID)
		if err != nil {
			panic(err)
		}
		// the database has a unique constraint on storageID and path so this is safe
		result[storageAndPath{utils.SliceToArr(storageID), path}] = storage_base.UploadedBlob{
			Path:     path,
			Checksum: checksum,
			Size:     size,
			BlobID:   blobID,
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return result
}
