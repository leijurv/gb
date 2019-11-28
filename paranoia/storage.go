package paranoia

import (
	"log"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

type storageAndPath struct { // can be used as a map key
	storageID [32]byte
	path      string
}

func StorageParanoia() {
	expected := fetchAllExpected()
	actual := fetchAllActual()
	log.Println("Comparing expected against actual")
	for k, v := range expected {
		realBlob, ok := actual[k]
		if !ok {
			log.Println("MISSING!!!")
			log.Println("Storage:", storage.GetByID(k.storageID[:]))
			log.Println("Path:", k.path)
			log.Println("Expected: ", v)
			continue
		}
		if realBlob.Checksum != v.Checksum || realBlob.Size != v.Size || realBlob.Path != v.Path {
			log.Println("INCORRECT METADATA!!")
			log.Println("Storage:", storage.GetByID(k.storageID[:]))
			log.Println("Actual:", realBlob)
			log.Println("Expected: ", v)
			if config.Config().TryRepairS3ETag {
				log.Println("Attempting to repair ETag by recalculating from local data... (config setting TryRepairS3ETag is enabled)")
				handleIncorrectMetadata(realBlob, v, storage.GetByID(k.storageID[:]))
			}
		}
	}
	for k, v := range actual {
		_, ok := expected[k] // already checked keys that exist in both maps, so this is just keys that aren't present in expected
		if !ok {
			log.Println("UNKNOWN / UNEXPECTED FILE!!")
			log.Println("Storage:", storage.GetByID(k.storageID[:]))
			log.Println("Info:", v)
		}
	}
	log.Println("Done")
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
