package download

import (
	"database/sql"
	"io"

	"github.com/leijurv/gb/compression"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/utils"
)

func CatReadCloser(hash []byte, tx *sql.Tx) io.ReadCloser {
	var blobID []byte
	var offset int64
	var length int64
	var compressionAlg string
	var key []byte
	var path string
	var storageID []byte
	var kind string
	var identifier string
	var rootPath string
	err := tx.QueryRow(`
			SELECT
				blob_entries.blob_id,
				blob_entries.offset, 
				blob_entries.final_size,
				blob_entries.compression_alg,
				blobs.encryption_key,
				blob_storage.path,
				storage.storage_id,
				storage.type,
				storage.identifier,
				storage.root_path
			FROM blob_entries
				INNER JOIN blobs ON blobs.blob_id = blob_entries.blob_id
				INNER JOIN blob_storage ON blob_storage.blob_id = blobs.blob_id
				INNER JOIN storage ON storage.storage_id = blob_storage.storage_id
			WHERE blob_entries.hash = ?


			ORDER BY storage.readable_label /* completely arbitrary. if there are many matching rows, just consistently pick it based on storage label. */
		`, hash).Scan(&blobID, &offset, &length, &compressionAlg, &key, &path, &storageID, &kind, &identifier, &rootPath)
	if err != nil {
		panic(err)
	}
	storageR := storage.StorageDataToStorage(storageID, kind, identifier, rootPath)
	reader := utils.ReadCloserToReader(storageR.DownloadSection(path, offset, length))
	decrypted := crypto.DecryptBlobEntry(reader, offset, key)
	return compression.ByAlgName(compressionAlg).Decompress(decrypted)
}

func Cat(hash []byte, sql *sql.Tx) io.Reader {
	return utils.ReadCloserToReader(CatReadCloser(hash, sql))
}

func CatEz(hash []byte) io.Reader {
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

	return Cat(hash, tx)
}
