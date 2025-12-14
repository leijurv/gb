package download

import (
	"database/sql"
	"io"

	"github.com/leijurv/gb/compression"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

type BlobEntryInfo struct {
	BlobID         []byte
	Offset         int64
	Length         int64
	CompressionAlg string
	Key            []byte
	StoragePath    string
}

func LookupBlobEntry(hash []byte, tx *sql.Tx, stor storage_base.Storage) BlobEntryInfo {
	var blobID []byte
	var offset int64
	var length int64
	var compressionAlg string
	var key []byte
	var path string

	err := tx.QueryRow(`
		SELECT
			blob_entries.blob_id,
			blob_entries.offset,
			blob_entries.final_size,
			blob_entries.compression_alg,
			blob_entries.encryption_key,
			blob_storage.path
		FROM blob_entries
			INNER JOIN blobs ON blobs.blob_id = blob_entries.blob_id
			INNER JOIN blob_storage ON blob_storage.blob_id = blobs.blob_id
		WHERE blob_entries.hash = ? AND blob_storage.storage_id = ?`,
		hash, stor.GetID()).Scan(&blobID, &offset, &length, &compressionAlg, &key, &path)
	if err != nil {
		panic(err)
	}

	return BlobEntryInfo{
		BlobID:         blobID,
		Offset:         offset,
		Length:         length,
		CompressionAlg: compressionAlg,
		Key:            key,
		StoragePath:    path,
	}
}

func CatReadCloser(hash []byte, tx *sql.Tx, stor storage_base.Storage) io.ReadCloser {
	info := LookupBlobEntry(hash, tx, stor)
	reader := utils.ReadCloserToReader(stor.DownloadSection(info.StoragePath, info.Offset, info.Length))
	decrypted := crypto.DecryptBlobEntry(reader, info.Offset, info.Key)
	return compression.ByAlgName(info.CompressionAlg).Decompress(decrypted)
}

func Cat(hash []byte, tx *sql.Tx, stor storage_base.Storage) io.Reader {
	return utils.ReadCloserToReader(CatReadCloser(hash, tx, stor))
}

func CatEz(hash []byte, stor storage_base.Storage) io.Reader {
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = tx.Commit() // this is ok since read-only
		if err != nil {
			panic(err)
		}
	}()

	return Cat(hash, tx, stor)
}
