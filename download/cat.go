package download

import (
	"bytes"
	"database/sql"
	"fmt"
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
	ExpectedSize   int64 // decompressed size from sizes table
}

// hashVerifyingReader wraps a reader and verifies SHA256 hash when the
// expected size is reached or EOF occurs.
type hashVerifyingReader struct {
	reader       io.ReadCloser
	hasher       utils.HasherSizer
	expectedHash []byte
	expectedSize int64
}

func (r *hashVerifyingReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.hasher.Write(p[:n])
	}
	// Verify when we've read the expected amount OR hit EOF
	if r.hasher.Size() >= r.expectedSize || err == io.EOF {
		actualHash, actualSize := r.hasher.HashAndSize()
		if actualSize != r.expectedSize {
			panic(fmt.Sprintf("hash verification failed: size mismatch (expected %d, got %d)", r.expectedSize, actualSize))
		}
		if !bytes.Equal(actualHash, r.expectedHash) {
			panic("hash verification failed in download/cat")
		}
	}
	return n, err
}

func (r *hashVerifyingReader) Close() error {
	return r.reader.Close()
}

func WrapWithHashVerification(reader io.ReadCloser, expectedHash []byte, expectedSize int64) io.ReadCloser {
	return &hashVerifyingReader{
		reader:       reader,
		hasher:       utils.NewSHA256HasherSizer(),
		expectedHash: expectedHash,
		expectedSize: expectedSize,
	}
}

func LookupBlobEntry(hash []byte, tx *sql.Tx, stor storage_base.Storage) BlobEntryInfo {
	var blobID []byte
	var offset int64
	var length int64
	var compressionAlg string
	var key []byte
	var path string
	var expectedSize int64

	err := tx.QueryRow(`
		SELECT
			blob_entries.blob_id,
			blob_entries.offset,
			blob_entries.final_size,
			blob_entries.compression_alg,
			blob_entries.encryption_key,
			blob_storage.path,
			sizes.size
		FROM blob_entries
			INNER JOIN blobs ON blobs.blob_id = blob_entries.blob_id
			INNER JOIN blob_storage ON blob_storage.blob_id = blobs.blob_id
			INNER JOIN sizes ON sizes.hash = blob_entries.hash
		WHERE blob_entries.hash = ? AND blob_storage.storage_id = ?`,
		hash, stor.GetID()).Scan(&blobID, &offset, &length, &compressionAlg, &key, &path, &expectedSize)
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
		ExpectedSize:   expectedSize,
	}
}

func CatReadCloser(hash []byte, tx *sql.Tx, stor storage_base.Storage) io.ReadCloser {
	info := LookupBlobEntry(hash, tx, stor)
	reader := utils.ReadCloserToReader(stor.DownloadSection(info.StoragePath, info.Offset, info.Length))
	decrypted := crypto.DecryptBlobEntry(reader, info.Offset, info.Key)
	decompressed := compression.ByAlgName(info.CompressionAlg).Decompress(decrypted)
	return WrapWithHashVerification(decompressed, hash, info.ExpectedSize)
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
