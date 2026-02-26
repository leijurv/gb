package paranoia

import (
	"bytes"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/leijurv/gb/compression"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

func BlobParanoia(label string) {
	log.Println("Blob paranoia")
	log.Println("This reads blobIDs (in hex) from stdin, fully downloads, decrypts, and decompresses them, and makes sure everything is as it should be")
	log.Println("It does not check remote metadata such as Etag or checksum (use paranoia storage for that)")
	log.Println("For example, you could pipe in like this: `sqlite3 ~/.gb.db \"select distinct hex(blob_id) from blob_entries where compression_alg='zstd'\" | gb paranoia blob` if, for some reason, you didn't trust zstd")
	log.Println()
	storage, ok := storage.StorageSelect(label)
	if !ok {
		return
	}

	stdin, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}
	lines := strings.Split(string(stdin), "\n")
	var sz int64
	for i, line := range lines {
		if line == "" {
			continue
		}
		log.Println("Processing input line:", line)
		if len(line) != 64 {
			panic("Line length is not 64")
		}
		blobID, err := hex.DecodeString(line)
		if err != nil {
			panic(err)
		}
		sz += BlobReaderParanoia(DownloadEntireBlob(blobID, storage), blobID, storage)
		log.Println("Processed", i+1, "blobs out of", len(lines), "and downloaded", utils.FormatCommas(sz), "bytes")
	}
}

func DownloadEntireBlob(blobID []byte, storage storage_base.Storage) io.Reader {
	var blobSize int64
	err := db.DB.QueryRow("SELECT size FROM blobs WHERE blob_id = ?", blobID).Scan(&blobSize)
	db.Must(err)
	var path string
	err = db.DB.QueryRow("SELECT path FROM blob_storage WHERE blob_id = ? AND storage_id = ?", blobID, storage.GetID()).Scan(&path)
	db.Must(err)
	return utils.ReadCloserToReader(storage.DownloadSection(path, 0, blobSize))
}

func BlobReaderParanoia(outerReader io.Reader, blobID []byte, storage storage_base.Storage) int64 {
	return BlobReaderParanoiaWithCallback(outerReader, blobID, storage, nil)
}

func BlobReaderParanoiaWithCallback(outerReader io.Reader, blobID []byte, storage storage_base.Storage, callback func(hash []byte, data []byte)) int64 {
	log.Println("Running paranoia on", hex.EncodeToString(blobID), "in storage", storage)
	if len(blobID) != 32 {
		panic("sanity check")
	}
	var paddingKey []byte
	var blobSize int64
	var hashPostEnc []byte
	err := db.DB.QueryRow("SELECT padding_key, size, final_hash FROM blobs WHERE blob_id = ?", blobID).Scan(&paddingKey, &blobSize, &hashPostEnc)
	db.Must(err)
	hasherPostEnc := utils.NewSHA256HasherSizer()
	encReader := io.TeeReader(outerReader, &hasherPostEnc)

	rows, err := db.DB.Query(`SELECT hash, encryption_key, final_size, offset, compression_alg, size FROM blob_entries INNER JOIN sizes USING (hash) WHERE blob_id = ? ORDER BY offset, final_size`, blobID) // the ", final_size" serves to ensure that the empty entry comes before the nonempty entry at the same offset
	db.Must(err)
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		var key []byte
		var entrySize int64
		var offset int64
		var compressionAlg string
		var expectedSize int64
		db.Must(rows.Scan(&hash, &key, &entrySize, &offset, &compressionAlg, &expectedSize))
		if hasherPostEnc.Size() != offset {
			panic("got misaligned somehow. gap between entries??")
		}
		log.Println("Expected hash for this entry is " + hex.EncodeToString(hash) + ", decompressing...")
		verify := utils.NewSHA256HasherSizer()
		decompressedReader := utils.ReadCloserToReader(compression.ByAlgName(compressionAlg).Decompress(io.LimitReader(crypto.DecryptBlobEntry(encReader, offset, key), entrySize)))
		var data []byte
		if callback != nil {
			// Buffer the decompressed data so we can pass it to the callback
			data, err = ioutil.ReadAll(decompressedReader)
			if err != nil {
				panic(err)
			}
			(&verify).Write(data)
		} else {
			utils.Copy(&verify, decompressedReader)
		}
		if hasherPostEnc.Size() != offset+entrySize {
			panic("entry was wrong size")
		}
		realHash, realSize := verify.HashAndSize()
		log.Println("Compressed size:", entrySize, "  Decompressed size:", realSize, "  Compression alg:", compressionAlg, "  Hash:", hex.EncodeToString(realHash))
		if !bytes.Equal(hash, realHash) {
			panic("decompressed to wrong data!")
		}
		if realSize != expectedSize {
			panic("decompressed to wrong size!")
		}
		log.Println("Hash is equal!")
		if callback != nil {
			if realSize >= config.Config().MinBlobSize {
				panic("entry size is >= MinBlobSize, this should not be repacked")
			}
			callback(realHash, data)
		}
	}
	db.Must(rows.Err())
	remain, err := ioutil.ReadAll(crypto.DecryptBlobEntry(encReader, hasherPostEnc.Size(), paddingKey))
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(remain, make([]byte, len(remain))) {
		panic("end padding was not all zeros!")
	}
	if hasherPostEnc.Size() != blobSize {
		panic("sanity check")
	}
	if !bytes.Equal(hashPostEnc, hasherPostEnc.Hash()) {
		panic("sanity check")
	}
	log.Println("Fully verified all hashes and paddings")
	return blobSize
}
