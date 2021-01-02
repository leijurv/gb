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
	if label == "" {
		log.Println("First, we need to pick a storage to fetch em from")
		log.Println("Options:")
		descs := storage.GetAllDescriptors()
		for _, d := range descs {
			var label string
			err := db.DB.QueryRow("SELECT readable_label FROM storage WHERE storage_id = ?", d.StorageID[:]).Scan(&label)
			if err != nil {
				panic(err)
			}
			log.Println("•", d.Kind, d.RootPath, "To use this one, do `gb paranoia blob --label=\""+label+"\"`")
		}
		return
	}
	storage.GetAll()
	var storageID []byte
	err := db.DB.QueryRow("SELECT storage_id FROM storage WHERE readable_label = ?", label).Scan(&storageID)
	if err != nil {
		panic(err)
	}
	storage := storage.GetByID(storageID)
	log.Println("Using storage:", storage)

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
		sz += blobParanoia(blobID, storage)
		log.Println("Processed", i+1, "blobs out of", len(lines), "and downloaded", sz, "bytes")
	}
}

func blobParanoia(blobID []byte, storage storage_base.Storage) int64 {
	log.Println("Running paranoia on", hex.EncodeToString(blobID), "in storage", storage)
	if len(blobID) != 32 {
		panic("sanity check")
	}
	var key []byte
	var blobSize int64
	var hashPreEnc []byte
	var hashPostEnc []byte
	err := db.DB.QueryRow("SELECT encryption_key, size, hash_pre_enc, hash_post_enc FROM blobs WHERE blob_id = ?", blobID).Scan(&key, &blobSize, &hashPreEnc, &hashPostEnc)
	if err != nil {
		log.Println("This blob id does not exist")
		panic(err)
	}
	var path string
	err = db.DB.QueryRow("SELECT path FROM blob_storage WHERE blob_id = ? AND storage_id = ?", blobID, storage.GetID()).Scan(&path)
	if err != nil {
		log.Println("Error while grabbing the path of this blob in that storage. Perhaps this blob was never backed up to there?")
		panic(err)
	}
	reader := utils.ReadCloserToReader(storage.DownloadSection(path, 0, blobSize))
	hasherPostEnc := utils.NewSHA256HasherSizer()
	reader = io.TeeReader(reader, &hasherPostEnc)
	reader = crypto.DecryptBlobEntry(reader, 0, key)
	hasherPreEnc := utils.NewSHA256HasherSizer()
	reader = io.TeeReader(reader, &hasherPreEnc)

	rows, err := db.DB.Query(`SELECT hash, final_size, offset, compression_alg FROM blob_entries WHERE blob_id = ? ORDER BY offset ASC`, blobID)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		var entrySize int64
		var offset int64
		var compressionAlg string
		err := rows.Scan(&hash, &entrySize, &offset, &compressionAlg)
		if err != nil {
			panic(err)
		}
		if hasherPreEnc.Size() != offset {
			panic("got misaligned somehow. gap between entries??")
		}
		log.Println("Expected hash for this entry is " + hex.EncodeToString(hash) + ", decompressing...")
		entryReader := io.LimitReader(reader, entrySize)
		finalReader := utils.ReadCloserToReader(compression.ByAlgName(compressionAlg).Decompress(entryReader))
		verify := utils.NewSHA256HasherSizer()
		utils.Copy(&verify, finalReader)
		if hasherPreEnc.Size() != offset+entrySize {
			panic("entry was wrong size")
		}
		realHash, realSize := verify.HashAndSize()
		log.Println("Compressed size:", entrySize, "  Decompressed size:", realSize, "  Compression alg:", compressionAlg, "  Hash:", hex.EncodeToString(realHash))
		if !bytes.Equal(hash, realHash) {
			panic("decompressed to wrong data!")
		}
		log.Println("Hash is equal!")
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	remain, err := ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(remain, make([]byte, len(remain))) {
		panic("end padding was not all zeros!")
	}
	if hasherPreEnc.Size() != hasherPostEnc.Size() {
		panic("sanity check")
	}
	if hasherPreEnc.Size() != blobSize {
		panic("sanity check")
	}
	if !bytes.Equal(hashPreEnc, hasherPreEnc.Hash()) {
		panic("sanity check")
	}
	if !bytes.Equal(hashPostEnc, hasherPostEnc.Hash()) {
		panic("sanity check")
	}
	log.Println("Fully verified all hashes and paddings")
	return blobSize
}
