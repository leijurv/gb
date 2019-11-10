package main

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
)

func cat(hash []byte, tx *sql.Tx) io.Reader {
	var blobID []byte
	var offset int64
	var length int64
	var compression *string
	var key []byte
	var fullPath string
	var storageID []byte
	var kind string
	var identifier string
	var rootPath string
	// TODO this could return more than one row if the same blob was backed up to more than one destination
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
		`, hash).Scan(&blobID, &offset, &length, &compression, &key, &fullPath, &storageID, &kind, &identifier, &rootPath)
	if err != nil {
		panic(err)
	}
	storageR := storage.StorageDataToStorage(storageID, kind, identifier, rootPath)
	reader := storageR.DownloadSection(blobID, offset, length)
	decrypted := crypto.DecryptBlobEntry(reader, offset, key)
	return decrypted
}

func downloadOne(hash []byte) {
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

	reader := cat(hash, tx)
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}
	log.Println(string(data))
}

func testAll() {
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
	rows, err := tx.Query(`SELECT DISTINCT hash FROM files`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		err := rows.Scan(&hash)
		if err != nil {
			panic(err)
		}
		log.Println("Testing fetching hash", hex.EncodeToString(hash))
		reader := cat(hash, tx)
		h := backup.NewSHA256HasherSizer()
		if _, err := io.Copy(&h, reader); err != nil {
			panic(err)
		}
		realHash, realSize := h.HashAndSize()
		log.Println("Size is", realSize, "and hash is", hex.EncodeToString(realHash))
		if !bytes.Equal(realHash, hash) {
			panic(":(")
		}
		log.Println("Hash is equal!")
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
}
