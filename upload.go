package main

import (
	"bytes"

	"database/sql"
	"encoding/hex"
	"io"
	"log"
	"os"
	"sort"
	"time"

	"github.com/leijurv/gb/config"
)

// a hash that we indend to upload, and the places on disk where we believe we will be able to find files containing this hash's original data
type ToUpload struct {
	hash    []byte
	size    int64
	options []UploadSource
}

type UploadSource struct {
	path        string
	fs_modified int64
}

// note that padding and location cannot be calculated until after the files to upload have been read and compressed
// (we can't know how large a file will be post-compression until we actually compress it)
type BlobPlan []ToUpload

// an entry in a blob that we have successfully uploaded (we know the post-compression size now!)
type BlobEntry struct {
	hash        []byte
	offset      int64
	length      int64
	compression *string
}

func upload() {
	log.Println("Checking for files to upload")
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		log.Println("Committing to database")
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
		log.Println("Done")
	}()
	plan := calcToUpload(tx)
	log.Println("ToUps", plan)
	blobPlans := bucket(plan)
	log.Println("BlobPlans", blobPlans)
	for _, blobPlan := range blobPlans {
		log.Println("Executing", blobPlan)
		execute(blobPlan, tx, GetAll(tx))
	}
}

func bucket(toUps []ToUpload) []BlobPlan {
	minSize := config.Config().MinBlobSize
	sort.Slice(toUps, func(i, j int) bool {
		return toUps[i].options[0].path < toUps[j].options[0].path
	})

	blobPlans := make([]BlobPlan, 0)

	var tmp BlobPlan
	tmpSize := int64(0)

	for _, toUp := range toUps {
		if toUp.size < minSize {
			tmp = append(tmp, toUp)
			tmpSize += toUp.size
			if tmpSize >= minSize {
				blobPlans = append(blobPlans, tmp)
				tmp = nil
				tmpSize = 0
			}
		} else {
			blobPlans = append(blobPlans, []ToUpload{toUp}) // big boys get to ride on their own
		}
	}
	if tmp != nil {
		blobPlans = append(blobPlans, tmp) // leftovers, not necessarily of min size, but still need to be accounted for
	}
	return blobPlans
}

func execute(plan BlobPlan, tx *sql.Tx, storageDests []Storage) {
	blobID := randBytes(32)

	uploads := make([]StorageUpload, 0)
	for _, storage := range storageDests {
		uploads = append(uploads, storage.BeginBlobUpload(blobID))
	}
	writers := make([]io.Writer, 0)
	for _, upload := range uploads {
		writers = append(writers, upload.Begin())
	}

	out := io.MultiWriter(writers...)

	postEncInfo := NewSHA256HasherSizer()
	out = io.MultiWriter(out, &postEncInfo)

	var key []byte
	out, key = EncryptBlob(out)

	preEncInfo := NewSHA256HasherSizer()
	out = io.MultiWriter(out, &preEncInfo)

	entries := make([]BlobEntry, 0)

outer:
	for _, toUp := range plan {
		log.Println("Adding", toUp)
		startOffset := preEncInfo.size
		for _, option := range toUp.options {
			path := option.path
			stat, err := os.Stat(path)
			if err != nil {
				log.Println("Option", path, "is no longer available:", err)
				continue
			}
			if stat.ModTime().Unix() != option.fs_modified {
				log.Println("Option", path, "is no longer usable due to fs last modified having changed: ", stat.ModTime().Unix(), "while expected", option.fs_modified)
				continue
			}
			if stat.Size() != toUp.size {
				log.Println("Option", path, "is no longer usable due to size having changed: ", stat.Size(), "while expected", toUp.size)
				continue
			}
			// going to use this option
			f, err := os.Open(path)
			if err != nil {
				log.Println("File exists but I can no longer read from it to back it up???", err)
				continue
			}
			verify := NewSHA256HasherSizer()
			// make function so we can defer
			func() {
				defer f.Close() // this is why we make a function here
				tmpOut := out   // TODO compressor(out)
				if _, err := io.Copy(io.MultiWriter(tmpOut, &verify), f); err != nil {
					// not recoverable since we have written an unknown amount of truncated bytes =(
					panic(err)
				}
			}()
			realHash, realSize := verify.HashAndSize()
			if realSize != toUp.size {
				// not recoverable since we have written incorrect data =(
				log.Println("File copied successfully, but bytes read was", realSize, "when we expected", toUp.size)
				break // panics
			}
			if !bytes.Equal(realHash, toUp.hash) {
				// not recoverable since we have written incorrect data =(
				log.Println("File copied successfully, but hash was", hex.EncodeToString(realHash), "when we expected", hex.EncodeToString(toUp.hash))
				break // panics
			}
			end := preEncInfo.size
			length := end - startOffset
			log.Println("File length was", realSize, "but was compressed to", length)
			entries = append(entries, BlobEntry{
				hash:        toUp.hash,
				offset:      startOffset,
				length:      length,
				compression: nil,
			})
			continue outer
		}
		panic("None of the options worked. Don't change files while I'm reading them please :sob: :sob:")
	}
	out.Write(make([]byte, 5021)) // padding
	log.Println("All bytes writen")
	completeds := make([]CompletedUpload, 0)
	for _, upload := range uploads {
		completeds = append(completeds, upload.End())
	}
	hashPreEnc, sizePreEnc := preEncInfo.HashAndSize()
	hashPostEnc, sizePostEnc := postEncInfo.HashAndSize()
	if sizePreEnc != sizePostEnc {
		panic("what??")
	}
	totalSize := sizePreEnc

	_, err := tx.Exec("INSERT INTO blobs (blob_id, encryption_key, size, hash_pre_enc, hash_post_enc) VALUES (?, ?, ?, ?, ?)", blobID, key, totalSize, hashPreEnc, hashPostEnc)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		_, err = tx.Exec("INSERT INTO blob_entries (hash, blob_id, final_size, offset, compression_alg) VALUES (?, ?, ?, ?, ?)", entry.hash, blobID, entry.length, entry.offset, entry.compression)
		if err != nil {
			panic(err)
		}
	}
	now := time.Now().Unix()
	for i, completed := range completeds {
		_, err := tx.Exec("INSERT INTO blob_storage (blob_id, storage_id, full_path, checksum, timestamp) VALUES (?, ?, ?, ?, ?)", blobID, storageDests[i].GetID(), completed.path, completed.checksum, now)
		if err != nil {
			panic(err)
		}
	}
}

func calcToUpload(tx *sql.Tx) []ToUpload {
	rows, err := tx.Query(`
		SELECT
			up_info.hash, up_info.size, files.path, files.fs_modified
		FROM
			(
				SELECT
					to_upload.hash, hashes.size
				FROM
					(
						SELECT
							uniq_hash.hash
						FROM
							(
								SELECT DISTINCT hash FROM files WHERE end IS NULL

							) uniq_hash    /* distinct hashes of all our that currently exist */
						LEFT OUTER JOIN blob_entries ON uniq_hash.hash = blob_entries.hash
						WHERE blob_entries.hash IS NULL /* but filter out hashes that have already been backed up (i.e. have an entry in blob_entries */
					
					) to_upload /* hashes that we're going to upload */
				INNER JOIN hashes ON to_upload.hash = hashes.hash

			) up_info /* hashes AND SIZES that we're going to upload lol */
		INNER JOIN files ON up_info.hash = files.hash
		WHERE files.end IS NULL
	`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	plan := make(map[[32]byte]ToUpload)
	for rows.Next() {
		var hashSlice []byte
		var size int64
		var path string
		var fs_modified int64
		err := rows.Scan(&hashSlice, &size, &path, &fs_modified)
		if err != nil {
			panic(err)
		}
		log.Println("Need to upload", hex.EncodeToString(hashSlice), "with size", size, "and one of the options is", path, "with expected last modified", fs_modified)
		hash := sliceToArr(hashSlice)
		toUp, ok := plan[hash]
		if !ok {
			toUp = ToUpload{hashSlice, size, nil}
		}
		toUp.options = append(toUp.options, UploadSource{path, fs_modified})
		plan[hash] = toUp
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	toUp := make([]ToUpload, 0)
	for _, v := range plan {
		toUp = append(toUp, v)
	}
	return toUp
}

func sliceToArr(in []byte) [32]byte {
	if len(in) != 32 {
		panic("database gave invalid row??")
	}
	var result [32]byte
	copy(result[:], in)
	return result
}
