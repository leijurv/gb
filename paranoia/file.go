package paranoia

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/cespare/diff"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/utils"
)

func ParanoiaFile(path string) {
	log.Println("Path is:", path)
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	log.Println("Converted to absolute:", path)
	stat, err := os.Stat(path)
	if err != nil {
		log.Println("Path doesn't exist?")
		return
	}
	log.Println("Paranoia level 0: Just check if this path is in the database, and see if it’s up to date by comparing the last modified time and file size.")
	log.Println("Paranoia level 1: Furthermore, print out the storage locations, paths, offsets, and encryption keys where this file can be found.")
	log.Println("-- end of local-only --")
	log.Println("Paranoia level 2: Furthermore, query that storage location to make sure it exists, and ask for the checksum/etag/md5 hash + file size to make sure it matches what the database says that blob should be.")
	log.Println("Paranoia level 3: Furthermore, actually fetch and download this file from the storage, and decrypt and decompress it, and output its sha256. Then, read the local file and output its sha256. Make sure that these two are equal to each other, and equal to what the database says it should be.")
	log.Println("Paranoia level 4: Trusting sha256 is for idiots. Instead of comparing the hashes, do a byte for byte comparison of the file from disk with the decrypted file being downloaded from the storage.")
	log.Print("Enter your paranoia level (0-4) > ")
	var level int
	fmt.Scanln(&level)
	log.Println("Your paranoia level is", level)
	if level == 2 && stat.IsDir() {
		log.Println("Warning: level 2 on a directory is incredibly inefficient and slow, you would be better off doing `gb paranoia storage` which makes bulk metadata queries that are literally hundreds of times faster")
		time.Sleep(1500 * time.Millisecond)
	}
	if level == 4 {
		repeat := "If I don't trust SHA-256, then NONE of gb is sound. Regardless, purely for my own paranoia, I want you to perform a useless byte by byte comparison, which will be slower for no reason."
		log.Println("I will do this for you, but only if you repeat after me:", repeat)
		log.Println("Repeat (let’s be real you're going to paste) that here >")
		reader := bufio.NewReader(os.Stdin)
		text, _ := reader.ReadString('\n')
		if text == repeat+"\n" {
			log.Println("Ok")
		} else {
			log.Println("Petulantly downgrading your paranoia level to 3")
			time.Sleep(1500 * time.Millisecond)
			level--
		}
	}
	if stat.IsDir() {
		utils.WalkFiles(path, func(path string, info os.FileInfo) {
			paranoia(path, info, level)
		})
	} else {
		paranoia(path, stat, level)
	}
}

func paranoia(path string, info os.FileInfo, level int) {
	log.Println("Running paranoia on", path)
	var dbmodified int64
	var dbsize int64
	err := db.DB.QueryRow("SELECT files.fs_modified, sizes.size FROM files INNER JOIN sizes ON files.hash = sizes.hash WHERE files.path = ? AND files.end IS NULL", path).Scan(&dbmodified, &dbsize)
	if err != nil {
		if err == db.ErrNoRows {
			panic("This path is not currently in the database. `gb backup` first? " + path)
		}
		panic(err)
	}
	log.Println("Database says:")
	log.Println("Size:", dbsize)
	log.Println("Modifed:", time.Unix(dbmodified, 0).Format(time.RFC3339))
	stat, err := os.Stat(path)
	if err != nil {
		panic(err)
	}
	log.Println("The actual filesystem currently says:")
	log.Println("Size:", stat.Size())
	log.Println("Modifed:", stat.ModTime().Format(time.RFC3339))
	if stat.Size() != dbsize || stat.ModTime().Unix() != dbmodified {
		panic("File has changed. Back it up again?")
	}
	log.Println("Size and modified matches what we expect!")

	if level == 0 {
		return
	}
	count := 0
	rows, err := db.DB.Query(`
			SELECT
				files.hash,
				blob_entries.blob_id,
				blob_entries.offset, 
				blob_entries.final_size,
				blob_entries.compression_alg,
				blobs.encryption_key,
				blobs.size,
				blob_storage.path,
				blob_storage.checksum,
				storage.storage_id,
				storage.type,
				storage.identifier,
				storage.root_path
			FROM files
				INNER JOIN blob_entries ON blob_entries.hash = files.hash
				INNER JOIN blobs ON blobs.blob_id = blob_entries.blob_id
				INNER JOIN blob_storage ON blob_storage.blob_id = blobs.blob_id
				INNER JOIN storage ON storage.storage_id = blob_storage.storage_id
			WHERE files.path = ? AND files.end IS NULL
		`, path)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		var blobID []byte
		var offset int64
		var length int64
		var compressionAlg string
		var key []byte
		var finalSize int64
		var pathInStorage string
		var checksum string
		var storageID []byte
		var kind string
		var identifier string
		var rootPath string

		err := rows.Scan(&hash, &blobID, &offset, &length, &compressionAlg, &key, &finalSize, &pathInStorage, &checksum, &storageID, &kind, &identifier, &rootPath)
		if err != nil {
			panic(err)
		}
		log.Println("This file can be found in blob ID", hex.EncodeToString(blobID), "which is located in storage", kind, "at the path", path, "decrypting with key", hex.EncodeToString(key), "seeking", offset, "bytes in and reading", length, "bytes from there, and decrypting using", compressionAlg)
		count++
		if level > 1 {
			log.Println("Fetching the metadata of the blob containing this file to verify that it's what we expect...")
			storageR := storage.StorageDataToStorage(storageID, kind, identifier, rootPath)
			fetchedChecksum, fetchedSize := storageR.Metadata(pathInStorage)
			log.Println("Checksum in fetched metadata:", fetchedChecksum)
			log.Println("Size in fetched metadata:", fetchedSize)

			log.Println("Checksum in database:", checksum)
			log.Println("Size in database:", finalSize)

			if fetchedChecksum != checksum || fetchedSize != finalSize {
				panic("Storage has changed checksum or size of this blob that has your file. UH OH LOL!")
			}
			log.Println("Checksum and size of the stored blob matches what we expect!")

			if level > 2 {
				log.Println("Actually doing that now (downloading that section of the blob and decrypting and decompressing)...")
				reader := download.CatEz(hash)
				if level > 3 {
					log.Println("Actually opening your file for this stupid byte by byte comparison now")
					f, err := os.Open(path)
					if err != nil {
						panic(err)
					}
					defer f.Close()
					different, err := diff.Readers(reader, f)
					if err != nil {
						panic(err)
					}
					if different {
						panic("they were different oh no")
					}
					log.Println("Stupid useless byte by byte comparison succeeded as expected... you should use the sha256 mode instead")
				} else {
					h := utils.NewSHA256HasherSizer()
					utils.Copy(&h, reader)
					realHash, realSize := h.HashAndSize()
					log.Println("Size is", realSize, "and hash is", hex.EncodeToString(realHash))
					if !bytes.Equal(realHash, hash) {
						panic(":(")
					}
					log.Println("Hash of downloaded file is as expected!")
				}
			}
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	if count == 0 {
		panic("this blob is not stored anywhere?!")
	}
}
