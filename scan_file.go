package main

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"io"
	"log"
	"os"
)

func backupOneFile(path string, info os.FileInfo, tx *sql.Tx) {
	var expectedLastModifiedTime int64
	var expectedHash []byte
	err := tx.QueryRow("SELECT fs_modified, hash FROM files WHERE path = ? AND end IS NULL", path).Scan(&expectedLastModifiedTime, &expectedHash)
	if err == nil {
		if expectedLastModifiedTime == info.ModTime().Unix() {
			log.Println("UNMODIFIED:", path, "ModTime is still", expectedLastModifiedTime)
			return
		}
		log.Println("MODIFIED:", path, "Was previously stored, but I'm updating it since the last modified time has changed from", expectedLastModifiedTime, "to", info.ModTime().Unix())
	} else {
		if err != ErrNoRows {
			panic(err) // sql syntax error?
		}
	}

	// TODO
	// db.QueryRow("SELECT FROM hashes WHERE size = ?")
	// if no rows, AND size greater than 16mb, skip directly to blob creation

	// now, it's time to hash the file to see if it needs to be backed up or if we've already got it
	log.Println("Beginning read for sha256 calc:", path)

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	hs := NewSHA256HasherSizer()
	if _, err := io.Copy(&hs, f); err != nil {
		panic(err)
	}
	hash, size := hs.HashAndSize()
	if size != info.Size() {
		panic("You really be changing things while I'm reading them huh " + path)
	}

	log.Println("sha256 is", hex.EncodeToString(hash), "and length is", size)

	if bytes.Equal(hash, expectedHash) {
		log.Println("This hash is unchanged from last time, even though last modified is changed...?")
		log.Println("Updating fs_modifed in db so next time I don't reread this for no reason lol")
		_, err := tx.Exec("UPDATE files SET fs_modified = ? WHERE path = ? AND end IS NULL", info.ModTime().Unix(), path)
		if err != nil {
			panic(err)
		}
		return
	}

	if expectedHash == nil {
		log.Println("NEW FILE:", path)
	} else {
		log.Println(path, "hash has changed from", hex.EncodeToString(expectedHash), "to", hex.EncodeToString(hash))
	}

	_, err = tx.Exec("UPDATE files SET end = ? WHERE end IS NULL AND path = ?", now, path)
	if err != nil {
		panic(err)
	}
	// ignore uniqueness constraint error: it's very possible a different file with identical contents (identical hash) was already added to this table
	_, err = tx.Exec("INSERT OR IGNORE INTO hashes (hash, size) VALUES (?, ?)", hash, size)
	if err != nil {
		panic(err)
	}
	_, err = tx.Exec("INSERT INTO files (path, hash, start, fs_modified) VALUES (?, ?, ?, ?)", path, hash, now, info.ModTime().Unix())
	if err != nil {
		panic(err)
	}
}
