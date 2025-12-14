package paranoia

import (
	"bytes"
	"encoding/hex"
	"log"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/utils"
)

func TestAllFiles() {
	hashes := make(chan []byte, 100)
	success := make(chan bool)
	nWorkers := 16
	for worker := 0; worker < nWorkers; worker++ {
		go func() {
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
			didISucceed := true
			for hash := range hashes {
				log.Println("Testing fetching hash", hex.EncodeToString(hash), "which is the contents of") //, path)
				reader := download.Cat(hash, tx)
				h := utils.NewSHA256HasherSizer()
				utils.Copy(&h, reader)
				realHash, realSize := h.HashAndSize()
				log.Println("Size is", realSize, "and hash is", hex.EncodeToString(realHash))
				if !bytes.Equal(realHash, hash) {
					log.Println("WRONG", hex.EncodeToString(hash), hex.EncodeToString(realHash), realSize)
					didISucceed = false
				} else {
					log.Println("Hash is equal!")
				}
			}
			success <- didISucceed
		}()
	}
	go func() {
		// TODO some other ordering idk? this is just the most recent files you uploaded, which is reasonable i think?
		//rows, err := tx.Query(`SELECT d.hash FROM (SELECT DISTINCT hash FROM files WHERE end IS NULL) d INNER JOIN sizes ON sizes.hash = d.hash WHERE sizes.size < 100000000 ORDER BY sizes.hash`)
		// SELECT DISTINCT hash, path FROM (SELECT files.*, sizes.size FROM files INNER JOIN sizes ON files.hash = sizes.hash) WHERE size < 10000000 ORDER BY start DESC
		rows, err := db.DB.Query(`SELECT hash FROM blob_entries WHERE compression_alg = "lepton" GROUP BY hash ORDER BY hash ASC`)
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		for rows.Next() {
			var hash []byte
			//var path string
			err := rows.Scan(&hash) //, &path)
			if err != nil {
				panic(err)
			}
			hashes <- hash
		}
		err = rows.Err()
		if err != nil {
			panic(err)
		}
		close(hashes)
	}()
	for worker := 0; worker < nWorkers; worker++ {
		if !<- success {
			panic("fail")
		}
	}
	log.Println("Success")
}
