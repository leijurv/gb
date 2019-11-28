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
	// TODO some other ordering idk? this is just the most recent files you uploaded, which is reasonable i think?
	rows, err := tx.Query(`SELECT d.hash FROM (SELECT DISTINCT hash FROM files WHERE end IS NULL) d INNER JOIN sizes ON sizes.hash = d.hash WHERE sizes.size < 100000000 ORDER BY sizes.hash`)
	// SELECT DISTINCT hash FROM blob_entries WHERE compression_alg = "lepton"
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
		reader := download.Cat(hash, tx)
		h := utils.NewSHA256HasherSizer()
		utils.Copy(&h, reader)
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
