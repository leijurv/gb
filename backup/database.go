package backup

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"strconv"

	"github.com/leijurv/gb/compression"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
	bip39 "github.com/tyler-smith/go-bip39"
)

func BackupDB() {
	log.Println("Backing up the database itself")

	key := DBKey()               // before shutdown since it's saved in the db
	storages := storage.GetAll() // also before shutdown
	log.Println("Closing database")
	db.ShutdownDatabase()
	log.Println("Database closed")

	loc := config.Config().DatabaseLocation
	if _, err := os.Stat(loc + "-wal"); !os.IsNotExist(err) {
		panic("closed the database but " + loc + "-wal still exists?! this can happen if you have the database open in another program like sqlite3 command line :)")
	}
	if _, err := os.Stat(loc + "-shm"); !os.IsNotExist(err) {
		panic("closed the database but " + loc + "-shm still exists?!")
	}

	f, err := os.Open(loc)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	fname := "db-v2backup-" + strconv.FormatInt(now, 10)
	uploads := make([]storage_base.StorageUpload, 0)
	writers := make([]io.Writer, 0)
	for _, s := range storages {
		upload := s.BeginDatabaseUpload(fname)
		uploads = append(uploads, upload)
		writers = append(writers, upload.Writer())
	}
	rawDB := utils.NewSHA256HasherSizer()
	out := crypto.EncryptDatabaseV2(io.MultiWriter(writers...), key)
	afterCompression := utils.NewSHA256HasherSizer()
	compression.VerifiedCompression(&compression.ZstdCompression{}, io.MultiWriter(&afterCompression, out), io.TeeReader(f, &rawDB), &rawDB)
	_, err = out.Write(crypto.ComputeMAC(afterCompression.Hash(), key))
	if err != nil {
		panic(err)
	}
	log.Println("Database", rawDB.Size(), "bytes, compressed encrypted to", afterCompression.Size(), "bytes")
	for _, upload := range uploads {
		upl := upload.End()
		log.Println("DB uploaded to", upl.Path)
	}
	log.Println("Exiting process since database is closed and backed up")
	os.Exit(0)
}

func DBKey() []byte {
	return dbKeyImpl(true)
}

func DBKeyNonInteractive() []byte {
	return dbKeyImpl(false)
}

func dbKeyImpl(interactive bool) []byte {
	var key []byte
	err := db.DB.QueryRow("SELECT key FROM db_key").Scan(&key)
	if err == db.ErrNoRows {
		log.Println("Randomly generating database encryption key")
		key = crypto.RandBytes(16)
		if interactive {
			Mnemonic(key)
		}
		_, err = db.DB.Exec("INSERT INTO db_key (key, id) VALUES (?, 0)", key)
	}
	if err != nil {
		panic(err)
	}
	if len(key) != 16 {
		panic("bad key")
	}
	return key
}

func Mnemonic(key []byte) {
	mnemonic, err := bip39.NewMnemonic(key)
	if err != nil {
		panic(err)
	}
	// <paranoia>
	test, err := bip39.EntropyFromMnemonic(mnemonic)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(test, key) {
		panic("bip39 bad?? wtf")
	}
	// </paranoia>
	log.Println("Your database encryption key mnemonic is:", mnemonic)
	log.Println("If you have either this key, or the unencrypted database file (" + config.Config().DatabaseLocation + "), your files are recoverable from wherever they're being backed up to")
	log.Println("\"gb mnemonic\" to print this out again")
	reader := bufio.NewReader(os.Stdin)
	log.Print("Confirm you understand that if you lose this mnemonic key AND the unencrypted database file, your files are completely unrecoverable (y): ")
	text, _ := reader.ReadString('\n')
	if text == "y\n" {
		log.Println("ok")
	} else {
		panic("must confirm")
	}
}
