package backup

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"strconv"

	"github.com/DataDog/zstd"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	bip39 "github.com/tyler-smith/go-bip39"
)

func BackupDB(noFileTimestamp bool) {
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

	log.Println("Reading database file")
	dbBytes, err := ioutil.ReadFile(loc)
	if err != nil {
		panic(err)
	}

	log.Println("Done reading, now compressing database file")
	compressed, err := zstd.Compress(nil, dbBytes)
	if err != nil {
		panic(err)
	}

	log.Println("Done compressing, now encrypting database file")
	enc := crypto.EncryptDatabase(compressed, key)
	log.Println("Database", len(dbBytes), "bytes, compressed encrypted to", len(enc), "bytes")

	// <paranoia>
	testDec := crypto.DecryptDatabase(enc, key)
	testDecomp, err := zstd.Decompress(nil, testDec)
	if err != nil {
		panic(err)
	}
	if !bytes.Equal(testDecomp, dbBytes) {
		panic("gcm and/or zstd have failed me")
	}
	log.Println("Decrypt and decompress paranoia verification succeeded")
	// </paranoia>

	var name string
	if !noFileTimestamp {
		name = "db-backup-" + strconv.FormatInt(now, 10)
	} else {
		// old database will be overwritten
		name = "db-backup"
	}
	for _, s := range storages {
		s.UploadDatabaseBackup(enc, name)
	}
	log.Println("Exiting process since database is closed and backed up")
	os.Exit(0)
}

func DBKey() []byte {
	var key []byte
	err := db.DB.QueryRow("SELECT key FROM db_key").Scan(&key)
	if err == db.ErrNoRows {
		log.Println("Randomly generating database encryption key")
		key = crypto.RandBytes(16)
		Mnemonic(key)
		_, err = db.DB.Exec("INSERT INTO db_key (key, id) VALUES (?, 0)", key)
	}
	if err != nil {
		panic(err)
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
		log.Println("That wasn't a 'y' but I'm going to accept it anyway lmao")
	}
}
