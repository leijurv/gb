package backup

import (
	"bytes"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"

	"github.com/tyler-smith/go-bip39"
)

func BackupADirectoryRecursively(path string) {
	log.Println("Going to back up this folder:", path)
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
	if !stat.IsDir() {
		log.Println("This is not a directory btw wtf single files are BaD and i wont deal with them owned")
		return
	}
	log.Println("Good this is a directory")
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	log.Println("Normalized to ensure trailing slash:", path)
	wg.Add(1)
	go scannerThread(path)
	for i := 0; i < config.Config().NumHasherThreads; i++ {
		wg.Add(1)
		go hasherThread()
	}
	go bucketerThread()
	for i := 0; i < config.Config().NumUploaderThreads; i++ {
		go uploaderThread()
	}
	go func() {
		for {
			log.Println("Bytes written:", stats.Total())
			time.Sleep(5 * time.Second)
		}
	}()
	wg.Wait()
	log.Println("Backup complete")
	BackupDB()
}

func BackupDB() {
	log.Println("Backing up the database itself")
	key := DBKey()               // before shutdown since it's saved in the db
	storages := storage.GetAll() // also before shutdown

	db.ShutdownDatabase()

	loc := config.Config().DatabaseLocation
	if _, err := os.Stat(loc + "-wal"); !os.IsNotExist(err) {
		panic("closed the database but " + loc + "-wal still exists?!")
	}
	if _, err := os.Stat(loc + "-shm"); !os.IsNotExist(err) {
		panic("closed the database but " + loc + "-shm still exists?!")
	}
	dbBytes, err := ioutil.ReadFile(loc)
	if err != nil {
		panic(err)
	}
	enc := crypto.EncryptDatabase(dbBytes, key)
	log.Println("Database", len(dbBytes), "bytes, encrypted to", len(enc), "bytes")
	// <paranoia>
	if !bytes.Equal(crypto.DecryptDatabase(enc, key), dbBytes) {
		panic("gcm has failed me")
	}
	// </paranoia>
	name := "db-backup-" + strconv.FormatInt(time.Now().Unix(), 10)
	for _, s := range storages {
		s.UploadDatabaseBackup(enc, name)
	}
	log.Println("Exiting process since running anything else would just crash with sql: database is closed")
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
	log.Println("I won't make you paste it back in to prove you've written it down, it's your funeral")
	log.Println("If you lose this key AND the unencrypted database file, your files are completely unrecoverable")
	log.Println("If you have either, they are recoverable from wherever they're being backed up to")
	log.Println("\"gb mnemonic\" to print this out again")
}
