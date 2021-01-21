package download

import (
	"bufio"
	"io/ioutil"
	"log"
	"os"

	"github.com/DataDog/zstd"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	bip39 "github.com/tyler-smith/go-bip39"
)

// just a simple utility to decrypt the database

func RestoreDB(path string) {
	outPath := path + ".decrypted"
	log.Println("Output will be written to", outPath)
	log.Println("You may want to replace your database file with that, just ensure that any files such as", config.Config().DatabaseLocation+"-wal", "or", config.Config().DatabaseLocation+"-shm", "are gone first")
	log.Println("Restoring a database backup from", path)
	encBytes, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	log.Println("Read", len(encBytes), "bytes")
	log.Print("Enter database encryption mnemonic: ")
	mnemonic, _ := bufio.NewReader(os.Stdin).ReadString('\n')
	database := decryptDatabase(encBytes, mnemonic)
	err = ioutil.WriteFile(outPath, database, 0644)
	if err != nil {
		panic(err)
	}
	log.Println("Successfully decrypted, decompressed, and written", len(database), "bytes to", outPath)
}

func decryptDatabase(encBytes []byte, keyMnemonic string) []byte {
	key, err := bip39.EntropyFromMnemonic(keyMnemonic)
	if err != nil {
		panic(err)
	}
	compressed := crypto.DecryptDatabase(encBytes, key)
	decompressed, err := zstd.Decompress(nil, compressed)
	if err != nil {
		panic(err)
	}
	return decompressed
}