package share

import (
	"encoding/hex"
	"log"
	"os"
	"path/filepath"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

// ResolvePathOrHash takes a path or hex hash string and returns the hash and a name for sharing.
// If pathOrHash is a valid file path, it verifies the file is backed up and uses the filename.
// If pathOrHash is a hex hash, overrideName must be provided.
func ResolvePathOrHash(pathOrHash string, overrideName string) (hash []byte, sharedName string) {
	var err error
	hash, err = hex.DecodeString(pathOrHash)
	if err != nil || len(hash) != 32 {
		path, err := filepath.Abs(pathOrHash)
		if err != nil {
			panic(err)
		}
		stat, err := os.Stat(path)
		if err != nil {
			panic(err)
		}
		if stat.IsDir() {
			panic("directories not yet supported")
		}
		if !utils.NormalFile(stat) {
			panic("this is something weird")
		}
		tx, err := db.DB.Begin()
		db.Must(err)
		defer tx.Rollback()
		status := backup.CompareFileToDb(path, stat, tx, false)
		if status.New || status.Modified {
			panic("backup the file before sharing it")
		}
		hash = status.Hash
		if overrideName == "" {
			sharedName = filepath.Base(path)
			log.Println("I'm going to name the file `" + sharedName + "` in the shared URL as default. You can override this with `--name=\"othername.ext\"`")
		} else {
			sharedName = overrideName
		}
	} else {
		log.Println("Interpreting `" + pathOrHash + "` as a hex SHA-256 hash. If it's a file, use its full path rather than a relative path.")
		if overrideName == "" {
			panic("since you just gave a sha256 hash, I don't know what to call the shared file. please provide a human-readable name with `--name=\"filename.ext\"`")
		}
		sharedName = overrideName
	}
	return hash, sharedName
}

