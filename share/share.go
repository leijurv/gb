package share

import (
	"bytes"
	"encoding/hex"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func CreateShareURL(pathOrHash string, overrideName string) {
	var sharedName string
	hash, err := hex.DecodeString(pathOrHash)
	if err != nil || len(hash) != 32 {
		log.Println("Interpreting `" + pathOrHash + "` as a path on your filesystem since it doesn't appear to be a hex SHA-256 hash")
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
		if err != nil {
			panic(err)
		}
		defer tx.Rollback()
		log.Println("Making sure this file is backed up")
		status := backup.CompareFileToDb(path, stat, tx, true)
		if status.New || status.Modified {
			panic("backup the file before sharing it")
		}
		log.Println("Ok, it is backed up")
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
	shareBase := config.Config().ShareBaseURL
	if shareBase == "" {
		log.Println("You don't appear to have `share_base_url` set in your .gb.conf")
		log.Println("If you were running `gb shared` on \"https://gb.yourdomain.com\", you'd want to set the `share_base_url` to that, then I can print out the full URL right here instead of just the path")
	} else {
		log.Printf("Using the share base URL of `%s` as defined in `share_base_url` of your .gb.conf\n", shareBase)
	}
	for strings.HasSuffix(shareBase, "/") {
		shareBase = shareBase[:len(shareBase)-1]
	}
	url := MakeShareURL(hash, sharedName)

	// sanity check
	verifyHash, err := ValidateURL(url)
	if err != nil {
		log.Println("error, this can happen if you try to share a sha256 that isn't actually in .gb.db")
		panic(err)
	}
	if !bytes.Equal(verifyHash, hash) {
		panic("didn't decode / verify")
	}
	log.Println("Verified that this URL can be correctly decoded and verified back to the original hash")
	log.Println(shareBase + url)
	// but i want to share directories too. without revealing the full path to that directory
	// ideas:
	// encrypted directory? too long and reveals length maybe?
	// new table in sqlite where its just two columns, the directory name and a random identifier?
	// give the hash of some element of the directory, then compute what directory it's in?
}
