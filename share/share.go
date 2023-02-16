package share

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"github.com/leijurv/gb/config"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/crypto"
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

func MakeShareURL(hash []byte, suffix string) string {
	// "/1" is going to mean the v1 of sharing a file
	// maybe "/2" will be the v1 of sharing a directory, hopefully?
	return "/1" + base64.RawURLEncoding.EncodeToString(hash[:6]) + signatureShouldBe(hash, suffix) + "/" + suffix
}

func signatureShouldBe(realHash []byte, suffix string) string {
	// note that what's signed is the full sha256 hash, not the prefix
	// idk if this actually prevents any real attacks? maybe an attacker knows a certain hash and wants its contents from you, so they maliciously send you a file with the same prefix, get you to share it back to them, then use that to get the original file?
	// but anyway it's much more correct and secure
	toSign := "https://github.com/leijurv/gb v1 file signature: " + hex.EncodeToString(realHash) + " suffix: " + suffix
	mac := crypto.ComputeMAC([]byte(toSign), SharingKey())
	return base64.RawURLEncoding.EncodeToString(mac[:9]) // 72 bit security is on top of the 48 bit security of the hash. if an attacker already knew the hash of the file then this would be only 48 bit security, but that's a weird case and I don't really know how that would happen. assuming the attacker doesn't know the hash of the file they want, it's just brute force against 72+48 = 120 bits of security, which is implausible (they have a one in 2^120 = 1329227995784915872903807060280344576 chance of getting an actual file)
}

func ValidateURL(url string) ([]byte, error) {
	origURL := url
	if !strings.HasPrefix(url, "/1") {
		return nil, errors.New("doesn't begin with /1")
	}
	url = url[2:]
	if len(url) < 8+12+1 { // hash, signature, slash
		return nil, errors.New("too short")
	}
	hash64 := url[:8]
	url = url[8:]
	signature64 := url[:12]
	url = url[12:]
	suffix := url[1:]
	if "/1"+hash64+signature64+"/"+suffix != origURL {
		panic("mistake")
	}
	hashPrefix, err := base64.RawURLEncoding.DecodeString(hash64)
	if err != nil {
		return nil, err
	}
	if len(hashPrefix) != 6 {
		panic("length should have been checked alreday")
	}
	realHash := pickCorrectHash(hashPrefix, func(candidateHash []byte) bool {
		return signature64 == signatureShouldBe(candidateHash, suffix)
	})
	if realHash == nil {
		return nil, errors.New("no candidate hashes matched signature")
	}
	log.Println(hash64, signature64, url, hashPrefix, realHash)
	return realHash, nil
}

func pickCorrectHash(hashPrefix []byte, test func([]byte) bool) []byte {
	// this query doesn't load the entire sizes table into ram (neither on the sqlite library side nor on the golang driver side) - I checked by putting a long sleep right here and noting that RAM usage did not increase afterwards
	// (if I replace `ORDER BY hash` with something like `ORDER BY ltrim(hex(hash), 'ABC')` then EXPLAIN QUERY PLAN says that a temp B-tree is now used, and RAM usage does increase by 5MB over the course of running the following line)
	rows, err := db.DB.Query("SELECT hash FROM sizes WHERE hash >= ? ORDER BY hash", hashPrefix)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	for rows.Next() {
		var hash []byte
		err = rows.Scan(&hash)
		if err != nil {
			panic(err)
		}
		if !bytes.HasPrefix(hash, hashPrefix) {
			// this will bail out early (and run the defer rows.Close()) after just one row basically always
			// note that a 6 byte prefix is 2^48 which is 281,474,976,710,656
			// so the odds of having another file that matches the same leading 48 bits of the sha256 is very very low (one in 281 trillion for a random pair of files to match up in this way)
			// according to https://www.bdayprob.com/ the odds of having even just one pair of files with a colliding 48-bit prefix goes over 50% once you have about 20 million files
			// and EVEN IF you have such a pair, and you share one of them, all that happens is this loop runs for two iterations instead of one, which is no big deal at all
			break
		}
		if test(hash) {
			return hash
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return nil
}
