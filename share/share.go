package share

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"github.com/leijurv/gb/utils"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
)

func CreateShareURL(path string) {
	path, err := filepath.Abs(path)
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

	// check to make sure path is backed up and size and modtime match db
	// get hash
	// get share base from gb.conf and explain if not present
	// compute url (sanity check verify it too)
	// maybe offer that the shared filename can be different via a cli option?

	// i think its fairly clear how to share a file - give some of the hash of the file, and a signature

	// but i want to share directories too. without revealing the full path to that directory
	// ideas:
	// encrypted directory? too long and reveals length maybe?
	// new table in sqlite where its just two columns, the directory name and a random identifier?
	// give the hash of some element of the directory, then compute what directory it's in?
}

func MakeShareURL(hash []byte, suffix string) string {
	// "/1/" is going to mean the v1 of sharing a file
	// maybe "/2/" will be the v1 of sharing a directory, hopefully?
	return "/1/" + base64.RawURLEncoding.EncodeToString(hash[:6]) + "/" + signatureShouldBe(hash, suffix) + "/" + suffix
}

func signatureShouldBe(realHash []byte, suffix string) string {
	// note that what's signed is the full sha256 hash, not the prefix
	// idk if this actually prevents any real attacks? maybe an attacker knows a certain hash and wants its contents from you, so they maliciously send you a file with the same prefix, get you to share it back to them, then use that to get the original file?
	// but anyway it's much more correct and secure
	toSign := "/1/" + hex.EncodeToString(realHash) + "/$signature$/" + suffix
	log.Println("Signing", toSign)
	mac := crypto.ComputeMAC([]byte(toSign), SharingKey())
	return base64.RawURLEncoding.EncodeToString(mac[:9]) // 72 bit security is on top of the 48 bit security of the hash. if an attacker already knew the hash of the file then this would be only 48 bit security, but that's a weird case and I don't really know how that would happen. assuming the attacker doesn't know the hash of the file they want, it's just brute force against 72+48 = 120 bits of security, which is implausible (they have a one in 2^120 = 1329227995784915872903807060280344576 chance of getting an actual file)
}

func ValidateURL(url string) ([]byte, error) {
	origURL := url
	if !strings.HasPrefix(url, "/1/") {
		return nil, errors.New("doesn't begin with /1/")
	}
	url = url[3:]
	firstSlash := strings.IndexRune(url, '/')
	if firstSlash == -1 {
		return nil, errors.New("no slash for hash")
	}
	hash64 := url[:firstSlash]
	url = url[firstSlash+1:]
	secondSlash := strings.IndexRune(url, '/')
	if secondSlash == -1 {
		return nil, errors.New("no slash for signature")
	}
	signature64 := url[:secondSlash]
	suffix := url[secondSlash+1:]
	if "/1/"+hash64+"/"+signature64+"/"+suffix != origURL {
		panic("mistake")
	}
	hashPrefix, err := base64.RawURLEncoding.DecodeString(hash64)
	if err != nil {
		return nil, err
	}
	if len(hashPrefix) != 6 {
		return nil, errors.New("prefix too short")
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

func Test() {
	hash, err := hex.DecodeString("70239b796baeb019af608ef3b77067d3e7c8e50b204c02b180554d04b9849035") // hash of skiing.mp4
	if err != nil {
		panic(err)
	}
	url := MakeShareURL(hash, "skiing.mp4")
	log.Println(url) // /1/cCObeWuu/1932Zm9QIuVa/skiing.mp4
	// https://share.leijurv.com/1/cCObeWuu/1932Zm9QIuVa/skiing.mp4
	// https://youtu.be/LxPqAve-NwM
	hash, err = ValidateURL(url)
	log.Println(hash, err)
}
