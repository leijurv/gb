package share

import (
	"bytes"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"log"
	"strings"

	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
)

func signatureShouldBe(realHash []byte, suffix string) string {
	if len(realHash) != 32 {
		panic("wrong length")
	}
	// note that what's signed is the full sha256 hash, not the prefix
	// idk if this actually prevents any real attacks? maybe an attacker knows a certain hash and wants its contents from you, so they maliciously send you a file with the same prefix, get you to share it back to them, then use that to get the original file?
	// but anyway it's much more correct and secure
	toSign := "https://github.com/leijurv/gb v1 file signature: " + hex.EncodeToString(realHash) + " suffix: " + suffix
	mac := crypto.ComputeMAC([]byte(toSign), SharingKey())
	return base64.RawURLEncoding.EncodeToString(mac[:9]) // 72 bit security is on top of the 48 bit security of the hash. if an attacker already knew the hash of the file then this would be only 72 bit security, but that's a weird case and I don't really know how that would happen. assuming the attacker doesn't know the hash of the file they want, it's just brute force against 72+48 = 120 bits of security, which is implausible (they have a one in 2^120 = 1329227995784915872903807060280344576 chance of getting an actual file)
}

func MakeShareURL(hash []byte, suffix string) string {
	// "/1" is going to mean the v1 of sharing a file
	// maybe "/2" will be the v1 of sharing a directory, hopefully?
	return "/1" + base64.RawURLEncoding.EncodeToString(hash[:6]) + signatureShouldBe(hash, suffix) + "/" + suffix
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
		// at least we can defend the signature against timing attacks
		// its a bit halfhearted because we've just previously done a big SQLite select which is not resistant against timing attacks
		// the worry is whether an attacker could test what sha256 prefixes you have in your .gb.db
		// with a timing attack against SQLite, maybe they could narrow down the hash prefix faster than guessing 1 out of 2^48? idk how to protect against this
		return subtle.ConstantTimeCompare([]byte(signature64), []byte(signatureShouldBe(candidateHash, suffix))) == 1
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
