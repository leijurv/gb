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
	realHash := pickCorrectHash(hashPrefix, func(candidateHash []byte) int {
		return subtle.ConstantTimeCompare([]byte(signature64), []byte(signatureShouldBe(candidateHash, suffix)))
	})
	if realHash == nil {
		return nil, errors.New("no candidate hashes matched signature")
	}
	log.Println(hash64, signature64, url, hashPrefix, realHash)
	return realHash, nil
}

func pickCorrectHash(hashPrefix []byte, testSignature func([]byte) int) []byte {
	// the big picture idea here is that an attacker shouldn't be able to "break down" the 120 bit security into a separate 48 bit security step and then a 72 bit security step
	// to achieve this, it should be impossible to detect whether the hashPrefix is correct (i.e. it's the beginning of an actual hash in the database)
	// in short, hash and signature both being correct should result in one behavior, and either/both being wrong should result in another
	// put in the simplest terms: "hash correct signature wrong" should take the same amount of time as "hash wrong signature wrong"
	selectedHash := make([]byte, 32)
	for _, hash := range getCandidateHashes(hashPrefix) {
		hashMatches := subtle.ConstantTimeCompare(hash[:len(hashPrefix)], hashPrefix) // this will probably be "0" for 15 of the 16 iterations of this loop in the case where the URL is correct, and 16 of 16 when the URL is bad
		signatureMatches := testSignature(hash)
		// test the signature and the hash every time. don't skip the signature check if the hash is wrong, obviously
		bothMatch := subtle.ConstantTimeByteEq(uint8(hashMatches+signatureMatches), 2) // don't use an if statement because something like "hashMatches && signatureMatches" will short circuit when hashMatches is true
		subtle.ConstantTimeCopy(bothMatch, selectedHash, hash)
		// if both match, this is the selectedHash
		//log.Println(hashMatches, signatureMatches, bothMatch, hash, selectedHash)
	}
	if bytes.Equal(selectedHash, make([]byte, 32)) { // ok because at this point the decision has already been made
		return nil
	} else {
		return selectedHash
	}
}

// the idea is that this function will always take essentially the exact same amount of time no matter whether there actually is any hash that begins with hashPrefix
// either way (whether there is or isn't), it traverses the sizes-by-hash index (technically "sqlite_autoindex_sizes_1") and grabs the next sixteen hashes without even checking if they begin with the prefix
// look, you can't exactly trust SQLite to be perfectly constant-time and resistant to timing attacks, but I think this is the best we can reasonably get lol
func getCandidateHashes(hashPrefix []byte) [][]byte {
	if len(hashPrefix) != 6 {
		panic("length should have been checked already")
	}
	// note that a 6 byte prefix is 2^48 which is 281,474,976,710,656
	// so the odds of having another file that matches the same leading 48 bits of the sha256 is very very low (one in 281 trillion for a random pair of files to match up in this way)
	// according to https://www.bdayprob.com/ the odds of having even just one pair of files with a colliding 48-bit prefix goes over 50% once you have about 20 million files
	// and EVEN IF you have such a pair, and you share one of them, everything still works because the pickCorrectHash loop doesn't need the correct signature to be first (i.e. it works even if several have hashMatches, because only one of those will also have signatureMatches)
	// having more than 16 hashes that share a 1/281,474,976,710,656 chance collision is practically impossible
	// so, it's fine to just grab the next 16
	// honestly I'd be confident with grabbing like 3 or 4 rows but let's just be on the safe side
	rows, err := db.DB.Query("SELECT hash FROM sizes WHERE hash >= ? ORDER BY hash LIMIT 16", hashPrefix /* note: do hashPrefix[:2] or something to see the correct hash not being first still work */)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	ret := make([][]byte, 0)
	for rows.Next() {
		var hash []byte
		err = rows.Scan(&hash)
		if err != nil {
			panic(err)
		}
		ret = append(ret, hash)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return ret
}
