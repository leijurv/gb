package share

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/proxy"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
)

func CreateShareURL(path string) {
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
	return "/1/" + base64.RawURLEncoding.EncodeToString(hash[:6]) + "/" + signatureShouldBe(hash, suffix) + "/" + suffix
}

func signatureShouldBe(realHash []byte, suffix string) string {
	toSign := "/1/" + hex.EncodeToString(realHash) + "/$signature$/" + suffix
	log.Println("Signing", toSign)
	mac := crypto.ComputeMAC([]byte(toSign), SharingKey())
	return base64.RawURLEncoding.EncodeToString(mac[:9])
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

func Shared(label string, listen string) {
	storage, ok := storage.StorageSelect(label)
	if !ok {
		return
	}
	server := &http.Server{
		Addr: listen,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handleHTTP(w, r, storage)
		}),
		TLSNextProto: make(map[string]func(*http.Server, *tls.Conn, http.Handler)), // disables http/2
	}
	log.Println("Listening for HTTP on", listen)
	log.Fatal(server.ListenAndServe())
}

func handleHTTP(w http.ResponseWriter, req *http.Request, storage storage_base.Storage) {
	path := req.URL.Path
	if strings.HasPrefix(path, "/1/") {
		log.Println("Request to", path, "is presumably for a v1 shared file")
		hash, err := ValidateURL(path)
		if err != nil {
			w.WriteHeader(404)
			w.Write([]byte("sorry"))
			return
		}
		proxy.ServeHashOverHTTP(hash, w, req, storage)
		return
	}
	w.WriteHeader(404)
	w.Write([]byte("idk"))
}

var shareKey []byte

func SharingKey() []byte {
	if len(shareKey) == 0 {
		shareKey = crypto.ComputeMAC([]byte("sharing"), backup.DBKey())
	}
	if len(shareKey) != 32 {
		panic("bad key")
	}
	return shareKey
}
