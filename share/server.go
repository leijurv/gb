package share

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/proxy"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
)

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
	if path == "/" {
		w.WriteHeader(200)
		w.Write([]byte("serving shared files using https://github.com/leijurv/gb"))
		return
	}
	if strings.HasPrefix(path, "/1") {
		log.Println("Request to", path, "is presumably for a v1 shared file")
		hash, err := ValidateURL(path)
		if err != nil {
			log.Println(err)
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

var webShareMasterKey []byte

// WebShareMasterKey returns the master key used for encrypting share JSONs.
// This key is derived from the database key and should be configured in the
// Cloudflare Worker environment as SHARE_MASTER_KEY.
func WebShareMasterKey() []byte {
	if len(webShareMasterKey) == 0 {
		webShareMasterKey = crypto.ComputeMAC([]byte("webshare"), backup.DBKey())
	}
	if len(webShareMasterKey) != 32 {
		panic("bad key")
	}
	return webShareMasterKey
}

// DeriveShareFilename derives the storage filename for a share from its password.
// Returns hex-encoded HMAC, used as: share/{filename}
func DeriveShareFilename(password string) string {
	mac := hmac.New(sha256.New, WebShareMasterKey())
	mac.Write([]byte("filename:"))
	mac.Write([]byte(password))
	// Use first 16 bytes (32 hex chars) for reasonable filename length
	return hex.EncodeToString(mac.Sum(nil)[:16])
}

// DeriveShareContentKey derives the AES key for encrypting a share's JSON content.
func DeriveShareContentKey(password string) []byte {
	mac := hmac.New(sha256.New, WebShareMasterKey())
	mac.Write([]byte("content:"))
	mac.Write([]byte(password))
	// Return 16 bytes for AES-128
	return mac.Sum(nil)[:16]
}

// EncryptShareJSON encrypts share JSON using AES-GCM with a synthetic IV.
// The nonce is derived from HMAC(key, plaintext), making encryption deterministic:
// same plaintext = same ciphertext (allows paranoia verification), but different
// plaintext = different nonce (safe against nonce reuse when content changes).
// Returns: nonce (12 bytes) || ciphertext || tag (16 bytes)
func EncryptShareJSON(plaintext []byte, password string) []byte {
	key := DeriveShareContentKey(password)
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	nonceMAC := hmac.New(sha256.New, key)
	nonceMAC.Write([]byte("siv nonce:"))
	nonceMAC.Write(plaintext)
	nonce := nonceMAC.Sum(nil)[:gcm.NonceSize()]
	ciphertext := gcm.Seal(nil, nonce, plaintext, nil)
	return append(nonce, ciphertext...)
}

// DecryptShareJSON decrypts share JSON using AES-GCM.
// Expects: nonce (12 bytes) || ciphertext || tag (16 bytes)
func DecryptShareJSON(ciphertext []byte, password string) ([]byte, error) {
	key := DeriveShareContentKey(password)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce := ciphertext[:nonceSize]
	ciphertext = ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}
