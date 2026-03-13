package share

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/crypto"
)

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
