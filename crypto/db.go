package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/sha256"
	"io"
)

func LegacyEncryptDatabase(data []byte, key []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	nonce := RandBytes(gcm.NonceSize())
	return gcm.Seal(nonce, nonce, data, nil)
}

func LegacyDecryptDatabase(data []byte, key []byte) []byte {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	nonceSize := gcm.NonceSize()
	plaintext, err := gcm.Open(nil, data[:nonceSize], data[nonceSize:], nil)
	if err != nil {
		panic(err)
	}
	return plaintext
}

func EncryptDatabaseV2(out io.Writer, dbKey []byte) io.Writer {
	iv := RandBytes(16)
	_, err := out.Write(iv)
	if err != nil {
		panic(err)
	}
	return &cipher.StreamWriter{S: createCipherStream(iv, dbKey), W: out}
}

// lazy buffer for now - need to peel the last 32 bytes off, which is super annoying to do on an io.Reader of unknown length!
func DecryptDatabaseV2(data []byte, dbKey []byte) []byte {
	iv := data[:16]
	decr := make([]byte, len(data)-16)
	createCipherStream(iv, dbKey).XORKeyStream(decr, data[16:])
	msg := decr[:len(decr)-32]
	msgHash := sha256.New()
	msgHash.Write(msg)
	expectedMAC := ComputeMAC(msgHash.Sum(nil), dbKey)
	if !bytes.Equal(expectedMAC, decr[len(decr)-32:]) {
		panic("wrong mac")
	}
	return msg
}

func ComputeMAC(messageHash []byte, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(messageHash)
	return mac.Sum(nil)
}
