package crypto

import (
	"crypto/aes"
	"crypto/cipher"
)

func EncryptDatabase(data []byte, key []byte) []byte {
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

func DecryptDatabase(data []byte, key []byte) []byte {
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
