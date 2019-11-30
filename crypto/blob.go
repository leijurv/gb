package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"math/big"
)

func EncryptBlob(out io.Writer) (io.Writer, []byte) {
	key := RandBytes(16)
	return EncryptBlobWithKey(out, key), key
}

func EncryptBlobWithKey(out io.Writer, key []byte) io.Writer {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	stream := cipher.NewCTR(block, make([]byte, 16))
	return &cipher.StreamWriter{S: stream, W: out}
}

// take advantage of AES-CTR by seeking
// assume seekOffset is where this reader is "starting", and the seeking has *already taken place* (e.g. by a Range query to s3)
func DecryptBlobEntry(in io.Reader, seekOffset int64, key []byte) io.Reader {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}

	// while encrypting, by the time it got to this location we know that
	// the IV will have incremented in a big endian manner up until floor(seekOffset/16)
	iv := new(big.Int).SetInt64(seekOffset / 16).Bytes() // if this were C I would just cast &seekOffset to a uint8_t* lol

	// big.Int.Bytes() will only be as long as it needs to be, so we need to:
	padding := make([]byte, 16-len(iv))
	iv = append(padding, iv...) // pad with leading zero bytes to be proper length

	stream := cipher.NewCTR(block, iv)
	// no guarantee that the files are aligned to multiples of 16 in length...
	// so we still need to advance by seekOffset%16 bytes, within this block
	// hack to advance, xor the right amount of garbage with the right amount of garbage
	stream.XORKeyStream(make([]byte, seekOffset%16), make([]byte, seekOffset%16))
	return &cipher.StreamReader{S: stream, R: in}
}

func RandBytes(length int) []byte {
	result := make([]byte, length)
	_, err := io.ReadFull(rand.Reader, result)
	if err != nil {
		panic(err)
	}
	return result
}
