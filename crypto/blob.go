package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"io"
	"math/big"
)

func EncryptBlob(out io.Writer, seekOffset int64) (io.Writer, []byte) {
	key := RandBytes(16)
	return EncryptBlobWithKey(out, seekOffset, key), key
}

func EncryptBlobWithKey(out io.Writer, seekOffset int64, key []byte) io.Writer {
	return &cipher.StreamWriter{S: createOffsetCipherStream(seekOffset, key), W: out}
}

// take advantage of AES-CTR by seeking
// assume seekOffset is where this reader is "starting", and the seeking has *already taken place* (e.g. by a Range query to s3)
func DecryptBlobEntry(in io.Reader, seekOffset int64, key []byte) io.Reader {
	return &cipher.StreamReader{S: createOffsetCipherStream(seekOffset, key), R: in}
}

func createCipherStream(iv []byte, key []byte) cipher.Stream {
	block, err := aes.NewCipher(key)
	if err != nil {
		panic(err)
	}
	return cipher.NewCTR(block, iv)
}

func createOffsetCipherStream(seekOffset int64, key []byte) cipher.Stream {
	iv, remainingSeek := CalcIVAndSeek(seekOffset)
	stream := createCipherStream(iv, key)
	// hack to advance, xor the right amount of garbage with the right amount of garbage
	stream.XORKeyStream(make([]byte, remainingSeek), make([]byte, remainingSeek))
	return stream
}

func CalcIVAndSeek(seekOffset int64) ([]byte, int64) {
	if seekOffset < 0 {
		panic("negative seek is impossible")
	}
	// while encrypting, by the time it got to this location we know that
	// the IV will have incremented in a big endian manner up until floor(seekOffset/16)
	iv := new(big.Int).SetInt64(seekOffset / 16).Bytes() // if this were C I would just cast &seekOffset to a uint8_t* lol

	// big.Int.Bytes() will only be as long as it needs to be, so we need to:
	padding := make([]byte, 16-len(iv))
	iv = append(padding, iv...) // pad with leading zero bytes to be proper length
	// no guarantee that the files are aligned to multiples of 16 in length...
	// so we still need to advance by seekOffset%16 bytes, within this block
	return iv, seekOffset % 16
}

func RandBytes(length int) []byte {
	result := make([]byte, length)
	_, err := io.ReadFull(rand.Reader, result)
	if err != nil {
		panic(err)
	}
	return result
}
