package crypto

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
)

func TestSeeking(t *testing.T) {
	data := RandBytes(1234)
	var encBuf bytes.Buffer
	w, key := EncryptBlob(&encBuf)
	if _, err := io.Copy(w, bytes.NewBuffer(data)); err != nil {
		t.Error(err)
	}
	enc := encBuf.Bytes()

	for offset := 0; offset < len(data); offset++ {
		r := DecryptBlobEntry(bytes.NewBuffer(enc[offset:]), int64(offset), key)
		dec, err := ioutil.ReadAll(r)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(dec, data[offset:]) {
			t.Error("Unequal", offset)
		}
	}
}
