package crypto

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
)

func TestVec(t *testing.T) {
	data := []byte("hello world")
	key := make([]byte, 16)
	var buf bytes.Buffer
	w := EncryptBlobWithKey(&buf, 1337, key)
	io.Copy(w, bytes.NewReader(data))
	expected := []byte{0x54, 0x7d, 0x0c, 0x22, 0x3f, 0x87, 0x7a, 0x0f, 0xb4, 0xa1, 0x40}
	if !bytes.Equal(buf.Bytes(), expected) {
		t.Errorf("ciphertext mismatch: got %x, want %x", buf.Bytes(), expected)
	}
}

func TestSeeking(t *testing.T) {
	for _, start_seek := range []int64{0, 1, 2, 3, 14, 15, 16, 17, 253, 254, 255, 256, 257, 258, 1337, 5021, 65534, 65535, 65536, 65537, 65538, 2147483646, 2147483647, 2147483648, 2147483649, 2147483650, 4294967294, 4294967295, 4294967296, 4294967297, 4294967298} {
		data := RandBytes(1234)
		var encBuf bytes.Buffer
		w, key := EncryptBlob(&encBuf, start_seek)
		if _, err := io.Copy(w, bytes.NewBuffer(data)); err != nil {
			t.Error(err)
		}
		enc := encBuf.Bytes()

		for offset := 0; offset <= len(data); offset++ {
			r := DecryptBlobEntry(bytes.NewBuffer(enc[offset:]), int64(offset)+start_seek, key)
			dec, err := ioutil.ReadAll(r)
			if err != nil {
				t.Error(err)
			}
			if !bytes.Equal(dec, data[offset:]) {
				t.Error("Unequal", offset)
			}
		}
	}
}
