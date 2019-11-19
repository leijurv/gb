package crypto

import (
	"bytes"
	"testing"
)

func TestEnc(t *testing.T) {
	data := RandBytes(5021)
	key := RandBytes(16)
	enc := EncryptDatabase(data, key)
	dec := DecryptDatabase(enc, key)
	if !bytes.Equal(dec, data) {
		t.Error("Unequal")
	}
}
