package crypto

import (
	"bytes"
	"testing"

	"github.com/leijurv/gb/utils"
)

func TestEnc(t *testing.T) {
	data := RandBytes(5021)
	key := RandBytes(16)
	enc := LegacyEncryptDatabase(data, key)
	dec := LegacyDecryptDatabase(enc, key)
	if !bytes.Equal(dec, data) {
		t.Error("Unequal")
	}
}

func TestV2Enc(t *testing.T) {
	data := RandBytes(5021)
	key := RandBytes(16)
	rawDB := utils.NewSHA256HasherSizer()
	var buf bytes.Buffer
	out := EncryptDatabaseV2(&buf, key)
	_, err := out.Write(data)
	if err != nil {
		t.Error(err)
	}
	_, err = rawDB.Write(data)
	if err != nil {
		t.Error(err)
	}
	_, err = out.Write(ComputeMAC(rawDB.Hash(), key))
	if err != nil {
		t.Error(err)
	}

	enc := buf.Bytes()
	if len(enc) != len(data)+16+32 {
		t.Error("wrong length")
	}

	decr := DecryptDatabaseV2(enc, key)
	if !bytes.Equal(decr, data) {
		t.Error("unequal")
	}
}
