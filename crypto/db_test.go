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
	if len(enc) != 16+len(data)+32 {
		t.Error("wrong length")
	}

	decr := DecryptDatabaseV2(enc, key)
	if !bytes.Equal(decr, data) {
		t.Error("unequal")
	}
}

func TestLegacyDecryptCorruptedData(t *testing.T) {
	data := RandBytes(100)
	key := RandBytes(16)
	enc := LegacyEncryptDatabase(data, key)

	corrupted := make([]byte, len(enc))
	copy(corrupted, enc)
	corrupted[len(corrupted)-1] ^= 0xff

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on corrupted GCM data")
		}
	}()
	LegacyDecryptDatabase(corrupted, key)
}

func TestV2DecryptCorruptedMAC(t *testing.T) {
	data := RandBytes(100)
	key := RandBytes(16)
	rawDB := utils.NewSHA256HasherSizer()
	var buf bytes.Buffer
	out := EncryptDatabaseV2(&buf, key)
	out.Write(data)
	rawDB.Write(data)
	out.Write(ComputeMAC(rawDB.Hash(), key))
	enc := buf.Bytes()

	corrupted := make([]byte, len(enc))
	copy(corrupted, enc)
	corrupted[len(corrupted)-1] ^= 0xff

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on corrupted MAC")
		}
	}()
	DecryptDatabaseV2(corrupted, key)
}

func TestV2DecryptWrongKey(t *testing.T) {
	data := RandBytes(100)
	key := RandBytes(16)
	wrongKey := RandBytes(16)
	rawDB := utils.NewSHA256HasherSizer()
	var buf bytes.Buffer
	out := EncryptDatabaseV2(&buf, key)
	out.Write(data)
	rawDB.Write(data)
	out.Write(ComputeMAC(rawDB.Hash(), key))
	enc := buf.Bytes()

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on wrong key")
		}
	}()
	DecryptDatabaseV2(enc, wrongKey)
}

func TestLegacyDecryptWrongKey(t *testing.T) {
	data := RandBytes(100)
	key := RandBytes(16)
	wrongKey := RandBytes(16)
	enc := LegacyEncryptDatabase(data, key)

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on wrong key")
		}
	}()
	LegacyDecryptDatabase(enc, wrongKey)
}
