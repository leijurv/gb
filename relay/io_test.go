package relay

import (
	"bytes"
	"testing"

	"github.com/leijurv/gb/crypto"
)

func TestWrite(t *testing.T) {
	for size := 0; size < 1000; size++ {
		data := crypto.RandBytes(size)

		var buf bytes.Buffer
		writeData(&buf, data)

		written := buf.Bytes()

		read := readData(bytes.NewBuffer(written))
		if !bytes.Equal(read, data) {
			t.Error("Unequal")
		}
	}
}
