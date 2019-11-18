package compression

import (
	"io"

	"github.com/leijurv/gb/utils"
)

type NoCompression struct{}

func (n *NoCompression) Compress(out io.Writer, in io.Reader) error {
	utils.Copy(out, in)
	return nil
}

func (n *NoCompression) Decompress(in io.Reader) io.ReadCloser {
	return utils.ReaderToReadCloser(in) // this gets undone by download.Cat's call to utils.ReadCloserToReader LMFAO
}

func (n *NoCompression) AlgName() string {
	return ""
}

func (n *NoCompression) Fallible() bool {
	return false
}
