package compression

import (
	"io"

	"github.com/leijurv/lepton_jpeg_go/lepton"
)

type LeptonCompression struct{}

func (n *LeptonCompression) Compress(out io.Writer, in io.Reader) error {
	return lepton.Encode(in, out)
}

func (n *LeptonCompression) Decompress(in io.Reader) io.ReadCloser {
	pr, pw := io.Pipe()
	go func() {
		err := lepton.DecodeLepton(in, pw)
		pw.CloseWithError(err)
	}()
	return pr
}

func (n *LeptonCompression) AlgName() string {
	return "lepton"
}

func (n *LeptonCompression) Fallible() bool {
	return true
}

func (n *LeptonCompression) DecompressionTrollBashCommandIncludingThePipe() string {
	return " | lepton -allowprogressive -memory=2048M -threadmemory=256M -"
}
