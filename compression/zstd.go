package compression

import (
	"io"

	"github.com/DataDog/zstd"
	"github.com/leijurv/gb/utils"
)

type ZstdCompression struct{}

func (n *ZstdCompression) Compress(out io.Writer, in io.Reader) error {
	w := zstd.NewWriter(out)
	defer w.Close()
	utils.Copy(w, in)
	return nil
}

func (n *ZstdCompression) Decompress(in io.Reader) io.ReadCloser {
	return zstd.NewReader(in)
}

func (n *ZstdCompression) AlgName() string {
	return "zstd"
}

func (n *ZstdCompression) Fallible() bool {
	return false
}

func (n *ZstdCompression) DecompressionTrollBashCommandIncludingThePipe() string {
	return " | zstd -d"
}
