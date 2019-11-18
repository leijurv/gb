package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"io"
	"strconv"
	"sync/atomic"
)

func SliceToArr(in []byte) [32]byte {
	if len(in) != 32 {
		panic("database gave invalid row??")
	}
	var result [32]byte
	copy(result[:], in)
	return result
}

type HasherSizer struct {
	size   int64
	hasher hash.Hash
}

func (hs *HasherSizer) Write(p []byte) (int, error) {
	n := len(p)
	atomic.AddInt64(&hs.size, int64(n))
	return hs.hasher.Write(p)
}

func (hs *HasherSizer) HashAndSize() ([]byte, int64) {
	return hs.hasher.Sum(nil), hs.Size()
}

func (hs *HasherSizer) Hash() []byte {
	return hs.hasher.Sum(nil)
}

func (hs *HasherSizer) Size() int64 {
	return atomic.LoadInt64(&hs.size)
}

func NewSHA256HasherSizer() HasherSizer {
	return HasherSizer{0, sha256.New()}
}

func NewMD5HasherSizer() HasherSizer {
	return HasherSizer{0, md5.New()}
}

type EmptyReadCloser struct{}

func (erc *EmptyReadCloser) Close() error {
	return nil
}
func (erc *EmptyReadCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

// do you find it annoying to have to close your readers? this function is for you
func ReadCloserToReader(in io.ReadCloser) io.Reader {
	frc, ok := in.(*fakeReadCloser)
	if ok {
		return frc.r
	}
	pipeR, pipeW := io.Pipe()
	go func() {
		defer in.Close()
		_, err := io.CopyBuffer(pipeW, in, make([]byte, 1024*1024)) // we're working with huge files, 1MB buffer is more reasonable than 32KB default
		pipeW.CloseWithError(err)                                   // nil is nil, error is error. this works properly
	}()
	return pipeR
}

func ReaderToReadCloser(in io.Reader) io.ReadCloser {
	rc, ok := in.(io.ReadCloser)
	if ok {
		return rc
	}
	return &fakeReadCloser{in}
}

type fakeReadCloser struct {
	r io.Reader
}

func (frc *fakeReadCloser) Read(data []byte) (int, error) {
	return frc.r.Read(data)
}

func (frc *fakeReadCloser) Close() error {
	return nil
}

func FormatHTTPRange(offset int64, length int64) string {
	return "bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+length-1, 10)
}

func Copy(out io.Writer, in io.Reader) {
	_, err := io.CopyBuffer(out, in, make([]byte, 1024*1024))
	if err != nil {
		panic(err)
	}
}
