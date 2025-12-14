package compression

import (
	"bufio"
	"bytes"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/utils"
)

type Compression interface {
	// these two should only return once they are completely finished
	// behavior: panic on IO error, panic on compression failure if thought to be infallible, return error on failable compression failure
	Compress(out io.Writer, in io.Reader) error

	Decompress(in io.Reader) io.ReadCloser

	AlgName() string

	// can this compression fail if fed unexpected input?
	// a general purpose compression like zstd or xz should return false, since they work on any arbitrary input bytes
	// special purpose compression such as lepton should return true, since it only works on well-formed jpgs
	Fallible() bool

	DecompressionTrollBashCommandIncludingThePipe() string
}

var compressionMap = make(map[string]Compression)

func init() {

	compressions := []Compression{
		&NoCompression{},
		&ZstdCompression{},
	}
	if isLeptonInPath() {
		compressions = append(compressions, &LeptonCompression{})
	}
	for _, c := range compressions {
		n := c.AlgName()
		if _, ok := compressionMap[n]; ok {
			panic("duplicate alg name " + n)
		}
		compressionMap[n] = c
	}
}

func CheckCompression() {
	if !isLeptonInPath() && !config.Config().DisableLepton {
		panic("lepton is not installed to your $PATH. gb uses https://github.com/dropbox/lepton/ to losslessly compress JPG files. if you'd rather not bother, set `\"disable_lepton\": true` in your .gb.conf")
	}
}

func isLeptonInPath() bool {
	_, err := exec.LookPath("lepton")
	return err == nil
}

func ByAlgName(algName string) Compression {
	if algName == "lepton" && config.Config().DisableLepton {
		panic("lepton has been disabled in your .gb.conf, it must be reenabled before i can decompress a file compressed using lepton")
	}
	// map is only written to on init, so no need to synchronize on read
	return compressionMap[algName]
}

func SelectCompressionForPath(path string) []Compression {
	path = strings.ToLower(path)
	stat, err := os.Stat(path)
	if err == nil && stat.Size() < config.Config().MinCompressSize {
		return []Compression{&NoCompression{}}
	}
	if !config.Config().DisableLepton && (strings.HasSuffix(path, ".jpg") || strings.HasSuffix(path, ".jpeg")) {
		return []Compression{&LeptonCompression{}, &NoCompression{}}
	}
	for _, ext := range config.Config().NoCompressionExts {
		if strings.HasSuffix(path, "."+ext) {
			return []Compression{&NoCompression{}}
		}
	}
	return []Compression{&ZstdCompression{}, &NoCompression{}}
}

func Compress(compOptions []Compression, out io.Writer, in io.Reader, hs *utils.HasherSizer) string {
	var inData []byte
	buffered := false
	for _, c := range compOptions {
		if c.Fallible() {
			if !buffered {
				var inBuf bytes.Buffer
				utils.Copy(&inBuf, in)
				inData = inBuf.Bytes() // buffer is not reusable
				buffered = true
			}
			var outBuf bytes.Buffer
			err := c.Compress(&outBuf, bytes.NewReader(inData))
			if err != nil {
				log.Println(c.AlgName(), "compression FAILED due to", err, "so FALLING BACK to next compression option")
				continue
			}
			outData := outBuf.Bytes()
			verify := utils.NewSHA256HasherSizer()
			d := c.Decompress(bytes.NewReader(outData))
			defer d.Close()
			utils.Copy(&verify, d)
			if !bytes.Equal(verify.Hash(), hs.Hash()) {
				log.Println(verify.Hash(), verify.Size(), hs.Hash(), hs.Size())
				panic("compression CLAIMED it succeeded but decompressed to DIFFERENT DATA this is VERY BAD")
			}
			if len(outData) > len(inData) {
				log.Println("Falling back to next compression option. Compression", c.AlgName(), "actually made the file LARGER, from", len(inData), "bytes to", len(outData), "bytes")
				continue
			}
			// success!
			utils.Copy(out, bytes.NewReader(outData))
			return c.AlgName()
		} else {
			// infallible
			var read io.Reader // the data to compress, whether we've buffered it already or not
			if buffered {
				read = bytes.NewReader(inData)
			} else {
				read = in
			}
			VerifiedCompression(c, out, read, hs)
			log.Println("Compression verified")

			return c.AlgName()
		}
	}
	panic("this should never happen, at least NoCompression should run on every possible file")
}

// compress data while also verifying that the stream will decompress back to the same data
// but without buffering - do the whole thing streaming
func VerifiedCompression(c Compression, out io.Writer, read io.Reader, hs *utils.HasherSizer) {
	pR, pW := io.Pipe()
	verify := utils.NewSHA256HasherSizer()
	done := make(chan struct{})
	go func() {
		decom := c.Decompress(pR)
		defer decom.Close()
		utils.Copy(&verify, decom) // this only returns once decom is EOF, which only happens strictly after pW.Close(), so this is correct
		done <- struct{}{}
	}()

	out = io.MultiWriter(out, pW)
	// wow infallible compression is so much easier wow
	bufout := bufio.NewWriterSize(out, 128*1024) // 128kb, because zstd sometimes writes in small chunks
	err := c.Compress(bufout, read)
	if err != nil {
		log.Println("you are infallible you cannot fail :cry:")
		panic(err)
	}
	err = bufout.Flush()
	if err != nil {
		panic(err)
	}
	pW.Close()
	<-done
	if !bytes.Equal(verify.Hash(), hs.Hash()) {
		log.Println(verify.Hash(), verify.Size(), hs.Hash(), hs.Size())
		panic("compression CLAIMED it succeeded but decompressed to DIFFERENT DATA this is VERY BAD")
	}
}
