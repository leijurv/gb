package s3

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"strconv"
)

type ETagCalculator struct {
	Writer *io.PipeWriter
	Result chan string
}

func CreateETagCalculator() *ETagCalculator {
	reader, writer := io.Pipe()
	result := make(chan string)
	calc := &ETagCalculator{
		Writer: writer,
		Result: result,
	}
	go func() {
		numParts := 0
		var allSums []byte
		for {
			lr := io.LimitReader(reader, s3PartSize)
			h := md5.New()
			// io.Copy's source code actually detects that the src is a LimitReader, BUT it doesn't help since the default size is actually SMALLER than our limit :(
			n, err := io.CopyBuffer(h, lr, make([]byte, s3PartSize))
			if err != nil {
				panic(err) // literally impossible
			}
			if n == 0 {
				break
			}
			allSums = append(allSums, h.Sum(nil)...)
			numParts++
		}
		if numParts == 1 {
			result <- hex.EncodeToString(allSums)
		} else {
			sum := md5.Sum(allSums)
			result <- hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(numParts)
		}
		close(result)
		reader.Close()
	}()
	return calc
}
