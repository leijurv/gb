package s3

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"strconv"
)

type ETagResult struct {
	ETag string
	Size int64
}

type ETagCalculator struct {
	Writer *io.PipeWriter
	Result chan ETagResult
}

func CreateETagCalculator() *ETagCalculator {
	reader, writer := io.Pipe()
	result := make(chan ETagResult)
	calc := &ETagCalculator{
		Writer: writer,
		Result: result,
	}
	go func() {
		numParts := 0
		var allSums []byte
		var totalSz int64
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
			totalSz += n
			numParts++
		}
		if numParts == 1 {
			result <- ETagResult{hex.EncodeToString(allSums), totalSz}
		} else {
			sum := md5.Sum(allSums)
			result <- ETagResult{hex.EncodeToString(sum[:]) + "-" + strconv.Itoa(numParts), totalSz}
		}
		close(result)
		reader.Close()
	}()
	return calc
}
