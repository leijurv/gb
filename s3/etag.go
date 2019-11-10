package s3

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"strconv"
)

type eTagCalculator struct {
	writer *io.PipeWriter
	result chan string
}

func createETagCalculator() *eTagCalculator {
	reader, writer := io.Pipe()
	result := make(chan string)
	calc := &eTagCalculator{
		writer: writer,
		result: result,
	}
	go func() {
		numParts := 0
		var allSums []byte
		for {
			lr := io.LimitReader(reader, s3PartSize)
			h := md5.New()
			n, err := io.Copy(h, lr)
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
