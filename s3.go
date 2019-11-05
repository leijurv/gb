package main

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var AWSSession = session.Must(session.NewSession(&aws.Config{Region: aws.String("us-west-1")}))

const s3PartSize = 5 * 1024 * 1024

type S3 struct {
	storageID []byte
	bucket    string
	rootPath  string
}

type s3Result struct {
	result *s3manager.UploadOutput
	err    error
}

type s3Upload struct {
	calc   *ETagCalculator
	writer *io.PipeWriter
	result chan s3Result
	path   string
	s3     *S3
}

type ETagCalculator struct {
	writer *io.PipeWriter
	result chan string
}

func (remote *S3) GetID() []byte {
	return remote.storageID
}

func (remote *S3) niceRootPath() string {
	path := remote.rootPath
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return path
}

func formatPath(blobID []byte) string {
	if len(blobID) != 32 {
		panic(len(blobID))
	}
	h := hex.EncodeToString(blobID)
	return h[:2] + "/" + h[2:4] + "/" + h
}

func (remote *S3) BeginBlobUpload(blobID []byte) StorageUpload {
	path := remote.niceRootPath() + formatPath(blobID)
	log.Println("Path is", path)
	pipeR, pipeW := io.Pipe()
	uploader := s3manager.NewUploader(AWSSession, func(u *s3manager.Uploader) {
		u.PartSize = s3PartSize
	})
	resultCh := make(chan s3Result)
	go func() {
		result, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(remote.bucket),
			Key:    aws.String(path),
			Body:   pipeR,
		})
		if err != nil {
			log.Println("s3 error", err)
		}
		pipeR.Close()
		resultCh <- s3Result{result, err}
	}()
	return &s3Upload{
		calc:   CreateETagCalculator(),
		writer: pipeW,
		result: resultCh,
		path:   path,
		s3:     remote,
	}
}

func (remote *S3) DownloadSection(blobID []byte, offset int64, length int64) io.Reader {
	path := remote.niceRootPath() + formatPath(blobID)
	log.Println("S3 key is", path)
	rangeStr := "bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+length-1, 10)
	log.Println("S3 range is", rangeStr)
	result, err := s3.New(AWSSession).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(remote.bucket),
		Key:    aws.String(path),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		panic(err)
	}
	return result.Body
}

func (up *s3Upload) Begin() io.Writer {
	return io.MultiWriter(up.calc.writer, up.writer)
}

func (up *s3Upload) End() CompletedUpload {
	up.writer.Close()
	up.calc.writer.Close()
	result := <-up.result
	if result.err != nil {
		panic(result.err)
	}
	log.Println("Upload output", result.result.Location)
	etag := <-up.calc.result
	log.Println("Expecting etag", etag)
	real := checkETag(up.s3.bucket, up.path)
	log.Println("Real etag was", real)
	if etag != real {
		panic("aws broke the etag lmao")
	}
	return CompletedUpload{
		path:     up.path,
		checksum: etag,
	}
}

func checkETag(bucket string, path string) string {
	result, err := s3.New(AWSSession).HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		panic(err)
	}
	ret := *result.ETag
	return ret[1 : len(ret)-1] // aws puts double quotes around the etag lol
}

func CreateETagCalculator() *ETagCalculator {
	reader, writer := io.Pipe()
	result := make(chan string)
	calc := &ETagCalculator{
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
			h := md5.New()
			_, err := h.Write(allSums)
			if err != nil {
				panic(err) // lmao?
			}
			result <- hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(numParts)
		}
		close(result)
		reader.Close()
	}()
	return calc
}
