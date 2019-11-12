package s3

import (
	"encoding/hex"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

var AWSSession = session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))

const s3PartSize = 5 * 1024 * 1024

type S3 struct {
	StorageID []byte
	Bucket    string
	RootPath  string
}

type s3Result struct {
	result *s3manager.UploadOutput
	err    error
}

type s3Upload struct {
	calc   *eTagCalculator
	writer *io.PipeWriter
	result chan s3Result
	path   string
	s3     *S3
}

func (remote *S3) GetID() []byte {
	return remote.StorageID
}

func (remote *S3) niceRootPath() string {
	path := remote.RootPath
	if path != "" && !strings.HasSuffix(path, "/") {
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

func (remote *S3) BeginBlobUpload(blobID []byte) storage_base.StorageUpload {
	path := remote.niceRootPath() + formatPath(blobID)
	log.Println("Path is", path)
	pipeR, pipeW := io.Pipe()
	uploader := s3manager.NewUploader(AWSSession, func(u *s3manager.Uploader) {
		u.PartSize = s3PartSize
	})
	resultCh := make(chan s3Result)
	go func() {
		defer pipeR.Close()
		result, err := uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(remote.Bucket),
			Key:    aws.String(path),
			Body:   pipeR,
		})
		if err != nil {
			log.Println("s3 error", err)
		}
		resultCh <- s3Result{result, err}
	}()
	return &s3Upload{
		calc:   createETagCalculator(),
		writer: pipeW,
		result: resultCh,
		path:   path,
		s3:     remote,
	}
}

func (remote *S3) DownloadSection(path string, offset int64, length int64) io.ReadCloser {
	if length == 0 {
		// a range of length 0 is invalid! we get a 400 instead of an empty 200!
		return &utils.EmptyReadCloser{}
	}
	log.Println("S3 key is", path)
	rangeStr := "bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+length-1, 10)
	log.Println("S3 range is", rangeStr)
	result, err := s3.New(AWSSession).GetObject(&s3.GetObjectInput{
		Bucket: aws.String(remote.Bucket),
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

func (up *s3Upload) End() storage_base.CompletedUpload {
	up.writer.Close()
	up.calc.writer.Close()
	result := <-up.result
	if result.err != nil {
		panic(result.err)
	}
	log.Println("Upload output", result.result.Location)
	etag := <-up.calc.result
	log.Println("Expecting etag", etag)
	real := fetchETag(up.s3.Bucket, up.path)
	log.Println("Real etag was", real)
	if etag != real {
		panic("aws broke the etag lmao")
	}
	return storage_base.CompletedUpload{
		Path:     up.path,
		Checksum: etag,
	}
}

func fetchETag(bucket string, path string) string {
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
