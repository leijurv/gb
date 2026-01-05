package s3

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

// DANGER DANGER DANGER
// EVEN THOUGH the default and RECOMMENDED part size is 5MB, you SHOULD NOT use that
// B E C A U S E when you transition to Glacier Deep Archive, they will REPACK and RECALCULATE the ETag with a chunk size of 16777216
// seriously, try it. upload a file of >16mb to s3 standard, then transition to deep archive. notice the etag changes
// im mad
const s3PartSize = 1 << 24 // this is 16777216

type S3 struct {
	StorageID []byte
	RootPath  string
	Data      S3DatabaseIdentifier
	client    *s3.Client
}

type s3Result struct {
	result *manager.UploadOutput
	err    error
}

type s3Upload struct {
	calc      *ETagCalculator
	writer    *io.PipeWriter
	result    chan s3Result
	path      string
	blobID    []byte
	s3        *S3
	completed bool
}

type S3DatabaseIdentifier struct {
	Bucket    string `json:"bucket"`
	KeyID     string `json:"aws_access_key_id"`
	SecretKey string `json:"aws_secret_access_key"`
	Region    string `json:"aws_region"`
	Endpoint  string `json:"endpoint"`
}

func LoadS3StorageInfoFromDatabase(storageID []byte, identifier string, rootPath string) storage_base.Storage {
	ident := &S3DatabaseIdentifier{}
	err := json.Unmarshal([]byte(identifier), ident)
	if err != nil {
		log.Println("Identifier was", identifier)
		panic("S3 database identifier is not in JSON format. This is probably not your fault, I had to change the S3 database format to include the AWS region + keys, not just the bucket name. It's JSON now.")
	}
	normalizedEndpoint := ident.Endpoint
	if normalizedEndpoint == "" {
		normalizedEndpoint = "amazonaws.com"
	}
	if !strings.HasPrefix(normalizedEndpoint, "https://") {
		// this works for AWS, and for Backblaze B2 compatibility, allowing you to leave endpoint as blank for AWS, or setting it to "backblazeb2.com" for B2, and allowing it to autofill the region in both cases
		// however, it does NOT work for Oracle Cloud compatibility. for that, you need to define your entire endpoint, including the namespace at the beginning (can be found under Bucket Details)
		// (mine looks like: https://abc123redactedabc123.compat.objectstorage.eu-zurich-1.oraclecloud.com)
		normalizedEndpoint = "https://s3." + ident.Region + "." + normalizedEndpoint + "/"
	}
	if !strings.HasSuffix(normalizedEndpoint, "/") {
		panic("S3 endpoint must end with a slash")
	}
	retryer := retry.NewStandard(func(o *retry.StandardOptions) {
		o.MaxAttempts = 11 // 10 retries + 1 initial attempt
		o.MaxBackoff = 10 * time.Second
		o.Retryables = append(o.Retryables, retry.IsErrorRetryableFunc(func(err error) aws.Ternary {
			// Check if the error is retryable using the default retry logic
			for _, retryable := range retry.DefaultRetryables {
				if result := retryable.IsErrorRetryable(err); result != aws.UnknownTernary {
					if result == aws.TrueTernary {
						log.Println("Retrying because error", err, "is retryable to", normalizedEndpoint)
						return aws.TrueTernary
					}
				}
			}
			log.Println("NOT retrying because error", err, "is not retryable to", normalizedEndpoint)
			return aws.FalseTernary
		}))
	})

	cfg := aws.Config{
		Region:      ident.Region,
		Credentials: credentials.NewStaticCredentialsProvider(ident.KeyID, ident.SecretKey, ""),
		Retryer:     func() aws.Retryer { return retryer },
	}

	if normalizedEndpoint != "https://s3."+ident.Region+".amazonaws.com/" {
		cfg.BaseEndpoint = aws.String(strings.TrimSuffix(normalizedEndpoint, "/"))
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		// I hate Oracle Cloud! I hate Oracle Cloud!
		o.UsePathStyle = strings.Contains(ident.Endpoint, "oraclecloud")
		// ^ this is needed because of https://stackoverflow.com/questions/55236708/how-to-config-oracle-cloud-certificate
		// without this, it attempts to connect to https://your-bucket-name.abc123redactedabc123.compat.objectstorage.eu-zurich-1.oraclecloud.com
		// AWS and Backblaze B2 can correctly provide a TLS certificate for the bucket-specific subdomain via SNI... but Oracle Cloud cannot
		// so, when requesting that, it provides the default TLS certificate for Oracle's Swift Object Storage
		// which makes GB panic with "x509: certificate is valid for swiftobjectstorage.eu-zurich-1.oraclecloud.com, not your-bucket-name.abc123redactedabc123.compat.objectstorage.eu-zurich-1.oraclecloud.com"
		// this fixes that by forcing the S3 client to use path-style queries (i.e. it will hit "https://s3.amazonaws.com/BUCKET/KEY" instead of the default "https://BUCKET.s3.amazonaws.com/KEY")
		// which Oracle Cloud DOES support SNI for, allowing the API requests to succeed
	})

	return &S3{
		StorageID: storageID,
		Data:      *ident,
		RootPath:  rootPath,
		client:    client,
	}
}

func (remote *S3) GetID() []byte {
	return remote.StorageID
}

func (remote *S3) NiceRootPath() string {
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

func (remote *S3) BeginDatabaseUpload(filename string) storage_base.StorageUpload {
	return remote.beginUpload(nil, remote.NiceRootPath()+filename)
}

func (remote *S3) BeginBlobUpload(blobID []byte) storage_base.StorageUpload {
	return remote.beginUpload(blobID, remote.NiceRootPath()+formatPath(blobID))
}

func (remote *S3) beginUpload(blobIDOptional []byte, path string) *s3Upload {
	pipeR, pipeW := io.Pipe()
	resultCh := make(chan s3Result)
	go func() {
		defer pipeR.Close()
		checksumSelection := types.ChecksumAlgorithmSha256
		if strings.Contains(remote.Data.Endpoint, "oraclecloud") || strings.Contains(remote.Data.Endpoint, "backblaze") {
			// the checksum thing is a new feature in v2 of the go sdk. it's not (yet) supported by oracle or backblaze
			checksumSelection = ""
		}
		result, err := manager.NewUploader(remote.client, func(u *manager.Uploader) {
			u.PartSize = s3PartSize
			u.LeavePartsOnError = false // explicitly abort incomplete multipart uploads on error
		}).Upload(context.Background(), &s3.PutObjectInput{
			Bucket:            aws.String(remote.Data.Bucket),
			Key:               aws.String(path),
			Body:              pipeR,
			ChecksumAlgorithm: checksumSelection,
		})
		if err != nil {
			log.Println("s3 error", err)
			pipeR.CloseWithError(err)
		}
		resultCh <- s3Result{result, err}
	}()
	return &s3Upload{
		calc:   CreateETagCalculator(),
		writer: pipeW,
		result: resultCh,
		path:   path,
		s3:     remote,
		blobID: blobIDOptional,
	}
}

func (remote *S3) Metadata(path string) (string, int64) {
	return fetchETagAndSize(remote, path)
}

func (remote *S3) DownloadSection(path string, offset int64, length int64) io.ReadCloser {
	if length == 0 {
		// a range of length 0 is invalid! we get a 400 instead of an empty 200!
		return &utils.EmptyReadCloser{}
	}
	log.Println("S3 key is", path)
	rangeStr := utils.FormatHTTPRange(offset, length)
	log.Println("S3 range is", rangeStr)
	result, err := remote.client.GetObject(context.Background(), &s3.GetObjectInput{
		Bucket: aws.String(remote.Data.Bucket),
		Key:    aws.String(path),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		panic(err)
	}
	return result.Body
}

func (remote *S3) ListBlobs() []storage_base.UploadedBlob {
	log.Println("Listing blobs in", remote)

	resultsCh := make(chan []storage_base.UploadedBlob)

	for prefix := 0; prefix < 256; prefix++ {
		go func(prefix int) {
			workerFiles := make([]storage_base.UploadedBlob, 0)

			paginator := s3.NewListObjectsV2Paginator(remote.client, &s3.ListObjectsV2Input{
				Bucket: aws.String(remote.Data.Bucket),
				Prefix: aws.String(remote.NiceRootPath() + hex.EncodeToString([]byte{byte(prefix)}) + "/"),
			})

			for paginator.HasMorePages() {
				page, err := paginator.NextPage(context.Background())
				if err != nil {
					panic(err)
				}
				for _, obj := range page.Contents {
					if strings.Contains(*obj.Key, "db-backup-") || strings.Contains(*obj.Key, "db-v2backup-") {
						continue // this is not a blob
					}
					etag := *obj.ETag
					etag = etag[1 : len(etag)-1] // aws puts double quotes around the etag lol
					blobID, err := hex.DecodeString((*obj.Key)[len(remote.NiceRootPath()+"XX/XX/"):])
					if err != nil || len(blobID) != 32 {
						panic("Unexpected file not following GB naming convention \"" + *obj.Key + "\"")
					}
					workerFiles = append(workerFiles, storage_base.UploadedBlob{
						StorageID: remote.StorageID,
						Path:      *obj.Key,
						Checksum:  etag,
						Size:      *obj.Size,
						BlobID:    blobID,
					})
				}
			}

			resultsCh <- workerFiles
		}(prefix)
	}

	files := make([]storage_base.UploadedBlob, 0)
	for i := 0; i < 256; i++ {
		workerFiles := <-resultsCh
		files = append(files, workerFiles...)
	}

	log.Println("Listed", len(files), "blobs in S3")
	return files
}

func (remote *S3) DeleteBlob(path string) {
	log.Println("Deleting S3 object at path:", path)
	_, err := remote.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: aws.String(remote.Data.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		panic("Error deleting S3 object: " + err.Error())
	}
	log.Println("Successfully deleted S3 object:", path)
}

func (remote *S3) PresignedURL(path string, expiry time.Duration) (string, error) {
	presignClient := s3.NewPresignClient(remote.client)
	input := &s3.GetObjectInput{
		Bucket: aws.String(remote.Data.Bucket),
		Key:    aws.String(path),
	}
	result, err := presignClient.PresignGetObject(context.Background(), input, s3.WithPresignExpires(expiry))
	if err != nil {
		return "", err
	}
	return result.URL, nil
}

func (remote *S3) String() string {
	return "S3 bucket " + remote.Data.Bucket + " at path " + remote.RootPath + " at endpoint " + remote.Data.Endpoint + " StorageID " + hex.EncodeToString(remote.StorageID[:])
}

func (up *s3Upload) Writer() io.Writer {
	return io.MultiWriter(up.calc.Writer, up.writer)
}

func (up *s3Upload) End() storage_base.UploadedBlob {
	up.writer.Close()
	up.calc.Writer.Close()
	result := <-up.result
	if result.err != nil {
		panic(result.err)
	}
	log.Println("Upload output:", result.result.Location)
	etag := <-up.calc.Result
	realEtag, realSize := fetchETagAndSize(up.s3, up.path)
	if etag.ETag != realEtag || etag.Size != realSize {
		panic("aws broke the etag or size lmao")
	}
	up.completed = true
	up.assertNotPubliclyAccessible(result.result.Location)
	return storage_base.UploadedBlob{
		StorageID: up.s3.StorageID,
		BlobID:    up.blobID,
		Path:      up.path,
		Checksum:  etag.ETag,
		Size:      realSize,
	}
}

func (up *s3Upload) Cancel() {
	if up.completed {
		log.Println("S3 upload already completed, deleting blob at path:", up.path)
		up.s3.DeleteBlob(up.path)
		return
	}
	log.Println("Cancelling S3 upload for path:", up.path)
	up.writer.CloseWithError(errors.New("upload cancelled"))
	up.calc.Writer.Close()
	<-up.result // wait for the upload goroutine to finish (it will error out)
}

func (up *s3Upload) assertNotPubliclyAccessible(url string) {
	resp, err := http.Get(url)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 && resp.StatusCode < 500 { // hilarious: backblaze gives 401, oracle cloud gives 404, AWS S3 gives 403
		// good, bucket is not publicly accessible
		return
	}
	up.s3.DeleteBlob(up.path)
	panic("Your bucket is publicly accessible, probably not a good idea! (Use `gb share` to share files). Got status " + resp.Status + " when fetching " + url)
}

func fetchETagAndSize(remote *S3, path string) (string, int64) {
	result, err := remote.client.HeadObject(context.Background(), &s3.HeadObjectInput{
		Bucket: aws.String(remote.Data.Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		panic(err)
	}
	etag := *result.ETag
	etag = etag[1 : len(etag)-1] // aws puts double quotes around the etag lol
	return etag, *result.ContentLength
}
