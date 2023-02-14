package s3

import (
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
	sess      *session.Session
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
	blobID []byte
	s3     *S3
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
	return &S3{
		StorageID: storageID,
		Data:      *ident,
		RootPath:  rootPath,
		sess: session.Must(session.NewSession(&aws.Config{
			Region:      aws.String(ident.Region),
			Credentials: credentials.NewStaticCredentials(ident.KeyID, ident.SecretKey, ""),
			EndpointResolver: endpoints.ResolverFunc(func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
				if service == endpoints.S3ServiceID {
					return endpoints.ResolvedEndpoint{
						URL:           normalizedEndpoint,
						SigningRegion: ident.Region,
					}, nil
				}
				return endpoints.DefaultResolver().EndpointFor(service, region, optFns...)
			}),
			// I hate Oracle Cloud! I hate Oracle Cloud!
			S3ForcePathStyle: aws.Bool(strings.Contains(normalizedEndpoint, "oraclecloud")),
			// ^ this is needed because of https://stackoverflow.com/questions/55236708/how-to-config-oracle-cloud-certificate
			// without this, it attempts to connect to https://your-bucket-name.abc123redactedabc123.compat.objectstorage.eu-zurich-1.oraclecloud.com
			// AWS and Backblaze B2 can correctly provide a TLS certificate for the bucket-specific subdomain via SNI... but Oracle Cloud cannot
			// so, when requesting that, it provides the default TLS certificate for Oracle's Swift Object Storage
			// which makes GB panic with "x509: certificate is valid for swiftobjectstorage.eu-zurich-1.oraclecloud.com, not your-bucket-name.abc123redactedabc123.compat.objectstorage.eu-zurich-1.oraclecloud.com"
			// this fixes that by forcing the S3 client to use path-style queries (i.e. it will hit "https://s3.amazonaws.com/BUCKET/KEY" instead of the default "https://BUCKET.s3.amazonaws.com/KEY")
			// which Oracle Cloud DOES support SNI for, allowing the API requests to succeed
			Retryer: GbCustomRetryer{
				DefaultRetryer: client.DefaultRetryer{
					NumMaxRetries: 10, // default is 3 retries but Backblaze IS TERRIBLE!!!
					// GbCustomRetrying is a tattletale that reveals that Backblaze is constantly replying with 500 and 503
					// S3 doesn't do it and Oracle Cloud doesn't do it, like, ever. when you're notably worse than Oracle Cloud then you're doing something wrong
					// so we are doing 10 retries now
					// also this https://www.backblaze.com/blog/b2-503-500-server-error/ is COPE
				},
			},
		})),
	}
}

type GbCustomRetryer struct {
	client.DefaultRetryer
}

func (r GbCustomRetryer) ShouldRetry(req *request.Request) bool {
	ret := r.DefaultRetryer.ShouldRetry(req)
	msg := "Retrying"
	if !ret {
		msg = "NOT retrying"
	}
	log.Println(msg, "after attempt number", req.RetryCount+1 /* first failure+retry has RetryCount==0 */, "delay", req.RetryDelay, "because error", req.HTTPResponse.StatusCode, req.Error, "while trying to request", req.HTTPRequest.URL, req.HTTPResponse)
	return ret
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

func (remote *S3) makeUploader() *s3manager.Uploader {
	return s3manager.NewUploader(remote.sess, func(u *s3manager.Uploader) {
		u.PartSize = s3PartSize
	})
}

func (remote *S3) BeginDatabaseUpload(filename string) storage_base.StorageUpload {
	return remote.beginUpload(nil, remote.niceRootPath()+filename)
}

func (remote *S3) BeginBlobUpload(blobID []byte) storage_base.StorageUpload {
	return remote.beginUpload(blobID, remote.niceRootPath()+formatPath(blobID))
}

func (remote *S3) beginUpload(blobIDOptional []byte, path string) *s3Upload {
	log.Println("Path is", path)
	pipeR, pipeW := io.Pipe()
	resultCh := make(chan s3Result)
	go func() {
		defer pipeR.Close()
		result, err := remote.makeUploader().Upload(&s3manager.UploadInput{
			Bucket: aws.String(remote.Data.Bucket),
			Key:    aws.String(path),
			Body:   pipeR,
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
	result, err := s3.New(remote.sess).GetObject(&s3.GetObjectInput{
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
	files := make([]storage_base.UploadedBlob, 0)
	err := s3.New(remote.sess).ListObjectsPages(&s3.ListObjectsInput{
		Bucket: aws.String(remote.Data.Bucket),
		Prefix: aws.String(remote.niceRootPath()),
	},
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			for _, obj := range page.Contents {
				if strings.Contains(*obj.Key, "db-backup-") || strings.Contains(*obj.Key, "db-v2backup-") {
					continue // this is not a blob
				}
				etag := *obj.ETag
				etag = etag[1 : len(etag)-1] // aws puts double quotes around the etag lol
				blobID, err := hex.DecodeString((*obj.Key)[len(remote.RootPath+"XX/XX/"):])
				if err != nil || len(blobID) != 32 {
					panic("Unexpected file not following GB naming convention \"" + *obj.Key + "\"")
				}
				files = append(files, storage_base.UploadedBlob{
					StorageID: remote.StorageID,
					Path:      *obj.Key,
					Checksum:  etag,
					Size:      *obj.Size,
					BlobID:    blobID,
				})
			}
			if !lastPage {
				log.Println("Fetched page from S3. Have", len(files), "blobs so far")
			}
			return true
		})
	if err != nil {
		panic(err)
	}
	log.Println("Listed", len(files), "blobs in S3")
	return files
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
	log.Println("Expecting etag", etag.ETag)
	realEtag, realSize := fetchETagAndSize(up.s3, up.path)
	log.Println("Real etag was", realEtag)
	if etag.ETag != realEtag || etag.Size != realSize {
		panic("aws broke the etag or size lmao")
	}
	return storage_base.UploadedBlob{
		StorageID: up.s3.StorageID,
		BlobID:    up.blobID,
		Path:      up.path,
		Checksum:  etag.ETag,
		Size:      realSize,
	}
}

func fetchETagAndSize(remote *S3, path string) (string, int64) {
	result, err := s3.New(remote.sess).HeadObject(&s3.HeadObjectInput{
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
