package storage_base

import (
	"io"
	"time"
)

// a place where blobs can be stored
type Storage interface {
	BeginBlobUpload(blobID []byte) StorageUpload
	BeginDatabaseUpload(filename string) StorageUpload
	DownloadSection(path string, offset int64, length int64) io.ReadCloser

	// it is like always faster to get a large list of path, checksum, size than to do it one file at a time
	ListBlobs() []UploadedBlob

	// list files with a given prefix (e.g., "share/")
	ListPrefix(prefix string) []ListedFile

	Metadata(path string) (string, int64) // checksum (can be empty) and size

	// delete a blob by its path
	DeleteBlob(path string)

	// generate a presigned URL for downloading a blob (use Range header with curl for sections)
	// returns empty string and error if not supported (e.g., Google Drive)
	PresignedURL(path string, expiry time.Duration) (string, error)

	GetID() []byte

	String() string
}

// a file listed from storage
type ListedFile struct {
	Path     string    // full path (for S3) or file ID (for GDrive)
	Name     string    // filename without the prefix (e.g., "abc123.json" from "share/abc123.json")
	Size     int64
	Modified time.Time
}

// metadata about a blob that has been successfully uploaded
// can be either immediately after an upload, or later on while listing
// therefore: should not rely on data that is only provided on a completed upload
type UploadedBlob struct {
	StorageID []byte
	BlobID    []byte // nil if fetched from a list operation, has data if fetched from database
	Path      string
	Checksum  string
	Size      int64
}

// an upload in progress
type StorageUpload interface {
	// simply calling BeginBlobUpload has already created the writer, this simply retrieves it
	Writer() io.Writer

	// flush and close the upload, **verify integrity by comparing the checksum**, then return the data
	End() UploadedBlob

	// abort the upload without completing it (e.g., for incomplete multipart uploads)
	Cancel()
}
