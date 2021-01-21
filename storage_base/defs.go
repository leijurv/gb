package storage_base

import (
	"io"
	"net/http"
)

// a place where blobs can be stored
type Storage interface {
	BeginBlobUpload(blobID []byte) StorageUpload
	DownloadSection(path string, offset int64, length int64) io.ReadCloser
	DownloadSectionHTTP(path string, offset int64, length int64) *http.Response

	UploadDatabaseBackup(encryptedDatabase []byte, name string)

	// it is like always faster to get a large list of path, checksum, size than to do it one file at a time
	ListBlobs() []UploadedBlob

	Metadata(path string) (string, int64) // checksum (can be empty) and size

	GetID() []byte

	String() string
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
}
