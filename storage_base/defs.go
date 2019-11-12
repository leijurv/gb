package storage_base

import (
	"io"
)

type Storage interface {
	BeginBlobUpload(blobID []byte) StorageUpload
	DownloadSection(path string, offset int64, length int64) io.ReadCloser
	GetID() []byte
}
type CompletedUpload struct {
	Path     string
	Checksum string
}
type StorageUpload interface {
	Begin() io.Writer
	End() CompletedUpload
}
