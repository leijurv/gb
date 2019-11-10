package storage_base

import (
	"io"
)

type Storage interface {
	BeginBlobUpload(blobID []byte) StorageUpload
	DownloadSection(blobID []byte, offset int64, length int64) io.Reader
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
