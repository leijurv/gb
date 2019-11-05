package main

import (
	"database/sql"
	"io"
)

type Storage interface {
	BeginBlobUpload(blobID []byte) StorageUpload
	DownloadSection(blobID []byte, offset int64, length int64) io.Reader
	GetID() []byte
}
type CompletedUpload struct {
	path     string
	checksum string
}
type StorageUpload interface {
	Begin() io.Writer
	End() CompletedUpload
}

func GetAll(tx *sql.Tx) []Storage {
	rows, err := tx.Query(`SELECT storage_id, type, identifier, root_path FROM storage`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	storages := make([]Storage, 0)
	for rows.Next() {
		var storageID []byte
		var kind string // owo
		var identifier string
		var rootPath string
		err := rows.Scan(&storageID, &kind, &identifier, &rootPath)
		if err != nil {
			panic(err)
		}
		storages = append(storages, StorageDataToStorage(storageID, kind, identifier, rootPath))
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return storages
}
func StorageDataToStorage(storageID []byte, kind string, identifier string, rootPath string) Storage {
	switch kind {
	case "S3":
		return &S3{
			storageID: storageID,
			bucket:    identifier,
			rootPath:  rootPath,
		}
	default:
		panic("Unknown storage type " + kind)
	}
}
