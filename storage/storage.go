package storage

import (
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/s3"
	"github.com/leijurv/gb/storage_base"
)

func GetAll() []storage_base.Storage {
	rows, err := db.DB.Query(`SELECT storage_id, type, identifier, root_path FROM storage`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	storages := make([]storage_base.Storage, 0)
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
func StorageDataToStorage(storageID []byte, kind string, identifier string, rootPath string) storage_base.Storage {
	switch kind {
	case "S3":
		return &s3.S3{
			StorageID: storageID,
			Bucket:    identifier,
			RootPath:  rootPath,
		}
	default:
		panic("Unknown storage type " + kind)
	}
}
