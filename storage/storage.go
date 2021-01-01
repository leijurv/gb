package storage

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"sync"

	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/gdrive"
	"github.com/leijurv/gb/s3"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

var cache = make(map[[32]byte]storage_base.Storage)
var cacheLock sync.Mutex

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

func GetByID(id []byte) storage_base.Storage {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	return cache[utils.SliceToArr(id)]
}

func StorageDataToStorage(storageID []byte, kind string, identifier string, rootPath string) storage_base.Storage {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	arr := utils.SliceToArr(storageID)
	_, ok := cache[arr]
	if !ok {
		cache[arr] = internalCreateStorage(storageID, kind, identifier, rootPath)
	}
	return cache[arr]
}

func NewStorage(kind string, identifier string, rootPath string, label string) storage_base.Storage {
	storageID := crypto.RandBytes(32)
	storage := StorageDataToStorage(storageID, kind, identifier, rootPath)
	if !bytes.Equal(storage.GetID(), storageID) {
		panic("sanity check")
	}
	_, err := db.DB.Exec("INSERT INTO storage (storage_id, type, identifier, root_path, readable_label) VALUES (?, ?, ?, ?, ?)", storageID, kind, identifier, rootPath, label)
	if err != nil {
		panic(err)
	}
	return storage
}

func NewGDriveStorage(label string) {
	identifier, rootPath := gdrive.CreateNewGDriveStorage()
	NewStorage("GDrive", identifier, rootPath, label)
}

func NewS3Storage(label string, bucket string, root string, region string, keyid string, secretkey string) {
	for strings.HasPrefix(root, "/") {
		log.Println("S3 keys shouldn't begin with \"/\" so I'm removing it, edit the database if you're absolutely sure you want that (hint: you don't).")
		root = root[1:]
	}
	if root == "" {
		log.Println("Will write to root of the bucket", bucket)
	} else {
		log.Println("Will write to", root, "in bucket", bucket)
	}
	id, err := json.Marshal(s3.S3DatabaseIdentifier{
		Bucket:    bucket,
		KeyID:     keyid,
		SecretKey: secretkey,
		Region:    region,
	})
	if err != nil {
		panic(err)
	}
	NewStorage("S3", string(id), root, label)
}

func internalCreateStorage(storageID []byte, kind string, identifier string, rootPath string) storage_base.Storage {
	switch kind {
	case "S3":
		return s3.LoadS3StorageInfoFromDatabase(storageID, identifier, rootPath)
	case "GDrive":
		return gdrive.LoadGDriveStorageInfoFromDatabase(storageID, identifier, rootPath)
	default:
		panic("Unknown storage type " + kind)
	}
}
