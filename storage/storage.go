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

type StorageDescriptor struct {
	StorageID  [32]byte
	Kind       string
	Identifier string
	RootPath   string
}

func GetAll() []storage_base.Storage {
	return ResolveDescriptors(GetAllDescriptors())
}

func ResolveDescriptors(descriptors []StorageDescriptor) []storage_base.Storage {
	storages := make([]storage_base.Storage, 0)
	for _, descriptor := range descriptors {
		storages = append(storages, StorageDataToStorage(descriptor))
	}
	return storages
}

func GetAllDescriptors() []StorageDescriptor {
	rows, err := db.DB.Query(`SELECT storage_id, type, identifier, root_path FROM storage`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	descriptors := make([]StorageDescriptor, 0)
	for rows.Next() {
		var descriptor StorageDescriptor
		var tmpsid []byte
		err := rows.Scan(&tmpsid, &descriptor.Kind, &descriptor.Identifier, &descriptor.RootPath)
		if err != nil {
			panic(err)
		}
		descriptor.StorageID = utils.SliceToArr(tmpsid)
		descriptors = append(descriptors, descriptor)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return descriptors
}

func GetByID(id []byte) storage_base.Storage {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	return cache[utils.SliceToArr(id)]
}

func StorageDataToStorage(descriptor StorageDescriptor) storage_base.Storage {
	cacheLock.Lock()
	defer cacheLock.Unlock()
	_, ok := cache[descriptor.StorageID]
	if !ok {
		cache[descriptor.StorageID] = internalCreateStorage(descriptor.StorageID[:], descriptor.Kind, descriptor.Identifier, descriptor.RootPath)
	}
	return cache[descriptor.StorageID]
}

func NewStorage(kind string, identifier string, rootPath string, label string) storage_base.Storage {
	storageID := crypto.RandBytes(32)
	storage := StorageDataToStorage(StorageDescriptor{
		StorageID:  utils.SliceToArr(storageID),
		Kind:       kind,
		Identifier: identifier,
		RootPath:   rootPath,
	})
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

func NewS3Storage(label string, bucket string, root string, region string, keyid string, secretkey string, endpoint string) {
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
		Endpoint:  endpoint,
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
