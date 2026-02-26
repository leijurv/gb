package storage

import (
	"bytes"
	"encoding/json"
	"log"
	"strings"
	"sync"

	"github.com/leijurv/gb/config"
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
	db.Must(err)
	defer rows.Close()
	descriptors := make([]StorageDescriptor, 0)
	for rows.Next() {
		var descriptor StorageDescriptor
		var tmpsid []byte
		db.Must(rows.Scan(&tmpsid, &descriptor.Kind, &descriptor.Identifier, &descriptor.RootPath))
		descriptor.StorageID = utils.SliceToArr(tmpsid)
		descriptors = append(descriptors, descriptor)
	}
	db.Must(rows.Err())
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
	db.Must(err)
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

func RegisterMockStorage(stor storage_base.Storage, label string) {
	storageID := stor.GetID()
	cacheLock.Lock()
	cache[utils.SliceToArr(storageID)] = stor
	cacheLock.Unlock()
	_, err := db.DB.Exec("INSERT INTO storage (storage_id, type, identifier, root_path, readable_label) VALUES (?, ?, ?, ?, ?)", storageID, "Mock", "mock-identifier", "/mock", label)
	db.Must(err)
}

func ClearCache() {
	cacheLock.Lock()
	cache = make(map[[32]byte]storage_base.Storage)
	cacheLock.Unlock()
}

func storageSelectPrintOptions() {
	descs := GetAllDescriptors()
	log.Println("Options:")
	for _, d := range descs {
		var label string
		db.Must(db.DB.QueryRow("SELECT readable_label FROM storage WHERE storage_id = ?", d.StorageID[:]).Scan(&label))
		log.Println("•", d.Kind, d.RootPath, "To use this one, add the option `--label=\""+label+"\"`")
	}
}

func StorageSelect(label string) (storage_base.Storage, bool) {
	if label == "" && config.Config().DefaultStorage != "" {
		label = config.Config().DefaultStorage
		log.Println("Using default storage from config:", label)
	}
	if label == "" {
		descs := GetAllDescriptors()
		if len(descs) == 1 {
			log.Println("Auto-selecting the only storage available")
			return StorageDataToStorage(descs[0]), true
		}
		log.Println("First, we need to pick a storage to fetch em from")
		storageSelectPrintOptions()
		return nil, false
	}
	GetAll()
	var storageID []byte
	err := db.DB.QueryRow("SELECT storage_id FROM storage WHERE readable_label = ?", label).Scan(&storageID)
	if err != nil {
		log.Println("No storage found with label:", label)
		storageSelectPrintOptions()
		return nil, false
	}
	storage := GetByID(storageID)
	log.Println("Using storage:", storage)
	return storage, true
}
