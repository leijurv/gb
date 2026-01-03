package share

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

const DefaultWebShareBaseURL = "https://leijurv.github.io/gb/share/share.html"

// CFShareMetadata is the JSON structure uploaded to storage for CF Worker shares
type CFShareMetadata struct {
	Name   string `json:"name"`
	Key    string `json:"key"`
	Offset int64  `json:"offset"`
	Length int64  `json:"length"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
	Cmp    string `json:"cmp"`
	Path   string `json:"path"`
}

// generatePassword creates a random alphanumeric password of the given length
func generatePassword(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]uint8, length)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	for i := range b {
		b[i] = charset[int(b[i])%len(charset)]
	}
	return string(b)
}

func WebShare(pathOrHash string, overrideName string, label string, expiry time.Duration) {
	webShareInternal(pathOrHash, overrideName, label, expiry, false)
}

func CFWorkerShare(pathOrHash string, overrideName string, label string) {
	webShareInternal(pathOrHash, overrideName, label, 0, true)
}

func webShareInternal(pathOrHash string, overrideName string, label string, expiry time.Duration, cfShare bool) {
	hash, sharedName := ResolvePathOrHash(pathOrHash, overrideName)

	stor, ok := storage.StorageSelect(label)
	if !ok {
		return
	}

	cfg := config.Config()
	if cfShare && cfg.CFShareBaseURL == "" {
		log.Println("You need to set `cf_share_base_url` in your .gb.conf to use --cf-worker mode")
		log.Println("This should be the base URL of your Cloudflare Worker, e.g. https://share.example.com")
		return
	}

	// Find the blob entry for this hash in the selected storage
	row := db.DB.QueryRow(`
		SELECT
			blob_entries.blob_id,
			blob_entries.offset,
			blob_entries.final_size,
			blob_entries.compression_alg,
			blob_entries.encryption_key,
			blob_storage.path,
			sizes.size,
			(SELECT COUNT(*) FROM blob_entries sibling WHERE sibling.blob_id = blob_entries.blob_id AND sibling.encryption_key = blob_entries.encryption_key) AS shared_key_count
		FROM blob_entries
			INNER JOIN blobs ON blobs.blob_id = blob_entries.blob_id
			INNER JOIN blob_storage ON blob_storage.blob_id = blobs.blob_id
			INNER JOIN sizes ON sizes.hash = blob_entries.hash
		WHERE blob_entries.hash = ? AND blob_storage.storage_id = ?
		LIMIT 1
	`, hash, stor.GetID())

	var blobID []byte
	var offset, length, originalSize int64
	var compressionAlg string
	var key []byte
	var pathInStorage string
	var sharedKeyCount int
	err := row.Scan(&blobID, &offset, &length, &compressionAlg, &key, &pathInStorage, &originalSize, &sharedKeyCount)
	if err != nil {
		panic(err)
	}

	if sharedKeyCount > 1 {
		log.Printf("Unfortunately this file was backed up with an older version of gb that shared encryption keys across distinct files that were backed up at one time (into a single blob). To fix this for just this blob, you can run `echo %s | gb repack`. To fix this for all blobs, you can run `gb upgrade-encryption`. Then rerun this command to securely share just this file.\n", hex.EncodeToString(blobID))
		return
	}

	var shareURL string
	if cfShare {
		shareURL = generateCFWorkerURL(stor, cfg, hash, sharedName, key, offset, length, originalSize, compressionAlg, pathInStorage)
	} else {
		shareURL = generatePresignedURL(stor, hash, sharedName, key, offset, length, originalSize, compressionAlg, pathInStorage, expiry)
	}

	log.Println()
	log.Printf("File: %s", sharedName)
	log.Printf("Size: %s uncompressed, %s compressed", utils.FormatCommas(originalSize), utils.FormatCommas(length))
	log.Printf("Compression: %s", compressionAlg)
	if !cfShare {
		log.Printf("URL expires: %s", time.Now().Add(expiry).Format(time.RFC3339))
	}
	log.Println()
	fmt.Println(shareURL)
}

func generatePresignedURL(stor storage_base.Storage, hash []byte, sharedName string, key []byte, offset, length, originalSize int64, compressionAlg, pathInStorage string, expiry time.Duration) string {
	presignedURL, err := stor.PresignedURL(pathInStorage, expiry)
	if err != nil {
		panic(fmt.Sprintf("Cannot generate presigned URL for this storage: %v", err))
	}

	params := url.Values{}
	params.Set("name", sharedName)
	params.Set("url", presignedURL)
	params.Set("key", hex.EncodeToString(key))
	params.Set("offset", fmt.Sprintf("%d", offset))
	params.Set("length", fmt.Sprintf("%d", length))
	params.Set("size", fmt.Sprintf("%d", originalSize))
	params.Set("sha256", base64.RawURLEncoding.EncodeToString(hash))
	params.Set("cmp", compressionAlg)

	return DefaultWebShareBaseURL + "#" + params.Encode()
}

func generateCFWorkerURL(stor storage_base.Storage, cfg config.ConfigData, hash []byte, sharedName string, key []byte, offset, length, originalSize int64, compressionAlg, pathInStorage string) string {
	metadata := CFShareMetadata{
		Name:   sharedName,
		Key:    hex.EncodeToString(key),
		Offset: offset,
		Length: length,
		Size:   originalSize,
		SHA256: base64.RawURLEncoding.EncodeToString(hash),
		Cmp:    compressionAlg,
		Path:   pathInStorage,
	}

	jsonData, err := json.Marshal(metadata)
	if err != nil {
		panic(err)
	}

	password := generatePassword(cfg.CFSharePasswordLength)

	uploadPath := "share/" + password + ".json"
	log.Printf("Uploading share metadata to %s", uploadPath)

	upload := stor.BeginDatabaseUpload(uploadPath)
	_, err = upload.Writer().Write(jsonData)
	if err != nil {
		panic(err)
	}
	result := upload.End()
	log.Printf("Uploaded %d bytes to %s", result.Size, result.Path)

	baseURL := cfg.CFShareBaseURL
	for strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL[:len(baseURL)-1]
	}
	return fmt.Sprintf("%s/%s/%s", baseURL, password, sharedName)
}
