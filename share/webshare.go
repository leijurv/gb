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

const DefaultWebShareBaseURL = "https://leijurv.github.io/gb/share/"

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
	if cfShare && cfg.ShortUrlShareBaseURL == "" {
		log.Println("You need to set `short_url_share_base_url` in your .gb.conf to use --cf-worker mode")
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

	params := map[string]string{
		"name":   sharedName,
		"key":    hex.EncodeToString(key),
		"offset": fmt.Sprintf("%d", offset),
		"length": fmt.Sprintf("%d", length),
		"size":   fmt.Sprintf("%d", originalSize),
		"sha256": base64.RawURLEncoding.EncodeToString(hash),
		"cmp":    compressionAlg,
	}

	var shareURL string
	if cfShare {
		shareURL = generateCFWorkerURL(stor, cfg, params, pathInStorage)
	} else {
		shareURL = generatePresignedURL(stor, params, expiry, pathInStorage)
	}

	log.Println()
	log.Printf("File: %s", sharedName)
	log.Printf("Size: %s uncompressed, %s compressed", utils.FormatCommas(originalSize), utils.FormatCommas(length))
	log.Printf("Compression: %s", compressionAlg)
	if !cfShare {
		log.Printf("URL EXPIRES: %s", time.Now().Add(expiry).Format(time.RFC3339))
	}
	fmt.Println()
	fmt.Println(shareURL)
	fmt.Println()
}

func generatePresignedURL(stor storage_base.Storage, params map[string]string, expiry time.Duration, pathInStorage string) string {
	presignedURL, err := stor.PresignedURL(pathInStorage, expiry)
	if err != nil {
		panic(fmt.Sprintf("Cannot generate presigned URL for this storage: %v", err))
	}
	params["url"] = presignedURL

	url_params := url.Values{}
	for k, v := range params {
		url_params.Set(k, v)
	}

	return DefaultWebShareBaseURL + "#" + url_params.Encode()
}

func generateCFWorkerURL(stor storage_base.Storage, cfg config.ConfigData, params map[string]string, pathInStorage string) string {
	params["path"] = pathInStorage
	jsonData, err := json.Marshal(params)
	if err != nil {
		panic(err)
	}

	password := generatePassword(cfg.ShortUrlSharePasswordLength)

	uploadPath := "share/" + password + ".json"

	upload := stor.BeginDatabaseUpload(uploadPath)
	_, err = upload.Writer().Write(jsonData)
	if err != nil {
		panic(err)
	}
	upload.End()

	baseURL := cfg.ShortUrlShareBaseURL
	for strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL[:len(baseURL)-1]
	}
	urlFriendlyName := strings.Replace(params["name"], " ", "_", -1)
	urlFriendlyName = url.PathEscape(urlFriendlyName) // might not actually be necessary
	return fmt.Sprintf("%s/%s/%s", baseURL, password, urlFriendlyName)
}
