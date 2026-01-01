package share

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/utils"
)

const DefaultWebShareBaseURL = "https://leijurv.github.io/gb/share/share.html"

func WebShare(pathOrHash string, overrideName string, label string, expiry time.Duration) {
	hash, sharedName := ResolvePathOrHash(pathOrHash, overrideName)

	stor, ok := storage.StorageSelect(label)
	if !ok {
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

	shareURL := DefaultWebShareBaseURL + "#" + params.Encode()

	log.Println()
	log.Printf("File: %s", sharedName)
	log.Printf("Size: %s uncompressed, %s compressed", utils.FormatCommas(originalSize), utils.FormatCommas(length))
	log.Printf("Compression: %s", compressionAlg)
	log.Printf("URL expires: %s", time.Now().Add(expiry).Format(time.RFC3339))
	log.Println()
	fmt.Println(shareURL)
}
