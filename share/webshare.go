package share

import (
	"bufio"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

const DefaultWebShareBaseURL = "https://leijurv.github.io/gb/webshare/"

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

func ParameterizedShare(pathOrHash string, overrideName string, label string, expiry time.Duration) {
	// For parametrized mode, empty expiry defaults to 7 days
	if expiry == 0 {
		expiry = 7 * 24 * time.Hour
	}
	webShareInternal([]string{pathOrHash}, overrideName, label, expiry, false)
}

func PasswordUrlShare(inputs []string, overrideName string, label string, expiry time.Duration) {
	// For password mode, empty expiry means no expiry
	webShareInternal(inputs, overrideName, label, expiry, true)
}

func isHash(str string) bool {
	hash, err := hex.DecodeString(str)
	return err == nil && len(hash) == 32
}

func verifySingleHashInput(inputs []string) {
	if len(inputs) <= 1 {
		return
	}
	for _, input := range inputs {
		if isHash(input) {
			panic("When sharing a file by hash only 1 input can be provided")
		}
	}
}

// commonPath returns the common path prefix shared by all paths in the array.
// Returns empty string if the array is empty or no common path exists.
func commonPath0(paths []string) string {
	if len(paths) == 0 {
		return ""
	}

	if len(paths) == 1 {
		return filepath.Dir(paths[0])
	}

	// Split all paths into components
	splitPaths := make([][]string, len(paths))
	for i, path := range paths {
		// Clean the path first to normalize it
		cleanPath := filepath.Clean(path)
		// Split by separator
		splitPaths[i] = strings.Split(cleanPath, string(filepath.Separator))
	}

	// Find the minimum length to avoid index out of bounds
	minLen := len(splitPaths[0])
	for _, sp := range splitPaths[1:] {
		if len(sp) < minLen {
			minLen = len(sp)
		}
	}

	// Find common components, but stop before the last component
	// (since we want the directory, not the file)
	var common []string
	for i := 0; i < minLen-1; i++ { // Changed: minLen-1 instead of minLen
		component := splitPaths[0][i]
		allMatch := true

		for j := 1; j < len(splitPaths); j++ {
			if splitPaths[j][i] != component {
				allMatch = false
				break
			}
		}

		if allMatch {
			common = append(common, component)
		} else {
			break
		}
	}

	if len(common) == 0 {
		return ""
	}

	// Join the common components back together
	return strings.Join(common, string(filepath.Separator))
}

func commonPath(entries []entry) string {
	paths := []string{}
	for _, e := range entries {
		paths = append(paths, e.path)
	}
	return commonPath0(paths)
}

type entry struct {
	hash []byte
	path string
}

func generateParams(e entry, stor storage_base.Storage) map[string]string {
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
	`, e.hash, stor.GetID())

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
		log.Printf("Unfortunately this file (%s) was backed up with an older version of gb that shared encryption keys across distinct files that were backed up at one time (into a single blob). To fix this for just this blob, you can run `echo %s | gb repack`. To fix this for all blobs, you can run `gb upgrade-encryption`. Then rerun this command to securely share just this file.\n", e.path, hex.EncodeToString(blobID))
		os.Exit(1)
	}

	params := map[string]string{
		"name":   e.path,
		"key":    hex.EncodeToString(key),
		"offset": fmt.Sprintf("%d", offset),
		"length": fmt.Sprintf("%d", length),
		"size":   fmt.Sprintf("%d", originalSize),
		"sha256": base64.RawURLEncoding.EncodeToString(e.hash),
		"cmp":    compressionAlg,
		"path":   pathInStorage,
	}
	return params
}

func webShareInternal(inputs []string, overrideName string, label string, expiry time.Duration, passwordUrl bool) {
	verifySingleHashInput(inputs)
	if !passwordUrl {
		if len(inputs) > 1 {
			panic("Can not create a parameterized share url with multiple files")
		}
		info, err := os.Stat(inputs[0])
		if err == nil && info.IsDir() {
			panic("Can not create a parameterized share url with a directory")
		}
	}
	resolvedInputs := []entry{}
	for _, input := range inputs {
		if !isHash(input) {
			utils.WalkFiles(input, func(path string, info os.FileInfo) {
				if info.IsDir() {
					return
				}
				hash, _ := ResolvePathOrHash(path, "这里只是为了防止出现日志消息。")
				resolvedInputs = append(resolvedInputs, entry{hash: hash, path: path})
			})
		} else {
			hash, sharedName := ResolvePathOrHash(input, overrideName)
			resolvedInputs = append(resolvedInputs, entry{hash: hash, path: sharedName})
		}
	}
	// If the user inputs a single directory, set the name of that directory as the name of the zip file
	if len(inputs) == 1 && len(resolvedInputs) > 1 && overrideName == "" {
		abs, err := filepath.Abs(inputs[0])
		if err != nil {
			panic(err)
		}
		overrideName = filepath.Base(abs)
	}
	if len(resolvedInputs) > 1 && overrideName == "" {
		fmt.Println("Enter a name for the zip file (or leave empty):")
		reader := bufio.NewReader(os.Stdin)
		input, err := reader.ReadString('\n')
		if err != nil {
			panic(err)
		}
		overrideName = strings.TrimSpace(input)
	}

	var common string
	if len(resolvedInputs) > 1 {
		common = commonPath(resolvedInputs)
	} else {
		common = filepath.Dir(resolvedInputs[0].path)
		if overrideName == "" {
			overrideName = filepath.Base(resolvedInputs[0].path)
		}
	}
	for i := range resolvedInputs {
		path, err := filepath.Rel(common, resolvedInputs[i].path)
		if err != nil {
			panic(err)
		}
		resolvedInputs[i].path = path
	}

	stor, ok := storage.StorageSelect(label)
	if !ok {
		return
	}

	cfg := config.Config()
	if passwordUrl && cfg.SharePasswordURL == "" {
		log.Println("You need to set `share_password_url` in your .gb.conf to use --password-url mode")
		log.Println("This should be the base URL of your share server, e.g. https://gb.example.com")
		log.Println("See https://github.com/leijurv/gb/tree/master/webshare/README.md for details on how to set this up")
		return
	}

	filesParams := []map[string]string{}
	for _, e := range resolvedInputs {
		filesParams = append(filesParams, generateParams(e, stor))
	}

	var shareURL string
	if passwordUrl {
		shareURL = generatePasswordURL(stor, cfg, filesParams, overrideName, expiry)
	} else {
		shareURL = generatePresignedURL(stor, filesParams[0], expiry, filesParams[0]["path"])
	}

	log.Println()
	if len(filesParams) == 1 {
		p := filesParams[0]
		log.Printf("File: %s", p["path"])
		log.Printf("Size: %s uncompressed, %s compressed", utils.FormatCommasStr(p["size"]), utils.FormatCommasStr(p["length"]))
		log.Printf("Compression: %s", p["cmp"])
	}

	if passwordUrl {
		if expiry > 0 {
			log.Printf("URL EXPIRES: %s", time.Now().Add(expiry).Format(time.RFC3339))
		} else {
			log.Printf("URL EXPIRES: never (no expiry set)")
		}
	} else {
		log.Printf("URL EXPIRES: %s", time.Now().Add(expiry).Format(time.RFC3339))
	}
	fmt.Println()
	fmt.Println(shareURL)
	fmt.Println()
}

func generatePresignedURL(stor storage_base.Storage, params map[string]string, expiry time.Duration, pathInStorage string) string {
	delete(params, "path")
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

func generatePasswordURL(stor storage_base.Storage, cfg config.ConfigData, filesParams []map[string]string, name string, expiry time.Duration) string {
	for _, params := range filesParams {
		if expiry > 0 {
			params["expires_at"] = fmt.Sprintf("%d", time.Now().Add(expiry).Unix())
		}
	}
	var jsonData []byte
	if len(filesParams) > 1 {
		json, err := json.Marshal(filesParams)
		if err != nil {
			panic(err)
		}
		jsonData = json
	} else {
		json, err := json.Marshal(filesParams[0])
		if err != nil {
			panic(err)
		}
		jsonData = json
	}

	password := generatePassword(cfg.ShareUrlPasswordLength)

	uploadPath := "share/" + password + ".json"

	upload := stor.BeginDatabaseUpload(uploadPath)
	_, err := upload.Writer().Write(jsonData)
	if err != nil {
		panic(err)
	}
	upload.End()

	baseURL := cfg.SharePasswordURL
	for strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL[:len(baseURL)-1]
	}
	urlStr := fmt.Sprintf("%s/%s", baseURL, password)
	if name != "" {
		urlFriendlyName := strings.Replace(name, " ", "_", -1)
		urlFriendlyName = url.PathEscape(urlFriendlyName) // might not actually be necessary
		urlStr = fmt.Sprintf("%s/%s", urlStr, urlFriendlyName)
	}
	return urlStr
}
