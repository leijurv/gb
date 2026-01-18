package share

import (
	"bufio"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

const DefaultWebShareBaseURL = "https://leijurv.github.io/gb/webshare/"

// RevokedShareJSON is the canonical JSON content for revoked shares.
// Used by both revoke.go and ExpectedShareJSONs to ensure consistency.
const RevokedShareJSON = `{"revoked":true}`

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
	webShareInternal([]string{pathOrHash}, overrideName, label, expiry, false, nil)
}

func PasswordUrlShare(inputs []string, overrideName string, label string, expiry time.Duration) {
	// For password mode, empty expiry means no expiry
	webShareInternal(inputs, overrideName, label, expiry, true, nil)
}

// PasswordUrlShareNonInteractive is the same as PasswordUrlShare but for testing.
// It takes a storage directly instead of selecting interactively.
// Returns the generated password.
func PasswordUrlShareNonInteractive(inputs []string, overrideName string, expiry time.Duration, stor storage_base.Storage) string {
	return webShareInternal(inputs, overrideName, "", expiry, true, stor)
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

// commonPath returns the common directory prefix shared by all paths.
func commonPath0(paths []string) string {
	if len(paths) == 0 {
		return ""
	}
	if len(paths) == 1 {
		return filepath.Dir(paths[0])
	}
	sort.Strings(paths)
	a := strings.Split(filepath.Clean(paths[0]), string(filepath.Separator))
	b := strings.Split(filepath.Clean(paths[len(paths)-1]), string(filepath.Separator))
	n := min(len(a), len(b)) - 1 // -1: want directory, not file
	var i int
	for i < n && a[i] == b[i] {
		i++
	}
	return strings.Join(a[:i], string(filepath.Separator))
}

func commonPath(entries []entry) string {
	paths := []string{}
	for _, e := range entries {
		paths = append(paths, e.path)
	}
	return commonPath0(paths)
}

type entry struct {
	hash   []byte
	path   string
	blobID []byte
}

// lookupBlobParams looks up blob entry details for a specific blob/hash in a given storage and returns
// the params map needed for share JSON. Used for generating storage-specific JSON.
func lookupBlobParams(hash []byte, blobID []byte, filename string, expiresAt *int64, stor storage_base.Storage) map[string]string {
	var offset, length, originalSize int64
	var compressionAlg string
	var key []byte
	var pathInStorage string
	err := db.DB.QueryRow(`
		SELECT blob_entries.offset, blob_entries.final_size, blob_entries.compression_alg,
		       blob_entries.encryption_key, blob_storage.path
		FROM blob_entries
			INNER JOIN blob_storage ON blob_storage.blob_id = blob_entries.blob_id
		WHERE blob_entries.hash = ? AND blob_entries.blob_id = ? AND blob_storage.storage_id = ?
		LIMIT 1
	`, hash, blobID, stor.GetID()).Scan(&offset, &length, &compressionAlg, &key, &pathInStorage)
	if err != nil {
		panic(err)
	}

	err = db.DB.QueryRow(`SELECT size FROM sizes WHERE hash = ?`, hash).Scan(&originalSize)
	if err != nil {
		panic(err)
	}

	params := map[string]string{
		"name":   filename,
		"key":    hex.EncodeToString(key),
		"offset": fmt.Sprintf("%d", offset),
		"length": fmt.Sprintf("%d", length),
		"size":   fmt.Sprintf("%d", originalSize),
		"sha256": base64.RawURLEncoding.EncodeToString(hash),
		"cmp":    compressionAlg,
		"path":   pathInStorage,
	}
	if expiresAt != nil {
		params["expires_at"] = fmt.Sprintf("%d", *expiresAt)
	}
	return params
}

// sanityCheckEntry verifies the hash exists in exactly one blob_id in this storage,
// checks that the encryption key is not shared with other entries (old blob compatibility
// check), and populates e.blobID. Returns true if the entry has shared encryption keys
// and needs repacking.
func sanityCheckEntry(e *entry, stor storage_base.Storage) bool {
	// Verify hash exists in exactly one blob in this storage
	var distinctBlobCount int
	err := db.DB.QueryRow(`
		SELECT COUNT(DISTINCT blob_entries.blob_id)
		FROM blob_entries
			INNER JOIN blob_storage ON blob_storage.blob_id = blob_entries.blob_id
		WHERE blob_entries.hash = ? AND blob_storage.storage_id = ?
	`, e.hash, stor.GetID()).Scan(&distinctBlobCount)
	if err != nil {
		panic(err)
	}
	if distinctBlobCount != 1 {
		panic(fmt.Sprintf("Expected hash %s to be in exactly 1 blob in storage %s, but found %d", hex.EncodeToString(e.hash), stor, distinctBlobCount))
	}

	// Get the blob_id and check for shared encryption keys
	var sharedKeyCount int
	err = db.DB.QueryRow(`
		SELECT
			blob_entries.blob_id,
			(SELECT COUNT(*) FROM blob_entries sibling WHERE sibling.blob_id = blob_entries.blob_id AND sibling.encryption_key = blob_entries.encryption_key) AS shared_key_count
		FROM blob_entries
			INNER JOIN blob_storage ON blob_storage.blob_id = blob_entries.blob_id
		WHERE blob_entries.hash = ? AND blob_storage.storage_id = ?
		LIMIT 1
	`, e.hash, stor.GetID()).Scan(&e.blobID, &sharedKeyCount)
	if err != nil {
		panic(err)
	}

	return sharedKeyCount > 1
}

// webShareInternal is the core share implementation. Returns the password for password-mode shares.
func webShareInternal(inputs []string, overrideName string, label string, expiry time.Duration, passwordUrl bool, providedStorage storage_base.Storage) string {
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

	cfg := config.Config()

	var stor storage_base.Storage
	if providedStorage != nil {
		stor = providedStorage
	} else {
		var ok bool
		stor, ok = storage.StorageSelect(label)
		if !ok {
			return ""
		}
	}

	// Sanity check all entries and populate blobID
	var blobsNeedingRepack []string
	seenBlobs := make(map[string]bool)
	for i := range resolvedInputs {
		needsRepack := sanityCheckEntry(&resolvedInputs[i], stor)
		if needsRepack {
			blobIDHex := hex.EncodeToString(resolvedInputs[i].blobID)
			if !seenBlobs[blobIDHex] {
				seenBlobs[blobIDHex] = true
				blobsNeedingRepack = append(blobsNeedingRepack, blobIDHex)
			}
		}
	}
	if len(blobsNeedingRepack) > 0 {
		log.Println("Unfortunately some files were backed up with an older version of gb that shared encryption keys across distinct files that were backed up at one time (into a single blob).")
		log.Println("To fix this, you can run the following command to repack the affected blobs:")
		log.Println()
		log.Printf("printf '%s\\n' | gb repack", strings.Join(blobsNeedingRepack, "\\n"))
		log.Println()
		log.Println("Alternatively, to fix this for all blobs, you can run `gb upgrade-encryption`.")
		log.Println("Then rerun this command to securely share these files.")
		os.Exit(1)
	}

	var shareURL string
	var password string
	if passwordUrl {
		if providedStorage == nil && cfg.SharePasswordURL == "" {
			log.Println("You need to set `share_password_url` in your .gb.conf to use --password-url mode")
			log.Println("This should be the base URL of your share server, e.g. https://gb.example.com")
			log.Println("See https://github.com/leijurv/gb/tree/master/webshare/README.md for details on how to set this up")
			return ""
		}

		shareURL, password = generatePasswordURL(stor, cfg, resolvedInputs, overrideName, expiry)
	} else {
		e := resolvedInputs[0]
		params := lookupBlobParams(e.hash, e.blobID, e.path, nil, stor)
		shareURL = generatePresignedURL(stor, params, expiry, params["path"])
		log.Println()
		log.Printf("File: %s", params["path"])
		log.Printf("Size: %s uncompressed, %s compressed", utils.FormatCommasStr(params["size"]), utils.FormatCommasStr(params["length"]))
		log.Printf("Compression: %s", params["cmp"])
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
	return password
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

// insertShare creates share database entries and uploads the share JSON.
// Returns the generated password.
func insertShare(entries []entry, name string, stor storage_base.Storage, expiry time.Duration, passwordLength int) string {
	password := generatePassword(passwordLength)
	now := time.Now().Unix()

	var expiresAt *int64
	if expiry > 0 {
		exp := time.Now().Add(expiry).Unix()
		expiresAt = &exp
	}

	// Insert into shares and share_entries tables
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()

	// Insert parent share record
	_, err = tx.Exec(`
		INSERT INTO shares (password, name, storage_id, shared_at, expires_at)
		VALUES (?, ?, ?, ?, ?)
	`, password, name, stor.GetID(), now, expiresAt)
	if err != nil {
		panic(err)
	}

	// Insert share entries with ordinal based on position in slice
	for i, e := range entries {
		_, err = tx.Exec(`
			INSERT INTO share_entries (password, hash, filename, blob_id, storage_id, ordinal)
			VALUES (?, ?, ?, ?, ?, ?)
		`, password, e.hash, e.path, e.blobID, stor.GetID(), i)
		if err != nil {
			panic(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	UploadShareJSON(password, stor)

	return password
}

// BuildShareURL constructs the full share URL from a password and name.
func BuildShareURL(password, name string) string {
	baseURL := config.Config().SharePasswordURL
	for strings.HasSuffix(baseURL, "/") {
		baseURL = baseURL[:len(baseURL)-1]
	}
	urlStr := fmt.Sprintf("%s/%s", baseURL, password)
	if name != "" {
		urlFriendlyName := strings.Replace(name, " ", "_", -1)
		urlFriendlyName = url.PathEscape(urlFriendlyName)
		urlStr = fmt.Sprintf("%s/%s", urlStr, urlFriendlyName)
	}
	return urlStr
}

func generatePasswordURL(stor storage_base.Storage, cfg config.ConfigData, entries []entry, name string, expiry time.Duration) (string, string) {
	shareName := name
	if shareName == "" {
		shareName = entries[0].path // fallback to first entry's filename
	}
	password := insertShare(entries, shareName, stor, expiry, cfg.ShareUrlPasswordLength)
	log.Printf("Uploaded share JSON to %s", stor)

	return BuildShareURL(password, shareName), password
}

// UploadShareJSON generates and uploads the encrypted share JSON for a given password to storage.
// This is the single place where share JSON upload logic lives - used by initial share
// creation, repack, and any other code that needs to regenerate share JSONs.
// The filename is derived from HMAC(masterKey, password) and content is AES-GCM encrypted.
func UploadShareJSON(password string, stor storage_base.Storage) {
	filename := DeriveShareFilename(password)
	uploadPath := "share/" + filename
	jsonBytes := GenerateShareJSON(password, stor)
	encrypted := EncryptShareJSON(jsonBytes, password)
	upload := stor.BeginDatabaseUpload(uploadPath)
	_, err := upload.Writer().Write(encrypted)
	if err != nil {
		panic(err)
	}
	upload.End()
}

// GenerateShareJSON generates the JSON array for a password-mode share by querying
// the share_entries table. This utility can be used for initial share creation as well as
// regenerating the JSON after modifications (like revoking individual files).
func GenerateShareJSON(password string, stor storage_base.Storage) []byte {
	// First check if the share is revoked
	var revokedAt *int64
	var expiresAt *int64
	err := db.DB.QueryRow(`
		SELECT expires_at, revoked_at FROM shares WHERE password = ? AND storage_id = ?
	`, password, stor.GetID()).Scan(&expiresAt, &revokedAt)
	if err != nil {
		panic(err)
	}

	if revokedAt != nil {
		return []byte(RevokedShareJSON)
	}

	rows, err := db.DB.Query(`
		SELECT hash, blob_id, filename
		FROM share_entries
		WHERE password = ? AND storage_id = ?
		ORDER BY ordinal
	`, password, stor.GetID())
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	filesParams := []map[string]string{}
	for rows.Next() {
		var hash []byte
		var blobID []byte
		var filename string

		err = rows.Scan(&hash, &blobID, &filename)
		if err != nil {
			panic(err)
		}

		params := lookupBlobParams(hash, blobID, filename, expiresAt, stor)
		filesParams = append(filesParams, params)
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}

	// If no entries exist, still return the revoked sentinel for safety
	if len(filesParams) == 0 {
		return []byte(RevokedShareJSON)
	}

	jsonBytes, err := json.Marshal(filesParams)
	if err != nil {
		panic(err)
	}
	return jsonBytes
}

// ExpectedShareFile represents an expected share JSON file in storage
type ExpectedShareFile struct {
	Path     string
	Size     int64
	Checksum string
}

// ExpectedShareJSONs returns all expected share JSON files for a given storage.
// This is used by paranoia to verify share files are correctly stored.
func ExpectedShareJSONs(stor storage_base.Storage) []ExpectedShareFile {
	// Get all shares for this storage
	rows, err := db.DB.Query(`
		SELECT password
		FROM shares
		WHERE storage_id = ?
	`, stor.GetID())
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var result []ExpectedShareFile
	for rows.Next() {
		var password string
		err = rows.Scan(&password)
		if err != nil {
			panic(err)
		}

		jsonBytes := GenerateShareJSON(password, stor)

		// Encrypt the JSON and compute checksum of encrypted content
		encrypted := EncryptShareJSON(jsonBytes, password)
		filename := DeriveShareFilename(password)

		sum := md5.Sum(encrypted)
		result = append(result, ExpectedShareFile{
			Path:     "share/" + filename,
			Size:     int64(len(encrypted)),
			Checksum: hex.EncodeToString(sum[:]),
		})
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}

	return result
}
