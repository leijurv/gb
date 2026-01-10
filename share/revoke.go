package share

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
)

type shareInfo struct {
	Name      string `json:"name"`
	Key       string `json:"key"`
	Offset    string `json:"offset"`
	Length    string `json:"length"`
	Size      string `json:"size"`
	SHA256    string `json:"sha256"`
	Cmp       string `json:"cmp"`
	Path      string `json:"path"`
	ExpiresAt int64  `json:"expires_at,omitempty,string"`
}

func ListShares(label string) {
	stor, ok := storage.StorageSelect(label)
	if !ok {
		return
	}

	files := stor.ListPrefix("share/")
	if len(files) == 0 {
		log.Println("No shares found")
		return
	}

	// Sort by modification time, most recent first
	sort.Slice(files, func(i, j int) bool {
		return files[i].Modified.After(files[j].Modified)
	})

	log.Printf("Found %d shares:\n", len(files))
	fmt.Println()
	for _, f := range files {
		password := strings.TrimSuffix(f.Name, ".json")
		fmt.Printf("  %s  %s\n", f.Modified.Format(time.RFC3339), password)
	}
	fmt.Println()
	log.Println("To revoke a share, run: gb revoke <password>")
}

func RevokeShare(label string, password string) {
	stor, ok := storage.StorageSelect(label)
	if !ok {
		return
	}

	// Find the share file
	files := stor.ListPrefix("share/")
	var found *storage_base.ListedFile
	for i := range files {
		if strings.TrimSuffix(files[i].Name, ".json") == password {
			found = &files[i]
			break
		}
	}

	if found == nil {
		log.Printf("Share with password '%s' not found\n", password)
		return
	}

	// Download and parse the JSON
	reader := stor.DownloadSection(found.Path, 0, found.Size)
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		panic(err)
	}

	var info shareInfo
	if err := json.Unmarshal(data, &info); err != nil {
		panic(err)
	}

	// Display share information
	fmt.Println()
	log.Println("Share Information:")
	log.Printf("  Password: %s", password)
	log.Printf("  Filename: %s", info.Name)
	log.Printf("  Size: %s bytes", info.Size)
	log.Printf("  Compression: %s", info.Cmp)
	log.Printf("  Created: %s", found.Modified.UTC().Format(time.RFC3339))

	if info.ExpiresAt != 0 {
		expiresAt := time.Unix(info.ExpiresAt, 0)
		if time.Now().After(expiresAt) {
			log.Printf("  Expires: %s (EXPIRED)", expiresAt.UTC().Format(time.RFC3339))
		} else {
			log.Printf("  Expires: %s", expiresAt.UTC().Format(time.RFC3339))
		}
	} else {
		log.Printf("  Expires: never")
	}

	// Try to find the file path from the database using the SHA256
	sha256Bytes, err := base64.RawURLEncoding.DecodeString(info.SHA256)
	if err != nil || len(sha256Bytes) != 32 {
		panic("malformed")
	}
	log.Printf("  SHA256: %s", hex.EncodeToString(sha256Bytes))
	var path string
	err = db.DB.QueryRow(`
		SELECT path FROM files
		WHERE hash = ?
		ORDER BY end IS NOT NULL, end DESC
		LIMIT 1
	`, sha256Bytes).Scan(&path)
	if err != nil {
		panic(err)
	}
	log.Printf("  File path: %s", path)

	fmt.Println()
	log.Print("Are you sure you want to revoke this share? Type 'yes' to confirm: ")
	var response string
	_, err = fmt.Scanln(&response)
	if err != nil || response != "yes" {
		log.Println("Revocation cancelled")
		return
	}
	stor.DeleteBlob(found.Path)
	log.Println("Share revoked successfully")
}
