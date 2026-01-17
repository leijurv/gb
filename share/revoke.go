package share

import (
	"fmt"
	"log"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
)

func ListShares() {
	rows, err := db.DB.Query(`
		SELECT password, filename, shared_at, expires_at, revoked_at
		FROM shares
		ORDER BY shared_at DESC, password, filename
	`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var password, filename string
		var sharedAt int64
		var expiresAt, revokedAt *int64

		err = rows.Scan(&password, &filename, &sharedAt, &expiresAt, &revokedAt)
		if err != nil {
			panic(err)
		}

		var status string
		if revokedAt != nil {
			status = "revoked"
		} else if expiresAt != nil && time.Now().Unix() > *expiresAt {
			status = "expired"
		} else {
			status = "active "
		}

		sharedTime := time.Unix(sharedAt, 0).Format(time.RFC3339)
		fmt.Printf("%s  %s  %s  %s\n", password, sharedTime, status, filename)
		count++
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}

	if count == 0 {
		log.Println("No shares found")
		return
	}

	fmt.Println()
	log.Printf("Found %d share entries\n", count)
	log.Println("To revoke a share, run: gb revoke <password>")
}

func RevokeShare(password string) {
	// Query shares for this password
	rows, err := db.DB.Query(`
		SELECT filename, storage_id, shared_at, expires_at, revoked_at
		FROM shares
		WHERE password = ?
		ORDER BY filename
	`, password)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	type shareEntry struct {
		filename  string
		storageID []byte
		sharedAt  int64
		expiresAt *int64
		revokedAt *int64
	}
	var entries []shareEntry

	for rows.Next() {
		var e shareEntry
		err = rows.Scan(&e.filename, &e.storageID, &e.sharedAt, &e.expiresAt, &e.revokedAt)
		if err != nil {
			panic(err)
		}
		entries = append(entries, e)
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}

	if len(entries) == 0 {
		log.Printf("Share with password '%s' not found\n", password)
		return
	}

	// Check if already revoked
	allRevoked := true
	for _, e := range entries {
		if e.revokedAt == nil {
			allRevoked = false
			break
		}
	}
	if allRevoked {
		log.Printf("Share '%s' is already revoked\n", password)
		return
	}

	// Display share information
	fmt.Println()
	log.Println("Share Information:")
	log.Printf("  Password: %s", password)
	log.Printf("  Created: %s", time.Unix(entries[0].sharedAt, 0).Format(time.RFC3339))

	if entries[0].expiresAt != nil {
		expiresAt := time.Unix(*entries[0].expiresAt, 0)
		if time.Now().After(expiresAt) {
			log.Printf("  Expires: %s (EXPIRED)", expiresAt.Format(time.RFC3339))
		} else {
			log.Printf("  Expires: %s", expiresAt.Format(time.RFC3339))
		}
	} else {
		log.Printf("  Expires: never")
	}

	if len(entries) == 1 {
		log.Printf("  File: %s", entries[0].filename)
	} else {
		log.Printf("  Files (%d):", len(entries))
		for _, e := range entries {
			log.Printf("    %s", e.filename)
		}
	}

	fmt.Println()
	log.Print("Are you sure you want to revoke this share? Type 'yes' to confirm: ")
	var response string
	_, err = fmt.Scanln(&response)
	if err != nil || response != "yes" {
		log.Println("Revocation cancelled")
		return
	}

	// Upload revoked JSON to the storage (do this first in case of failure)
	stor := storage.GetByID(entries[0].storageID)
	uploadPath := "share/" + password + ".json"
	jsonBytes := []byte(`[{"revoked":true}]`)
	upload := stor.BeginDatabaseUpload(uploadPath)
	_, err = upload.Writer().Write(jsonBytes)
	if err != nil {
		panic(err)
	}
	upload.End()
	log.Printf("Uploaded revoked JSON to %s", stor)

	// Set revoked_at for all entries with this password
	now := time.Now().Unix()
	_, err = db.DB.Exec(`
		UPDATE shares SET revoked_at = ? WHERE password = ?
	`, now, password)
	if err != nil {
		panic(err)
	}

	log.Println("Share revoked successfully")
}
