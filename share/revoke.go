package share

import (
	"fmt"
	"log"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
)

func ListShares() {
	// Query all shares
	rows, err := db.DB.Query(`
		SELECT password, name, shared_at, expires_at, revoked_at,
			(SELECT COUNT(*) FROM share_entries WHERE share_entries.password = shares.password) as file_count
		FROM shares
		ORDER BY shared_at DESC
	`)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	shareCount := 0
	for rows.Next() {
		var password, name string
		var sharedAt int64
		var expiresAt, revokedAt *int64
		var fileCount int

		err = rows.Scan(&password, &name, &sharedAt, &expiresAt, &revokedAt, &fileCount)
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
		shareURL := BuildShareURL(password, name)

		fmt.Printf("%s  %s  %s  (%d files)\n", shareURL, sharedTime, status, fileCount)

		// Query and print filenames for this share
		fileRows, err := db.DB.Query(`
			SELECT filename FROM share_entries WHERE password = ? ORDER BY ordinal
		`, password)
		if err != nil {
			panic(err)
		}
		for fileRows.Next() {
			var filename string
			err = fileRows.Scan(&filename)
			if err != nil {
				panic(err)
			}
			fmt.Printf("    %s\n", filename)
		}
		if err = fileRows.Err(); err != nil {
			panic(err)
		}
		fileRows.Close()

		shareCount++
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}

	if shareCount == 0 {
		log.Println("No shares found")
		return
	}

	fmt.Println()
	log.Printf("Found %d shares\n", shareCount)
	log.Println("To revoke a share, run: gb revoke <password>")
}

func RevokeShare(password string) {
	// Query share metadata
	var name string
	var storageID []byte
	var sharedAt int64
	var expiresAt, revokedAt *int64
	err := db.DB.QueryRow(`
		SELECT name, storage_id, shared_at, expires_at, revoked_at
		FROM shares
		WHERE password = ?
	`, password).Scan(&name, &storageID, &sharedAt, &expiresAt, &revokedAt)
	if err == db.ErrNoRows {
		log.Printf("Share with password '%s' not found\n", password)
		return
	}
	if err != nil {
		panic(err)
	}

	// Check if already revoked
	if revokedAt != nil {
		log.Printf("Share '%s' is already revoked\n", password)
		return
	}

	// Query share entries (filenames)
	rows, err := db.DB.Query(`
		SELECT filename FROM share_entries WHERE password = ? ORDER BY ordinal
	`, password)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	var filenames []string
	for rows.Next() {
		var filename string
		err = rows.Scan(&filename)
		if err != nil {
			panic(err)
		}
		filenames = append(filenames, filename)
	}
	if err = rows.Err(); err != nil {
		panic(err)
	}

	// Display share information
	fmt.Println()
	log.Println("Share Information:")
	log.Printf("  Password: %s", password)
	log.Printf("  Name: %s", name)
	log.Printf("  Created: %s", time.Unix(sharedAt, 0).Format(time.RFC3339))

	if expiresAt != nil {
		expiresTime := time.Unix(*expiresAt, 0)
		if time.Now().After(expiresTime) {
			log.Printf("  Expires: %s (EXPIRED)", expiresTime.Format(time.RFC3339))
		} else {
			log.Printf("  Expires: %s", expiresTime.Format(time.RFC3339))
		}
	} else {
		log.Printf("  Expires: never")
	}

	if len(filenames) == 1 {
		log.Printf("  File: %s", filenames[0])
	} else if len(filenames) > 1 {
		log.Printf("  Files (%d):", len(filenames))
		for _, f := range filenames {
			log.Printf("    %s", f)
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
	stor := storage.GetByID(storageID)
	uploadPath := "share/" + password + ".json"
	jsonBytes := []byte(`[{"revoked":true}]`)
	upload := stor.BeginDatabaseUpload(uploadPath)
	_, err = upload.Writer().Write(jsonBytes)
	if err != nil {
		panic(err)
	}
	upload.End()
	log.Printf("Uploaded revoked JSON to %s", stor)

	// Set revoked_at in shares table
	now := time.Now().Unix()
	_, err = db.DB.Exec(`
		UPDATE shares SET revoked_at = ? WHERE password = ?
	`, now, password)
	if err != nil {
		panic(err)
	}

	log.Println("Share revoked successfully")
}
