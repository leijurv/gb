package main

import (
	"crypto/sha256"
	"testing"
)

func WithTestingDatabase(t *testing.T, fn func()) {
	SetupDatabaseTestMode()
	defer ShutdownDatabase()
	fn()
}

func TestInitialSetup(t *testing.T) {
	WithTestingDatabase(t, func() {
		var i int64
		err := db.QueryRow("SELECT 1+1").Scan(&i)
		if err != nil {
			t.Error(err)
		}
		if i != 2 {
			t.Errorf("1+1 != 2")
		}
	})
}

func TestConstraints(t *testing.T) {
	WithTestingDatabase(t, func() {
		_, err := db.Exec("INSERT INTO hashes (hash, size) VALUES (?, ?)", make([]byte, 5), 0)
		if err == nil {
			t.Errorf("should not be allowed ")
		}
	})
}

func TestBlobFetch(t *testing.T) {
	WithTestingDatabase(t, func() {
		meme := sha256.Sum256([]byte("meme"))
		_, err := db.Exec("INSERT INTO hashes (hash, size) VALUES (?, ?)", meme[:], 5021)
		if err != nil {
			t.Error(err)
		}
		var resp []byte
		err = db.QueryRow("SELECT hash FROM hashes WHERE size = ?", 5021).Scan(&resp)
		if err != nil {
			t.Error(err)
		}
		for i := range resp {
			if resp[i] != meme[i] {
				t.Errorf("wrong")
			}
		}
	})
}
