package db

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"testing"
)

func WithTestingDatabase(t *testing.T, setupSchema bool, fn func()) {
	SetupDatabaseTestMode(setupSchema)
	defer ShutdownDatabase()
	fn()
}

func TestSqliteWorks(t *testing.T) {
	WithTestingDatabase(t, false, func() {
		var i int64
		err := DB.QueryRow("SELECT 1+1").Scan(&i)
		if err != nil {
			t.Error(err)
		}
		if i != 2 {
			t.Errorf("1+1 != 2")
		}
	})
}

func TestInitialSetup(t *testing.T) {
	WithTestingDatabase(t, false, func() {
		if determineDatabaseLayer() != DATABASE_LAYER_EMPTY {
			t.Errorf("empty database should be empty")
		}
		err := schemaVersionOne()
		if err != nil {
			t.Error(err)
		}
		if determineDatabaseLayer() != DATABASE_LAYER_1 {
			t.Errorf("schema version one should work")
		}
		if !areForeignKeysEnforced(t) {
			t.Errorf("schema one should stay with foreign keys enforced")
		}
		err = schemaVersionTwo()
		if err != nil {
			t.Error(err)
		}
		if determineDatabaseLayer() != DATABASE_LAYER_2 {
			t.Errorf("schema version two should work")
		}
		if !areForeignKeysEnforced(t) {
			t.Errorf("schema two should stay with foreign keys enforced")
		}
		err = schemaVersionThree()
		if err != nil {
			t.Error(err)
		}
		if determineDatabaseLayer() != DATABASE_LAYER_3 {
			t.Errorf("schema version three should work")
		}
		if !areForeignKeysEnforced(t) {
			t.Errorf("schema three should stay with foreign keys enforced")
		}
	})
}

func TestLayerOneDoesntWorkTwice(t *testing.T) {
	WithTestingDatabase(t, false, func() {
		if determineDatabaseLayer() != DATABASE_LAYER_EMPTY {
			t.Errorf("empty database should be empty")
		}
		err := schemaVersionOne()
		if err != nil {
			t.Error(err)
		}
		err = schemaVersionOne()
		if err == nil || err.Error() != "table sizes already exists" {
			t.Errorf("shouldn't work twice")
		}
	})
}

func TestLayerTwoDoesntWorkTwice(t *testing.T) {
	WithTestingDatabase(t, false, func() {
		err := schemaVersionOne()
		if err != nil {
			t.Error(err)
		}
		err = schemaVersionTwo()
		if err != nil {
			t.Error(err)
		}
		err = schemaVersionTwo()
		if err == nil || err.Error() != "no such column: encryption_key" {
			t.Errorf("shouldn't work twice")
		}
	})
}

func TestLayerThreeDoesntWorkTwice(t *testing.T) {
	WithTestingDatabase(t, false, func() {
		err := schemaVersionOne()
		if err != nil {
			t.Error(err)
		}
		err = schemaVersionTwo()
		if err != nil {
			t.Error(err)
		}
		err = schemaVersionThree()
		if err != nil {
			t.Error(err)
		}
		err = schemaVersionThree()
		if err == nil || err.Error() != "no such index: blob_entries_by_blob_id" {
			t.Errorf("shouldn't work twice, got: %v", err)
		}
	})
}

func TestConstraints(t *testing.T) {
	WithTestingDatabase(t, true, func() {
		_, err := DB.Exec("INSERT INTO sizes (hash, size) VALUES (?, ?)", make([]byte, 5), 0)
		if err == nil {
			t.Errorf("hash of length 5 should not be allowed")
		}
		_, err = DB.Exec("INSERT INTO sizes (hash, size) VALUES (?, ?)", make([]byte, 32), 0)
		if err != nil {
			t.Errorf("hash of length 32 should be allowed")
		}
	})
}

func TestBlobFetch(t *testing.T) {
	WithTestingDatabase(t, true, func() {
		err := insertTestSize()
		if err != nil {
			t.Error(err)
		}
	})
}

func TestEmptySizesCanBeCleared(t *testing.T) {
	WithTestingDatabase(t, true, func() {
		if !canSizesTableBeClearedWithoutError(t) {
			t.Errorf("sizes should be clearable")
		}
	})
}

func TestUnreferencedSizesCanBeCleared(t *testing.T) {
	WithTestingDatabase(t, true, func() {
		err := insertTestSize()
		if err != nil {
			t.Error(err)
		}
		sz := numSizes(t)
		if sz != 1 {
			t.Errorf("expected one size")
		}
		if !canSizesTableBeClearedWithoutError(t) {
			t.Errorf("sizes should be clearable")
		}
		sz = numSizes(t)
		if sz != 1 {
			t.Errorf("expected one size")
		}
	})
}

func TestReferencedSizesCanBeCleared(t *testing.T) {
	WithTestingDatabase(t, true, func() {
		err := insertTestSize()
		if err != nil {
			t.Error(err)
		}
		err = insertTestFile()
		if err != nil {
			t.Error(err)
		}
		sz := numSizes(t)
		if sz != 1 {
			t.Errorf("expected one size")
		}
		if canSizesTableBeClearedWithoutError(t) {
			t.Errorf("sizes shouldn't be clearable")
		}
		sz = numSizes(t)
		if sz != 1 {
			t.Errorf("expected one size")
		}
	})
}

func TestForeignKeysDisableable(t *testing.T) {
	WithTestingDatabase(t, true, func() {
		if !areForeignKeysEnforced(t) {
			t.Errorf("foreign keys should be enforced")
		}
		DB.Exec("PRAGMA foreign_keys = OFF")
		if areForeignKeysEnforced(t) {
			t.Errorf("foreign keys shouldn't be enforced")
		}
		DB.Exec("PRAGMA foreign_keys = ON")
		if !areForeignKeysEnforced(t) {
			t.Errorf("foreign keys should be enforced")
		}
	})
}

func TestLayerTwoMigration(t *testing.T) {
	WithTestingDatabase(t, false, func() {
		err := schemaVersionOne()
		if err != nil {
			t.Error(err)
		}
		err = insertTestSize()
		if err != nil {
			t.Error(err)
		}
		_, err = DB.Exec("INSERT INTO blobs(blob_id, encryption_key, size, hash_pre_enc, hash_post_enc) VALUES (?, ?, 1337, ?, ?)", testingHash("blob"), testingHash("key")[:16], testingHash("pre"), testingHash("post"))
		if err != nil {
			t.Error(err)
		}
		_, err = DB.Exec("INSERT INTO blob_entries(blob_id, hash, final_size, offset, compression_alg) VALUES (?, ?, 5021, 0, 'meow')", testingHash("blob"), testingHash("file"))
		if err != nil {
			t.Error(err)
		}
		err = schemaVersionTwo()
		if err != nil {
			t.Error(err)
		}
		var blobEntryEnc []byte
		err = DB.QueryRow("SELECT encryption_key FROM blob_entries WHERE blob_id = ? AND hash = ? AND final_size = 5021 AND offset = 0 AND compression_alg = 'meow'", testingHash("blob"), testingHash("file")).Scan(&blobEntryEnc)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(blobEntryEnc, testingHash("key")[:16]) {
			t.Errorf("wrong")
		}
		var blobPaddingEnc []byte
		err = DB.QueryRow("SELECT padding_key FROM blobs WHERE blob_id = ? AND size = 1337 AND final_hash = ?", testingHash("blob"), testingHash("post")).Scan(&blobPaddingEnc)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(blobPaddingEnc, testingHash("key")[:16]) {
			t.Errorf("wrong")
		}
	})
}

func TestStartsWithPattern(t *testing.T) {
	WithTestingDatabase(t, false, func() {
		pattern := "abc"
		if !startsWith(t, pattern, pattern) {
			t.Errorf("string should start with itself")
		}
		prev := []byte(pattern)
		prev[len(prev)-1]--
		if startsWith(t, string(prev), pattern) {
			t.Errorf("string should not start with prev string 'abb'")
		}
		next := []byte(pattern)
		next[len(next)-1]++
		if startsWith(t, string(next), pattern) {
			t.Errorf("string should not start with next string 'abd'")
		}
		for ch := 0; ch < 256; ch++ {
			longer := append([]byte(pattern), byte(ch))
			if !startsWith(t, string(longer), pattern) {
				t.Errorf("appending any byte to the pattern should still start with the pattern")
			}
		}
	})
}

func startsWith(t *testing.T, str string, pattern string) bool {
	var ret bool
	err := DB.QueryRow("SELECT ?1 "+StartsWithPattern(2), str, pattern).Scan(&ret)
	if err != nil {
		t.Error(err)
	}
	return ret
}

func testingHash(usage string) []byte {
	meme := sha256.Sum256([]byte(usage))
	return meme[:]
}

func insertTestSize() error {
	_, err := DB.Exec("INSERT INTO sizes (hash, size) VALUES (?, ?)", testingHash("file"), 5021)
	if err != nil {
		return err
	}
	var resp []byte
	err = DB.QueryRow("SELECT hash FROM sizes WHERE size = ?", 5021).Scan(&resp)
	if err != nil {
		return err
	}
	for i := range resp {
		if resp[i] != testingHash("file")[i] {
			return errors.New("wrong")
		}
	}
	return nil
}

func insertTestFile() error {
	_, err := DB.Exec("INSERT INTO files(path, hash, start, end, fs_modified, permissions) VALUES ('/meow', ?, 1, 2, 1, 0)", testingHash("file"))
	return err
}

func areForeignKeysEnforced(t *testing.T) bool {
	insertTestSize() // ignore error since it might already be there
	insertTestFile() // again ignore error since the row might already be in files
	return !canSizesTableBeClearedWithoutError(t)
}

func canSizesTableBeClearedWithoutError(t *testing.T) bool {
	tx, err := DB.Begin()
	if err != nil {
		t.Error(err)
	}
	defer tx.Rollback() // don't ACTUALLY clear it
	_, err = tx.Exec("DELETE FROM sizes")
	return err == nil
}

func numSizes(t *testing.T) int {
	var ret int
	err := DB.QueryRow("SELECT COUNT(*) FROM sizes").Scan(&ret)
	if err != nil {
		t.Error(err)
	}
	return ret
}
