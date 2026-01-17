package e2e

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/paranoia"
	"github.com/leijurv/gb/repack"
	"github.com/leijurv/gb/share"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	bip39 "github.com/tyler-smith/go-bip39"
)

type testEnv struct {
	tmpDir     string
	srcDir     string
	restoreDir string
	mockStor   *storage_base.MockStorage
	t          *testing.T
}

func TestBackupAndRestore(t *testing.T) {
	env := setupTestEnv(t, "test")
	defer env.cleanup()

	testFiles := map[string][]byte{
		"file1.txt":          []byte("hello world"),
		"file2.txt":          []byte("this is a test file with some content"),
		"subdir/file3.txt":   []byte("nested file content"),
		"subdir/deep/f4.txt": []byte("deeply nested"),
		"binary.bin":         makeBinaryData(1024),
		"larger.bin":         makeBinaryData(10000),
	}

	originalHashes := make(map[string][32]byte)
	for name, content := range testFiles {
		env.writeFile(name, content)
		originalHashes[name] = sha256.Sum256(content)
	}

	env.backup()

	for name := range testFiles {
		env.removeFile(name)
	}

	env.restore()

	for name, expectedHash := range originalHashes {
		env.verifyRestored(name, expectedHash)
	}
}

func TestBackupAndRestoreSingleFile(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gb-e2e-single-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	mockStor := setupDB(t, tmpDir)
	defer db.ShutdownDatabase()

	srcFile := filepath.Join(tmpDir, "source.txt")
	restoreFile := filepath.Join(tmpDir, "restored.txt")

	content := []byte("single file test content")
	expectedHash := sha256.Sum256(content)

	if err := os.WriteFile(srcFile, content, 0644); err != nil {
		t.Fatal(err)
	}

	backup.ResetForTesting()
	backup.BackupNonInteractive([]string{srcFile})

	if err := os.Remove(srcFile); err != nil {
		t.Fatal(err)
	}

	download.RestoreNonInteractive(srcFile, restoreFile, backup.GetTestingTimestamp(), mockStor)

	restoredContent, err := os.ReadFile(restoreFile)
	if err != nil {
		t.Fatalf("failed to read restored file: %v", err)
	}
	actualHash := sha256.Sum256(restoredContent)
	if actualHash != expectedHash {
		t.Errorf("hash mismatch: expected %x, got %x", expectedHash, actualHash)
	}
}

func TestBackupDedupe(t *testing.T) {
	env := setupTestEnv(t, "dedupe")
	defer env.cleanup()

	duplicateContent := makeBinaryData(5000)
	duplicateContent2 := makeBinaryData(5000)
	duplicateContent2[0] += 1
	if sha256.Sum256(duplicateContent) == sha256.Sum256(duplicateContent2) {
		t.Fatal()
	}

	for i := 1; i <= 3; i++ {
		env.writeFile("dup"+string(rune('0'+i))+".bin", duplicateContent)
	}
	env.backup()
	for i := 4; i <= 5; i++ {
		env.writeFile("dup"+string(rune('0'+i))+".bin", duplicateContent)
	}
	env.backup()

	var blobCount int
	err := db.DB.QueryRow("SELECT COUNT(*) FROM blob_entries").Scan(&blobCount)
	if err != nil {
		t.Fatal(err)
	}
	if blobCount != 1 {
		t.Errorf("expected 1 blob entry for deduplicated files, got %d", blobCount)
	}

	for i := 6; i <= 7; i++ {
		env.writeFile("dup"+string(rune('0'+i))+".bin", duplicateContent)
	}
	for i := 8; i <= 10; i++ {
		env.writeFile("dup"+string(rune('0'+i))+".bin", duplicateContent2)
	}
	env.backup()

	err = db.DB.QueryRow("SELECT COUNT(*) FROM blob_entries").Scan(&blobCount)
	if err != nil {
		t.Fatal(err)
	}
	if blobCount != 2 {
		t.Errorf("expected 2 blob entries for deduplicated files, got %d", blobCount)
	}

	for i := 1; i <= 10; i++ {
		env.removeFile("dup" + string(rune('0'+i)) + ".bin")
	}
	env.restore()
	for i := 1; i <= 7; i++ {
		env.verifyRestored("dup"+string(rune('0'+i))+".bin", sha256.Sum256(duplicateContent))
	}
	for i := 8; i <= 10; i++ {
		env.verifyRestored("dup"+string(rune('0'+i))+".bin", sha256.Sum256(duplicateContent2))
	}
}

func TestRestoreUsesLocalSource(t *testing.T) {
	env := setupTestEnv(t, "local")
	defer env.cleanup()

	content := makeBinaryData(2000)
	env.writeFile("testfile.bin", content)

	env.backup()
	env.restore()

	env.verifyRestored("testfile.bin", sha256.Sum256(content))
}

func TestMultipleBackupsAndRestore(t *testing.T) {
	env := setupTestEnv(t, "multi")
	defer env.cleanup()

	content1 := []byte("initial content")
	env.writeFile("changing.txt", content1)

	env.backup()

	content2 := []byte("modified content that is different")
	env.writeFile("changing.txt", content2)

	env.backup()

	env.removeFile("changing.txt")

	env.restore()

	env.verifyRestored("changing.txt", sha256.Sum256(content2))
}

func makeBinaryData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i * 17 % 256)
	}
	return data
}

func setupDB(t *testing.T, tmpDir string) *storage_base.MockStorage {
	dbPath := filepath.Join(tmpDir, "test.db")
	if err := os.WriteFile(dbPath, nil, 0644); err != nil {
		t.Fatal(err)
	}
	config.SetTestConfig(dbPath)
	config.DatabaseLocation = dbPath
	db.SetupDatabase()

	mockStor := storage_base.NewMockStorage(crypto.RandBytes(32))
	storage.ClearCache()
	storage.RegisterMockStorage(mockStor, "test-storage")
	return mockStor
}

func setupTestEnv(t *testing.T, name string) *testEnv {
	tmpDir, err := os.MkdirTemp("", "gb-e2e-"+name+"-*")
	if err != nil {
		t.Fatal(err)
	}

	srcDir := filepath.Join(tmpDir, "source")
	restoreDir := filepath.Join(tmpDir, "restored")

	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(restoreDir, 0755); err != nil {
		t.Fatal(err)
	}

	mockStor := setupDB(t, tmpDir)

	return &testEnv{
		tmpDir:     tmpDir,
		srcDir:     srcDir,
		restoreDir: restoreDir,
		mockStor:   mockStor,
		t:          t,
	}
}

func (e *testEnv) cleanup() {
	db.ShutdownDatabase()
	os.RemoveAll(e.tmpDir)
}

func (e *testEnv) writeFile(relPath string, content []byte) {
	path := filepath.Join(e.srcDir, relPath)
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		e.t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0644); err != nil {
		e.t.Fatal(err)
	}
}

func (e *testEnv) removeFile(relPath string) {
	path := filepath.Join(e.srcDir, relPath)
	if err := os.Remove(path); err != nil {
		e.t.Fatal(err)
	}
}

func (e *testEnv) removeRestored(relPath string) {
	path := filepath.Join(e.restoreDir, relPath)
	if err := os.Remove(path); err != nil {
		e.t.Fatal(err)
	}
}

func (e *testEnv) backup() {
	backup.ResetForTesting()
	backup.BackupNonInteractive([]string{e.srcDir})

	paranoia.DBParanoia()
	if !paranoia.StorageParanoia(false) {
		panic("shouldn't be any unknown files")
	}
}

func (e *testEnv) restore() {
	download.RestoreNonInteractive(e.srcDir, e.restoreDir, backup.GetTestingTimestamp(), e.mockStor)
}

func (e *testEnv) verifyRestored(relPath string, expectedHash [32]byte) {
	restoredPath := filepath.Join(e.restoreDir, relPath)
	content, err := os.ReadFile(restoredPath)
	if err != nil {
		e.t.Errorf("failed to read restored file %s: %v", relPath, err)
		return
	}
	actualHash := sha256.Sum256(content)
	if actualHash != expectedHash {
		e.t.Errorf("hash mismatch for %s: expected %x, got %x", relPath, expectedHash, actualHash)
	}
}

func TestBackupIntegrity(t *testing.T) {
	env := setupTestEnv(t, "integrity")
	defer env.cleanup()

	// Create a file and back it up
	content := makeBinaryData(2000)
	hash := sha256.Sum256(content)
	env.writeFile("testfile.zip", content)
	env.backup()

	// If you corrupt a byte of zstd compressed data, obviously zstd will notice
	// So we need this to be uncompressed, to ensure that we are actually testing OUR integrity checks
	var compression string
	err := db.DB.QueryRow("SELECT compression_alg FROM blob_entries WHERE compression_alg != ''").Scan(&compression)
	if err != sql.ErrNoRows {
		t.Fatal(compression)
	}

	// Query the database to find the blob ID
	var blobID []byte
	err = db.DB.QueryRow("SELECT blob_id FROM blob_entries LIMIT 1").Scan(&blobID)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt a byte at offset 0 in the blob
	env.mockStor.CorruptByte(blobID, 0)

	// Remove the original file so restore must fetch from storage
	env.removeFile("testfile.zip")

	// Attempt restore - should panic due to integrity failure
	panicked := false
	var panicMsg any
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicMsg = r
			}
		}()
		env.restore()
	}()

	if !panicked {
		t.Error("expected restore to panic due to corrupted blob")
	}
	if panicMsg != "hash verification failed in download/cat" {
		t.Error("the failed hash should have been caught by cat, rather than in restore", panicMsg)
	}

	if entries, err := os.ReadDir(env.restoreDir); err != nil || len(entries) != 0 {
		t.Errorf("expected empty restore directory, got %d entries", len(entries))
	}

	env.writeFile("testfile.zip", content)

	// this restore will be successful because it will use the local source
	env.restore() // flaky warning: this only passes because the mtime of testfile.zip in units of seconds is unchanged, but that's only true because the above code takes way less than 1 second to run... :(
	env.verifyRestored("testfile.zip", hash)

	env.removeRestored("testfile.zip")
	content[0] ^= 0xff
	env.writeFile("testfile.zip", content)
	// now the restore will ATTEMPT to use the testfile.zip from the src dir, but the hash will not match
	panicked = false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
				panicMsg = r
			}
		}()
		env.restore()
	}()

	if !panicked {
		t.Error("expected restore to panic due to corrupted local source (size and mtime matches, but hash is unexpected)")
	}
	if panicMsg != "hash verification failed in restore" {
		t.Error("unexpected:", panicMsg)
	}

	if entries, err := os.ReadDir(env.restoreDir); err != nil || len(entries) != 0 {
		t.Errorf("expected empty restore directory, got %d entries", len(entries))
	}

	env.removeFile("testfile.zip")

	// un-corrupt (re-invert the byte back to how it originally was)
	env.mockStor.CorruptByte(blobID, 0)
	env.restore() // back to working properly
	env.verifyRestored("testfile.zip", hash)
}

func TestRestoreDB(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "gb-e2e-restoredb-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	srcDir := filepath.Join(tmpDir, "source")
	if err := os.MkdirAll(srcDir, 0755); err != nil {
		t.Fatal(err)
	}

	mockStor := setupDB(t, tmpDir)

	// Create test files and run backup to populate the database
	testFile := filepath.Join(srcDir, "testfile.txt")
	if err := os.WriteFile(testFile, []byte("test content for db backup"), 0644); err != nil {
		t.Fatal(err)
	}

	backup.ResetForTesting()
	backup.BackupNonInteractive([]string{srcDir})

	// Get the db key and convert to mnemonic
	dbKey := backup.DBKeyNonInteractive()
	mnemonic, err := bip39.NewMnemonic(dbKey)
	if err != nil {
		t.Fatal(err)
	}

	// Get the database file path before BackupDB closes it
	dbPath := config.Config().DatabaseLocation

	// Run BackupDB which will close the database and upload encrypted backup
	backup.BackupDB()

	// Read the original database file (now closed)
	originalDB, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatalf("failed to read original db: %v", err)
	}

	// Get the encrypted backup from MockStorage
	backupFilename := "db-v2backup-" + strconv.FormatInt(backup.GetTestingTimestamp(), 10)
	_, size := mockStor.Metadata(backupFilename)
	if size == 0 {
		t.Fatal("backup not found in storage")
	}
	encryptedReader := mockStor.DownloadSection(backupFilename, 0, size)
	encryptedData, err := io.ReadAll(encryptedReader)
	encryptedReader.Close()
	if err != nil {
		t.Fatalf("failed to read encrypted backup: %v", err)
	}

	// Write encrypted backup to a file for RestoreDBNonInteractive
	encryptedPath := filepath.Join(tmpDir, backupFilename)
	if err := os.WriteFile(encryptedPath, encryptedData, 0644); err != nil {
		t.Fatalf("failed to write encrypted backup: %v", err)
	}

	// Call RestoreDBNonInteractive
	download.RestoreDBNonInteractive(encryptedPath, mnemonic)

	// Read the decrypted output
	decryptedPath := encryptedPath + ".decrypted"
	decryptedDB, err := os.ReadFile(decryptedPath)
	if err != nil {
		t.Fatalf("failed to read decrypted db: %v", err)
	}

	// Compare original and decrypted
	if !bytes.Equal(originalDB, decryptedDB) {
		t.Errorf("database mismatch: original %d bytes, decrypted %d bytes", len(originalDB), len(decryptedDB))
	}
}

func TestRepackSharedFile(t *testing.T) {
	env := setupTestEnv(t, "repack-share")
	defer env.cleanup()

	// Create a file and back it up (size must be < MinBlobSize=1000 in test config)
	content := makeBinaryData(500)
	env.writeFile("shared.bin", content)
	env.backup()

	// Get the blob_id for this file (for verification after repack)
	var blobID []byte
	err := db.DB.QueryRow(`
		SELECT blob_entries.blob_id
		FROM blob_entries
		INNER JOIN sizes ON sizes.hash = blob_entries.hash
		WHERE sizes.size = ?
	`, len(content)).Scan(&blobID)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Backed up file has blob_id %s", hex.EncodeToString(blobID))

	// Create a share for this file using the same function as main.go
	filePath := filepath.Join(env.srcDir, "shared.bin")
	password := share.PasswordUrlShareNonInteractive([]string{filePath}, "", 0, env.mockStor)
	t.Logf("Created share with password: %s", password)

	// Verify paranoia passes before repack
	paranoia.DBParanoia()
	if !paranoia.StorageParanoia(false) {
		t.Fatal("storage paranoia failed before repack")
	}

	// Get the old blob's path so we can delete it after repack
	var oldBlobPath string
	err = db.DB.QueryRow(`SELECT path FROM blob_storage WHERE blob_id = ?`, blobID).Scan(&oldBlobPath)
	if err != nil {
		t.Fatal(err)
	}

	// Now repack this blob (with allowSingleEntryBlobs=true for testing)
	repack.RepackBlobIDs([][]byte{blobID}, env.mockStor, true)

	// Repack closes the database for backup, so reopen it for testing
	db.SetupDatabase()

	// Verify the share_entry was updated to point to the new blob
	var newBlobID []byte
	err = db.DB.QueryRow(`SELECT blob_id FROM share_entries WHERE password = ?`, password).Scan(&newBlobID)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(newBlobID, blobID) {
		t.Error("share_entry blob_id was not updated after repack")
	}
	t.Logf("Share entry blob_id updated from %s to %s", hex.EncodeToString(blobID), hex.EncodeToString(newBlobID))

	// Verify the new blob_id exists in blob_entries
	var count int
	err = db.DB.QueryRow(`SELECT COUNT(*) FROM blob_entries WHERE blob_id = ?`, newBlobID).Scan(&count)
	if err != nil {
		t.Fatal(err)
	}
	if count == 0 {
		t.Error("new blob_id from share doesn't exist in blob_entries")
	}

	// Delete old blob and db backup files from storage so paranoia passes
	env.mockStor.DeleteBlob(oldBlobPath)
	// Delete db backup files (they start with "db-v2backup-")
	for _, f := range env.mockStor.ListPrefix("db-v2backup-") {
		env.mockStor.DeleteBlob(f.Path)
	}

	// Run paranoia to verify everything is consistent
	paranoia.DBParanoia()
	if !paranoia.StorageParanoia(false) {
		t.Fatal("storage paranoia failed after repack")
	}

	// Verify the file can still be restored
	env.removeFile("shared.bin")
	env.restore()
	env.verifyRestored("shared.bin", sha256.Sum256(content))
}
