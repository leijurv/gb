package e2e

import (
	"bytes"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/leijurv/gb/backup"
	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
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
	expectedHash := sha256.Sum256(duplicateContent)

	for i := 1; i <= 3; i++ {
		env.writeFile("dup"+string(rune('0'+i))+".bin", duplicateContent)
	}

	env.backup()

	var blobCount int
	err := db.DB.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&blobCount)
	if err != nil {
		t.Fatal(err)
	}
	if blobCount != 1 {
		t.Errorf("expected 1 blob for deduplicated files, got %d", blobCount)
	}

	for i := 1; i <= 3; i++ {
		env.removeFile("dup" + string(rune('0'+i)) + ".bin")
	}

	env.restore()

	for i := 1; i <= 3; i++ {
		env.verifyRestored("dup"+string(rune('0'+i))+".bin", expectedHash)
	}
}

func TestRestoreUsesLocalSource(t *testing.T) {
	env := setupTestEnv(t, "local")
	defer env.cleanup()

	content := makeBinaryData(2000)
	expectedHash := sha256.Sum256(content)
	env.writeFile("testfile.bin", content)

	env.backup()
	env.restore()

	env.verifyRestored("testfile.bin", expectedHash)
}

func TestMultipleBackupsAndRestore(t *testing.T) {
	env := setupTestEnv(t, "multi")
	defer env.cleanup()

	content1 := []byte("initial content")
	env.writeFile("changing.txt", content1)

	env.backup()

	content2 := []byte("modified content that is different")
	expectedHash := sha256.Sum256(content2)
	env.writeFile("changing.txt", content2)

	env.backup()

	env.removeFile("changing.txt")

	env.restore()

	restoredPath := filepath.Join(env.restoreDir, "changing.txt")
	restoredContent, err := os.ReadFile(restoredPath)
	if err != nil {
		t.Fatalf("failed to read restored file: %v", err)
	}
	actualHash := sha256.Sum256(restoredContent)
	if actualHash != expectedHash {
		t.Errorf("hash mismatch: expected %x, got %x", expectedHash, actualHash)
	}
	if !bytes.Equal(restoredContent, content2) {
		t.Errorf("expected latest content, got: %s", string(restoredContent))
	}
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

func (e *testEnv) backup() {
	backup.ResetForTesting()
	backup.BackupNonInteractive([]string{e.srcDir})
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
