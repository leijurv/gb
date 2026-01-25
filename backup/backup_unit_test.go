package backup

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
)

// testEnv holds the test environment state.
type testEnv struct {
	tmpDir     string
	mockWalker *mockWalker
	mockFS     *mockFileOpener
	mockStor   *storage_base.MockStorage
	t          *testing.T
	done       chan struct{}
}

func setupUnitTestEnv(t *testing.T) *testEnv {
	tmpDir, err := os.MkdirTemp("", "gb-unit-*")
	if err != nil {
		t.Fatal(err)
	}

	// Set up database
	dbPath := filepath.Join(tmpDir, "test.db")
	if err := os.WriteFile(dbPath, nil, 0644); err != nil {
		t.Fatal(err)
	}
	config.SetTestConfig(dbPath)
	config.DatabaseLocation = dbPath
	db.SetupDatabase()

	// Set up mock storage
	mockStor := storage_base.NewMockStorage(crypto.RandBytes(32))
	storage.ClearCache()
	storage.RegisterMockStorage(mockStor, "test-storage")

	// Reset backup state and inject mocks
	ResetForTesting()

	mockW := newMockWalker()
	mockF := newMockFileOpener(t)
	walker = mockW
	fileOpener = mockF

	return &testEnv{
		tmpDir:     tmpDir,
		mockWalker: mockW,
		mockFS:     mockF,
		mockStor:   mockStor,
		t:          t,
	}
}

func (e *testEnv) cleanup() {
	db.ShutdownDatabase()
	os.RemoveAll(e.tmpDir)
}

// reset prepares the environment for another backup run.
// Call this between backups in tests that need multiple backup passes.
func (e *testEnv) reset() {
	ResetForTesting()
	e.mockWalker = newMockWalker()
	e.mockFS = newMockFileOpener(e.t)
	walker = e.mockWalker
	fileOpener = e.mockFS
}

// beginBackupOnDir starts a backup on a fake directory and waits for the walker to be ready.
// After this returns, you can call SendFile() to inject files.
func (e *testEnv) beginBackupOnDir(dir string) {
	// Create fake directory info
	dirInfo := fakeFileInfo{
		name:    filepath.Base(dir),
		size:    0,
		mode:    os.ModeDir | 0755,
		modTime: time.Now(),
		isDir:   true,
	}

	// Normalize path (remove trailing slash for stat)
	statPath := dir
	if len(statPath) > 1 && statPath[len(statPath)-1] == '/' {
		statPath = statPath[:len(statPath)-1]
	}

	// Ensure path has trailing slash for backup
	backupPath := dir
	if backupPath[len(backupPath)-1] != '/' {
		backupPath = backupPath + "/"
	}

	e.done = make(chan struct{})
	go func() {
		defer close(e.done)
		backupImpl([]string{backupPath})
	}()

	// Handle the stat call for the input path
	e.mockFS.shouldStat(statPath, dirInfo, nil)

	// Wait for walker to be ready to receive files
	e.mockWalker.WaitForStart()
}

// sendFile sends a file to the backup system via the walker.
func (e *testEnv) sendFile(path string, content []byte) {
	info := newFakeFileInfo(filepath.Base(path), int64(len(content)))
	e.mockWalker.SendFile(path, info)
}

// endWalk signals that directory walking is complete.
func (e *testEnv) endWalk() {
	e.mockWalker.End()
}

// shouldOpen expects and handles a file open call.
func (e *testEnv) shouldOpen(path string, content []byte) {
	e.mockFS.shouldOpen(path, content, nil)
}

// shouldOpenError expects a file open call and returns an error.
func (e *testEnv) shouldOpenError(path string, err error) {
	e.mockFS.shouldOpen(path, nil, err)
}

// completeBackup waits for the backup to finish.
func (e *testEnv) completeBackup() {
	<-e.done
}

// assertUploaded verifies that a file with the given path and content was uploaded.
func (e *testEnv) assertUploaded(path string, expectedContent []byte) {
	expectedHash := sha256.Sum256(expectedContent)

	// Query the database to verify the file was backed up
	var hash []byte
	var size int64
	err := db.DB.QueryRow(`
		SELECT files.hash, sizes.size
		FROM files
		INNER JOIN sizes ON files.hash = sizes.hash
		WHERE files.path = ? AND files.end IS NULL
	`, path).Scan(&hash, &size)

	if err != nil {
		e.t.Errorf("file %s not found in database: %v", path, err)
		return
	}

	if size != int64(len(expectedContent)) {
		e.t.Errorf("file %s: expected size %d, got %d", path, len(expectedContent), size)
	}

	var hashArr [32]byte
	copy(hashArr[:], hash)
	if hashArr != expectedHash {
		e.t.Errorf("file %s: hash mismatch", path)
	}
}

// assertFileCount verifies the number of files in the database.
func (e *testEnv) assertFileCount(expected int) {
	var count int
	err := db.DB.QueryRow("SELECT COUNT(*) FROM files WHERE end IS NULL").Scan(&count)
	if err != nil {
		e.t.Fatal(err)
	}
	if count != expected {
		e.t.Errorf("expected %d files in database, got %d", expected, count)
	}
}

func (e *testEnv) assertNonCurrentFileCount(expected int) {
	var count int
	err := db.DB.QueryRow("SELECT COUNT(*) FROM files WHERE end IS NOT NULL").Scan(&count)
	if err != nil {
		e.t.Fatal(err)
	}
	if count != expected {
		e.t.Errorf("expected %d files in database, got %d", expected, count)
	}
}

// assertBlobEntries verifies the number of blob entries in the database.
func (e *testEnv) assertBlobEntries(expected int) {
	var count int
	err := db.DB.QueryRow("SELECT COUNT(*) FROM blob_entries").Scan(&count)
	if err != nil {
		e.t.Fatal(err)
	}
	if count != expected {
		e.t.Errorf("expected %d blob entries, got %d", expected, count)
	}
}

func (e *testEnv) assertBlobCount(expected int) {
	var count int
	err := db.DB.QueryRow("SELECT COUNT(*) FROM blobs").Scan(&count)
	if err != nil {
		e.t.Fatal(err)
	}
	if count != expected {
		e.t.Errorf("expected %d blob entries, got %d", expected, count)
	}
}

// Test basic single file backup from a directory.
func TestBackupSingleFile(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content := []byte("hello world this is a test file")
	filePath := "/mock/testfile.txt"

	env.beginBackupOnDir("/mock/")
	env.sendFile(filePath, content)
	env.endWalk()
	env.shouldOpen(filePath, content)
	env.completeBackup()

	env.assertUploaded(filePath, content)
	env.assertFileCount(1)
	env.assertBlobEntries(1)
}

// Test backing up a single file directly (not a directory).
// This exercises the "This is a single file...?" path in statInputPaths.
func TestBackupSingleFileDirectly(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content := []byte("single file backup content")
	filePath := "/mock/singlefile.txt"

	// Create fake file info (not a directory)
	fileInfo := fakeFileInfo{
		name:    "singlefile.txt",
		size:    int64(len(content)),
		mode:    0644,
		modTime: time.Now(),
		isDir:   false,
	}

	env.done = make(chan struct{})
	go func() {
		defer close(env.done)
		backupImpl([]string{filePath})
	}()

	// Handle the stat call for the single file
	env.mockFS.shouldStat(filePath, fileInfo, nil)

	// Single file goes directly to scanFile, then to bucketer
	env.shouldOpen(filePath, content)
	env.completeBackup()

	env.assertUploaded(filePath, content)
	env.assertFileCount(1)
	env.assertBlobEntries(1)
}

// Test that files with unique sizes stake a claim and skip hashing entirely,
// going directly from scanner to bucketer without opening the file until upload.
func TestBackupUniqueSizeSkipsHashing(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content1 := []byte("first file with unique size")  // 27 bytes
	content2 := []byte("second file different size!!") // 28 bytes
	file1Path := "/mock/file1.txt"
	file2Path := "/mock/file2.txt"

	env.beginBackupOnDir("/mock/")
	env.sendFile(file1Path, content1)
	env.sendFile(file2Path, content2)
	env.endWalk()

	// Both files have unique sizes, so both skip hashing and go to bucketer.
	// Uploader opens them in order after unstick.
	env.shouldOpen(file1Path, content1)
	env.shouldOpen(file2Path, content2)
	env.completeBackup()

	env.assertUploaded(file1Path, content1)
	env.assertUploaded(file2Path, content2)
	env.assertFileCount(2)
	env.assertBlobEntries(2) // 2 different hashes (batched into one physical blob, but 2 blob_entries)
}

// Test that files >= MinBlobSize (1000) are uploaded immediately without
// waiting for the unstick goroutine to flush the bucketer.
func TestBackupLargeFileBypassesBucketerBuffering(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	// Create content larger than MinBlobSize (1000)
	content := make([]byte, 1500)
	for i := range content {
		content[i] = byte(i % 256)
	}
	filePath := "/mock/largefile.bin"

	env.beginBackupOnDir("/mock/")
	env.sendFile(filePath, content)

	// Large file goes directly to uploader without waiting for endWalk/unstick
	env.shouldOpen(filePath, content)

	env.endWalk()
	env.completeBackup()

	env.assertUploaded(filePath, content)
	env.assertFileCount(1)
	env.assertBlobEntries(1)
}

// Test that two files with the same content are deduplicated - only one
// blob entry is created, but both files are recorded in the files table.
func TestBackupDeduplicatesSameContent(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content := []byte("duplicate content that should only be stored once")
	file1Path := "/mock/file1.txt"
	file2Path := "/mock/file2.txt"

	env.beginBackupOnDir("/mock/")
	env.sendFile(file1Path, content)
	env.sendFile(file2Path, content)

	// file1 stakes size claim, goes to bucketer
	// file2 can't stake claim (same size), goes to hasher which opens it
	env.shouldOpen(file2Path, content)

	env.endWalk()

	// uploader opens file1 after unstick
	env.shouldOpen(file1Path, content)

	env.completeBackup()

	env.assertUploaded(file1Path, content)
	env.assertUploaded(file2Path, content)
	env.assertFileCount(2)
	env.assertBlobEntries(1) // deduplication - same hash stored once
}

// Test that two files with same size but different content both get uploaded.
// The second file must wait for the first's size claim to be released before
// it can determine whether it needs to be uploaded.
func TestBackupSameSizeDifferentContent(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content1 := []byte("first file with this exact size!") // 32 bytes
	content2 := []byte("other file with this exact size!") // 32 bytes (same size, different content)
	file1Path := "/mock/file1.txt"
	file2Path := "/mock/file2.txt"

	env.beginBackupOnDir("/mock/")
	env.sendFile(file1Path, content1)
	env.sendFile(file2Path, content2)

	// file1 stakes size claim, goes to bucketer
	// file2 can't stake claim, goes to hasher which opens and hashes it
	env.shouldOpen(file2Path, content2)

	env.endWalk()

	// uploader opens file1
	env.shouldOpen(file1Path, content1)

	// After file1's upload completes and releases the size claim,
	// file2's hash is compared - it's different, so file2 gets uploaded too
	env.shouldOpen(file2Path, content2)

	env.completeBackup()

	env.assertUploaded(file1Path, content1)
	env.assertUploaded(file2Path, content2)
	env.assertFileCount(2)
	env.assertBlobEntries(2) // different hashes, both uploaded
}

// Test that if a file is appended to between hashing and uploading,
// we detect the size mismatch, still upload the new content, and correctly
// record it. Additionally, if another file was waiting on that hash, it
// gets re-queued for upload since the first file's upload didn't match.
func TestBackupFileGrowsBetweenHashAndUpload(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	originalContent := []byte("original content before append")
	appendedContent := []byte("original content before append" + " APPENDED DATA")
	file1Path := "/mock/file1.txt"
	file2Path := "/mock/file2.txt"

	env.beginBackupOnDir("/mock/")
	// Both files have same size initially
	env.sendFile(file1Path, originalContent)
	env.sendFile(file2Path, originalContent)

	// file2 goes to hasher, reads original content
	env.shouldOpen(file2Path, originalContent)

	env.endWalk()

	// file1 goes to uploader, but the file has grown!
	// Uploader reads the appended content instead
	env.shouldOpen(file1Path, appendedContent)

	// Since file1's uploaded hash doesn't match what file2 expected,
	// file2 gets re-queued and uploaded
	env.shouldOpen(file2Path, originalContent)

	env.completeBackup()

	// file1 is recorded with the appended content's hash
	env.assertUploaded(file1Path, appendedContent)
	// file2 is recorded with the original content's hash
	env.assertUploaded(file2Path, originalContent)
	env.assertFileCount(2)
	env.assertBlobEntries(2) // two different hashes now
}

// Test the hashLateMap revival path with multiple waiters: when multiple files
// hash to the same value, and the first one's upload fails (file changed),
// the next file is revived and must process ALL remaining waiters.
//
// Setup: file0 stakes the size claim (different content, same size).
// file1, file2, file3 all hash to originalContent. file1 goes first, file2 and
// file3 add themselves to hashLateMap. file1's upload fails (file grew).
// file2 is revived, uploads successfully, and must record both file2 AND file3.
func TestBackupHashLateMapRevival(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	// All four files have the same size (30 bytes)
	file0Content := []byte("different content, same size!!")               // 30 bytes, different hash
	originalContent := []byte("original content before grow!!")            // 30 bytes
	appendedContent := []byte("original content before grow!!" + " GROWN") // 36 bytes

	file0Path := "/mock/file0.txt"
	file1Path := "/mock/file1.txt"
	file2Path := "/mock/file2.txt"
	file3Path := "/mock/file3.txt"

	env.beginBackupOnDir("/mock/")

	// file0 stakes the size claim (30 bytes)
	env.sendFile(file0Path, file0Content)

	// file1 goes to hasher (can't stake, size claimed by file0)
	env.sendFile(file1Path, originalContent)
	env.shouldOpen(file1Path, originalContent)

	// file2 goes to hasher, calculates same hash, waits on size mutex
	env.sendFile(file2Path, originalContent)
	env.shouldOpen(file2Path, originalContent)

	// file3 goes to hasher, calculates same hash, waits on size mutex
	env.sendFile(file3Path, originalContent)
	env.shouldOpen(file3Path, originalContent)

	env.endWalk()

	// Uploader opens file0 (from bucketer after unstick)
	env.shouldOpen(file0Path, file0Content)

	// After file0 uploads, size claim released. file1, file2, file3 goroutines unblock.
	// file1 reaches hashLateMap first, adds itself, sends to bucketer.
	// file2 finds file1 in hashLateMap, appends itself.
	// file3 finds [file1, file2] in hashLateMap, appends itself.
	// hashLateMap[H] = [file1, file2, file3]

	// file1 goes to uploader but has grown! Hash mismatch.
	env.shouldOpen(file1Path, appendedContent)

	// uploadFailure removes file1, revives file2 from hashLateMap[H] = [file2, file3]
	// file2 uploads successfully
	env.shouldOpen(file2Path, originalContent)

	// file2's upload matches expected hash, so it processes hashLateMap[H] = [file2, file3]
	// Both file2 and file3 are recorded in the files table

	env.completeBackup()

	env.assertUploaded(file0Path, file0Content)
	env.assertUploaded(file1Path, appendedContent) // recorded with actual content
	env.assertUploaded(file2Path, originalContent) // original hash backed up
	env.assertUploaded(file3Path, originalContent) // file3 also recorded via hashLateMap
	env.assertFileCount(4)
	env.assertBlobEntries(3) // file0's hash, file1's grown hash, and shared hash for file2+file3
}

// Test that when a staked size claim file fails to open during upload,
// the size claim is properly released and waiting files can proceed.
func TestBackupStakedClaimOpenFailure(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content1 := []byte("file one content here") // 21 bytes
	content2 := []byte("file two content here") // 21 bytes (same size, different content)

	file1Path := "/mock/file1.txt"
	file2Path := "/mock/file2.txt"

	env.beginBackupOnDir("/mock/")

	// file1 stakes size claim
	env.sendFile(file1Path, content1)

	// file2 has same size, goes to hasher, will wait on size mutex
	env.sendFile(file2Path, content2)
	env.shouldOpen(file2Path, content2) // hasher reads file2

	env.endWalk()

	// file1's upload fails - can't open the file!
	env.mockFS.shouldOpen(file1Path, nil, os.ErrNotExist)

	// Size claim is released, file2's goroutine unblocks
	// file2 finds its hash not in DB, sends to bucketer, uploads
	env.shouldOpen(file2Path, content2)

	env.completeBackup()

	// file1 was NOT recorded (open failed)
	env.assertFileCount(1)
	env.assertUploaded(file2Path, content2)
	env.assertBlobEntries(1)
}

// Test that when a file's hash already exists in the database from a previous
// backup, the hasher recognizes this and records the file without re-uploading.
func TestBackupHashAlreadyInDatabase(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content := []byte("content that will be backed up twice")
	file1Path := "/mock1/file1.txt"
	file2Path := "/mock2/file2.txt"

	// First backup: upload file1
	env.beginBackupOnDir("/mock1/")
	env.sendFile(file1Path, content)
	env.endWalk()
	env.shouldOpen(file1Path, content)
	env.completeBackup()

	env.assertFileCount(1)
	env.assertBlobEntries(1)

	env.reset()

	// Second backup: file2 in a DIFFERENT directory has same content as file1
	// Using a different directory so file1 doesn't get pruned
	env.beginBackupOnDir("/mock2/")
	env.sendFile(file2Path, content)

	// Size exists in sizes table from file1, so file2 can't stake a claim
	// It goes to hasher, which opens and hashes it
	env.shouldOpen(file2Path, content)

	env.endWalk()
	// No more opens expected - hash is found in blob_entries, so no upload needed
	env.completeBackup()

	env.assertFileCount(2)   // both files recorded
	env.assertBlobEntries(1) // still only one blob entry - deduplication across backups!
	env.assertUploaded(file2Path, content)
}

// Test that when a file's content hasn't changed but its modtime has,
// the hasher updates fs_modified without re-uploading.
func TestBackupHashUnchangedModtimeChanged(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content := []byte("content with changing modtime")
	filePath := "/mock/file.txt"

	// First backup: upload the file
	env.beginBackupOnDir("/mock/")
	env.sendFile(filePath, content)
	env.endWalk()
	env.shouldOpen(filePath, content)
	env.completeBackup()

	env.assertFileCount(1)
	env.assertBlobEntries(1)

	// Get the original fs_modified
	var originalFsModified int64
	err := db.DB.QueryRow("SELECT fs_modified FROM files WHERE path = ? AND end IS NULL", filePath).Scan(&originalFsModified)
	if err != nil {
		t.Fatal(err)
	}

	env.reset()

	// Second backup: same file, same content, but different modtime
	env.beginBackupOnDir("/mock/")

	// Send file with a newer modtime (simulating touch)
	info := fakeFileInfo{
		name:    "file.txt",
		size:    int64(len(content)),
		mode:    0644,
		modTime: time.Now().Add(1 * time.Hour), // 1 hour in the future
		isDir:   false,
	}
	env.mockWalker.SendFile(filePath, info)

	// File has different modtime, so it goes to hasher
	// Hasher opens and reads it to calculate hash
	env.shouldOpen(filePath, content)

	env.endWalk()
	// Hash matches expected hash from DB - no upload, just update fs_modified
	env.completeBackup()

	// Still only 1 file record (not a new version, just updated fs_modified)
	env.assertFileCount(1)
	env.assertBlobEntries(1)

	// Verify fs_modified was updated
	var newFsModified int64
	err = db.DB.QueryRow("SELECT fs_modified FROM files WHERE path = ? AND end IS NULL", filePath).Scan(&newFsModified)
	if err != nil {
		t.Fatal(err)
	}
	if newFsModified == originalFsModified {
		t.Errorf("fs_modified should have been updated, but it wasn't")
	}
	env.assertNonCurrentFileCount(0)
}

// Test that when a file's content actually changes (not just modtime),
// the hasher detects the new hash and uploads the new content.
// This exercises the "hash has changed" log path (hasher.go:59-60).
func TestBackupModifiedFileHashChanged(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	// Both contents must be the same size so the second backup goes through hasher
	// (if size differs, it would take the staked claim path since new size is unique)
	originalContent := []byte("original file content!!!") // 24 bytes
	modifiedContent := []byte("modified file content!!!") // 24 bytes (same size, different content)
	filePath := "/mock/file.txt"

	// First backup: upload the file with original content
	env.beginBackupOnDir("/mock/")
	env.sendFile(filePath, originalContent)
	env.endWalk()
	env.shouldOpen(filePath, originalContent)
	env.completeBackup()

	env.assertFileCount(1)
	env.assertBlobEntries(1)

	env.reset()

	// Second backup: same file, same size, but different content AND different modtime
	env.beginBackupOnDir("/mock/")

	// Send file with newer modtime (same size, different content)
	info := fakeFileInfo{
		name:    "file.txt",
		size:    int64(len(modifiedContent)), // same size as original
		mode:    0644,
		modTime: time.Now().Add(1 * time.Hour),
		isDir:   false,
	}
	env.mockWalker.SendFile(filePath, info)

	// File is MODIFIED (exists in DB with different modtime)
	// Size exists in sizes table, so it goes to hasher which opens it
	env.shouldOpen(filePath, modifiedContent)

	env.endWalk()

	// Hash is different from expected (was originalContent's hash, now modifiedContent's hash)
	// This triggers "hash has changed" log and sends to bucketer for upload
	env.shouldOpen(filePath, modifiedContent)

	env.completeBackup()

	// Old version is ended, new version is current
	env.assertFileCount(1)   // only 1 current (end IS NULL)
	env.assertBlobEntries(2) // two different hashes
	env.assertUploaded(filePath, modifiedContent)

	// Verify old version was ended
	var endedCount int
	err := db.DB.QueryRow("SELECT COUNT(*) FROM files WHERE path = ? AND end IS NOT NULL", filePath).Scan(&endedCount)
	if err != nil {
		t.Fatal(err)
	}
	if endedCount != 1 {
		t.Errorf("expected 1 ended file record, got %d", endedCount)
	}
}

// Test that if a file shrinks between hashing and uploading,
// we detect the change and upload the new (smaller) content.
func TestBackupFileShrinksBetweenHashAndUpload(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	originalContent := []byte("original content that is longer!!!!")
	shrunkContent := []byte("shrunk content")
	file1Path := "/mock/file1.txt"
	file2Path := "/mock/file2.txt"

	env.beginBackupOnDir("/mock/")

	// Both files reported as same size (original length)
	env.sendFile(file1Path, originalContent)
	env.sendFile(file2Path, originalContent)

	// file2 goes to hasher, reads original content
	env.shouldOpen(file2Path, originalContent)

	env.endWalk()

	// file1 goes to uploader, but the file has shrunk!
	env.shouldOpen(file1Path, shrunkContent)

	// file1's hash doesn't match what file2 expected, file2 gets re-queued
	env.shouldOpen(file2Path, originalContent)

	env.completeBackup()

	// file1 recorded with shrunk content's hash
	env.assertUploaded(file1Path, shrunkContent)
	// file2 recorded with original content's hash
	env.assertUploaded(file2Path, originalContent)
	env.assertFileCount(2)
	env.assertBlobEntries(2)
}

// Test that when SkipHashFailures is true and hashing fails,
// the file is skipped without panicking.
func TestBackupSkipHashFailuresTrue(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	// Enable SkipHashFailures
	config.SetSkipHashFailures(true)
	defer config.SetSkipHashFailures(false)

	content1 := []byte("first file to establish size")
	content2 := []byte("second file same size as 1st") // same size, will go to hasher
	file1Path := "/mock1/file1.txt"
	file2Path := "/mock2/file2.txt"

	// First backup: upload file1 to establish size in DB
	env.beginBackupOnDir("/mock1/")
	env.sendFile(file1Path, content1)
	env.endWalk()
	env.shouldOpen(file1Path, content1)
	env.completeBackup()

	env.assertFileCount(1)

	env.reset()

	// Second backup in DIFFERENT directory: file2 has same size, goes to hasher
	// But we'll make the open fail
	env.beginBackupOnDir("/mock2/")
	env.sendFile(file2Path, content2)

	// Hasher tries to open file2 for hashing - we return an error
	env.shouldOpenError(file2Path, os.ErrNotExist)

	env.endWalk()
	env.completeBackup()

	// file2 was skipped due to SkipHashFailures=true
	// file1 still exists (different directory, not pruned)
	env.assertFileCount(1)
}

// NOTE: We'd also like to test that when SkipHashFailures is false and hashing
// fails, the backup panics. However, the panic occurs in the hasher goroutine,
// not the main goroutine, so we can't catch it with recover() in a test.
// The behavior has been manually verified: when SkipHashFailures=false and
// hashAFile returns an error, hasher.go:36 panics with the error.

// Test that many small files get batched together by the bucketer.
// This exercises the "Dumping blob" path when tmpSize >= minSize.
func TestBackupSmallFilesBatching(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	// Create enough small files that their total size exceeds MinBlobSize (1000)
	// Sizes: 80+81+82+83+84+85+86+87+88+89+90+91 = 1026 bytes > 1000
	numFiles := 12
	files := make([]struct {
		path    string
		content []byte
	}, numFiles)

	for i := 0; i < numFiles; i++ {
		// Each file has unique size to avoid hasher path
		content := make([]byte, 80+i) // 80, 81, 82, ... 91 bytes
		for j := range content {
			content[j] = byte('a' + i)
		}
		files[i].path = fmt.Sprintf("/mock/file%02d.txt", i)
		files[i].content = content
	}

	env.beginBackupOnDir("/mock/")

	// Send all files - each stakes a unique size claim
	for _, f := range files {
		env.sendFile(f.path, f.content)
	}

	// KEY ASSERTION: The bucketer should dump the blob BEFORE endWalk is called,
	// because the cumulative size (1026) exceeds MinBlobSize (1000).
	// If shouldOpen succeeds before endWalk, it proves the blob was released
	// due to the batching threshold, not because of the unstick goroutine.
	for _, f := range files {
		env.shouldOpen(f.path, f.content)
	}

	env.endWalk()
	env.completeBackup()

	// All files should be recorded
	env.assertFileCount(numFiles)
	env.assertBlobCount(1)
	env.assertBlobEntries(numFiles)
	for _, f := range files {
		env.assertUploaded(f.path, f.content)
	}
}

// Test that unmodified files (same modtime, same size) are skipped entirely.
// This exercises the early return in CompareFileToDb when nothing has changed.
func TestBackupUnmodifiedFilesSkipped(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content := []byte("file that won't change")
	filePath := "/mock/unchanged.txt"

	// First backup: upload the file
	env.beginBackupOnDir("/mock/")
	env.sendFile(filePath, content)
	env.endWalk()
	env.shouldOpen(filePath, content)
	env.completeBackup()

	env.assertFileCount(1)
	env.assertBlobEntries(1)

	// Get the modtime that was recorded
	var fsModified int64
	err := db.DB.QueryRow("SELECT fs_modified FROM files WHERE path = ? AND end IS NULL", filePath).Scan(&fsModified)
	if err != nil {
		t.Fatal(err)
	}

	// Reset for second backup
	env.reset()

	// Second backup: same file with SAME modtime and size - should be skipped entirely
	env.beginBackupOnDir("/mock/")

	// Send file with the exact same modtime as recorded in DB
	info := fakeFileInfo{
		name:    "unchanged.txt",
		size:    int64(len(content)),
		mode:    0644,
		modTime: time.Unix(fsModified, 0), // same modtime as first backup
		isDir:   false,
	}
	env.mockWalker.SendFile(filePath, info)

	env.endWalk()
	// NO shouldOpen call - the file should be skipped entirely without being read!
	env.completeBackup()

	// Still only 1 file, 1 blob entry - nothing changed
	env.assertFileCount(1)
	env.assertBlobEntries(1)
}

// Test that deleted files are marked as ended by pruneDeletedFiles.
func TestBackupPruneDeletedFiles(t *testing.T) {
	env := setupUnitTestEnv(t)
	defer env.cleanup()

	content1 := []byte("file that will be deleted")
	content2 := []byte("file that will remain!!!")
	file1Path := "/mock/deleted.txt"
	file2Path := "/mock/remains.txt"

	// First backup: upload both files
	env.beginBackupOnDir("/mock/")
	env.sendFile(file1Path, content1)
	env.sendFile(file2Path, content2)
	env.endWalk()
	env.shouldOpen(file1Path, content1)
	env.shouldOpen(file2Path, content2)
	env.completeBackup()

	env.assertFileCount(2)
	env.assertNonCurrentFileCount(0) // no ended files yet

	// Reset for second backup
	env.reset()

	// Second backup: only file2 exists, file1 was "deleted"
	env.beginBackupOnDir("/mock/")

	// Only send file2 - file1 is "deleted" (not in the walk)
	// Use same modtime so file2 is skipped as unmodified
	var fsModified int64
	err := db.DB.QueryRow("SELECT fs_modified FROM files WHERE path = ? AND end IS NULL", file2Path).Scan(&fsModified)
	if err != nil {
		t.Fatal(err)
	}
	info := fakeFileInfo{
		name:    "remains.txt",
		size:    int64(len(content2)),
		mode:    0644,
		modTime: time.Unix(fsModified, 0),
		isDir:   false,
	}
	env.mockWalker.SendFile(file2Path, info)

	env.endWalk()
	// No shouldOpen - file2 is unmodified, file1 doesn't exist
	env.completeBackup()

	// file2 is still current, file1 is now ended
	env.assertFileCount(1)           // only file2 is current
	env.assertNonCurrentFileCount(1) // file1 is ended
	env.assertBlobEntries(2)         // both blob entries still exist

	// Verify file1 was marked as ended
	var endedPath string
	err = db.DB.QueryRow("SELECT path FROM files WHERE end IS NOT NULL").Scan(&endedPath)
	if err != nil {
		t.Fatal(err)
	}
	if endedPath != file1Path {
		t.Errorf("expected %s to be ended, got %s", file1Path, endedPath)
	}
}
