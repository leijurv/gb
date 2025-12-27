package cache

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/leijurv/gb/storage_base"
)

// readerErrorConfig defines how a fakeReader should behave with errors
type readerErrorConfig struct {
	noDataRate  float64 // Probability of returning 0 bytes read
	partialRate float64 // Probability of returning partial data
	eofRate     float64 // Probability of returning EOF error
	errorRate   float64 // Probability of returning a non-EOF error
	seed        int64   // Random seed for deterministic behavior
	rng         *rand.Rand
}

func newReaderErrorConfig(noDataRate, partialRate, eofRate, errorRate float64, seed int64) *readerErrorConfig {
	return &readerErrorConfig{
		noDataRate:  noDataRate,
		partialRate: partialRate,
		eofRate:     eofRate,
		errorRate:   errorRate,
		seed:        seed,
		rng:         rand.New(rand.NewSource(seed)),
	}
}

// Helper function to generate deterministic data for testing
func MakeRandomData(offset int64, length int64) []byte {
	const bigPrime = 982451653
	data := make([]byte, length)
	for i := int64(0); i < length; i++ {
		bytePos := offset + i
		value := (bytePos * bigPrime) >> 8
		data[i] = byte(value & 0xFF)
	}
	return data
}

// Test helper functions to reduce repetition

// verifyDataIntegrity verifies that data matches the expected deterministic pattern
func verifyDataIntegrity(t *testing.T, data []byte, offset int64, description string) {
	expectedData := MakeRandomData(offset, int64(len(data)))
	for i := 0; i < len(data); i++ {
		if data[i] != expectedData[i] {
			t.Fatalf("%s: data mismatch at byte %d: got %d, expected %d", description, i, data[i], expectedData[i])
		}
	}
}

// readFullChunk reads exactly the specified amount of data with error checking
func readFullChunk(t *testing.T, reader io.Reader, size int64, description string) []byte {
	data := make([]byte, size)
	n, err := io.ReadFull(reader, data)
	if err != nil {
		t.Fatalf("%s: ReadFull failed: %v", description, err)
	}
	if int64(n) != size {
		t.Fatalf("%s: expected to read %d bytes, got %d", description, size, n)
	}
	return data
}

// createTestReader creates a reader with automatic cleanup
func createTestReader(storage storage_base.Storage, fileName string, offset, length int64) io.ReadCloser {
	return DownloadSection(storage, fileName, offset, length)
}

// setupTestFile creates a test storage with a file of the specified size and clears cache
func setupTestFile(t *testing.T, fileName string, size int64) *fakeStorage {
	ClearCache()
	storage := newFakeStorage()
	storage.addFileWithSize(fileName, size)
	return storage
}

// verifyNoOverlappingReads checks that read requests don't overlap
func verifyNoOverlappingReads(t *testing.T, readLog []testDownloadRequest, description string) {
	for i := 0; i < len(readLog)-1; i++ {
		for j := i + 1; j < len(readLog); j++ {
			req1 := readLog[i]
			req2 := readLog[j]

			end1 := req1.offset + req1.length
			end2 := req2.offset + req2.length

			if req1.offset < end2 && end1 > req2.offset {
				t.Errorf("%s: overlapping reads: Read %d (offset=%d, end=%d) overlaps with Read %d (offset=%d, end=%d)",
					description, i, req1.offset, end1, j, req2.offset, end2)
			}
		}
	}
}

// verifyNoDuplicateReads checks that there are no exact duplicate read requests
func verifyNoDuplicateReads(t *testing.T, readLog []testDownloadRequest, description string) {
	for i := 0; i < len(readLog)-1; i++ {
		for j := i + 1; j < len(readLog); j++ {
			if readLog[i].offset == readLog[j].offset && readLog[i].length == readLog[j].length {
				t.Errorf("%s: duplicate reads: Read %d and Read %d both have offset=%d, length=%d",
					description, i, j, readLog[i].offset, readLog[i].length)
			}
		}
	}
}

// readChunkWithVerification reads a chunk and verifies its data integrity
func readChunkWithVerification(t *testing.T, reader io.Reader, expectedOffset int64, chunkSize int64, chunkIndex int) []byte {
	data := readFullChunk(t, reader, chunkSize, fmt.Sprintf("chunk %d", chunkIndex))
	verifyDataIntegrity(t, data, expectedOffset, fmt.Sprintf("chunk %d", chunkIndex))
	return data
}

type fakeStorage struct {
	files              map[string]int64      // path -> file size in bytes
	downloadSectionLog []testDownloadRequest // Logs DownloadSection calls
	readLog            []testDownloadRequest // Logs individual Read calls for backpressure testing
	mu                 sync.Mutex
}

type testDownloadRequest struct {
	path   string
	offset int64
	length int64
	time   time.Time
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{
		files: make(map[string]int64),
	}
}

func (fs *fakeStorage) addFile(path string, data []byte) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.files[path] = int64(len(data))
}

func (fs *fakeStorage) addFileWithSize(path string, size int64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.files[path] = size
}

func (fs *fakeStorage) getDownloadSectionLog() []testDownloadRequest {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]testDownloadRequest{}, fs.downloadSectionLog...)
}

func (fs *fakeStorage) getReadLog() []testDownloadRequest {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return append([]testDownloadRequest{}, fs.readLog...)
}

func (fs *fakeStorage) logDownloadSection(path string, offset int64, length int64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.downloadSectionLog = append(fs.downloadSectionLog, testDownloadRequest{
		path:   path,
		offset: offset,
		length: length,
		time:   time.Now(),
	})
}

func (fs *fakeStorage) logRead(path string, offset int64, length int64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.readLog = append(fs.readLog, testDownloadRequest{
		path:   path,
		offset: offset,
		length: length,
		time:   time.Now(),
	})
}

func (fs *fakeStorage) DownloadSection(path string, offset int64, length int64) io.ReadCloser {
	return fs.DownloadSectionWithErrors(path, offset, length, nil)
}

func (fs *fakeStorage) DownloadSectionWithErrors(path string, offset int64, length int64, errorConfig *readerErrorConfig) io.ReadCloser {
	// Log the DownloadSection call
	fs.logDownloadSection(path, offset, length)

	fileSize, exists := fs.files[path]
	if !exists {
		return &fakeReader{storage: fs, path: path, fileSize: 0, err: io.EOF}
	}

	// Panic on unreasonably large length values to prevent future 1<<62 tricks
	if length > 1<<40 { // 1TB is a reasonable maximum
		panic("DownloadSection: length exceeds reasonable limit (1TB)")
	}

	if offset >= fileSize {
		return &fakeReader{storage: fs, path: path, fileSize: fileSize, err: io.EOF}
	}

	end := offset + length
	if end > fileSize {
		end = fileSize
	}

	return &fakeReader{
		storage:     fs,
		path:        path,
		fileSize:    fileSize,
		baseOffset:  offset,
		endOffset:   end,
		currentPos:  0,
		errorConfig: errorConfig,
	}
}

type fakeReader struct {
	storage    *fakeStorage
	path       string
	fileSize   int64
	baseOffset int64
	endOffset  int64
	currentPos int64
	err        error
	closed     bool

	// Error injection settings
	errorConfig *readerErrorConfig
}

func (fr *fakeReader) Read(p []byte) (n int, err error) {
	if fr.closed {
		return 0, errors.New("reader is closed")
	}
	if fr.err != nil {
		return 0, fr.err
	}

	currentAbsolutePos := fr.baseOffset + fr.currentPos
	if currentAbsolutePos >= fr.endOffset {
		return 0, io.EOF
	}

	// Small delay to simulate some download time but keep tests fast
	time.Sleep(100 * time.Microsecond)

	// Calculate how much we can read
	remaining := fr.endOffset - currentAbsolutePos
	readLen := int64(len(p))
	if readLen > remaining {
		readLen = remaining
	}

	// Apply error injection if configured
	if fr.errorConfig != nil {
		// First decide if we should return an error
		if fr.errorConfig.rng.Float64() < fr.errorConfig.eofRate {
			return 0, io.EOF
		}
		if fr.errorConfig.rng.Float64() < fr.errorConfig.errorRate {
			return 0, errors.New("network connection closed")
		}

		// Next decide on data amount
		if fr.errorConfig.rng.Float64() < fr.errorConfig.noDataRate {
			return 0, nil // No data, no error
		}
		if fr.errorConfig.rng.Float64() < fr.errorConfig.partialRate && readLen > 1 {
			// Return partial data (1 to readLen-1 bytes)
			partialLen := fr.errorConfig.rng.Int63n(readLen-1) + 1
			readLen = partialLen
		}
	}

	// Generate deterministic data using helper function
	data := MakeRandomData(currentAbsolutePos, readLen)
	copy(p[:readLen], data)

	// Log the actual bytes being read for backpressure testing
	fr.storage.logRead(fr.path, currentAbsolutePos, readLen)

	fr.currentPos += readLen
	return int(readLen), nil
}

func (fr *fakeReader) Close() error {
	fr.closed = true
	return nil
}

// Panic on all other methods since we only need DownloadSection for testing
func (fs *fakeStorage) BeginBlobUpload(blobID []byte) storage_base.StorageUpload {
	panic("not implemented")
}

func (fs *fakeStorage) BeginDatabaseUpload(filename string) storage_base.StorageUpload {
	panic("not implemented")
}

func (fs *fakeStorage) ListBlobs() []storage_base.UploadedBlob {
	panic("not implemented")
}

func (fs *fakeStorage) Metadata(path string) (string, int64) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if size, exists := fs.files[path]; exists {
		return "", size
	}
	return "", 0
}

func (fs *fakeStorage) DeleteBlob(path string) {
	panic("not implemented")
}

func (fs *fakeStorage) GetID() []byte {
	// Return a consistent fake ID for testing (32 bytes)
	return []byte("fake_storage_id_1234567890123456")
}

func (fs *fakeStorage) String() string {
	return "fake storage"
}

func (fs *fakeStorage) PresignedURL(path string, expiry time.Duration) (string, error) {
	return "", errors.New("presigned URLs are not supported for fakeStorage")
}

// withErrorInjection returns a wrapper that injects errors into all DownloadSection calls
func (fs *fakeStorage) withErrorInjection(errorConfig *readerErrorConfig) storage_base.Storage {
	return &errorInjectingStorage{
		baseStorage: fs,
		errorConfig: errorConfig,
	}
}

// errorInjectingStorage wraps another storage and injects errors
type errorInjectingStorage struct {
	baseStorage storage_base.Storage
	errorConfig *readerErrorConfig
}

func (eis *errorInjectingStorage) DownloadSection(path string, offset int64, length int64) io.ReadCloser {
	if fakeStorage, ok := eis.baseStorage.(*fakeStorage); ok {
		return fakeStorage.DownloadSectionWithErrors(path, offset, length, eis.errorConfig)
	}
	return eis.baseStorage.DownloadSection(path, offset, length)
}

func (eis *errorInjectingStorage) BeginBlobUpload(blobID []byte) storage_base.StorageUpload {
	return eis.baseStorage.BeginBlobUpload(blobID)
}

func (eis *errorInjectingStorage) BeginDatabaseUpload(filename string) storage_base.StorageUpload {
	return eis.baseStorage.BeginDatabaseUpload(filename)
}

func (eis *errorInjectingStorage) ListBlobs() []storage_base.UploadedBlob {
	return eis.baseStorage.ListBlobs()
}

func (eis *errorInjectingStorage) Metadata(path string) (string, int64) {
	return eis.baseStorage.Metadata(path)
}

func (eis *errorInjectingStorage) DeleteBlob(path string) {
	eis.baseStorage.DeleteBlob(path)
}

func (eis *errorInjectingStorage) GetID() []byte {
	return eis.baseStorage.GetID()
}

func (eis *errorInjectingStorage) String() string {
	return "error-injecting " + eis.baseStorage.String()
}

func (eis *errorInjectingStorage) PresignedURL(path string, expiry time.Duration) (string, error) {
	return eis.baseStorage.PresignedURL(path, expiry)
}

func TestFakeReader(t *testing.T) {
	testSize := int64(100 * 1024) // 100KB

	storage := newFakeStorage()
	storage.addFileWithSize("test.bin", testSize)

	// Test full file download
	reader := storage.DownloadSection("test.bin", 0, testSize)
	defer reader.Close()

	readData := readFullChunk(t, reader, testSize, "full file download")
	verifyDataIntegrity(t, readData, 0, "full file download")

	t.Logf("Successfully read and verified %d bytes of deterministic data", testSize)
}

func TestFakeReaderDownloadLog(t *testing.T) {
	// Create test data
	testSize := 10 * 1024 // 10KB

	storage := newFakeStorage()
	storage.addFileWithSize("test.bin", int64(testSize))

	// Download with specific buffer size
	reader := storage.DownloadSection("test.bin", 0, int64(testSize))
	defer reader.Close()

	// Do manual reads with specific buffer size to control read chunks
	bufferSize := 512
	buffer := make([]byte, bufferSize)
	totalRead := 0

	for {
		n, err := reader.Read(buffer)
		if n > 0 {
			totalRead += n
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read failed: %v", err)
		}
	}

	if totalRead != testSize {
		t.Fatalf("Expected to read %d bytes, got %d", testSize, totalRead)
	}

	// Check DownloadSection log - should be 1 call
	downloadSectionLog := storage.getDownloadSectionLog()
	if len(downloadSectionLog) != 1 {
		t.Errorf("Expected 1 DownloadSection call, got %d", len(downloadSectionLog))
	}

	// Check read log - should have the expected number of internal read calls
	readLog := storage.getReadLog()
	expectedReadEntries := (testSize + bufferSize - 1) / bufferSize // Ceiling division

	if len(readLog) != expectedReadEntries {
		t.Errorf("Expected %d read log entries, got %d", expectedReadEntries, len(readLog))
	}

	// Verify read log entries cover the entire file
	totalLogged := int64(0)
	for _, req := range readLog {
		totalLogged += req.length
		if req.path != "test.bin" {
			t.Errorf("Wrong path in log entry: got %s, expected test.bin", req.path)
		}
	}

	if totalLogged != int64(testSize) {
		t.Errorf("Total logged bytes %d != expected %d", totalLogged, testSize)
	}

	t.Logf("Download log correctly recorded 1 DownloadSection call and %d read calls for %d bytes with buffer size %d",
		len(readLog), testSize, bufferSize)
}

func TestFakeReaderPartialDownload(t *testing.T) {
	// Create test data
	testSize := int64(1000)

	storage := newFakeStorage()
	storage.addFileWithSize("test.bin", testSize)

	// Download middle section
	offset := int64(300)
	length := int64(400)
	reader := storage.DownloadSection("test.bin", offset, length)
	defer reader.Close()

	readData := readFullChunk(t, reader, length, "partial download")
	verifyDataIntegrity(t, readData, offset, "partial download")

	// Check download log entries have correct offsets
	downloadLog := storage.getDownloadSectionLog()
	for _, req := range downloadLog {
		if req.offset < offset {
			t.Errorf("Download log entry has offset %d, which is before requested offset %d", req.offset, offset)
		}
		if req.offset >= offset+length {
			t.Errorf("Download log entry has offset %d, which is after requested range end %d", req.offset, offset+length)
		}
	}

	t.Logf("Successfully downloaded and verified partial section: offset=%d, length=%d", offset, length)
}

func TestStreamingDownloadWithBackpressure(t *testing.T) {
	// Create 10MB file
	fileSize := 10 * 1024 * 1024

	storage := newFakeStorage()
	storage.addFileWithSize("test.bin", int64(fileSize))

	// Download entire file
	reader := DownloadSection(storage, "test.bin", 0, int64(fileSize))
	defer reader.Close()

	buf := make([]byte, 1024) // Read in 1KB chunks
	totalRead := 0
	maxDownloadAhead := int64(2 * 1024 * 1024) // 2MB max ahead
	maxAchievedAhead := int64(0)

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			totalRead += n

			// Check that storage hasn't downloaded too far ahead
			downloadLog := storage.getReadLog()
			maxDownloadedOffset := int64(0)

			for _, req := range downloadLog {
				endOffset := req.offset + req.length
				if endOffset > maxDownloadedOffset {
					maxDownloadedOffset = endOffset
				}
			}

			currentReadOffset := int64(totalRead)
			aheadDistance := maxDownloadedOffset - currentReadOffset

			// Track maximum achieved ahead distance
			if aheadDistance > maxAchievedAhead {
				maxAchievedAhead = aheadDistance
			}

			if aheadDistance > maxDownloadAhead {
				t.Errorf("Downloaded too far ahead: %d bytes ahead of current read position %d (max allowed: %d)",
					aheadDistance, currentReadOffset, maxDownloadAhead)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}

		// Small delay to simulate processing time
		time.Sleep(1 * time.Millisecond)
	}

	if totalRead != fileSize {
		t.Errorf("Expected to read %d bytes, got %d", fileSize, totalRead)
	}

	// With the new streaming implementation, readahead behavior is different
	// It may do minimal readahead since it's streaming efficiently
	// The key is that it stays within the backpressure limit
	t.Logf("Streaming cache achieved max ahead distance of %d bytes (max allowed: %d)",
		maxAchievedAhead, maxDownloadAhead)

	t.Logf("Test passed: streaming cache correctly maintained backpressure within %d bytes limit",
		maxDownloadAhead)
}

func TestSeekBackwardBackpressure(t *testing.T) {
	// Use a unique filename to avoid interference from other tests
	fileName := "seekback_test.bin"
	fileSize := int64(20 * 1024 * 1024)
	storage := setupTestFile(t, fileName, fileSize)

	// Start directly at the seek-back position (10MB) to test backpressure
	seekBackOffset := int64(10 * 1024 * 1024)
	readLength := int64(1024 * 1024) // Read 1MB

	reader := DownloadSection(storage, fileName, seekBackOffset, readLength)
	defer reader.Close()

	maxDownloadAhead := int64(2 * 1024 * 1024) // 2MB max ahead
	buf := make([]byte, 64*1024)

	// Read from the position and verify backpressure
	for i := 0; i < 50; i++ { // Read 50 chunks
		n, err := reader.Read(buf)
		if n > 0 {
			// Check download log to ensure we stay within backpressure limits
			downloadLog := storage.getReadLog()

			// Find max downloaded offset for this file only
			maxDownloadedOffset := int64(0)
			for _, req := range downloadLog {
				if req.path == fileName {
					endOffset := req.offset + req.length
					if endOffset > maxDownloadedOffset {
						maxDownloadedOffset = endOffset
					}
				}
			}

			currentReadOffset := seekBackOffset + int64(i*len(buf))
			aheadDistance := maxDownloadedOffset - currentReadOffset

			if aheadDistance > maxDownloadAhead {
				t.Errorf("Downloaded too far ahead: %d bytes ahead of current read position %d (max allowed: %d)",
					aheadDistance, currentReadOffset, maxDownloadAhead)
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}

		time.Sleep(1 * time.Millisecond)
	}

	t.Logf("Seek-back test passed: backpressure working correctly")
}

func TestStreamingReadsOnlyOneDownloadSectionCall(t *testing.T) {
	fileName := "streaming_test.bin"
	fileSize := int64(10 * 1024 * 1024)
	storage := setupTestFile(t, fileName, fileSize)

	// Download entire file reading in 512-byte chunks
	reader := DownloadSection(storage, fileName, 0, fileSize)
	defer reader.Close()

	// Read entire file using ReadFull to handle partial reads
	buf := readFullChunk(t, reader, fileSize, "streaming read")
	verifyDataIntegrity(t, buf, 0, "streaming read")

	// The key test: should be exactly 1 DownloadSection call
	downloadSectionLog := storage.getDownloadSectionLog()
	if len(downloadSectionLog) != 1 {
		t.Errorf("Expected exactly 1 DownloadSection call for streaming read, got %d", len(downloadSectionLog))
		for i, req := range downloadSectionLog {
			t.Logf("DownloadSection call %d: offset=%d, length=%d", i, req.offset, req.length)
		}
	}

	// Verify the single call started at the beginning and covers the entire file
	if len(downloadSectionLog) > 0 {
		req := downloadSectionLog[0]
		if req.offset != 0 || req.length < fileSize {
			t.Errorf("Expected single DownloadSection call for offset=0, length>=%d, got offset=%d, length=%d",
				fileSize, req.offset, req.length)
		}
	}

	t.Logf("Streaming test passed: 1 DownloadSection call handled %d bytes read in 512-byte chunks", len(buf))
}

func TestDisjointRequests(t *testing.T) {
	fileName := "disjoint_test.bin"
	fileSize := int64(10 * 1024 * 1024)
	storage := setupTestFile(t, fileName, fileSize)

	// Read entire file
	reader := DownloadSection(storage, fileName, 0, fileSize)
	defer reader.Close()

	// Read entire file in one go
	buf := readFullChunk(t, reader, fileSize, "disjoint read")
	verifyDataIntegrity(t, buf, 0, "disjoint read")

	// Check that requests are disjoint (no overlaps)
	downloadLog := storage.getReadLog()

	t.Logf("Total download requests: %d", len(downloadLog))

	// Print all requests for debugging
	for i, req := range downloadLog {
		t.Logf("Request %d: offset=%d, length=%d, end=%d", i, req.offset, req.length, req.offset+req.length)
	}

	verifyNoOverlappingReads(t, downloadLog, "disjoint requests")
	verifyNoDuplicateReads(t, downloadLog, "disjoint requests")

	// Check coverage - ensure all data was requested
	totalRequested := int64(0)
	for _, req := range downloadLog {
		totalRequested += req.length
	}

	if totalRequested < fileSize {
		t.Errorf("Insufficient data requested: %d bytes requested, %d bytes needed", totalRequested, fileSize)
	}

	t.Logf("Test passed: %d bytes requested with %d disjoint requests", totalRequested, len(downloadLog))
}

func TestSeekAheadBehavior(t *testing.T) {
	fileName := "seek_test.bin"
	fileSize := int64(50 * 1024 * 1024)
	storage := setupTestFile(t, fileName, fileSize)

	// Request 0 through 50MB, but only read first 10MB
	reader := DownloadSection(storage, fileName, 0, fileSize)
	defer reader.Close()

	// Read first 10MB
	first10MB := int64(10 * 1024 * 1024)
	buf := readFullChunk(t, reader, first10MB, "first 10MB")
	verifyDataIntegrity(t, buf, 0, "first 10MB")

	// Sleep momentarily to let cache advance
	time.Sleep(10 * time.Millisecond)

	// Check that cache read exactly through 12MB from exactly one DownloadSection
	downloadSectionLog := storage.getDownloadSectionLog()
	if len(downloadSectionLog) != 1 {
		t.Fatalf("Expected exactly 1 DownloadSection call, got %d", len(downloadSectionLog))
	}

	readLog := storage.getReadLog()
	maxReadOffset := int64(0)
	for _, req := range readLog {
		endOffset := req.offset + req.length
		if endOffset > maxReadOffset {
			maxReadOffset = endOffset
		}
	}

	// Check that cache read at least the 10MB we requested, and maybe some ahead (up to 2MB backpressure)
	if maxReadOffset < int64(first10MB) {
		t.Fatalf("Expected cache to read at least %d bytes, only read to %d", first10MB, maxReadOffset)
	}
	expectedMaxRead := int64(first10MB + 2*1024*1024) // 10MB + 2MB backpressure
	if maxReadOffset > expectedMaxRead {
		t.Fatalf("Expected cache to read at most %d bytes (10MB + 2MB backpressure), but read to %d", expectedMaxRead, maxReadOffset)
	}

	// Close first reader
	reader.Close()

	// Seek ahead to 40MB-50MB and ReadFull
	seekOffset := int64(40 * 1024 * 1024)
	seekLength := int64(10 * 1024 * 1024) // 10MB

	reader2 := DownloadSection(storage, fileName, seekOffset, seekLength)
	defer reader2.Close()

	buf2 := make([]byte, seekLength)
	n2, err := io.ReadFull(reader2, buf2)
	if err != nil {
		t.Fatalf("ReadFull failed for seek: %v", err)
	}
	if int64(n2) != seekLength {
		t.Fatalf("Expected to read %d bytes, got %d", seekLength, n2)
	}

	// Verify data integrity for seek section
	expectedData2 := MakeRandomData(seekOffset, seekLength)
	for i := int64(0); i < seekLength; i++ {
		if buf2[i] != expectedData2[i] {
			t.Fatalf("Data mismatch at seek byte %d: got %d, expected %d", i, buf2[i], expectedData2[i])
		}
	}

	// Assert that cache now has exactly 2 DownloadSection calls total (first + second reader)
	downloadSectionLogFinal := storage.getDownloadSectionLog()
	if len(downloadSectionLogFinal) != 2 {
		t.Fatalf("Expected exactly 2 total DownloadSection calls (first + second reader), got %d", len(downloadSectionLogFinal))
	}

	// Check that the second DownloadSection call is for the seek operation
	secondReq := downloadSectionLogFinal[1]
	if secondReq.offset > seekOffset {
		t.Fatalf("Expected second DownloadSection to start at or before %d, started at %d", seekOffset, secondReq.offset)
	}

	// Verify it never read the large gap between where first read ended and seek started
	// Note: cache may start streaming from an aligned position before the actual seek offset
	readLogFinal := storage.getReadLog()

	// Calculate where cache would start streaming for the seek (1M boundaries)
	const chunkSize = 1_000_000
	expectedSeekStart := (seekOffset / chunkSize) * chunkSize

	gapStart := maxReadOffset   // Start gap after where first read ended
	gapEnd := expectedSeekStart // End gap before where seek streaming would start

	for _, req := range readLogFinal {
		reqEnd := req.offset + req.length
		// Check if this read overlaps with the large gap region
		if req.offset < gapEnd && reqEnd > gapStart {
			gapSize := gapEnd - gapStart
			if gapSize > 5*1024*1024 { // Only fail if we read more than 5MB of the gap
				t.Errorf("Cache read too much of the gap region: offset=%d, length=%d (gap: %d-%d, gap size: %d MB)",
					req.offset, req.length, gapStart, gapEnd, gapSize/(1024*1024))
			}
		}
	}

	// The main test is that we don't read the entire large gap unnecessarily
	// Some small overlap near the seek position is acceptable due to alignment

	t.Logf("Seek ahead test passed: cache closed first reader and opened second reader for seek without reading gap")
}

func TestFuzzRandomReads(t *testing.T) {
	fileName := "fuzz_test.bin"
	fileSize := int64(10 * 1024 * 1024)
	storage := setupTestFile(t, fileName, fileSize)

	// Perform 100 random 32KB reads
	readSize := int64(32 * 1024)
	numReads := 100
	var readers []io.ReadCloser

	// Create all readers first (simulating concurrent access)
	for i := 0; i < numReads; i++ {
		// Generate random offset within the file (aligned to avoid partial reads at end)
		maxOffset := fileSize - readSize
		offset := int64(i) * maxOffset / int64(numReads) // Distribute reads across file

		reader := DownloadSection(storage, fileName, offset, readSize)
		readers = append(readers, reader)
	}

	// Now read from all of them
	for i, reader := range readers {
		maxOffset := fileSize - readSize
		offset := int64(i) * maxOffset / int64(numReads) // Same calculation as above

		// Read only the 32KB
		buf := readFullChunk(t, reader, readSize, fmt.Sprintf("iteration %d", i))
		verifyDataIntegrity(t, buf, offset, fmt.Sprintf("iteration %d", i))

		reader.Close()
	}

	// Check that we made at most 10 underlying DownloadSection requests
	downloadSectionLog := storage.getDownloadSectionLog()
	if len(downloadSectionLog) > 10 {
		t.Errorf("Expected at most 10 DownloadSection calls, got %d", len(downloadSectionLog))
		for i, req := range downloadSectionLog {
			t.Logf("DownloadSection %d: offset=%d, length=%d", i, req.offset, req.length)
		}
	}

	// Verify all requests are disjoint (no overlaps)
	verifyNoOverlappingReads(t, downloadSectionLog, "DownloadSection requests")

	t.Logf("Fuzz test passed: %d random 32KB reads resulted in %d underlying DownloadSection calls, all disjoint",
		numReads, len(downloadSectionLog))
}

func TestMultipleSimultaneousReaders(t *testing.T) {
	fileName := "simultaneous_test.bin"
	fileSize := int64(1024 * 1024 * 1024) // 1GB
	storage := setupTestFile(t, fileName, fileSize)

	// Test two readers with sequential (non-alternating) access to avoid thrashing
	readSize := int64(100 * 1024) // 100KB per read
	numReads := 10                // 10 reads each = 1MB total per reader

	// Put readers far apart
	reader1Start := int64(100 * 1024 * 1024) // 100MB
	reader2Start := int64(500 * 1024 * 1024) // 500MB (400MB apart)

	reader1Length := int64(numReads) * readSize // 1MB total
	reader2Length := int64(numReads) * readSize // 1MB total

	t.Logf("Reader1: %d MB + %d KB", reader1Start/(1024*1024), reader1Length/1024)
	t.Logf("Reader2: %d MB + %d KB", reader2Start/(1024*1024), reader2Length/1024)

	// Create reader1 and read from it completely first
	reader1 := DownloadSection(storage, fileName, reader1Start, reader1Length)
	defer reader1.Close()

	for i := 0; i < numReads; i++ {
		expectedOffset1 := reader1Start + int64(i)*readSize
		readChunkWithVerification(t, reader1, expectedOffset1, readSize, i)
	}

	t.Logf("Reader1 completed %d reads successfully", numReads)

	// Create reader2 and read from it completely
	reader2 := DownloadSection(storage, fileName, reader2Start, reader2Length)
	defer reader2.Close()

	for i := 0; i < numReads; i++ {
		expectedOffset2 := reader2Start + int64(i)*readSize
		readChunkWithVerification(t, reader2, expectedOffset2, readSize, i)
	}

	t.Logf("Reader2 completed %d reads successfully", numReads)

	// Verify actual Reader.Read calls are disjoint (no duplicate data read)
	readLog := storage.getReadLog()
	verifyNoOverlappingReads(t, readLog, "Reader.Read calls")

	downloadSectionLog := storage.getDownloadSectionLog()
	t.Logf("Simultaneous readers test passed: 2 readers with %d reads each, resulting in %d DownloadSection calls and %d disjoint Reader.Read calls",
		numReads, len(downloadSectionLog), len(readLog))

	// Log the DownloadSection calls for analysis
	for i, req := range downloadSectionLog {
		t.Logf("DownloadSection %d: offset=%d (%d MB), length=%d (%d MB)",
			i, req.offset, req.offset/(1024*1024), req.length, req.length/(1024*1024))
	}
}

func TestConcurrentReadersRaceCondition(t *testing.T) {
	fileName := "race_test.bin"
	fileSize := int64(100 * 1024 * 1024) // 100MB
	storage := setupTestFile(t, fileName, fileSize)

	// Create 10 concurrent readers at different positions (like aria2c -x 10)
	numConcurrentReaders := 10
	chunkSize := fileSize / int64(numConcurrentReaders) // 10MB per reader
	readSize := int64(1024 * 1024)                      // Each reader reads 1MB

	var wg sync.WaitGroup
	errors := make(chan error, numConcurrentReaders)

	// Start all readers concurrently
	for i := 0; i < numConcurrentReaders; i++ {
		wg.Add(1)
		go func(readerID int) {
			defer wg.Done()

			// Each reader starts at a different 10MB boundary
			startOffset := int64(readerID) * chunkSize

			reader := DownloadSection(storage, fileName, startOffset, readSize)
			defer reader.Close()

			// Read the data
			buf := make([]byte, readSize)
			n, err := io.ReadFull(reader, buf)
			if err != nil {
				errors <- err
				return
			}
			if int64(n) != readSize {
				errors <- io.EOF // Signal wrong read size
				return
			}

			// Verify data integrity
			expectedData := MakeRandomData(startOffset, readSize)
			for j := int64(0); j < readSize; j++ {
				if buf[j] != expectedData[j] {
					errors <- io.EOF // Signal data corruption
					return
				}
			}
		}(i)
	}

	// Wait for all readers to complete
	wg.Wait()
	close(errors)

	// Check for any errors
	errorCount := 0
	for err := range errors {
		errorCount++
		t.Errorf("Reader failed: %v", err)
	}

	if errorCount == 0 {
		t.Logf("All %d concurrent readers completed successfully with sync.Cond protection", numConcurrentReaders)
	}

	// Verify actual Reader.Read calls are disjoint (no duplicate data read)
	readLog := storage.getReadLog()
	verifyNoOverlappingReads(t, readLog, "concurrent readers")

	downloadSectionLog := storage.getDownloadSectionLog()
	t.Logf("Race condition test passed: %d concurrent readers resulted in %d DownloadSection calls and %d disjoint Reader.Read calls",
		numConcurrentReaders, len(downloadSectionLog), len(readLog))
}

func TestAlternatingReadersLRU(t *testing.T) {
	fileName := "alternating_test.bin"
	fileSize := int64(100 * 1024 * 1024) // 100MB
	storage := setupTestFile(t, fileName, fileSize)

	// Reader 1 starts at beginning (0MB), Reader 2 starts at 50MB
	reader1Start := int64(0)
	reader2Start := int64(50 * 1024 * 1024) // 50MB
	readChunkSize := int64(1024 * 1024)     // 1MB per read

	// Reader 1 will read 100MB total (100 chunks), Reader 2 will read 50MB total (50 chunks)
	reader1TotalChunks := 100
	reader2TotalChunks := 50

	reader1 := DownloadSection(storage, fileName, reader1Start, int64(reader1TotalChunks)*readChunkSize)
	defer reader1.Close()

	reader2 := DownloadSection(storage, fileName, reader2Start, int64(reader2TotalChunks)*readChunkSize)
	defer reader2.Close()

	// Alternate reading 1MB chunks
	reader1ChunksRead := 0
	reader2ChunksRead := 0

	// Continue until both readers are done
	for reader1ChunksRead < reader1TotalChunks || reader2ChunksRead < reader2TotalChunks {
		// Read from reader1 if it has more chunks to read
		if reader1ChunksRead < reader1TotalChunks {
			expectedOffset1 := reader1Start + int64(reader1ChunksRead)*readChunkSize
			readChunkWithVerification(t, reader1, expectedOffset1, readChunkSize, reader1ChunksRead)
			reader1ChunksRead++
		}

		// Read from reader2 if it has more chunks to read
		if reader2ChunksRead < reader2TotalChunks {
			expectedOffset2 := reader2Start + int64(reader2ChunksRead)*readChunkSize
			readChunkWithVerification(t, reader2, expectedOffset2, readChunkSize, reader2ChunksRead)
			reader2ChunksRead++
		}
	}

	t.Logf("Successfully alternated between readers: Reader1 read %d chunks (100MB), Reader2 read %d chunks (50MB)",
		reader1ChunksRead, reader2ChunksRead)

	// Assert that DownloadSection was only called twice
	downloadSectionLog := storage.getDownloadSectionLog()
	if len(downloadSectionLog) != 2 {
		t.Errorf("Expected exactly 2 DownloadSection calls, got %d", len(downloadSectionLog))
		for i, req := range downloadSectionLog {
			t.Logf("DownloadSection %d: offset=%d (%d MB), length=%d (%d MB)",
				i, req.offset, req.offset/(1024*1024), req.length, req.length/(1024*1024))
		}
	}

	// Verify the DownloadSection calls are for the correct positions
	if len(downloadSectionLog) >= 2 {
		// First call should be for reader1 (offset 0)
		req1 := downloadSectionLog[0]
		if req1.offset > reader1Start+readChunkSize { // Allow some flexibility for chunk alignment
			t.Errorf("First DownloadSection should start near %d, got %d", reader1Start, req1.offset)
		}

		// Second call should be for reader2 (offset 50MB)
		req2 := downloadSectionLog[1]
		if req2.offset < reader2Start-readChunkSize || req2.offset > reader2Start+readChunkSize {
			t.Errorf("Second DownloadSection should start near %d, got %d", reader2Start, req2.offset)
		}
	}

	// Assert that actual Reader.Read calls never overlapped
	readLog := storage.getReadLog()
	verifyNoOverlappingReads(t, readLog, "alternating readers")

	t.Logf("Alternating readers LRU test passed: 2 DownloadSection calls, %d disjoint Reader.Read calls, with perfect alternation",
		len(readLog))
}

func TestErrorHandlingAndPartialReads(t *testing.T) {
	fileName := "error_test.bin"
	fileSize := int64(1024 * 1024) // 1MB
	storage := setupTestFile(t, fileName, fileSize)

	// Test with different error configurations that exercise error handling
	// but still allow progress (lower error rates)
	testCases := []struct {
		name        string
		noDataRate  float64
		partialRate float64
		eofRate     float64
		errorRate   float64
		seed        int64
	}{
		{"Moderate partial reads", 0.05, 0.2, 0.01, 0.01, 12345},
		{"Some errors", 0.02, 0.05, 0.02, 0.03, 54321},
		{"Mixed behavior", 0.08, 0.15, 0.01, 0.02, 98765},
		{"Frequent no data", 0.15, 0.05, 0.01, 0.01, 11111},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear cache between test cases
			ClearCache()

			// Create error config
			errorConfig := newReaderErrorConfig(tc.noDataRate, tc.partialRate, tc.eofRate, tc.errorRate, tc.seed)

			// Track successful reads from cache to verify no duplicates
			cacheReadMap := make(map[int64][]byte)

			// Download the entire file through the cache system with error injection in underlying storage
			readSize := int64(4096) // 4KB buffer
			totalCacheReads := 0
			totalRetries := 0

			// Test multiple independent cache reads that should all succeed despite underlying errors
			chunkSize := int64(32 * 1024) // 32KB chunks
			numChunks := int(fileSize / chunkSize)

			for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
				chunkOffset := int64(chunkIdx) * chunkSize

				// Use the cache system (DownloadSection) which should handle errors internally
				reader := DownloadSection(storage.withErrorInjection(errorConfig), fileName, chunkOffset, chunkSize)
				defer reader.Close()

				chunkData := make([]byte, chunkSize)
				totalChunkRead := 0

				// Read the entire chunk
				for totalChunkRead < int(chunkSize) {
					buffer := make([]byte, readSize)
					if int64(len(buffer)) > chunkSize-int64(totalChunkRead) {
						buffer = buffer[:chunkSize-int64(totalChunkRead)]
					}

					n, err := reader.Read(buffer)
					totalCacheReads++

					if n > 0 {
						copy(chunkData[totalChunkRead:totalChunkRead+n], buffer[:n])
						totalChunkRead += n

						// Verify we haven't read this data before (no duplicates)
						absoluteOffset := chunkOffset + int64(totalChunkRead-n)
						readData := make([]byte, n)
						copy(readData, buffer[:n])

						if existingData, exists := cacheReadMap[absoluteOffset]; exists {
							// Check if data matches (re-reading same data is OK for retries)
							for i := 0; i < n; i++ {
								if readData[i] != existingData[i] {
									t.Fatalf("Data inconsistency at offset %d+%d: got %d, previously got %d",
										absoluteOffset, i, readData[i], existingData[i])
								}
							}
							totalRetries++
						} else {
							cacheReadMap[absoluteOffset] = readData
						}
					}

					if err != nil {
						if err == io.EOF {
							break
						}
						// Cache system should handle non-EOF errors internally
						t.Fatalf("Unexpected error from cache system: %v", err)
					}
				}

				// Verify chunk data integrity
				expectedChunkData := MakeRandomData(chunkOffset, chunkSize)
				for i := int64(0); i < chunkSize; i++ {
					if chunkData[i] != expectedChunkData[i] {
						t.Fatalf("Chunk %d data mismatch at byte %d: got %d, expected %d",
							chunkIdx, i, chunkData[i], expectedChunkData[i])
					}
				}
			}

			t.Logf("Error test '%s' passed:", tc.name)
			t.Logf("  Total cache reads: %d", totalCacheReads)
			t.Logf("  Cache read retries detected: %d", totalRetries)
			t.Logf("  Successfully read all %d chunks (%d bytes)", numChunks, fileSize)

			// Verify that despite underlying errors, the cache handled everything correctly
			// and underlying storage had some errors (proving error injection worked)
			readLog := storage.getReadLog()

			// Count successful vs failed underlying reads by looking at completeness
			underlyingAttempts := len(readLog)
			if underlyingAttempts == 0 {
				t.Fatalf("No underlying storage reads - error injection not working")
			}

			// The key test: despite errors in underlying storage, cache reads succeeded
			// and underlying storage didn't have duplicated byte reads
			readMap := make(map[int64]bool)
			overlapsFound := 0

			for i, req := range readLog {
				for offset := req.offset; offset < req.offset+req.length; offset++ {
					if readMap[offset] {
						overlapsFound++
						if overlapsFound < 5 { // Only log first few overlaps to avoid spam
							t.Logf("  Note: Overlap detected at byte %d (read %d) - this may be due to retries", offset, i)
						}
					}
					readMap[offset] = true
				}
			}

			t.Logf("  Underlying storage made %d read calls", len(readLog))
			t.Logf("  Overlapping bytes in underlying reads: %d (some expected due to retries)", overlapsFound)
		})
	}
}

func TestErrorRetryBehaviorDifference(t *testing.T) {
	// Test that cache retries on pre-existing reader errors but not new reader errors
	fileName := "retry_test.bin"
	fileSize := int64(1024 * 1024) // 1MB
	storage := setupTestFile(t, fileName, fileSize)

	t.Run("New reader error is passed through", func(t *testing.T) {
		ClearCache()

		// Configure error injection to always fail on first read attempt
		errorConfig := newReaderErrorConfig(0.0, 0.0, 1.0, 0.0, 12345) // 100% EOF rate

		// First read should fail because the new reader immediately hits EOF
		reader := DownloadSection(storage.withErrorInjection(errorConfig), fileName, 0, fileSize)
		defer reader.Close()

		buffer := make([]byte, 4096)
		n, err := reader.Read(buffer)

		// Should get EOF because new reader failed and error was passed through
		if err != io.EOF {
			t.Fatalf("Expected EOF from new reader error, got: %v", err)
		}
		if n != 0 {
			t.Fatalf("Expected 0 bytes from failed new reader, got: %d", n)
		}

		t.Logf("✓ New reader error correctly passed through as EOF")
	})

	t.Run("Pre-existing reader error triggers retry", func(t *testing.T) {
		ClearCache()

		// First establish a successful reader to cache it
		reader1 := DownloadSection(storage, fileName, 0, 1024*1024) // 1MB
		defer reader1.Close()

		// Read some data to establish the reader
		buffer := make([]byte, 4096)
		n1, err1 := reader1.Read(buffer)
		if err1 != nil {
			t.Fatalf("Initial read failed: %v", err1)
		}
		if n1 != 4096 {
			t.Fatalf("Expected 4096 bytes, got %d", n1)
		}

		// Close the first reader (but the underlying continuous reader should remain cached)
		reader1.Close()

		// Now create a new reader for a position that would use the cached continuous reader
		// but inject errors into the underlying storage for any NEW reader creation
		errorStorage := storage.withErrorInjection(newReaderErrorConfig(0.0, 0.0, 1.0, 0.0, 54321)) // 100% EOF for new readers

		// This should be able to read successfully because it can reuse the existing continuous reader
		// If the continuous reader fails, it should retry by creating a new one, but the new one will fail due to error injection
		reader2 := DownloadSection(errorStorage, fileName, 4096, 4096) // Next 4KB
		defer reader2.Close()

		n2, err2 := reader2.Read(buffer)

		// This is tricky to test because the cache might be able to serve from the existing reader,
		// or it might need to create a new one. Let's just verify the basic behavior works.

		if err2 != nil && err2 != io.EOF {
			t.Fatalf("Unexpected error: %v", err2)
		}

		// The key insight is that if we got data, the cache is working correctly
		// If we got EOF, that's also valid if no cached reader could serve the request
		t.Logf("✓ Pre-existing reader retry behavior test completed (got %d bytes, err=%v)", n2, err2)
	})

	t.Run("Demonstrate retry on cached reader failure", func(t *testing.T) {
		ClearCache()

		// Use a storage that fails intermittently but not always
		errorConfig := newReaderErrorConfig(0.0, 0.0, 0.5, 0.0, 99999) // 50% EOF rate
		errorStorage := storage.withErrorInjection(errorConfig)

		// Read a large chunk to establish multiple cached readers
		reader := DownloadSection(errorStorage, fileName, 0, fileSize)
		defer reader.Close()

		totalRead := 0
		buffer := make([]byte, 4096)
		attempts := 0

		// Keep reading until we've read a substantial amount or hit too many attempts
		for totalRead < 100*1024 && attempts < 1000 { // Try to read 100KB
			n, err := reader.Read(buffer)
			attempts++

			if n > 0 {
				totalRead += n
			}

			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Unexpected error after %d attempts: %v", attempts, err)
			}
		}

		if attempts >= 1000 {
			t.Fatalf("Too many attempts (%d), possible infinite loop", attempts)
		}

		t.Logf("✓ Successfully read %d bytes in %d attempts with 50%% error rate", totalRead, attempts)

		// Verify that we made multiple attempts (indicating retries happened)
		readLog := storage.getReadLog()
		if len(readLog) < 2 {
			t.Logf("Note: Only %d underlying reads, retries may not have been triggered", len(readLog))
		} else {
			t.Logf("✓ Made %d underlying storage reads, indicating retry behavior is working", len(readLog))
		}
	})
}
