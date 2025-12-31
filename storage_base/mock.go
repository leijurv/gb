package storage_base

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"sync"
	"time"
)

type mockBlobData struct {
	data     []byte
	checksum string
}

type MockStorage struct {
	ID       []byte
	blobs    map[string]mockBlobData
	blobLock sync.RWMutex
}

func NewMockStorage(id []byte) *MockStorage {
	return &MockStorage{
		ID:    id,
		blobs: make(map[string]mockBlobData),
	}
}

func (m *MockStorage) BeginBlobUpload(blobID []byte) StorageUpload {
	path := blobIDToPath(blobID)
	return &mockUpload{
		storage: m,
		blobID:  blobID,
		path:    path,
		buf:     &bytes.Buffer{},
	}
}

func (m *MockStorage) BeginDatabaseUpload(filename string) StorageUpload {
	return &mockUpload{
		storage: m,
		blobID:  nil,
		path:    filename,
		buf:     &bytes.Buffer{},
	}
}

func (m *MockStorage) DownloadSection(path string, offset int64, length int64) io.ReadCloser {
	m.blobLock.RLock()
	defer m.blobLock.RUnlock()
	blob, ok := m.blobs[path]
	if !ok {
		panic("blob not found: " + path)
	}
	if offset+length > int64(len(blob.data)) {
		panic("out of range")
	}
	return io.NopCloser(bytes.NewReader(blob.data[offset : offset+length]))
}

func (m *MockStorage) ListBlobs() []UploadedBlob {
	m.blobLock.RLock()
	defer m.blobLock.RUnlock()
	result := make([]UploadedBlob, 0, len(m.blobs))
	for path, blob := range m.blobs {
		result = append(result, UploadedBlob{
			StorageID: m.ID,
			Path:      path,
			Checksum:  blob.checksum,
			Size:      int64(len(blob.data)),
		})
	}
	return result
}

func (m *MockStorage) Metadata(path string) (string, int64) {
	m.blobLock.RLock()
	defer m.blobLock.RUnlock()
	blob, ok := m.blobs[path]
	if !ok {
		return "", 0
	}
	return blob.checksum, int64(len(blob.data))
}

func (m *MockStorage) DeleteBlob(path string) {
	m.blobLock.Lock()
	defer m.blobLock.Unlock()
	delete(m.blobs, path)
}

func (m *MockStorage) GetID() []byte {
	return m.ID
}

func (m *MockStorage) String() string {
	return "MockStorage"
}

func (m *MockStorage) PresignedURL(path string, expiry time.Duration) (string, error) {
	return "", errors.New("presigned URLs are not supported for MockStorage")
}

func (m *MockStorage) storeBlob(path string, data []byte, checksum string) {
	m.blobLock.Lock()
	defer m.blobLock.Unlock()
	m.blobs[path] = mockBlobData{data: data, checksum: checksum}
}

func blobIDToPath(blobID []byte) string {
	h := hex.EncodeToString(blobID)
	return h[:2] + "/" + h[2:4] + "/" + h
}

// CorruptByte flips all bits at the given offset in the specified blob. Used for testing integrity verification.
func (m *MockStorage) CorruptByte(blobID []byte, offset int) {
	m.blobLock.Lock()
	defer m.blobLock.Unlock()

	path := blobIDToPath(blobID)
	blob, ok := m.blobs[path]
	if !ok {
		panic("blob not found: " + path)
	}
	if offset < 0 || offset >= len(blob.data) {
		panic("offset out of range")
	}

	corrupted := make([]byte, len(blob.data))
	copy(corrupted, blob.data)
	corrupted[offset] ^= 0xFF

	m.blobs[path] = mockBlobData{data: corrupted, checksum: blob.checksum}
}

type mockUpload struct {
	storage *MockStorage
	blobID  []byte
	path    string
	buf     *bytes.Buffer
}

func (u *mockUpload) Writer() io.Writer {
	return u.buf
}

func (u *mockUpload) End() UploadedBlob {
	data := u.buf.Bytes()
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	hash := sha256.Sum256(data)
	checksum := hex.EncodeToString(hash[:])
	u.storage.storeBlob(u.path, dataCopy, checksum)
	return UploadedBlob{
		StorageID: u.storage.ID,
		BlobID:    u.blobID,
		Path:      u.path,
		Checksum:  checksum,
		Size:      int64(len(data)),
	}
}
