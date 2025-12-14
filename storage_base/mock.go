package storage_base

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"sync"
)

type MockStorage struct {
	ID       []byte
	blobs    map[string][]byte
	blobLock sync.RWMutex
}

func NewMockStorage(id []byte) *MockStorage {
	return &MockStorage{
		ID:    id,
		blobs: make(map[string][]byte),
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
	data, ok := m.blobs[path]
	if !ok {
		panic("blob not found: " + path)
	}
	if offset+length > int64(len(data)) {
		panic("out of range")
	}
	return io.NopCloser(bytes.NewReader(data[offset : offset+length]))
}

func (m *MockStorage) ListBlobs() []UploadedBlob {
	m.blobLock.RLock()
	defer m.blobLock.RUnlock()
	result := make([]UploadedBlob, 0, len(m.blobs))
	for path, data := range m.blobs {
		result = append(result, UploadedBlob{
			StorageID: m.ID,
			Path:      path,
			Size:      int64(len(data)),
		})
	}
	return result
}

func (m *MockStorage) Metadata(path string) (string, int64) {
	m.blobLock.RLock()
	defer m.blobLock.RUnlock()
	data, ok := m.blobs[path]
	if !ok {
		return "", 0
	}
	return "", int64(len(data))
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

func (m *MockStorage) storeBlob(path string, data []byte) {
	m.blobLock.Lock()
	defer m.blobLock.Unlock()
	m.blobs[path] = data
}

func blobIDToPath(blobID []byte) string {
	h := hex.EncodeToString(blobID)
	return h[:2] + "/" + h[2:4] + "/" + h
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
	u.storage.storeBlob(u.path, dataCopy)
	hash := sha256.Sum256(data)
	return UploadedBlob{
		StorageID: u.storage.ID,
		BlobID:    u.blobID,
		Path:      u.path,
		Checksum:  hex.EncodeToString(hash[:]),
		Size:      int64(len(data)),
	}
}
