package backup

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/leijurv/gb/storage_base"
)

// mockWalker allows tests to control exactly when files "appear" to the backup system.
// Walk() blocks until End() is called, giving tests full control over timing.
type mockWalker struct {
	callback func(path string, info os.FileInfo)
	done     chan struct{}
	started  chan struct{} // signals that Walk has been called and callback is set
}

func newMockWalker() *mockWalker {
	return &mockWalker{
		done:    make(chan struct{}),
		started: make(chan struct{}),
	}
}

func (m *mockWalker) Walk(roots []string, callback func(path string, info os.FileInfo)) error {
	m.callback = callback
	close(m.started) // signal that we're ready to receive files
	<-m.done         // block until test calls End()
	return nil
}

// SendFile sends a file to the backup system. Blocks until callback returns.
func (m *mockWalker) SendFile(path string, info os.FileInfo) {
	m.callback(path, info)
}

// End signals that the walk is complete. Walk() will return after this.
func (m *mockWalker) End() {
	m.done <- struct{}{}
}

// WaitForStart blocks until Walk has been called and is ready to receive files.
func (m *mockWalker) WaitForStart() {
	<-m.started
}

// mockFileOpener provides rendezvous-style synchronization for file operations.
// Each Stat/Open call blocks until the test provides a response via shouldStat/shouldOpen.
type mockFileOpener struct {
	t         *testing.T
	statCalls chan statCall
	openCalls chan openCall
}

type statCall struct {
	path     string
	response chan statResponse
}

type statResponse struct {
	info os.FileInfo
	err  error
}

type openCall struct {
	path     string
	response chan openResponse
}

type openResponse struct {
	reader io.ReadCloser
	err    error
}

func newMockFileOpener(t *testing.T) *mockFileOpener {
	return &mockFileOpener{
		t:         t,
		statCalls: make(chan statCall),
		openCalls: make(chan openCall),
	}
}

func (m *mockFileOpener) Stat(path string) (os.FileInfo, error) {
	resp := make(chan statResponse)
	m.statCalls <- statCall{path: path, response: resp}
	r := <-resp
	return r.info, r.err
}

func (m *mockFileOpener) Open(path string) (io.ReadCloser, error) {
	resp := make(chan openResponse)
	m.openCalls <- openCall{path: path, response: resp}
	r := <-resp
	return r.reader, r.err
}

// shouldStat waits for a Stat call and provides the response.
// Panics if the path doesn't match expected.
func (m *mockFileOpener) shouldStat(path string, info os.FileInfo, err error) {
	select {
	case call := <-m.statCalls:
		if call.path != path {
			m.t.Fatalf("expected stat(%q), got stat(%q)", path, call.path)
		}
		call.response <- statResponse{info: info, err: err}
	case <-time.After(5 * time.Second):
		m.t.Fatalf("timeout waiting for stat(%q)", path)
	}
}

// shouldOpen waits for an Open call and provides the response.
// Panics if the path doesn't match expected.
func (m *mockFileOpener) shouldOpen(path string, content []byte, err error) {
	select {
	case call := <-m.openCalls:
		if call.path != path {
			m.t.Fatalf("expected open(%q), got open(%q)", path, call.path)
		}
		if err != nil {
			call.response <- openResponse{err: err}
		} else {
			call.response <- openResponse{reader: io.NopCloser(bytes.NewReader(content))}
		}
	case <-time.After(5 * time.Second):
		m.t.Fatalf("timeout waiting for open(%q)", path)
	}
}

// shouldStatAny waits for any Stat call and provides the response.
// Returns the path that was stat'd.
func (m *mockFileOpener) shouldStatAny(info os.FileInfo, err error) string {
	select {
	case call := <-m.statCalls:
		call.response <- statResponse{info: info, err: err}
		return call.path
	case <-time.After(5 * time.Second):
		m.t.Fatal("timeout waiting for stat")
		return ""
	}
}

// shouldOpenAny waits for any Open call and provides the response.
// Returns the path that was opened.
func (m *mockFileOpener) shouldOpenAny(content []byte, err error) string {
	select {
	case call := <-m.openCalls:
		if err != nil {
			call.response <- openResponse{err: err}
		} else {
			call.response <- openResponse{reader: io.NopCloser(bytes.NewReader(content))}
		}
		return call.path
	case <-time.After(5 * time.Second):
		m.t.Fatal("timeout waiting for open")
		return ""
	}
}

// fakeFileInfo implements os.FileInfo for testing.
type fakeFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (f fakeFileInfo) Name() string       { return f.name }
func (f fakeFileInfo) Size() int64        { return f.size }
func (f fakeFileInfo) Mode() os.FileMode  { return f.mode }
func (f fakeFileInfo) ModTime() time.Time { return f.modTime }
func (f fakeFileInfo) IsDir() bool        { return f.isDir }
func (f fakeFileInfo) Sys() interface{}   { return nil }

func newFakeFileInfo(name string, size int64) fakeFileInfo {
	return fakeFileInfo{
		name:    name,
		size:    size,
		mode:    0644,
		modTime: time.Now(),
		isDir:   false,
	}
}

// mockUploadService captures what was uploaded for test verification.
type mockUploadService struct {
	mu       sync.Mutex
	uploads  []uploadedFile
	current  *bytes.Buffer
	blobID   []byte
	canceled bool
}

type uploadedFile struct {
	path    string
	hash    []byte
	size    int64
	content []byte
}

func newMockUploadService() *mockUploadService {
	return &mockUploadService{
		uploads: make([]uploadedFile, 0),
	}
}

func (m *mockUploadService) Begin(blobID []byte) io.Writer {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobID = blobID
	m.current = &bytes.Buffer{}
	m.canceled = false
	return m.current
}

func (m *mockUploadService) End(sha256 []byte, size int64) []storage_base.UploadedBlob {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return a fake uploaded blob
	return []storage_base.UploadedBlob{{
		StorageID: []byte("mock-storage"),
		BlobID:    m.blobID,
		Path:      fmt.Sprintf("mock/%x", m.blobID[:4]),
		Checksum:  fmt.Sprintf("%x", sha256[:8]),
		Size:      size,
	}}
}

func (m *mockUploadService) Cancel() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.canceled = true
}
