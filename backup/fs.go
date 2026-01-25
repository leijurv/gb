package backup

import (
	"io"
	"os"

	"github.com/leijurv/gb/utils"
)

// Walker abstracts directory traversal. Walk must block until all callbacks
// have been invoked and returns only after traversal is complete.
// Tests can implement this to control exactly when files "appear" to the backup system.
type Walker interface {
	Walk(roots []string, callback func(path string, info os.FileInfo)) error
}

// FileOpener abstracts file read operations.
type FileOpener interface {
	Open(path string) (io.ReadCloser, error)
	Stat(path string) (os.FileInfo, error)
}

// osFileOpener is the production implementation using real os calls.
type osFileOpener struct{}

func (osFileOpener) Open(path string) (io.ReadCloser, error) {
	return os.Open(path)
}

func (osFileOpener) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

// defaultWalker wraps utils.WalkFiles for production use.
type defaultWalker struct{}

func (defaultWalker) Walk(roots []string, callback func(path string, info os.FileInfo)) error {
	for _, root := range roots {
		utils.WalkFiles(root, callback)
	}
	return nil
}

// Global instances - will be replaced by mocks in tests.
// These will be moved into a BackupSession struct in step 3.
var walker Walker = defaultWalker{}
var fileOpener FileOpener = osFileOpener{}
