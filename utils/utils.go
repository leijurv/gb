package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"github.com/muja/goconfig"
	"hash"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"unicode/utf8"

	"golang.org/x/sys/unix"

	"github.com/leijurv/gb/config"
)

func SliceToArr(in []byte) [32]byte {
	if len(in) != 32 {
		panic("database gave invalid row??")
	}
	var result [32]byte
	copy(result[:], in)
	return result
}

// return true if and only if the provided FileInfo represents a completely normal file, and nothing weird like a directory, symlink, pipe, socket, block device, etc
func NormalFile(info os.FileInfo) bool {
	return info.Mode()&os.ModeType == 0
}

func HaveReadPermission(path string) bool {
	err := syscall.Access(path, unix.R_OK)
	return err != syscall.EACCES
}

// pretend an error didn't happen if it was a permission error and we chose to ignore permission errors
func isErrorButDontCare(path string, err error) bool {
	if err != nil {
		if oserr, ok := err.(*os.PathError); ok && config.Config().IgnorePermissionErrors {
			if oserr.Err == syscall.EACCES {
				log.Printf("permission error for %s, skipping...", path)
				return true
			}
		}
		log.Println("While traversing those files, I got this error:")
		log.Println(err)
		log.Println("while looking at this path:")
		log.Println(path)
		return false
	}
	return false
}

type pathAndDirent struct {
	path  string
	entry fs.DirEntry
}
type pathAndInfo struct {
	path string
	info os.FileInfo
}

func shouldSkipPath(startPath string, path string, entry fs.DirEntry) bool {
	if !entry.IsDir() && !entry.Type().IsRegular() {
		return true
	}
	if entry.IsDir() {
		// ReadDir returns directory paths without a trailing / and if an exclude prefix does end in a trailing / it will
		// not match the directory causing us to unnecessarily visit all the children.
		// Appending a / will make sure we match the exclude prefix whether or not it has a trailing /
		path += "/"
	}
	if config.ExcludeFromBackup(startPath, path) {
		log.Println("EXCLUDING this path and pretending it doesn't exist, due to your exclude config:", path)
		return true
	}
	if IsDatabaseFile(path) {
		log.Println("EXCLUDING this path because it is the gb database:", path)
		return true
	}
	if config.Config().IgnorePermissionErrors && !HaveReadPermission(path) {
		log.Println("EXCLUDING this path because we don't have read permission but don't care:", path)
		return true
	}
	return false
}

func checkUtf8(path string) {
	if !utf8.ValidString(path) {
		panic("invalid utf8 on your filesystem at " + path)
	}
}

func findFile(name string, ls []fs.DirEntry) int {
	fileIdx := sort.Search(len(ls), func(i int) bool {
		return ls[i].Name() >= name
	})
	if fileIdx < len(ls) && ls[fileIdx].Name() == name {
		return fileIdx
	}
	return -1
}

func stringInSlice(needle string, slice []string) bool {
	for _, s := range slice {
		if s == needle {
			return true
		}
	}
	return false
}

type ignoreWrapper struct {
	basePathAbs string
	*GitIgnore
}

type gitState struct {
	repoPathAbs       string
	submodulePathsAbs []string
	ignores           []ignoreWrapper
}

func isIgnored(absPath string, isDir bool, gitIgnores []ignoreWrapper) bool {
	// make sure the dir itself matches so we don't unnecessarily recurse
	if isDir {
		absPath += "/"
	}
	ignored := false
	for _, gitIgnore := range gitIgnores {
		relative := absPath[len(gitIgnore.basePathAbs)+1:]
		matches, pattern := gitIgnore.MatchesPathHow(relative)
		if matches {
			ignored = !pattern.Negate
		}
	}
	return ignored
}

func walkFiles(startPath string, path string, ignoreState *gitState, filesCh chan pathAndDirent) {
	checkUtf8(path)
	ls, err := os.ReadDir(path)
	if isErrorButDontCare(path, err) {
		return
	}
	if err != nil {
		panic(err)
	}
	if config.Config().UseGitignore {
		// binary search because ReadDir sorts
		if findFile(".gitignore", ls) != -1 {
			if ignoreState == nil {
				ignoreState = &gitState{path, []string{}, []ignoreWrapper{}}
			} else {
				// create a new object so our changes don't leak to the caller
				gitCopy := *ignoreState
				ignoreState = &gitCopy
			}
			gitignorePath := path + "/.gitignore"
			gitignore, err := CompileIgnoreFile(gitignorePath)
			if isErrorButDontCare(gitignorePath, err) {
				goto dothething
			}
			if err != nil {
				panic(err)
			}

			ignoreState.ignores = append(ignoreState.ignores, ignoreWrapper{path, gitignore})
			if findFile(".gitmodules", ls) != -1 {
				gitmodulesPath := path + "/.gitmodules"
				bytes, err := os.ReadFile(gitmodulesPath)
				if isErrorButDontCare(gitmodulesPath, err) {
					goto dothething
				}
				if err != nil {
					panic(err)
				}
				modulesConfig, _, err := goconfig.Parse(bytes)
				if err != nil {
					panic(err)
				}

				for k, v := range modulesConfig {
					if strings.HasSuffix(k, ".path") {
						ignoreState.submodulePathsAbs = append(ignoreState.submodulePathsAbs, path+"/"+v)
					}
				}
			}
		}
	}

dothething:
	for _, entry := range ls {
		fullPath := path + "/" + entry.Name()
		checkUtf8(path)
		if (ignoreState == nil || !isIgnored(fullPath, entry.IsDir(), ignoreState.ignores)) && !shouldSkipPath(startPath, fullPath, entry) {
			if entry.IsDir() {
				// if we recurse into a submodule don't use any of the parent's gitignore
				if ignoreState != nil && stringInSlice(fullPath, ignoreState.submodulePathsAbs) {
					walkFiles(startPath, fullPath, nil, filesCh)
				} else {
					walkFiles(startPath, fullPath, ignoreState, filesCh)
				}
			} else {
				filesCh <- pathAndDirent{fullPath, entry}
			}
		}
	}
}

// walk a directory recursively, but only call the provided function for normal files that don't error on os.Stat
func WalkFiles(startPath string, fn func(path string, info os.FileInfo)) {
	filesCh := make(chan pathAndDirent, 32)
	outputCh := make(chan pathAndInfo, 32)
	done := make(chan struct{})
	go func() {
		for file := range filesCh {
			info, err := file.entry.Info()
			if isErrorButDontCare(file.path, err) {
				continue
			}
			if err != nil {
				panic(err)
			}
			outputCh <- pathAndInfo{file.path, info}
		}
		close(outputCh)
	}()
	go func() {
		for file := range outputCh {
			fn(file.path, file.info)
		}
		log.Println("Scan processor signaling done")
		done <- struct{}{}
	}()

	walkFiles(startPath, filepath.Clean(startPath), nil, filesCh)
	log.Println("Walker thread done")
	close(filesCh)
	<-done
	log.Println("Scan processor done")
}

type HasherSizer struct {
	size   int64
	hasher hash.Hash
}

func (hs *HasherSizer) Write(p []byte) (int, error) {
	n := len(p)
	atomic.AddInt64(&hs.size, int64(n))
	return hs.hasher.Write(p)
}

func (hs *HasherSizer) HashAndSize() ([]byte, int64) {
	return hs.Hash(), hs.Size()
}

func (hs *HasherSizer) Hash() []byte {
	return hs.hasher.Sum(nil)
}

func (hs *HasherSizer) Size() int64 {
	return atomic.LoadInt64(&hs.size)
}

func NewSHA256HasherSizer() HasherSizer {
	return HasherSizer{0, sha256.New()}
}

func NewMD5HasherSizer() HasherSizer {
	return HasherSizer{0, md5.New()}
}

type EmptyReadCloser struct{}

func (erc *EmptyReadCloser) Close() error {
	return nil
}
func (erc *EmptyReadCloser) Read(p []byte) (int, error) {
	return 0, io.EOF
}

// do you find it annoying to have to close your readers? this function is for you
func ReadCloserToReader(in io.ReadCloser) io.Reader {
	frc, ok := in.(*fakeReadCloser)
	if ok {
		return frc.r
	}
	return &fakeReader{in, nil}
}

type fakeReader struct {
	rc    io.ReadCloser
	pipeR *io.PipeReader
}

func (fr *fakeReader) Read(data []byte) (int, error) {
	if fr.pipeR == nil {
		pipeR, pipeW := io.Pipe()
		go func() {
			defer fr.rc.Close()
			_, err := io.CopyBuffer(pipeW, fr.rc, make([]byte, 1024*1024)) // we're working with huge files, 1MB buffer is more reasonable than 32KB default
			pipeW.CloseWithError(err)                                      // nil is nil, error is error. this works properly
		}()
		fr.pipeR = pipeR
	}
	return fr.pipeR.Read(data)
}

func ReaderToReadCloser(in io.Reader) io.ReadCloser {
	fr, ok := in.(*fakeReader)
	if ok && fr.pipeR == nil {
		// this is really a ReadCloser in disguise, wrapped in a fakeReader
		// AND, it hasn't been copied into a pipe yet
		return fr.rc
	}
	rc, ok := in.(io.ReadCloser)
	if ok {
		// oh you poor thing. how did this happen??
		return rc
	}
	return &fakeReadCloser{in}
}

type fakeReadCloser struct {
	r io.Reader
}

func (frc *fakeReadCloser) Read(data []byte) (int, error) {
	return frc.r.Read(data)
}

func (frc *fakeReadCloser) Close() error {
	return nil
}

func FormatHTTPRange(offset int64, length int64) string {
	return "bytes=" + strconv.FormatInt(offset, 10) + "-" + strconv.FormatInt(offset+length-1, 10)
}

func Copy(out io.Writer, in io.Reader) {
	rc := ReaderToReadCloser(in)
	defer rc.Close() // if this really is a readcloser, we should close it
	_, err := io.CopyBuffer(out, rc, make([]byte, 1024*1024))
	if err != nil {
		panic(err)
	}
}

func FormatCommas(num int64) string {
	str := strconv.FormatInt(num, 10)
	re := regexp.MustCompile("(\\d+)(\\d{3})")
	for n := ""; n != str; {
		n = str
		str = re.ReplaceAllString(str, "$1,$2")
	}
	return str
}

func IsDatabaseFile(path string) bool {
	dbPath := config.Config().DatabaseLocation
	return path == dbPath || path == dbPath+"-wal" || path == dbPath+"-shm"
}
