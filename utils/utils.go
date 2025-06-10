package utils

import (
	"crypto/md5"
	"crypto/sha256"
	"database/sql"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"unicode/utf8"

	"golang.org/x/sys/unix"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
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

// walk a directory recursively, but only call the provided function for normal files that don't error on os.Stat
func WalkFiles(startPath string, fn func(path string, info os.FileInfo)) {
	type PathAndInfo struct {
		path string
		info os.FileInfo
	}
	filesCh := make(chan PathAndInfo, 32)
	done := make(chan struct{})
	go func() {
		for file := range filesCh {
			fn(file.path, file.info)
		}
		log.Println("Scan processor signaling done")
		done <- struct{}{}
	}()
	err := filepath.Walk(startPath, func(path string, info os.FileInfo, err error) error {
		if !utf8.ValidString(path) {
			panic("invalid utf8 on your filesystem at " + path)
		}
		if config.ExcludeFromBackup(startPath, path) {
			if info == nil {
				log.Println("EXCLUDING & ERROR while reading path which is ignored by your configuration:", path, err)
				return nil
			}

			log.Println("EXCLUDING this path and pretending it doesn't exist, due to your exclude config:", path)

			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if IsDatabaseFile(path) {
			log.Println("EXCLUDING this path because it is the gb database:", path)
			return nil
		}
		ignoreErrors := config.Config().IgnorePermissionErrors
		if err != nil {
			if oserr, ok := err.(*os.PathError); ok && ignoreErrors {
				if oserr.Err == syscall.EACCES {
					log.Printf("permission error for %s, skipping...", path)
					return nil
				}
			}
			log.Println("While traversing those files, I got this error:")
			log.Println(err)
			log.Println("while looking at this path:")
			log.Println(path)
			return err
		}
		if !NormalFile(info) { // **THIS IS WHAT SKIPS DIRECTORIES**
			return nil
		}
		if ignoreErrors && !HaveReadPermission(path) {
			return nil // skip this file
		}
		filesCh <- PathAndInfo{path, info}
		return nil
	})
	if err != nil {
		// permission error while traversing
		// we should *not* continue, because that would mark all further files as "deleted"
		// aka, do not continue with a partially complete traversal of the directory lmao
		panic(err)
	}
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

var commaRegex = regexp.MustCompile("(\\d+)(\\d{3})")

func FormatCommas(num int64) string {
	str := strconv.FormatInt(num, 10)
	for n := ""; n != str; {
		n = str
		str = commaRegex.ReplaceAllString(str, "$1,$2")
	}
	return str
}

func IsDatabaseFile(path string) bool {
	dbPath := config.Config().DatabaseLocation
	return path == dbPath || path == dbPath+"-wal" || path == dbPath+"-shm"
}

type GBdirent struct {
	IsDirectory bool
	Path        string
	Size        int64
	FsModified  int64
	Start       int64
	Hash        []byte
	Permissions int32
	CompAlgo    string
}

func ListDirectory(dir string) []GBdirent {
	return ListDirectoryAtTime(dir, 0) // 0 means current time
}

func ListDirectoryAtTime(dir string, timestamp int64) []GBdirent {
	ret := make([]GBdirent, 0)
	directoriesSeen := make(map[string]struct{})
	cursor := dir
	for {
		// note that we query for paths strictly greater than the cursor
		var rows *sql.Rows
		var err error
		if timestamp == 0 {
			rows, err = db.DB.Query("SELECT files.path, sizes.size, files.fs_modified, files.start, files.hash, files.permissions, COALESCE(blob_entries.compression_alg, '') FROM files INNER JOIN sizes ON files.hash = sizes.hash INNER JOIN blob_entries ON blob_entries.hash = files.hash WHERE end IS NULL AND path > ? AND path < (? || x'ff') ORDER BY path ASC LIMIT 100", cursor, dir)
		} else {
			rows, err = db.DB.Query("SELECT files.path, sizes.size, files.fs_modified, files.start, files.hash, files.permissions, COALESCE(blob_entries.compression_alg, '') FROM files INNER JOIN sizes ON files.hash = sizes.hash INNER JOIN blob_entries ON blob_entries.hash = files.hash WHERE (? >= start AND (end > ? OR end IS NULL)) AND path > ? AND path < (? || x'ff') ORDER BY path ASC LIMIT 100", timestamp, timestamp, cursor, dir)
		}
		if err != nil {
			panic(err)
		}
		defer rows.Close()
		any := false
		for rows.Next() {
			var path string
			var size int64
			var fsModified int64
			var start int64
			var hash []byte
			var permissions int32
			var compAlgo string
			err = rows.Scan(&path, &size, &fsModified, &start, &hash, &permissions, &compAlgo)
			if err != nil {
				panic(err)
			}
			name := path[len(dir):]
			if strings.Contains(name, "/") {
				// this is a directory
				subdir := dir + strings.Split(name, "/")[0] + "/"
				if _, ok := directoriesSeen[subdir]; !ok {
					ret = append(ret, GBdirent{IsDirectory: true, Path: subdir})
					directoriesSeen[subdir] = struct{}{}
				}
				cursor = subdir + string([]byte{0xff}) // advance to after this entire subdir (this is the important line that makes this function run quickly even on "/")
			} else {
				// this is a file
				ret = append(ret, GBdirent{IsDirectory: false, Path: path, Size: size, FsModified: fsModified, Start: start, Hash: hash, Permissions: permissions, CompAlgo: compAlgo})
				cursor = path
			}
			any = true
		}
		err = rows.Err()
		if err != nil {
			panic(err)
		}
		err = rows.Close()
		if err != nil {
			panic(err)
		}
		if !any {
			break
		}
	}
	return ret
}
