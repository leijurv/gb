//go:build linux || freebsd
// +build linux freebsd

package gbfs

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"bazil.org/fuse"
	fuseFs "bazil.org/fuse/fs"
	"github.com/leijurv/gb/cache"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

type File struct {
	path         string
	hash         *[]byte // I love go
	modifiedTime uint64
	flags        int32
	size         uint64
	inode        uint64 // generated
	compAlgo     string
}

func (f File) name() string {
	idx := strings.LastIndex(f.path, "/")
	return f.path[idx+1:]
}

type Dir struct {
	path      string // full path including trailing slash
	timestamp int64  // for querying historical data
	inode     uint64 // generated
}

type GBFS struct {
	root      Dir
	timestamp int64
}

type FileHandle interface{}

type CompressedFileHandle struct {
	reader io.ReadCloser
	// for sanity checking
	currentOffset int64
}

type UncompressedFileHandle struct {
	storagePath string
	blobOffset  int64
	length      int64
	key         *[]byte
	storage     storage_base.Storage
}

func timeMillis(millis int64) time.Time {
	return time.Unix(0, millis*int64(time.Millisecond))
}

func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = pathToInode(d.path)
	attr.Uid = 1000
	attr.Gid = 100
	attr.Mode = os.ModeDir | 0o555
	attr.Nlink = 2
	return nil
}

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = pathToInode(f.path)
	attr.Uid = 1000
	attr.Gid = 100
	mtime := timeMillis(int64(f.modifiedTime))
	attr.Mtime = mtime
	attr.Mode = os.FileMode(f.flags)
	attr.Size = f.size
	return nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries := utils.ListDirectoryAtTime(d.path, d.timestamp)
	out := make([]fuse.Dirent, 0, len(entries)+2)

	out = append(out, fuse.Dirent{
		Inode: pathToInode(d.path),
		Name:  ".",
		Type:  fuse.DT_Dir,
	})
	// Calculate parent directory path for ".."
	parentPath := d.path
	if parentPath != "/" {
		parentPath = strings.TrimSuffix(parentPath, "/")
		lastSlash := strings.LastIndex(parentPath, "/")
		if lastSlash >= 0 {
			parentPath = parentPath[:lastSlash+1]
		} else {
			parentPath = "/"
		}
	}
	out = append(out, fuse.Dirent{
		Inode: pathToInode(parentPath),
		Name:  "..",
		Type:  fuse.DT_Dir,
	})

	for _, entry := range entries {
		if entry.IsDirectory {
			// Extract just the directory name from the full path
			name := strings.TrimSuffix(entry.Path[len(d.path):], "/")
			out = append(out, fuse.Dirent{
				Inode: pathToInode(entry.Path),
				Name:  name,
				Type:  fuse.DT_Dir,
			})
		} else {
			// Extract just the filename from the full path
			name := entry.Path[strings.LastIndex(entry.Path, "/")+1:]
			out = append(out, fuse.Dirent{
				Inode: pathToInode(entry.Path),
				Name:  name,
				Type:  fuse.DT_File,
			})
		}
	}

	return out, nil
}

var _ fuseFs.Node = (*File)(nil)
var _ = fuseFs.NodeOpener(&File{})

func newUncompressedHandle(hash []byte, tx *sql.Tx) UncompressedFileHandle {
	// pasted from cat.go lol
	var blobID []byte
	var offset int64
	var length int64
	var key []byte
	var path string
	var storageID []byte
	var kind string
	var identifier string
	var rootPath string
	err := tx.QueryRow(`
			SELECT
				blob_entries.blob_id,
				blob_entries.offset, 
				blob_entries.final_size,
				blob_entries.encryption_key,
				blob_storage.path,
				storage.storage_id,
				storage.type,
				storage.identifier,
				storage.root_path
			FROM blob_entries
				INNER JOIN blobs ON blobs.blob_id = blob_entries.blob_id
				INNER JOIN blob_storage ON blob_storage.blob_id = blobs.blob_id
				INNER JOIN storage ON storage.storage_id = blob_storage.storage_id
			WHERE blob_entries.hash = ?


			ORDER BY storage.readable_label /* completely arbitrary. if there are many matching rows, just consistently pick it based on storage label. */
		`, hash).Scan(&blobID, &offset, &length, &key, &path, &storageID, &kind, &identifier, &rootPath)
	if err != nil {
		panic(err)
	}
	storageR := storage.StorageDataToStorage(storage.StorageDescriptor{
		StorageID:  utils.SliceToArr(storageID),
		Kind:       kind,
		Identifier: identifier,
		RootPath:   rootPath,
	})

	return UncompressedFileHandle{
		storagePath: path,
		blobOffset:  offset,
		length:      length,
		key:         &key,
		storage:     storageR,
	}
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fuseFs.Handle, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		err := tx.Commit()
		if err != nil {
			panic(err)
		}
	}()

	if f.compAlgo != "" {
		reader := download.CatReadCloser(*f.hash, tx)
		resp.Flags |= fuse.OpenNonSeekable
		return &CompressedFileHandle{reader, 0}, nil
	} else {
		handle := newUncompressedHandle(*f.hash, tx)
		return &handle, nil
	}
}

func (fh *CompressedFileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return fh.reader.Close()
}

var _ = fuseFs.HandleReader(&CompressedFileHandle{})
var _ = fuseFs.HandleReader(&UncompressedFileHandle{})

func (fh *CompressedFileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fmt.Println("CompressedFileHandle.Read()")
	buf := make([]byte, req.Size)
	if req.Offset != fh.currentOffset {
		fmt.Println("Attempt to read from wrong blobOffset (", req.Offset, ") expected (", fh.currentOffset, ")")
		return os.ErrInvalid
	}
	n, err := io.ReadFull(fh.reader, buf)
	fh.currentOffset += int64(n)

	// not sure if this makes sense but this is what the official example does
	// https://github.com/bazil/zipfs/blob/master/main.go#L221
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		err = nil
	}
	resp.Data = buf[:n]
	return err
}

func (fh *UncompressedFileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	offset := fh.blobOffset + req.Offset
	reader := cache.DownloadSection(fh.storage, fh.storagePath, offset, int64(req.Size))
	decrypted := crypto.DecryptBlobEntry(reader, offset, *fh.key)
	defer reader.Close()
	n, err := io.ReadFull(decrypted, buf)
	// same as above
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		err = nil
	}
	resp.Data = buf[:n]
	return err
}

func (d *Dir) Lookup(ctx context.Context, name string) (fuseFs.Node, error) {
	// First check if it's a directory
	subdirPath := d.path + name + "/"
	if directoryExists(subdirPath, d.timestamp) {
		return &Dir{
			path:      subdirPath,
			timestamp: d.timestamp,
			inode:     pathToInode(subdirPath),
		}, nil
	}

	// Then check if it's a file
	filePath := d.path + name
	if file := lookupFile(filePath, d.timestamp); file != nil {
		return file, nil
	}

	return nil, syscall.ENOENT
}

func Mount(mountpoint string, path string, timestamp int64) {
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}

	root := Dir{
		path:      path,
		timestamp: timestamp,
		inode:     pathToInode(path),
	}

	conn, err := fuse.Mount(mountpoint,
		fuse.ReadOnly(),
		fuse.DefaultPermissions(),
		fuse.FSName("gbfs"),
		fuse.MaxReadahead(128*1024), // this is what restic uses
	)
	if err != nil {
		panic(err)
	}

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start serving in a goroutine
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- fuseFs.Serve(conn, GBFS{root, timestamp})
	}()

	// Wait for either a signal or serve to complete
	for {
		select {
		case sig := <-sigChan:
			log.Printf("Received signal %v, attempting to unmount...", sig)
			err = fuse.Unmount(mountpoint)
			if err != nil {
				log.Printf("Cannot unmount: %v (filesystem may be busy, files still open, or in use)", err)
				log.Printf("GBFS will continue running. Try closing all files and press Ctrl+C again, or use 'fusermount -u %s' to force unmount.", mountpoint)
				continue // Stay in the loop, don't exit
			}
			log.Println("GBFS unmounted cleanly")
			goto cleanup
		case err = <-serveDone:
			if err != nil {
				log.Printf("FUSE serve error: %v", err)
			}
			goto cleanup
		}
	}

cleanup:
	// Close the connection
	err = conn.Close()
	if err != nil {
		log.Printf("Error closing FUSE connection: %v", err)
	}
}

func (gb GBFS) Root() (fuseFs.Node, error) {
	return &gb.root, nil
}

// Generate a consistent inode from a path by hashing it
func pathToInode(path string) uint64 {
	h := sha256.Sum256([]byte(path))
	// Use the first 8 bytes as a uint64, but ensure it's never 0 or 1 (reserved for . and ..)
	inode := binary.LittleEndian.Uint64(h[:8])
	if inode <= 1 {
		inode = 2
	}
	return inode
}

func directoryExists(path string, timestamp int64) bool {
	// Check if any files exist that start with this path
	row := db.DB.QueryRow("SELECT 1 FROM files WHERE (? >= start AND (end > ? OR end IS NULL)) AND path "+db.StartsWithPattern(3)+" LIMIT 1", timestamp, timestamp, path)
	var exists int
	err := row.Scan(&exists)
	return err == nil
}

func lookupFile(path string, timestamp int64) *File {
	row := db.DB.QueryRow(`SELECT files.path, files.hash, files.fs_modified, files.permissions, sizes.size, COALESCE(blob_entries.compression_alg, '')
		FROM files
		INNER JOIN sizes ON sizes.hash = files.hash
		INNER JOIN blob_entries ON blob_entries.hash = files.hash
		WHERE (? >= files.start AND (files.end > ? OR files.end IS NULL)) AND files.path = ?`, timestamp, timestamp, path)

	var file File
	var hash []byte
	err := row.Scan(&file.path, &hash, &file.modifiedTime, &file.flags, &file.size, &file.compAlgo)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		panic(err)
	}

	file.hash = &hash
	file.inode = pathToInode(file.path)
	return &file
}
