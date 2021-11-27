package gbfs

import (
	"bazil.org/fuse"
	fuseFs "bazil.org/fuse/fs"
	"context"
	"fmt"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"io"
	"os"
	"strings"
	"syscall"
	"time"
)

type File struct {
	path         string
	hash         *[]byte // I love go
	modifiedTime uint64
	flags        int32
	size         uint64
	inode        uint64 // generated
}

func (f File) name() string {
	idx := strings.LastIndex(f.path, "/")
	return f.path[idx+1:]
}

type Dir struct {
	name  string // empty for the root dir
	files map[string]File
	dirs  map[string]*Dir
	inode uint64 // generated
}

type GBFS struct {
	root Dir
}

type FileHandle struct {
	reader io.ReadCloser
	// for sanity checking
	currentOffset int64
}

func timeMillis(millis int64) time.Time {
	return time.Unix(0, millis*int64(time.Millisecond))
}

func (d *Dir) Attr(ctx context.Context, attr *fuse.Attr) error {
	//attr.Inode = d.inode
	attr.Uid = 1000
	attr.Gid = 100
	attr.Mode = os.ModeDir | 0o555
	attr.Nlink = 2
	return nil
}

func (f *File) Attr(ctx context.Context, attr *fuse.Attr) error {
	//attr.Inode = f.inode
	attr.Uid = 1000
	attr.Gid = 100
	//attr.Mode = 0o444
	mtime := timeMillis(int64(f.modifiedTime))
	attr.Mtime = mtime
	attr.Mode = os.FileMode(f.flags)
	attr.Size = f.size
	return nil
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	out := make([]fuse.Dirent, 0, len(d.dirs)+len(d.files)+2)

	out = append(out, fuse.Dirent{
		//Inode: d.inode,
		Name: ".",
		Type: fuse.DT_Dir,
	})
	out = append(out, fuse.Dirent{
		Name: "..",
		Type: fuse.DT_Dir,
	})

	for _, subdir := range d.dirs {
		out = append(out, fuse.Dirent{
			//Inode: subdir.inode,
			Name: subdir.name,
			Type: fuse.DT_Dir,
		})
	}
	for _, f := range d.files {
		name := f.name()
		out = append(out, fuse.Dirent{
			//Inode: f.inode,
			Name: name,
			Type: fuse.DT_File,
		})
	}

	return out, nil
}

var _ fuseFs.Node = (*File)(nil)
var _ = fuseFs.NodeOpener(&File{})

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fuseFs.Handle, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
	}()

	reader := download.CatReadCloser(*f.hash, tx)
	// TODO: support seeking for uncompressed file (possibly useful for videos)
	resp.Flags |= fuse.OpenNonSeekable

	return &FileHandle{reader, 0}, nil
}

func (fh *FileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return fh.reader.Close()
}

var _ = fuseFs.HandleReader(&FileHandle{})

func (fh *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	if req.Offset != fh.currentOffset {
		fmt.Println("Attempt to read from wrong offset (", req.Offset, ") expected (", fh.currentOffset, ")")
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

func (d *Dir) Lookup(ctx context.Context, name string) (fuseFs.Node, error) {
	if subdir, ok := d.dirs[name]; ok {
		return subdir, nil
	}
	if f, ok := d.files[name]; ok {
		return &f, nil
	}

	return nil, syscall.ENOENT
}

func Mount(mountpoint string, path string, timestamp int64) {
	root := parseDirectoryStructure(queryAllFiles(path, timestamp))
	// TODO: store blob info so we don't need to query it later
	//db.ShutdownDatabase()

	conn, err := fuse.Mount(mountpoint,
		fuse.ReadOnly(),
		fuse.DefaultPermissions(),
		fuse.FSName("gbfs"),
		fuse.MaxReadahead(128*1024), // this is what restic uses
	)
	if err != nil {
		panic(err)
	}
	defer func(conn *fuse.Conn) {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
	}(conn)

	err = fuseFs.Serve(conn, GBFS{root})
	if err != nil {
		panic(err)
	}
}

func (gb GBFS) Root() (fuseFs.Node, error) {
	return &gb.root, nil
}

const (
	QUERY = "SELECT files.path, files.hash, files.fs_modified, files.permissions, sizes.size FROM files INNER JOIN sizes ON files.hash = sizes.hash WHERE (? >= files.start AND (files.end > ? OR files.end IS NULL)) AND files.path GLOB ?"
)

func queryAllFiles(path string, timestamp int64) []File {
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	defer func() {
		err = tx.Commit()
		if err != nil {
			panic(err)
		}
	}()

	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	rows, err := tx.Query(QUERY, timestamp, timestamp, path+"*")
	if err != nil {
		panic(err)
	}
	var files []File
	for rows.Next() {
		var file File
		err = rows.Scan(&file.path, &file.hash, &file.modifiedTime, &file.flags, &file.size)
		if err != nil {
			panic(err)
		}
		files = append(files, file)
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
	return files
}

func makeDir(name string, inode uint64) Dir {
	return Dir{
		name:  name,
		files: make(map[string]File),
		dirs:  make(map[string]*Dir),
		inode: inode,
	}
}

func parseDirectoryStructure(files []File) Dir {
	root := makeDir("", 0)
	nextInode := uint64(1)
	for _, f := range files {
		dir := &root
		parts := strings.Split(f.path, "/")
		for i := 1; i < len(parts); i++ {
			element := parts[i]

			if i == len(parts)-1 {
				f.inode = nextInode
				nextInode++
				dir.files[element] = f
			} else {
				if val, ok := dir.dirs[element]; ok {
					dir = val
				} else {
					cringe := makeDir(element, nextInode)
					nextInode++
					newDir := &cringe
					dir.dirs[element] = newDir
					dir = newDir
				}
			}
		}
	}

	return root
}
