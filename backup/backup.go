package backup

import (
	"database/sql"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/utils"
)

func statInputPaths(rawPaths []string) []File {
	files := make([]File, 0)
	for _, path := range rawPaths {
		log.Println("Going to back up this path:", path)

		var err error
		path, err = filepath.Abs(path)
		if err != nil {
			panic(err)
		}
		log.Println("Converted to absolute:", path)

		stat, err := os.Stat(path)
		if err != nil {
			panic("Path doesn't exist?")
		}

		if stat.IsDir() {
			log.Println("This is a directory, good!")
			if !strings.HasSuffix(path, "/") {
				path += "/"
			}
			log.Println("Normalized to ensure trailing slash:", path)
		} else {
			if !NormalFile(stat) {
				panic("This file is not normal. Perhaps a symlink or something? Not supported sorry!")
			}
			log.Println("This is a single file...?")
		}
		files = append(files, File{path: path, info: stat})
	}
	return files
}

func Backup(rawPaths []string, serviceCh UploadServiceFactory) {
	inputs := statInputPaths(rawPaths)

	for i := 0; i < config.Config().NumHasherThreads; i++ {
		wg.Add(1)
		go hasherThread()
	}

	go bucketerThread()

	for i := 0; i < config.Config().NumUploaderThreads; i++ {
		go uploaderThread(<-serviceCh)
	}

	if config.Config().UploadStatusInterval != -1 {
		go func() {
			for {
				uploading := stats.CurrentlyUploading()
				if len(uploading) > 0 {
					log.Println("Currently uploading:", strings.Join(uploading, ","))
				}
				log.Println("Bytes written:", utils.FormatCommas(stats.Total()))
				time.Sleep(time.Duration(config.Config().UploadStatusInterval) * time.Second)
			}
		}()
	}
	scannerThread(inputs)
	wg.Wait()
	log.Println("Backup complete")
}

type file_status struct {
	file File
	// no enums lol
	modified bool
	new      bool
}

func compareFileToDb(path string, info os.FileInfo, tx *sql.Tx) file_status {
	var expectedLastModifiedTime int64
	var expectedSize int64
	size := info.Size()
	err := tx.QueryRow("SELECT files.fs_modified, sizes.size FROM files INNER JOIN sizes ON files.hash = sizes.hash WHERE files.path = ? AND files.end IS NULL", path).Scan(&expectedLastModifiedTime, &expectedSize)
	if err == nil {
		if expectedLastModifiedTime != info.ModTime().Unix() || expectedSize != size {
			return file_status{file: File{path, info}, modified: true}
		} else {
			return file_status{file: File{path, info}}
		}
	} else {
		if err != db.ErrNoRows {
			panic(err)
		}
		return file_status{file: File{path, info}, new: true}
	}
}

func DryBackup(rawPaths []string) {
	inputs := statInputPaths(rawPaths)

	// scanning
	tx, err := db.DB.Begin()
	if err != nil {
		panic(err)
	}
	statuses := make([]file_status, 0)
	for _, input := range inputs {
		if input.info.IsDir() {
			filesMap := make(map[string]os.FileInfo)
			pathsToBackup := getDirectoriesToScan(input.path, config.Config().Includes)
			for _, path := range pathsToBackup {
				utils.WalkFiles(path, func(path string, info os.FileInfo) {
					filesMap[path] = info
					comparison := compareFileToDb(path, info, tx)
					if comparison.modified || comparison.new {
						statuses = append(statuses, comparison)
					}
				})
			}
		} else {
			comparison := compareFileToDb(input.path, input.info, tx)
			if comparison.modified || comparison.new {
				statuses = append(statuses, comparison)
			}
		}
	}
	sort.Slice(statuses, func(i, j int) bool {
		return statuses[i].file.info.Size() < statuses[j].file.info.Size()
	})
	var size int64
	for _, f := range statuses {
		size += f.file.info.Size()
	}

	log.Printf("%d paths to be backed up (%s bytes)", len(statuses), utils.FormatCommas(size))
	for _, f := range statuses {
		var why string
		if f.modified {
			why = "modified"
		}
		if f.new {
			why = "new"
		}
		log.Printf("%s (%s, %s)", f.file.path, utils.FormatCommas(f.file.info.Size()), why)
	}
}
