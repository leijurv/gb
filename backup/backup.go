package backup

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/utils"
)

func Backup(path string) {
	log.Println("Going to back up this path:", path)
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		panic(err)
	}
	log.Println("Converted to absolute:", path)

	stat, err := os.Stat(path)
	if err != nil {
		log.Println("Path doesn't exist?")
		return
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

	wg.Add(1)
	go scannerThread(path, stat)

	for i := 0; i < config.Config().NumHasherThreads; i++ {
		wg.Add(1)
		go hasherThread()
	}

	go bucketerThread()

	for i := 0; i < config.Config().NumUploaderThreads; i++ {
		go uploaderThread()
	}

	if config.Config().UploadStatusInterval != -1 {
		go func() {
			for {
				log.Println("Bytes written:", utils.FormatCommas(stats.Total()))
				time.Sleep(time.Duration(config.Config().UploadStatusInterval) * time.Second)
			}
		}()
	}
	wg.Wait()
	log.Println("Backup complete")
	BackupDB()
}
