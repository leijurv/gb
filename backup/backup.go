package backup

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/leijurv/gb/config"
)

func BackupADirectoryRecursively(path string) {
	log.Println("Going to back up this folder:", path)
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
	if !stat.IsDir() {
		log.Println("This is not a directory btw wtf single files are BaD and i wont deal with them owned")
		return
	}
	log.Println("Good this is a directory")
	if !strings.HasSuffix(path, "/") {
		path += "/"
	}
	log.Println("Normalized to ensure trailing slash:", path)
	wg.Add(1)
	go scannerThread(path)
	for i := 0; i < config.Config().NumHasherThreads; i++ {
		wg.Add(1)
		go hasherThread()
	}
	go bucketerThread()
	for i := 0; i < config.Config().NumUploaderThreads; i++ {
		go uploaderThread()
	}
	go func() {
		for {
			log.Println("Bytes written:", stats.Total())
			time.Sleep(5 * time.Second)
		}
	}()
	wg.Wait()
}
