package replicate

import (
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/paranoia"
	storagepkg "github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/utils"
	"github.com/leijurv/gb/storage_base"
)

func ReplicateBlobs(label string) {
	log.Println("Replicate blobs. This is a good idea if you add a new storage and want to bring it up to speed. This only copies blobs, not db-backup (because there isn't really much reason to).")
	log.Println("Define which storage to pull from")
	storage, ok := storagepkg.StorageSelect(label)
	if !ok {
		return
	}
	log.Println("This pulls from", storage)
	log.Println("The intended usage is `gb paranoia storage` (a clean run with nothing flagged), then add your new storage, then this")
	log.Println("This will just go through the blobs in", storage)
	log.Println("It won't go through what's in the database, so make sure that that's all good (such as with `gb paranoia storage` lol)")
	toReplicate := storage.ListBlobs()
	sz := new(int64)
	for _, dst := range storagepkg.GetAll() {
		if dst == storage {
			continue
		}
		alreadyHere := make(map[[32]byte]struct{})
		for _, inDst := range dst.ListBlobs() {
			alreadyHere[utils.SliceToArr(inDst.BlobID)] = struct{}{}
		}
		todo := make(chan storage_base.UploadedBlob)
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			j := i
			wg.Add(1)
			go func() {
				defer wg.Done()
				for blob := range todo {
					log.Println("Copy", blob, "from", storage, "to", dst)
					log.Println("Done", atomic.LoadInt64(sz), "bytes, thread", j)
					reader := paranoia.DownloadEntireBlob(blob.BlobID, storage)
					out := dst.BeginBlobUpload(blob.BlobID)
					rd := io.TeeReader(reader, out.Writer())
					bytes := paranoia.BlobReaderParanoia(rd, blob.BlobID, storage)
					atomic.AddInt64(sz, bytes)
					completed := out.End()
					_, err := db.DB.Exec("INSERT INTO blob_storage (blob_id, storage_id, path, checksum, timestamp) VALUES (?, ?, ?, ?, ?)", blob.BlobID, completed.StorageID, completed.Path, completed.Checksum, time.Now().Unix())
					if err != nil {
						panic(err)
					}
				}
			}()
		}
		for _, blob := range toReplicate {
			if _, ok := alreadyHere[utils.SliceToArr(blob.BlobID)]; ok {
				continue
			}
			todo <- blob
		}
		close(todo)
		wg.Wait()
	}
}
