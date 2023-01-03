package replicate

import (
	"io"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/paranoia"
	storagepkg "github.com/leijurv/gb/storage"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
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
	log.Println()
	log.Println("Seriously, make sure that `gb paranoia db` and `gb paranoia storage` complete cleanly with no issues BOTH before AND after using `gb replicate`")
	log.Println()
	log.Println("Some considerations to think about when running this:")
	log.Println("• Google Drive has a 10 terabyte per 24 hour limit on downloads, beyond which you'll get \"panic: googleapi: Error 403: The download quota for this file has been exceeded., downloadQuotaExceeded\". Consider how much bandwidth you need - anything above 1gbps will be overkill since GDrive throttles you to less than that over 24h (assuming you have over 10tb to begin with)")
	log.Println("• If `gb replicate` says it's completed, but it isn't, look at `gb paranoia storage` and clear any files marked as \"UNKNOWN / UNEXPECTED\". That can happen if a file uploads successfully, but GB doesn't commit it to the database (can happen if another thread among the 8 replicate threads crashes at that exact moment). After you clear that file, `gb replicate` will again notice that it isn't in the destination, and will try copying it again.")
	log.Println("• Backblaze has very frequent 500s and 503s. Consider running `gb replicate` in a loop. I use a 100 second delay between runs.")
	toReplicate := storage.ListBlobs()
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(toReplicate), func(i int, j int) {
		toReplicate[i], toReplicate[j] = toReplicate[j], toReplicate[i]
	})
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
					log.Println("Done", utils.FormatCommas(atomic.LoadInt64(sz)), "bytes, thread", j)
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
	log.Println("Done replicating. Now you should do `gb paranoia db` and `gb paranoia storage`!")
}
