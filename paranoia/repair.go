package paranoia

import (
	"bytes"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/crypto"
	"github.com/leijurv/gb/db"
	"github.com/leijurv/gb/download"
	"github.com/leijurv/gb/s3"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

// attempt to repair a s3 etag, by recalculating it locally

// this is safe: the s3 etag depends on how many "parts" you split the file into, so there any many possible valid etags for the same data, depending on part size

// we're just recalculating with the correct part size

// so, transitioning to deep archive will "repack" your file, and recalculate your etag in chunks of size 16777216, regardless of the chunk size that you uploaded
const deepArchivePartSize = 1 << 24 // this is 16777216

// this results in a different etag (unless you uploaded in that chunk size to begin with, or your file was only in 1 chunk both before and after this repack)

// this will recalculate it by reading from disk

// (i have tested this myself by uploading a file of length 16777216 and one of length 16777217, it isn't just a guess that they probably picked 2^24, it's confirmed)

// note that this is undefined behavior of AWS. that is true. but if you think about it, if we can locally calculate an alternative etag (using a specific chunk size) that matches what they got, then why NOT do that, if we can validate what they did as being correct, might as well
func handleIncorrectMetadata(actual storage_base.UploadedBlob, expected storage_base.UploadedBlob, storage storage_base.Storage) {
	if !strings.Contains(storage.String(), "S3") {
		// hack
		return
	}
	if actual.Size != expected.Size {
		log.Println("wrong size")
		// this repair can't help if size is wrong
		return
	}
	size := actual.Size

	realEtag := actual.Checksum
	expectEtag := expected.Checksum

	var err error
	numPartsReal := 1
	if strings.Contains(realEtag, "-") {
		numPartsReal, err = strconv.Atoi(strings.Split(realEtag, "-")[1])
		if err != nil {
			log.Println("what", err)
			return
		}
	}
	numPartsExpect := 1
	if strings.Contains(expectEtag, "-") {
		numPartsExpect, err = strconv.Atoi(strings.Split(expectEtag, "-")[1])
		if err != nil {
			log.Println("what", err)
			return
		}
	}

	partsShouldBe := size / deepArchivePartSize // now we calculate *exactly* how many parts deep archive would split this file into
	if size%deepArchivePartSize != 0 {
		partsShouldBe++ // extremely unlikely to be an exact multiple, but BE PREPARED lol
	}

	if numPartsReal != int(partsShouldBe) {
		log.Println("File has", numPartsReal, "parts, but repacking into 2^24 byte parts should have yielded", partsShouldBe, "so it's corrupted for some other reason, sorry.")
		return
	}

	if numPartsReal == numPartsExpect {
		log.Println("WARNING: number of parts matches, but this doesn't mean part size was exactly the same (it could just be close). I'm going to continue under that assumption, but this is useless unless your upload part size was very close but not exactly equal to 16777216.")
		//return
	}

	// how many entries are in this blob?
	// this is a simple heuristic to decide if we should recalculate the etag using local data (reading a file), or by downloading the whole blob from another source
	var numEntries int
	err = db.DB.QueryRow(`SELECT COUNT(*) FROM blob_entries WHERE blob_id = ?`, expected.BlobID).Scan(&numEntries)
	if err != nil {
		panic(err)
	}

	var key []byte
	var paddedSize int64
	var hashPostEnc []byte
	err = db.DB.QueryRow(`SELECT encryption_key, size, hash_post_enc FROM blobs WHERE blob_id = ?`, expected.BlobID).Scan(&key, &paddedSize, &hashPostEnc)
	if err != nil {
		panic(err)
	}

	if paddedSize != size {
		log.Println(paddedSize, size)
		panic("wtf")
	}

	etag := s3.CreateETagCalculator()
	hs := utils.NewSHA256HasherSizer()
	out := io.MultiWriter(etag.Writer, &hs)
	out = crypto.EncryptBlobWithKey(out, key)

	done := false
	if numEntries == 1 {
		var filePath string
		var fileModified int64
		var fileSize int64
		err = db.DB.QueryRow(`SELECT files.path, files.fs_modified, sizes.size FROM blob_entries INNER JOIN files ON files.hash = blob_entries.hash INNER JOIN sizes ON sizes.hash = files.hash WHERE files.end IS NULL AND blob_entries.blob_id = ?`, expected.BlobID).Scan(&filePath, &fileModified, &fileSize)
		if err != nil {
			log.Println("error while finding an existing local file")
			if size > 2*config.Config().MinBlobSize {
				// if you have bandwidth to spare, just comment out this panic it'll work fine, just slowly and bandwidth-expensive-ly
				panic("no local file for a big boy")
			}
		} else {
			info, err := os.Stat(filePath)
			if err == nil && info.ModTime().Unix() == fileModified && info.Size() == fileSize {
				log.Println("Going to repair using", filePath)
				f, err := os.Open(filePath)
				if err == nil {
					defer f.Close()
					_, err = io.Copy(out, f)
					if err != nil {
						panic(err)
					}
					if hs.Size() != fileSize {
						panic("invalid")
					}
					done = true
				}
			}
		}
	}

	if !done {
		log.Println("Falling back to fetch from storage")
		_, err = io.CopyBuffer(out, download.CatBlob(expected.BlobID), make([]byte, 1024*1024)) // technically, this fallback results in a decrypt then encrypt. but who cares.
		if err != nil {
			panic(err)
		}
	}

	out.Write(make([]byte, size-hs.Size()))
	writtenHash, _ := hs.HashAndSize()
	if !bytes.Equal(writtenHash, hashPostEnc) {
		panic("wrong hash")
	}
	etag.Writer.Close()
	calculated := <-etag.Result

	log.Println("i got the etag as", calculated)

	if calculated == actual.Checksum {
		_, err := db.DB.Exec("UPDATE blob_storage SET checksum = ? WHERE blob_id = ? AND path = ? AND storage_id = ? AND checksum = ?", calculated, expected.BlobID, expected.Path, storage.GetID(), expected.Checksum)
		if err != nil {
			panic(err)
		}
		log.Println("etag in database updated sucessfully from", expected.Checksum, "to", calculated)
	} else {
		panic("I COULD NOT MAKE THE ETAG WORK OH GOD")
	}

}
