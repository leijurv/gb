package backup

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/leijurv/gb/config"
	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

var now = time.Now().Unix() // all files whose contents are set during this backup are set to the same "now", explanation is in the spec

type File struct {
	path string
	info os.FileInfo
}

type Planned struct {
	File

	// hash and confirmedSize must either both be nil, or both be non-nil
	hash          []byte
	confirmedSize *int64

	// if this is non-nil, then this is a staked size claim, and hash and confirmedSize MUST both be nil.
	stakedClaim *int64

	// if all three are nil, this is a dummy plan used to signal the bucketer that all its inputs are "done", so it should write whatever it has so far, even if it isn't big enough
}

type HashPlan struct {
	File

	// the hash of this file as of the last time we read it
	// if it's the same, we don't need to upload it or even create a new entry in files, we just update the last modified time to be accurate
	expectedHash []byte
}

type BlobPlan []Planned

type Stats struct {
	inProgress         []*utils.HasherSizer
	currentlyUploading map[string]*utils.HasherSizer
	lock               sync.Mutex
}

// an abstraction over uploading to our storage destinations
// stateful, End must be called after Begin (so, obviously, cannot be used from multiple threads)
// can be reused sequentially, though
type UploadService interface {
	Begin(blobID []byte) io.Writer
	End(sha256 []byte, size int64) []storage_base.UploadedBlob
}

// a map to manage gb's size optimization
// (which is: if we see a file whose size is X, and we've never seen a file of that size before, we know it's going to be unique (and should be uploaded) without needing to calculate its hash)
var sizeClaimMap = make(map[int64]*sync.Mutex)
var sizeClaimMapLock sync.Mutex

// a map to manage the behavior when multiple distinct files with the same hash are to be backed up
var hashLateMap = make(map[[32]byte][]File)
var hashLateMapLock sync.Mutex

var hasherCh = make(chan HashPlan)
var bucketerCh = make(chan Planned)
var uploaderCh = make(chan BlobPlan)

var wg sync.WaitGroup // files + threads

var stats = Stats{
	currentlyUploading: make(map[string]*utils.HasherSizer),
}

func ResetForTesting() {
	newNow := time.Now().Unix()
	if newNow <= now {
		newNow = now + 1
	}
	now = newNow
	sizeClaimMap = make(map[int64]*sync.Mutex)
	hashLateMap = make(map[[32]byte][]File)
	hasherCh = make(chan HashPlan)
	bucketerCh = make(chan Planned)
	uploaderCh = make(chan BlobPlan)
	wg = sync.WaitGroup{}
	stats = Stats{
		currentlyUploading: make(map[string]*utils.HasherSizer),
	}
}

func GetTestingTimestamp() int64 {
	return now
}

func (s *Stats) Add(hs *utils.HasherSizer) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.inProgress = append(s.inProgress, hs)
}

func (s *Stats) Total() int64 {
	s.lock.Lock()
	defer s.lock.Unlock()
	var sum int64
	for _, hs := range s.inProgress {
		sum += hs.Size()
	}
	return sum
}

func (s *Stats) CurrentlyUploading() []string {
	s.lock.Lock()
	defer s.lock.Unlock()
	keys := make([]string, 0, len(s.currentlyUploading))
	for k, v := range s.currentlyUploading {
		stat, err := os.Stat(k)
		if err == nil {
			sz := stat.Size()
			progress := v.Size()
			k += fmt.Sprintf(" %.2f%% done", float64(progress)/float64(sz)*100)
		}
		keys = append(keys, k)
	}
	return keys
}

func (s *Stats) AddCurrentlyUploading(path string, sizer *utils.HasherSizer) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.currentlyUploading[path] = sizer
}

func (s *Stats) FinishedUploading(path string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.currentlyUploading, path)
}

// attempt to exclusively claim files of this size
// if this succeeds, the upload will begin immediately, without reading the file to calculate its hash
// for that reason, this puts an exclusive lock on all uploads of this size
// if there happens to be more files of the exact same size, they will stay queued until the first one completes its upload
// (and that upload also calculates its hash)
// so only once the first upload is finished can gb properly decide if any further ones should be uploaded, by comparing hashes
func stakeSizeClaim(size int64) bool {
	sizeClaimMapLock.Lock()
	defer sizeClaimMapLock.Unlock()
	_, ok := sizeClaimMap[size]
	if ok {
		return false
	}
	mut := &sync.Mutex{}
	mut.Lock()
	sizeClaimMap[size] = mut
	return true
}

// once the first file of this size is uploaded and its hash tabulated in the database, this unstakes its claim, and allows other files of the same size to proceed
func releaseAndUnstakeSizeClaim(size int64) {
	log.Println("UNSTAKING", size)
	sizeClaimMapLock.Lock()
	defer sizeClaimMapLock.Unlock()
	lock, ok := sizeClaimMap[size]
	if !ok {
		panic("i must have screwed up the concurrency :(")
	}
	lock.Unlock()
}

// check if this size is staked, and, if so, fetch the mutex that we are to block on before proceeding
func fetchContentionMutex(size int64) (*sync.Mutex, bool) {
	sizeClaimMapLock.Lock()
	defer sizeClaimMapLock.Unlock()
	lock, ok := sizeClaimMap[size]
	return lock, ok
}

func SamplePaddingLength(size int64) int64 {
	rand.Seed(time.Now().UnixNano())
	conf := config.Config()

	ret := conf.PaddingMinBytes + rand.Int63n(conf.PaddingMaxBytes-conf.PaddingMinBytes+1)
	ret += int64(float64(size) * (conf.PaddingMinPercent + rand.Float64()*(conf.PaddingMaxPercent-conf.PaddingMinPercent)) / 100) // reee percent means percent
	log.Println("Adding", ret, "padding bytes onto the end of a blob of true length", size)
	return ret
}

func hashAFile(path string) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	hs := utils.NewSHA256HasherSizer()
	if _, err := io.CopyBuffer(&hs, f, make([]byte, 1024*1024)); err != nil {
		return nil, 0, err
	}
	hash, size := hs.HashAndSize()
	return hash, size, nil // go is a BIGOT for not letting me do return hs.HashAndSize(), nil
}

func BeginDirectUpload(storages []storage_base.Storage) UploadService {
	if len(storages) == 0 {
		panic("no storage")
	}
	return &directUpload{
		storages: storages,
	}
}

type directUpload struct {
	storages []storage_base.Storage
	uploads  []storage_base.StorageUpload
}

func (du *directUpload) Begin(blobID []byte) io.Writer {
	if len(blobID) != 32 {
		panic("sanity check")
	}
	du.uploads = make([]storage_base.StorageUpload, 0)
	for _, storage := range du.storages {
		du.uploads = append(du.uploads, storage.BeginBlobUpload(blobID))
	}
	writers := make([]io.Writer, 0)
	for _, upload := range du.uploads {
		writers = append(writers, upload.Writer())
	}
	return &multithreadedMultiWriter{writers}
}

func (du *directUpload) End(sha256 []byte, size int64) []storage_base.UploadedBlob {
	completeds := make([]storage_base.UploadedBlob, len(du.uploads))
	var wg sync.WaitGroup
	for i := range completeds {
		i := i
		wg.Add(1)
		go func() {
			completeds[i] = du.uploads[i].End()
			wg.Done() // don't use defer because we only want to call wg.Done if .End didn't panic
		}()
	}
	wg.Wait()
	for _, c := range completeds {
		if c.Size != size {
			log.Println(c.Size, size)
			panic("sanity check")
		}
	}
	return completeds
}

type multithreadedMultiWriter struct {
	writers []io.Writer
}

func (t *multithreadedMultiWriter) Write(p []byte) (int, error) {
	if len(t.writers) == 1 {
		// fast case for gb users with only one storage
		n, err := t.writers[0].Write(p)
		return n, err
	}
	errs := make([]error, len(t.writers))
	ns := make([]int, len(t.writers))
	var wg sync.WaitGroup
	for i := range t.writers {
		i := i
		wg.Add(1)
		go func() {
			n, err := t.writers[i].Write(p)
			if err == nil && n != len(p) { // a short write is still an error
				err = io.ErrShortWrite
			}
			errs[i] = err
			ns[i] = n
			wg.Done()
		}()
	}
	wg.Wait()
	for i := range errs {
		if errs[i] != nil {
			return ns[i], errs[i]
		}
		if ns[i] != len(p) {
			panic("sanity check failed")
		}
	}
	return len(p), nil
}
