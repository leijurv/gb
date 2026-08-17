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
}

type HashPlan struct {
	File

	// the hash of this file as of the last time we read it
	// if it's the same, we don't need to upload it or even create a new entry in files, we just update the last modified time to be accurate
	expectedHash []byte
}

type BlobPlan []Planned

// sizeClaim tracks a staked size claim and any callbacks waiting for it to be released.
// This ensures FIFO ordering: callbacks registered before release are invoked in order,
// and callbacks registered after release will execute synchronously.
type sizeClaim struct {
	callbacks []func()
	released  bool
	mu        sync.Mutex // protects callbacks and released
}

// bucketerState answers one question for the bucketer: can the pipeline make progress
// without flushing a blob that isn't full yet?
//
// Only two things ever write to bucketerCh: the scanner (staked size claims, written
// synchronously) and the callbacks registered by hashOneFile. Each registered callback is
// either runnable (executing now, or about to be) or parked in a sizeClaim, in which case
// only a completing upload can ever wake it.
//
// So once no new registrations are possible, the pipeline is deadlocked exactly when every
// pending write is parked and no upload is running to unpark one. At that point the only
// claims still held belong to files sitting in the bucketer's own buffer (every staked file
// has already been received by the bucketer, so it's either in a finished blob - claim
// already released - or in the buffer), which means flushing the buffer is guaranteed to
// release them. That's a precise condition, so the bucketer never needs a timeout.
type bucketerState struct {
	mu          sync.Mutex
	pending     int  // registered bucketerCh writes that have not been delivered yet
	parked      int  // subset of pending that is waiting on a sizeClaim to be released
	uploading   int  // blob plans dispatched to uploaderCh that have not finished yet
	noMoreInput bool // the scanner has returned and every hasher goroutine has exited

	poke chan struct{} // capacity 1, wakes the bucketer up to re-evaluate action()
}

// update mutates the state and then wakes the bucketer. The order matters: the bucketer
// re-reads the whole state after draining a poke, so a mutation that becomes visible before
// its poke can never be missed.
func (b *bucketerState) update(f func()) {
	b.mu.Lock()
	f()
	b.mu.Unlock()
	select {
	case b.poke <- struct{}{}:
	default: // a wakeup is already queued, and it'll observe our mutation too
	}
}

type bucketerAction int

const (
	bucketerWait bucketerAction = iota
	bucketerFlush
	bucketerDone
)

func (b *bucketerState) action() bucketerAction {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case !b.noMoreInput:
		// the scanner and/or a hasher can still produce more work, unprompted
		return bucketerWait
	case b.pending == 0:
		// nothing can ever arrive again
		return bucketerDone
	case b.pending == b.parked && b.uploading == 0:
		// everything left is parked on a size claim, and no upload is running to release
		// one. every claim still held belongs to a file in the bucketer's buffer, so
		// nothing moves until we flush it.
		return bucketerFlush
	default:
		return bucketerWait
	}
}

// an abstraction over uploading to our storage destinations
// stateful, End must be called after Begin (so, obviously, cannot be used from multiple threads)
// can be reused sequentially, though
type UploadService interface {
	Begin(blobID []byte) io.Writer
	End(sha256 []byte, size int64) []storage_base.UploadedBlob
	Cancel()
}

// BackupSession holds all state for a single backup operation.
// This replaces the previous package-level global variables.
type BackupSession struct {
	// all files whose contents are set during this backup are set to the same "now"
	now int64

	// a map to manage gb's size optimization
	// (which is: if we see a file whose size is X, and we've never seen a file of that size before,
	// we know it's going to be unique (and should be uploaded) without needing to calculate its hash)
	sizeClaimMap     map[int64]*sizeClaim
	sizeClaimMapLock sync.Mutex

	// a map to manage the behavior when multiple distinct files with the same hash are to be backed up
	hashLateMap     map[[32]byte][]File
	hashLateMapLock sync.Mutex

	// Pipeline channels
	hasherCh   chan HashPlan
	bucketerCh chan Planned
	uploaderCh chan BlobPlan

	// tracks whether anything can still reach the bucketer, and whether the bucketer is
	// the only thing standing in the way of progress
	bucketer bucketerState

	// Synchronization
	filesWg  sync.WaitGroup // files in the upload pipeline
	hasherWg sync.WaitGroup // hasher goroutines only

	// Stats for tracking upload progress
	statsLock          sync.Mutex
	statsInProgress    []*utils.HasherSizer
	currentlyUploading map[string]*utils.HasherSizer

	// Filesystem abstraction (injectable for tests)
	Walker     Walker
	FileOpener FileOpener
}

// lastSessionTime tracks the last timestamp used to ensure monotonically increasing timestamps.
// This is necessary because the database has a constraint that end > start, and rapid
// successive backups could otherwise get the same second-resolution timestamp.
var lastSessionTime int64
var lastSessionTimeLock sync.Mutex

// NewBackupSession creates a new backup session with all state initialized.
func NewBackupSession() *BackupSession {
	lastSessionTimeLock.Lock()
	now := time.Now().Unix()
	if now <= lastSessionTime {
		now = lastSessionTime + 1
	}
	lastSessionTime = now
	lastSessionTimeLock.Unlock()

	s := &BackupSession{
		now:                now,
		sizeClaimMap:       make(map[int64]*sizeClaim),
		hashLateMap:        make(map[[32]byte][]File),
		hasherCh:           make(chan HashPlan),
		bucketerCh:         make(chan Planned),
		uploaderCh:         make(chan BlobPlan),
		currentlyUploading: make(map[string]*utils.HasherSizer),
		Walker:             defaultWalker{},
		FileOpener:         osFileOpener{},
	}
	s.bucketer.poke = make(chan struct{}, 1)
	return s
}

// NewBackupSessionWithTime creates a new backup session with a specific timestamp.
// Used by tests to ensure monotonically increasing timestamps.
func NewBackupSessionWithTime(now int64) *BackupSession {
	s := NewBackupSession()
	s.now = now
	return s
}

// GetTimestamp returns the backup session's timestamp.
func (s *BackupSession) GetTimestamp() int64 {
	return s.now
}

// GetLastSessionTimestamp returns the timestamp used by the most recent BackupSession.
// This is useful for tests that need to restore to the exact time a backup was made.
func GetLastSessionTimestamp() int64 {
	lastSessionTimeLock.Lock()
	defer lastSessionTimeLock.Unlock()
	if lastSessionTime == 0 {
		panic("hasn't happened yet")
	}
	return lastSessionTime
}

func (s *BackupSession) addUploadStats(hs *utils.HasherSizer) {
	s.statsLock.Lock()
	defer s.statsLock.Unlock()
	s.statsInProgress = append(s.statsInProgress, hs)
}

func (s *BackupSession) totalBytesWritten() int64 {
	s.statsLock.Lock()
	defer s.statsLock.Unlock()
	var sum int64
	for _, hs := range s.statsInProgress {
		sum += hs.Size()
	}
	return sum
}

func (s *BackupSession) currentlyUploadingPaths() []string {
	s.statsLock.Lock()
	defer s.statsLock.Unlock()
	keys := make([]string, 0, len(s.currentlyUploading))
	for k, v := range s.currentlyUploading {
		stat, err := s.FileOpener.Stat(k)
		if err == nil {
			sz := stat.Size()
			progress := v.Size()
			k += fmt.Sprintf(" %.2f%% done", float64(progress)/float64(sz)*100)
		}
		keys = append(keys, k)
	}
	return keys
}

func (s *BackupSession) addCurrentlyUploading(path string, sizer *utils.HasherSizer) {
	s.statsLock.Lock()
	defer s.statsLock.Unlock()
	s.currentlyUploading[path] = sizer
}

func (s *BackupSession) finishedUploading(path string) {
	s.statsLock.Lock()
	defer s.statsLock.Unlock()
	delete(s.currentlyUploading, path)
}

// attempt to exclusively claim files of this size
// if this succeeds, the upload will begin immediately, without reading the file to calculate its hash
// for that reason, this puts an exclusive lock on all uploads of this size
// if there happens to be more files of the exact same size, they will stay queued until the first one completes its upload
// (and that upload also calculates its hash)
// so only once the first upload is finished can gb properly decide if any further ones should be uploaded, by comparing hashes
func (s *BackupSession) stakeSizeClaim(size int64) bool {
	s.sizeClaimMapLock.Lock()
	defer s.sizeClaimMapLock.Unlock()
	_, ok := s.sizeClaimMap[size]
	if ok {
		return false
	}
	s.sizeClaimMap[size] = &sizeClaim{}
	return true
}

// once the first file of this size is uploaded and its hash tabulated in the database, this unstakes its claim, and allows other files of the same size to proceed
func (s *BackupSession) releaseAndUnstakeSizeClaim(size int64) {
	log.Println("UNSTAKING", size)
	s.sizeClaimMapLock.Lock()
	claim, ok := s.sizeClaimMap[size]
	if !ok {
		panic("i must have screwed up the concurrency :(")
	}
	s.sizeClaimMapLock.Unlock()
	// any call to registerSizeClaimCallback around here will successfully register a callback - works fine
	claim.mu.Lock() // will be released in the goroutine below
	if claim.released {
		panic("sanity")
	}
	claim.released = true
	callbacks := claim.callbacks
	claim.callbacks = nil
	// these are about to become runnable, so they no longer count against the bucketer
	s.bucketer.update(func() { s.bucketer.parked -= len(callbacks) })
	go func() {
		// any call to registerSizeClaimCallback around here will block on claim.mu.Lock - works fine
		for _, cb := range callbacks {
			cb() // note that this callback could block for a little while; it writes to bucketerCh which has backpressure from the uploaderCh
		}
		claim.mu.Unlock()
	}()
}

// registerSizeClaimCallback registers a callback to be invoked when the size claim is released.
// If already released, invokes callback immediately (after any earlier callbacks finish).
// This ensures FIFO ordering for all callbacks on a given size claim IF they are registered while waiting for the original staked upload to finish.
func (s *BackupSession) registerSizeClaimCallback(size int64, callback func()) {
	s.sizeClaimMapLock.Lock()
	claim, ok := s.sizeClaimMap[size]
	s.sizeClaimMapLock.Unlock()
	if !ok {
		// This size is not claimed at all - means that a previous backup session uploaded a file of this size, so the current backup session was unable to bypass hashing
		callback()
		return
	}
	claim.mu.Lock()
	// It is possible for hashers to race here (whether the claim is released or not) and hit claim.mu.Lock nondeterminism ^ but this is fine, since it can only happen if the hashers were racing to complete in the first place (already nondeterministic)
	if claim.released {
		claim.mu.Unlock()
		callback() // callback outside the lock
		return
	}
	claim.callbacks = append(claim.callbacks, callback)
	// this write to bucketerCh can now only happen once some upload releases this claim,
	// which the bucketer needs to know about to detect that it's the one holding things up
	s.bucketer.update(func() { s.bucketer.parked++ })
	claim.mu.Unlock() // lock protects claim.callbacks
}

func SamplePaddingLength(size int64) int64 {
	conf := config.Config()

	ret := conf.PaddingMinBytes + rand.Int63n(conf.PaddingMaxBytes-conf.PaddingMinBytes+1)
	ret += int64(float64(size) * (conf.PaddingMinPercent + rand.Float64()*(conf.PaddingMaxPercent-conf.PaddingMinPercent)) / 100) // reee percent means percent
	log.Println("Adding", ret, "padding bytes onto the end of a blob of true length", size)
	return ret
}

func (s *BackupSession) hashAFile(path string) ([]byte, int64, error) {
	f, err := s.FileOpener.Open(path)
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
	completeds := make([]storage_base.UploadedBlob, 0)
	for _, upload := range du.uploads {
		c := upload.End()
		if c.Size != size {
			log.Println(c.Size, size)
			panic("sanity check")
		}
		completeds = append(completeds, c)
	}
	return completeds
}

func (du *directUpload) Cancel() {
	for i := range du.uploads {
		du.uploads[i].Cancel()
	}
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
