package backup

import (
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/leijurv/gb/config"
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
	inProgress []*utils.HasherSizer
	lock       sync.Mutex
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

var stats Stats

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

func fetchContentionMutex(size int64) (*sync.Mutex, bool) {
	sizeClaimMapLock.Lock()
	defer sizeClaimMapLock.Unlock()
	lock, ok := sizeClaimMap[size]
	return lock, ok
}

func samplePaddingLength(size int64) int64 {
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

func formatCommas(num int64) string {
	str := strconv.FormatInt(num, 10)
	re := regexp.MustCompile("(\\d+)(\\d{3})")
	for n := ""; n != str; {
		n = str
		str = re.ReplaceAllString(str, "$1,$2")
	}
	return str
}

// walk a directory recursively, but only call the provided function for normal files that don't error on os.Stat
func WalkFiles(path string, fn func(path string, info os.FileInfo)) {
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("While traversing those files, I got this error:")
			log.Println(err)
			log.Println("while looking at this path:")
			log.Println(path)
			return err
		}
		if !NormalFile(info) { // **THIS IS WHAT SKIPS DIRECTORIES**
			return nil
		}
		if config.ExcludeFromBackup(path) {
			log.Println("EXCLUDING this path and pretending it doesn't exist, due to your exclude config:", path)
			return nil
		}
		fn(path, info)
		return nil
	})
	if err != nil {
		// permission error while traversing
		// we should *not* continue, because that would mark all further files as "deleted"
		// aka, do not continue with a partially complete traversal of the directory lmao
		panic(err)
	}
}

// return true if and only if the provided FileInfo represents a completely normal file, and nothing weird like a directory, symlink, pipe, socket, block device, etc
func NormalFile(info os.FileInfo) bool {
	return info.Mode()&os.ModeType == 0
}
