package backup

import (
	"io"
	"log"
	"math/rand"
	"os"
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
	hash          []byte // CAN BE NIL!!!!!
	confirmedSize *int64 // CAN BE NIL!!!!!
	stakedClaim   *int64 // CAN BE NIL!!!!!
}

type HashPlan struct {
	File
	expectedHash []byte // CAN BE NIL!!!!!
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
	if _, err := io.Copy(&hs, f); err != nil {
		return nil, 0, err
	}
	hash, size := hs.HashAndSize()
	return hash, size, nil // go is a BIGOT for not letting me do return hs.HashAndSize(), nil
}
