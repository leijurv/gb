package cache

import (
	"io"
	"sync"
	"time"

	"github.com/leijurv/gb/storage_base"
	"github.com/leijurv/gb/utils"
)

const (
	chunkSize            = 1_000_000
	maxContinuousReaders = 2
	maxCacheSize         = 500
	cacheExpiration      = 5 * time.Minute
)

type CacheReader struct {
	storage storage_base.Storage
	path    string
	offset  int64
	length  int64
	pos     int64
}

type fileInfo struct {
	size    int64
	mutex   *sync.Mutex
	readers *lruCache[int64, *readerEntry]
}

// Global cache state
var (
	fileInfos  = make(map[cacheKey]*fileInfo)
	chunkCache = newLRUCache[chunkKey, []byte](maxCacheSize)
	globalMu   sync.RWMutex
)

func init() {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			CleanupExpiredEntries()
		}
	}()
}

type cacheKey struct {
	storageID [32]byte
	path      string
}

type chunkKey struct {
	cacheKey
	chunkIdx int64 // chunk index (offset / chunkSize)
}

type readerEntry struct {
	reader       io.ReadCloser
	nextChunkIdx int64 // Next chunk index to read (currentPos / chunkSize)
	lastAccess   time.Time
}

func (r *readerEntry) Close() error {
	return r.reader.Close()
}

// DownloadSection returns a CacheReader that handles each read request separately
func DownloadSection(storage storage_base.Storage, path string, offset int64, length int64) io.ReadCloser {
	return &CacheReader{
		storage: storage,
		path:    path,
		offset:  offset,
		length:  length,
		pos:     0,
	}
}

func (cr *CacheReader) Read(p []byte) (n int, err error) {
	if cr.pos >= cr.length {
		return 0, io.EOF
	}

	currentOffset := cr.offset + cr.pos
	remainingInFile := cr.length - cr.pos
	requestLen := int64(len(p))
	if requestLen > remainingInFile {
		requestLen = remainingInFile
	}

	// Get file size from cache
	fileSize := cr.getFileSize()
	if currentOffset >= fileSize {
		return 0, io.EOF
	}

	// Cap request to file size
	if currentOffset+requestLen > fileSize {
		requestLen = fileSize - currentOffset
	}

	// Calculate which 1MB chunk we need
	chunkIdx := currentOffset / chunkSize
	chunkStart := chunkIdx * chunkSize
	chunkEnd := chunkStart + chunkSize
	if chunkEnd > fileSize {
		chunkEnd = fileSize
	}

	// Optimistic check: see if chunk is already cached before acquiring fileMutex
	chunkKey := chunkKey{cacheKey: cacheKey{storageID: utils.SliceToArr(cr.storage.GetID()), path: cr.path}, chunkIdx: chunkIdx}
	globalMu.RLock()
	chunkData, ok := chunkCache.get(chunkKey)
	globalMu.RUnlock()

	if !ok {
		// Get file mutex and lock for entire operation
		mutex := cr.getFileMutex()
		mutex.Lock()
		defer mutex.Unlock()

		// Check again after acquiring lock (double-checked locking pattern)
		chunkData = cr.getOrDownloadChunk(chunkIdx, fileSize)
	}
	if chunkData == nil {
		return 0, io.EOF
	}

	// Calculate offset within the chunk
	offsetInChunk := currentOffset - chunkIdx*chunkSize
	availableInChunk := int64(len(chunkData)) - offsetInChunk

	// Read from the cached chunk
	actualRead := requestLen
	if actualRead > availableInChunk {
		actualRead = availableInChunk
	}
	if actualRead > int64(len(p)) {
		actualRead = int64(len(p))
	}

	copy(p[:actualRead], chunkData[offsetInChunk:offsetInChunk+actualRead])
	cr.pos += actualRead

	return int(actualRead), nil
}

func (cr *CacheReader) Close() error {
	// CacheReader close doesn't need to do anything
	// Cached chunks remain in memory for reuse
	return nil
}

func (cr *CacheReader) getOrCreateFileInfo() *fileInfo {
	key := cacheKey{storageID: utils.SliceToArr(cr.storage.GetID()), path: cr.path}

	globalMu.RLock()
	info, ok := fileInfos[key]
	globalMu.RUnlock()

	if !ok {
		globalMu.Lock()
		info, ok = fileInfos[key]
		if !ok {
			_, size := cr.storage.Metadata(cr.path)
			info = &fileInfo{
				size:    size,
				mutex:   &sync.Mutex{},
				readers: newLRUCache[int64, *readerEntry](maxContinuousReaders),
			}
			fileInfos[key] = info
		}
		globalMu.Unlock()
	}

	return info
}

func (cr *CacheReader) getFileSize() int64 {
	return cr.getOrCreateFileInfo().size
}

func (cr *CacheReader) getFileMutex() *sync.Mutex {
	return cr.getOrCreateFileInfo().mutex
}

func (cr *CacheReader) getOrDownloadChunk(chunkIdx, fileSize int64) []byte {
	key := chunkKey{cacheKey: cacheKey{storageID: utils.SliceToArr(cr.storage.GetID()), path: cr.path}, chunkIdx: chunkIdx}

	// Check if chunk is already cached
	globalMu.RLock()
	chunkData, ok := chunkCache.get(key)
	globalMu.RUnlock()

	if ok {
		return chunkData
	}

	// Need to download this chunk - use or create continuous reader
	chunkData = cr.downloadChunkWithContinuousReader(chunkIdx, fileSize)

	if chunkData != nil {
		// Cache the chunk
		globalMu.Lock()
		chunkCache.put(key, chunkData)
		globalMu.Unlock()
	}

	return chunkData
}

func (cr *CacheReader) downloadChunkWithContinuousReader(chunkIdx, fileSize int64) []byte {
	chunkStart := chunkIdx * chunkSize
	chunkEnd := chunkStart + chunkSize
	if chunkEnd > fileSize {
		chunkEnd = fileSize
	}
	key := cacheKey{storageID: utils.SliceToArr(cr.storage.GetID()), path: cr.path}

	globalMu.RLock()
	info, ok := fileInfos[key]
	globalMu.RUnlock()

	// Check if we can use existing continuous reader
	if ok {
		// Look for a reader at the exact chunk we need
		if entry, found := info.readers.get(chunkIdx); found {
			// Perfect! We can continue reading from where we left off
			chunkLength := chunkEnd - chunkStart
			chunkData := make([]byte, chunkLength)

			_, err := io.ReadFull(entry.reader, chunkData)
			if err == nil {
				// Update next chunk index and keep in cache (already moved to front by get)
				entry.nextChunkIdx += 1
				// Update the key in the cache
				info.readers.moveKey(chunkIdx, entry.nextChunkIdx)
				return chunkData
			}
			// Any error (including EOF) closes the reader and removes it
			info.readers.remove(chunkIdx)
		}
	}

	// Need to create new continuous reader
	// Download from chunk start to end of file for maximum reusability
	downloadLength := fileSize - chunkStart

	reader := cr.storage.DownloadSection(cr.path, chunkStart, downloadLength)

	newEntry := &readerEntry{
		reader:       reader,
		nextChunkIdx: chunkIdx,
	}

	// Get or create fileInfo
	info = cr.getOrCreateFileInfo()

	// Check if nextChunkIdx is already in cache OR if we already have a reader at that position
	globalMu.Lock()
	if _, found := info.readers.get(chunkIdx); found {
		// Close and discard the existing reader (duck typing will handle this)
		info.readers.remove(chunkIdx)
	}
	// Also check for any reader already positioned at this nextChunkIdx
	for nextIdx := range info.readers.items {
		if nextIdx == chunkIdx {
			info.readers.remove(nextIdx)
			break
		}
	}

	// Add new reader to cache
	info.readers.put(newEntry.nextChunkIdx, newEntry)
	globalMu.Unlock()

	// Now read the chunk we need
	chunkLength := chunkEnd - chunkStart
	chunkData := make([]byte, chunkLength)

	n, err := io.ReadFull(newEntry.reader, chunkData)
	if err != nil {
		// Any error (including EOF) closes the reader
		globalMu.Lock()
		info.readers.remove(chunkIdx)
		globalMu.Unlock()
		return nil
	}

	// Update next chunk index in the reader
	globalMu.Lock()
	nextPos := chunkStart + int64(n)
	if nextPos == fileSize {
		// nothing more to read
		info.readers.remove(chunkIdx)
	} else {
		// Update next chunk index (position should always be chunk-aligned now)
		newEntry.nextChunkIdx = nextPos / chunkSize
		info.readers.moveKey(chunkIdx, newEntry.nextChunkIdx)
	}
	globalMu.Unlock()

	return chunkData
}

// CleanupExpiredEntries removes expired entries from the cache
func CleanupExpiredEntries() {
	globalMu.Lock()
	defer globalMu.Unlock()
	chunkCache.cleanup()

	// Also cleanup expired readers
	for _, info := range fileInfos {
		info.readers.cleanup()
	}
}

// ClearCache clears all cache state - used for testing
func ClearCache() {
	globalMu.Lock()
	defer globalMu.Unlock()

	// Close all continuous readers (duck typing will handle this automatically when we clear)
	for _, info := range fileInfos {
		for elem := info.readers.order.Front(); elem != nil; elem = elem.Next() {
			entry := elem.Value.(*cacheEntry[int64, *readerEntry])
			closeIfClosable(entry.value)
		}
	}

	// Clear all maps
	fileInfos = make(map[cacheKey]*fileInfo)
	chunkCache = newLRUCache[chunkKey, []byte](maxCacheSize)
}
