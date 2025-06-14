package cache

import (
	"container/list"
	"time"
)

type cacheEntry[K comparable, V any] struct {
	key        K
	value      V
	lastAccess time.Time
}

type lruCache[K comparable, V any] struct {
	items   map[K]*list.Element // O(1) lookup
	order   *list.List          // doubly-linked list for LRU
	maxSize int
}

func newLRUCache[K comparable, V any](maxSize int) *lruCache[K, V] {
	return &lruCache[K, V]{
		items:   make(map[K]*list.Element),
		order:   list.New(),
		maxSize: maxSize,
	}
}

func (c *lruCache[K, V]) get(key K) (V, bool) {
	var zero V
	elem, ok := c.items[key]
	if !ok {
		return zero, false
	}
	entry := elem.Value.(*cacheEntry[K, V])
	// Move to front (most recently used)
	c.order.MoveToFront(elem)
	entry.lastAccess = time.Now()
	return entry.value, true
}

func (c *lruCache[K, V]) put(key K, value V) {
	if elem, ok := c.items[key]; ok {
		// Update existing entry - close old value if it implements Close
		entry := elem.Value.(*cacheEntry[K, V])
		closeIfClosable(entry.value)
		entry.value = value
		entry.lastAccess = time.Now()
		c.order.MoveToFront(elem)
		return
	}

	// Add new entry
	entry := &cacheEntry[K, V]{
		key:        key,
		value:      value,
		lastAccess: time.Now(),
	}

	elem := c.order.PushFront(entry)
	c.items[key] = elem

	// Evict if over capacity
	if len(c.items) > c.maxSize {
		oldest := c.order.Back()
		if oldest != nil {
			oldEntry := oldest.Value.(*cacheEntry[K, V])
			// Close evicted value if it implements Close
			closeIfClosable(oldEntry.value)
			c.order.Remove(oldest)
			delete(c.items, oldEntry.key)
		}
	}
}

func (c *lruCache[K, V]) remove(key K) {
	if elem, ok := c.items[key]; ok {
		entry := elem.Value.(*cacheEntry[K, V])
		// Close removed value if it implements Close
		closeIfClosable(entry.value)
		c.order.Remove(elem)
		delete(c.items, key)
	}
}

func (c *lruCache[K, V]) moveKey(oldKey K, newKey K) bool {
	elem, ok := c.items[oldKey]
	if !ok {
		return false
	}

	// Check if newKey already exists
	if _, exists := c.items[newKey]; exists {
		return false
	}

	// Update the entry's key and move to new map key
	entry := elem.Value.(*cacheEntry[K, V])
	entry.key = newKey
	entry.lastAccess = time.Now()

	// Update map
	delete(c.items, oldKey)
	c.items[newKey] = elem

	// Move to front (most recently used)
	c.order.MoveToFront(elem)

	return true
}

func (c *lruCache[K, V]) cleanup() {
	now := time.Now()
	for elem := c.order.Back(); elem != nil; {
		entry := elem.Value.(*cacheEntry[K, V])
		if now.Sub(entry.lastAccess) > cacheExpiration {
			prev := elem.Prev()
			// Close expired value if it implements Close
			closeIfClosable(entry.value)
			c.order.Remove(elem)
			delete(c.items, entry.key)
			elem = prev
		} else {
			break // Since we're going from back to front, newer entries won't be expired
		}
	}
}

func closeIfClosable[V any](value V) {
	if closer, ok := any(value).(interface{ Close() error }); ok {
		closer.Close()
	}
}
