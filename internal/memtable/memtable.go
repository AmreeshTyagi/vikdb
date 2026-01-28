package memtable

import (
	"bytes"
	"sort"
	"sync"

	"vikdb/internal/kv"
)

// MemTable is an in-memory sorted key-value store
// It maintains entries in sorted order by key for efficient range queries
type MemTable struct {
	mu      sync.RWMutex
	entries []kv.KeyValue
	size    int64 // Total size in bytes (keys + values)
	maxSize int64 // Maximum size before flush is needed
}

// NewMemTable creates a new MemTable with the specified maximum size
// When the MemTable exceeds maxSize, it should be flushed to disk
func NewMemTable(maxSize int64) *MemTable {
	return &MemTable{
		entries: make([]kv.KeyValue, 0),
		size:    0,
		maxSize: maxSize,
	}
}

// Put inserts or updates a key-value pair
func (mt *MemTable) Put(key, value []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	// Create copies to avoid external modifications
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	valueCopy := make([]byte, len(value))
	copy(valueCopy, value)

	// Find the position where this key should be inserted
	idx := mt.findIndex(keyCopy)

	var oldValueSize int64
	if idx < len(mt.entries) && bytes.Equal(mt.entries[idx].Key, keyCopy) {
		// Key exists, update the value
		oldValueSize = int64(len(mt.entries[idx].Value))
		mt.entries[idx].Value = valueCopy
	} else {
		// New key, insert it
		oldValueSize = 0
		// Insert at the correct position to maintain sorted order
		mt.entries = append(mt.entries, kv.KeyValue{})
		copy(mt.entries[idx+1:], mt.entries[idx:])
		mt.entries[idx] = kv.KeyValue{Key: keyCopy, Value: valueCopy}
	}

	// Update size
	mt.size += int64(len(valueCopy)) - oldValueSize
	// Key size is already accounted for (or will be added)
	if oldValueSize == 0 {
		mt.size += int64(len(keyCopy))
	}

	return nil
}

// Get retrieves the value for a given key
// Returns nil if the key doesn't exist
func (mt *MemTable) Get(key []byte) ([]byte, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	idx := mt.findIndex(key)
	if idx < len(mt.entries) && bytes.Equal(mt.entries[idx].Key, key) {
		// Return a copy to prevent external modifications
		value := make([]byte, len(mt.entries[idx].Value))
		copy(value, mt.entries[idx].Value)
		return value, true
	}
	return nil, false
}

// Delete marks a key as deleted (tombstone)
// In LSM-trees, deletes are handled by inserting a tombstone marker
// For now, we'll actually remove it from the MemTable
func (mt *MemTable) Delete(key []byte) error {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	idx := mt.findIndex(key)
	if idx < len(mt.entries) && bytes.Equal(mt.entries[idx].Key, key) {
		// Remove the entry
		oldEntry := mt.entries[idx]
		mt.entries = append(mt.entries[:idx], mt.entries[idx+1:]...)

		// Update size
		mt.size -= int64(len(oldEntry.Key) + len(oldEntry.Value))
	}
	return nil
}

// GetRange returns all key-value pairs where startKey <= key < endKey
// If endKey is nil, it returns all keys >= startKey
func (mt *MemTable) GetRange(startKey, endKey []byte) []kv.KeyValue {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	if len(mt.entries) == 0 {
		return []kv.KeyValue{}
	}

	// Find the starting index
	startIdx := mt.findIndex(startKey)

	// If startIdx is beyond all entries, return empty
	if startIdx >= len(mt.entries) {
		return []kv.KeyValue{}
	}

	// Verify that the key at startIdx is actually >= startKey
	if bytes.Compare(mt.entries[startIdx].Key, startKey) < 0 {
		return []kv.KeyValue{}
	}

	// Find the ending index
	var endIdx int
	if endKey == nil {
		endIdx = len(mt.entries)
	} else {
		endIdx = mt.findIndex(endKey)
		if endIdx < len(mt.entries) && bytes.Equal(mt.entries[endIdx].Key, endKey) {
			endIdx++
		}
	}

	// Extract the range
	result := make([]kv.KeyValue, 0, endIdx-startIdx)
	for i := startIdx; i < endIdx && i < len(mt.entries); i++ {
		key := mt.entries[i].Key

		if bytes.Compare(key, startKey) < 0 {
			continue
		}
		if endKey != nil && bytes.Compare(key, endKey) >= 0 {
			break
		}

		entry := kv.KeyValue{
			Key:   make([]byte, len(key)),
			Value: make([]byte, len(mt.entries[i].Value)),
		}
		copy(entry.Key, key)
		copy(entry.Value, mt.entries[i].Value)
		result = append(result, entry)
	}

	return result
}

// Size returns the current size of the MemTable in bytes
func (mt *MemTable) Size() int64 {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// ShouldFlush returns true if the MemTable should be flushed to disk
func (mt *MemTable) ShouldFlush() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size >= mt.maxSize
}

// IsEmpty returns true if the MemTable has no entries
func (mt *MemTable) IsEmpty() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return len(mt.entries) == 0
}

// GetAllEntries returns all entries in sorted order
// This is used when flushing the MemTable to disk
func (mt *MemTable) GetAllEntries() []kv.KeyValue {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	result := make([]kv.KeyValue, len(mt.entries))
	for i, entry := range mt.entries {
		result[i] = kv.KeyValue{
			Key:   make([]byte, len(entry.Key)),
			Value: make([]byte, len(entry.Value)),
		}
		copy(result[i].Key, entry.Key)
		copy(result[i].Value, entry.Value)
	}
	return result
}

// Clear removes all entries from the MemTable
// This is called after flushing to disk
func (mt *MemTable) Clear() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.entries = make([]kv.KeyValue, 0)
	mt.size = 0
}

// findIndex finds the index where a key should be inserted or exists
func (mt *MemTable) findIndex(key []byte) int {
	return sort.Search(len(mt.entries), func(i int) bool {
		return bytes.Compare(mt.entries[i].Key, key) >= 0
	})
}
