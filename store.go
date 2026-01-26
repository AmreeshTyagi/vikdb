package vikdb

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Store represents the main KV store that combines MemTable and WAL
type Store struct {
	memTable    *MemTable
	wal         *WAL
	walPath     string
	sstableDir  string
	sstables    []*SSTable
	sequenceNum int64
	mu          sync.RWMutex
}

// NewStore creates a new Store instance
// It will create/recover from WAL if walPath is provided
func NewStore(memTableMaxSize int64, walPath string, sstableDir string) (*Store, error) {
	memTable := NewMemTable(memTableMaxSize)

	var wal *WAL
	var err error

	if walPath != "" {
		// Ensure directory exists
		dir := filepath.Dir(walPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}

		wal, err = NewWAL(walPath)
		if err != nil {
			return nil, err
		}

		// Replay WAL to recover MemTable state
		if err := wal.Replay(func(entry WALEntry) error {
			return applyWALEntry(memTable, entry)
		}); err != nil {
			wal.Close()
			return nil, err
		}
	}

	store := &Store{
		memTable:    memTable,
		wal:         wal,
		walPath:     walPath,
		sstableDir:  sstableDir,
		sstables:    make([]*SSTable, 0),
		sequenceNum: 1,
	}

	// Load existing SSTables
	if sstableDir != "" {
		if err := store.loadSSTables(); err != nil {
			store.Close()
			return nil, err
		}
	}

	return store, nil
}

// loadSSTables loads all existing SSTables from disk
func (s *Store) loadSSTables() error {
	if s.sstableDir == "" {
		return nil
	}

	// Ensure directory exists
	if err := os.MkdirAll(s.sstableDir, 0755); err != nil {
		return err
	}

	// List all .sst files
	files, err := filepath.Glob(filepath.Join(s.sstableDir, "*.sst"))
	if err != nil {
		return err
	}

	// Open each SSTable
	for _, file := range files {
		sst, err := OpenSSTable(file)
		if err != nil {
			return err
		}
		s.sstables = append(s.sstables, sst)
	}

	return nil
}

// applyWALEntry applies a WAL entry to the MemTable
func applyWALEntry(memTable *MemTable, entry WALEntry) error {
	switch entry.Type {
	case WALEntryPut:
		return memTable.Put(entry.Key, entry.Value)
	case WALEntryDelete:
		return memTable.Delete(entry.Key)
	default:
		return errors.New("unknown WAL entry type")
	}
}

// Put inserts or updates a key-value pair
// It writes to WAL first, then applies to MemTable
func (s *Store) Put(key, value []byte) error {
	// Write to WAL first (for durability)
	if s.wal != nil {
		entry := WALEntry{
			Type:  WALEntryPut,
			Key:   key,
			Value: value,
		}
		if err := s.wal.Append(entry); err != nil {
			return err
		}
	}

	// Apply to MemTable
	return s.memTable.Put(key, value)
}

// Get retrieves a value by key
// Checks MemTable first, then SSTables (newest to oldest)
func (s *Store) Get(key []byte) ([]byte, bool) {
	// Check MemTable first
	value, exists := s.memTable.Get(key)
	if exists {
		return value, true
	}

	// Check SSTables (newest to oldest)
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i := len(s.sstables) - 1; i >= 0; i-- {
		value, exists := s.sstables[i].Get(key)
		if exists {
			return value, true
		}
	}

	return nil, false
}

// Delete removes a key
// It writes to WAL first, then applies to MemTable
func (s *Store) Delete(key []byte) error {
	// Write to WAL first (for durability)
	if s.wal != nil {
		entry := WALEntry{
			Type:  WALEntryDelete,
			Key:   key,
			Value: nil,
		}
		if err := s.wal.Append(entry); err != nil {
			return err
		}
	}

	// Apply to MemTable
	return s.memTable.Delete(key)
}

// GetRange returns all key-value pairs in the specified range
// Checks MemTable and all SSTables
func (s *Store) GetRange(startKey, endKey []byte) []KeyValue {
	result := make([]KeyValue, 0)

	// Get from MemTable
	memTableResults := s.memTable.GetRange(startKey, endKey)
	result = append(result, memTableResults...)

	// Get from SSTables
	s.mu.RLock()
	sstablesCopy := make([]*SSTable, len(s.sstables))
	copy(sstablesCopy, s.sstables)
	s.mu.RUnlock()

	for _, sst := range sstablesCopy {
		sstResults := sst.GetRange(startKey, endKey)
		result = append(result, sstResults...)
	}

	// Sort and deduplicate (MemTable entries take precedence)
	return deduplicateAndSort(result)
}

// deduplicateAndSort removes duplicates (keeping first occurrence) and sorts
func deduplicateAndSort(entries []KeyValue) []KeyValue {
	if len(entries) == 0 {
		return entries
	}

	seen := make(map[string]bool)
	unique := make([]KeyValue, 0)

	// First pass: collect unique entries (first occurrence wins)
	for _, entry := range entries {
		keyStr := string(entry.Key)
		if !seen[keyStr] {
			seen[keyStr] = true
			unique = append(unique, entry)
		}
	}

	// Sort
	sort.Sort(KeyValueSlice(unique))
	return unique
}

// BatchPut performs a batch put operation
// All operations are logged to WAL before being applied
func (s *Store) BatchPut(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.New("keys and values must have the same length")
	}

	// Write all entries to WAL first
	if s.wal != nil {
		for i := range keys {
			entry := WALEntry{
				Type:  WALEntryPut,
				Key:   keys[i],
				Value: values[i],
			}
			if err := s.wal.Append(entry); err != nil {
				return err
			}
		}
	}

	// Apply all to MemTable
	for i := range keys {
		if err := s.memTable.Put(keys[i], values[i]); err != nil {
			return err
		}
	}

	return nil
}

// ShouldFlush returns true if the MemTable should be flushed to disk
func (s *Store) ShouldFlush() bool {
	return s.memTable.ShouldFlush()
}

// FlushMemTable writes the MemTable to an SSTable and clears it
func (s *Store) FlushMemTable() error {
	// Get all entries from MemTable
	entries := s.memTable.GetAllEntries()
	if len(entries) == 0 {
		return nil // Nothing to flush
	}

	// Write to SSTable
	s.mu.Lock()
	filePath, err := WriteSSTable(entries, s.sstableDir, s.sequenceNum)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	// Open the new SSTable
	sst, err := OpenSSTable(filePath)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	// Add to SSTables list
	s.sstables = append(s.sstables, sst)
	s.sequenceNum++
	s.mu.Unlock()

	// Clear MemTable and truncate WAL
	s.memTable.Clear()
	if s.wal != nil {
		return s.wal.Truncate()
	}

	return nil
}

// Close closes the WAL file and all SSTables
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Close all SSTables
	for _, sst := range s.sstables {
		if err := sst.Close(); err != nil {
			return err
		}
	}

	// Close WAL
	if s.wal != nil {
		return s.wal.Close()
	}
	return nil
}

// GetMemTableSize returns the current size of the MemTable
func (s *Store) GetMemTableSize() int64 {
	return s.memTable.Size()
}

// GetWALSize returns the current size of the WAL file
func (s *Store) GetWALSize() (int64, error) {
	if s.wal == nil {
		return 0, nil
	}
	return s.wal.Size()
}

// GetSSTableCount returns the number of SSTables
func (s *Store) GetSSTableCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sstables)
}
