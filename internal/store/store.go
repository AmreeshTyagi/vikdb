package store

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"vikdb/internal/compactor"
	"vikdb/internal/kv"
	"vikdb/internal/memtable"
	"vikdb/internal/sstable"
	"vikdb/internal/wal"
)

// Store represents the main KV store that combines MemTable and WAL
type Store struct {
	memTable    *memtable.MemTable
	wal         *wal.WAL
	walPath     string
	sstableDir  string
	sstables    []*sstable.SSTable
	sequenceNum int64
	compactor   *compactor.Compactor
	maxSSTables int
	mu          sync.RWMutex
}

// NewStore creates a new Store instance
func NewStore(memTableMaxSize int64, walPath string, sstableDir string) (*Store, error) {
	return NewStoreWithCompaction(memTableMaxSize, walPath, sstableDir, 5)
}

// NewStoreWithCompaction creates a new Store instance with custom compaction settings
func NewStoreWithCompaction(memTableMaxSize int64, walPath string, sstableDir string, maxSSTables int) (*Store, error) {
	memTable := memtable.NewMemTable(memTableMaxSize)

	var w *wal.WAL
	var err error

	if walPath != "" {
		dir := filepath.Dir(walPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}

		w, err = wal.NewWAL(walPath)
		if err != nil {
			return nil, err
		}

		if err := w.Replay(func(entry wal.WALEntry) error {
			return applyWALEntry(memTable, entry)
		}); err != nil {
			w.Close()
			return nil, err
		}
	}

	s := &Store{
		memTable:    memTable,
		wal:         w,
		walPath:     walPath,
		sstableDir:  sstableDir,
		sstables:    make([]*sstable.SSTable, 0),
		sequenceNum: 1,
		maxSSTables: maxSSTables,
		compactor:   compactor.NewCompactor(sstableDir, maxSSTables),
	}

	if sstableDir != "" {
		if err := s.loadSSTables(); err != nil {
			s.Close()
			return nil, err
		}
	}

	return s, nil
}

func (s *Store) loadSSTables() error {
	if s.sstableDir == "" {
		return nil
	}

	if err := os.MkdirAll(s.sstableDir, 0755); err != nil {
		return err
	}

	files, err := filepath.Glob(filepath.Join(s.sstableDir, "*.sst"))
	if err != nil {
		return err
	}

	for _, file := range files {
		sst, err := sstable.OpenSSTable(file)
		if err != nil {
			return err
		}
		s.sstables = append(s.sstables, sst)
	}

	return nil
}

func applyWALEntry(memTable *memtable.MemTable, entry wal.WALEntry) error {
	switch entry.Type {
	case wal.WALEntryPut:
		return memTable.Put(entry.Key, entry.Value)
	case wal.WALEntryDelete:
		return memTable.Delete(entry.Key)
	default:
		return errors.New("unknown WAL entry type")
	}
}

// Put inserts or updates a key-value pair
func (s *Store) Put(key, value []byte) error {
	if s.wal != nil {
		entry := wal.WALEntry{
			Type:  wal.WALEntryPut,
			Key:   key,
			Value: value,
		}
		if err := s.wal.Append(entry); err != nil {
			return err
		}
	}

	if err := s.memTable.Put(key, value); err != nil {
		return err
	}

	if s.ShouldFlush() {
		if err := s.FlushMemTable(); err != nil {
			return err
		}
	}

	return nil
}

// Read retrieves a value by key
func (s *Store) Read(key []byte) ([]byte, bool) {
	value, exists := s.memTable.Get(key)
	if exists {
		return value, true
	}

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
func (s *Store) Delete(key []byte) error {
	if s.wal != nil {
		entry := wal.WALEntry{
			Type:  wal.WALEntryDelete,
			Key:   key,
			Value: nil,
		}
		if err := s.wal.Append(entry); err != nil {
			return err
		}
	}

	return s.memTable.Delete(key)
}

// ReadKeyRange returns all key-value pairs in the specified range
func (s *Store) ReadKeyRange(startKey, endKey []byte) []kv.KeyValue {
	result := make([]kv.KeyValue, 0)

	memTableResults := s.memTable.GetRange(startKey, endKey)
	result = append(result, memTableResults...)

	s.mu.RLock()
	sstablesCopy := make([]*sstable.SSTable, len(s.sstables))
	copy(sstablesCopy, s.sstables)
	s.mu.RUnlock()

	for _, sst := range sstablesCopy {
		sstResults := sst.GetRange(startKey, endKey)
		result = append(result, sstResults...)
	}

	return deduplicateAndSort(result)
}

func deduplicateAndSort(entries []kv.KeyValue) []kv.KeyValue {
	if len(entries) == 0 {
		return entries
	}

	seen := make(map[string]bool)
	unique := make([]kv.KeyValue, 0)

	for _, entry := range entries {
		keyStr := string(entry.Key)
		if !seen[keyStr] {
			seen[keyStr] = true
			unique = append(unique, entry)
		}
	}

	sort.Sort(sstable.KeyValueSlice(unique))
	return unique
}

// BatchPut performs a batch put operation
func (s *Store) BatchPut(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return errors.New("keys and values must have the same length")
	}

	if s.wal != nil {
		for i := range keys {
			entry := wal.WALEntry{
				Type:  wal.WALEntryPut,
				Key:   keys[i],
				Value: values[i],
			}
			if err := s.wal.Append(entry); err != nil {
				return err
			}
		}
	}

	for i := range keys {
		if err := s.memTable.Put(keys[i], values[i]); err != nil {
			return err
		}
	}

	if s.ShouldFlush() {
		if err := s.FlushMemTable(); err != nil {
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
	entries := s.memTable.GetAllEntries()
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	filePath, err := sstable.WriteSSTable(entries, s.sstableDir, s.sequenceNum)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	sst, err := sstable.OpenSSTable(filePath)
	if err != nil {
		s.mu.Unlock()
		return err
	}

	s.sstables = append(s.sstables, sst)
	s.sequenceNum++
	s.mu.Unlock()

	s.memTable.Clear()
	if s.wal != nil {
		if err := s.wal.Truncate(); err != nil {
			return err
		}
	}

	s.mu.RLock()
	sstableCount := len(s.sstables)
	s.mu.RUnlock()

	if s.compactor.ShouldCompact(sstableCount) {
		if err := s.Compact(); err != nil {
			return err
		}
	}

	return nil
}

// Compact performs compaction on the Store's SSTables
func (s *Store) Compact() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.sstables) < 2 {
		return nil
	}

	mergedPath, err := s.compactor.Compact(s.sstables, s.sequenceNum)
	if err != nil {
		return err
	}

	mergedSST, err := sstable.OpenSSTable(mergedPath)
	if err != nil {
		return err
	}

	s.sstables = []*sstable.SSTable{mergedSST}
	s.sequenceNum++

	return nil
}

// Close closes the WAL file and all SSTables
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, sst := range s.sstables {
		if err := sst.Close(); err != nil {
			return err
		}
	}

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

// ApplyWALEntry applies a WAL entry to the MemTable only (no WAL write).
// Used when applying replicated log entries from Raft.
func (s *Store) ApplyWALEntry(entry wal.WALEntry) error {
	switch entry.Type {
	case wal.WALEntryPut:
		return s.memTable.Put(entry.Key, entry.Value)
	case wal.WALEntryDelete:
		return s.memTable.Delete(entry.Key)
	default:
		return errors.New("unknown WAL entry type")
	}
}

// AppendToWAL appends a WAL entry to the WAL file only (no MemTable).
// Used by ReplicatedStore when persisting replicated entries locally.
func (s *Store) AppendToWAL(entry wal.WALEntry) error {
	if s.wal == nil {
		return nil
	}
	return s.wal.Append(entry)
}
