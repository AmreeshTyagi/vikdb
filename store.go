package vikdb

import (
	"errors"
	"os"
	"path/filepath"
)

// Store represents the main KV store that combines MemTable and WAL
type Store struct {
	memTable *MemTable
	wal      *WAL
	walPath  string
}

// NewStore creates a new Store instance
// It will create/recover from WAL if walPath is provided
func NewStore(memTableMaxSize int64, walPath string) (*Store, error) {
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

	return &Store{
		memTable: memTable,
		wal:      wal,
		walPath:  walPath,
	}, nil
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
func (s *Store) Get(key []byte) ([]byte, bool) {
	return s.memTable.Get(key)
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
func (s *Store) GetRange(startKey, endKey []byte) []KeyValue {
	return s.memTable.GetRange(startKey, endKey)
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

// FlushMemTable clears the MemTable and truncates the WAL
// This should be called after successfully writing MemTable to SSTable
func (s *Store) FlushMemTable() error {
	s.memTable.Clear()

	if s.wal != nil {
		return s.wal.Truncate()
	}
	return nil
}

// Close closes the WAL file
func (s *Store) Close() error {
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
