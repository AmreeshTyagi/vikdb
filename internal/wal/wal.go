package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// WALEntryType represents the type of operation
type WALEntryType uint8

const (
	WALEntryPut    WALEntryType = 1
	WALEntryDelete WALEntryType = 2
)

// WALEntry represents a single entry in the WAL
type WALEntry struct {
	Type  WALEntryType
	Key   []byte
	Value []byte // Empty for delete operations
}

// WAL represents a Write-Ahead Log for crash recovery
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	filePath string
}

// NewWAL creates a new WAL instance. If the file exists, it is opened in append mode.
// Time: O(1) open. Space: O(1).
func NewWAL(filePath string) (*WAL, error) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:     file,
		filePath: filePath,
	}, nil
}

// Close closes the WAL file. Time: O(1). Space: O(1).
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file != nil {
		err := w.file.Close()
		w.file = nil
		return err
	}
	return nil
}

// Append writes an entry to the WAL and syncs to disk. Ensures durability before applying to MemTable.
// Time: O(len(key)+len(value)) write + sync. Space: O(1).
func (w *WAL) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL file is closed")
	}

	// Write entry type (1 byte)
	if err := binary.Write(w.file, binary.BigEndian, entry.Type); err != nil {
		return err
	}

	// Write key length (4 bytes) and key
	keyLen := uint32(len(entry.Key))
	if err := binary.Write(w.file, binary.BigEndian, keyLen); err != nil {
		return err
	}
	if _, err := w.file.Write(entry.Key); err != nil {
		return err
	}

	// Write value length (4 bytes) and value
	valueLen := uint32(len(entry.Value))
	if err := binary.Write(w.file, binary.BigEndian, valueLen); err != nil {
		return err
	}
	if _, err := w.file.Write(entry.Value); err != nil {
		return err
	}

	// Sync to disk to ensure durability
	return w.file.Sync()
}

// Replay reads all entries from the WAL and calls the callback for each. Used at startup to recover MemTable state.
// Time: O(file size) reads + O(N)·cost(callback); N = entry count. Space: O(1) plus callback allocations.
func (w *WAL) Replay(callback func(WALEntry) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL file is closed")
	}

	// Seek to the beginning of the file
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {
		entry, err := w.readEntry()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if err := callback(entry); err != nil {
			return err
		}
	}

	return nil
}

// readEntry reads a single entry from the current file position.
// Time: O(len(key)+len(value)) read. Space: O(len(key)+len(value)).
func (w *WAL) readEntry() (WALEntry, error) {
	var entry WALEntry

	// Read entry type
	var entryType uint8
	if err := binary.Read(w.file, binary.BigEndian, &entryType); err != nil {
		return entry, err
	}
	entry.Type = WALEntryType(entryType)

	// Read key length and key
	var keyLen uint32
	if err := binary.Read(w.file, binary.BigEndian, &keyLen); err != nil {
		return entry, err
	}
	entry.Key = make([]byte, keyLen)
	if _, err := io.ReadFull(w.file, entry.Key); err != nil {
		return entry, err
	}

	// Read value length and value
	var valueLen uint32
	if err := binary.Read(w.file, binary.BigEndian, &valueLen); err != nil {
		return entry, err
	}
	entry.Value = make([]byte, valueLen)
	if _, err := io.ReadFull(w.file, entry.Value); err != nil {
		return entry, err
	}

	return entry, nil
}

// Truncate clears the WAL file. Call after successfully flushing the MemTable to disk.
// Time: O(1) truncate + sync. Space: O(1).
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return errors.New("WAL file is closed")
	}

	// Truncate the file to size 0
	if err := w.file.Truncate(0); err != nil {
		return err
	}

	// Seek to the beginning
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	// Sync to ensure the truncation is persisted
	return w.file.Sync()
}

// Size returns the current size of the WAL file in bytes. Time: O(1) stat. Space: O(1).
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return 0, errors.New("WAL file is closed")
	}

	stat, err := w.file.Stat()
	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

// FilePath returns the path to the WAL file
func (w *WAL) FilePath() string {
	return w.filePath
}
