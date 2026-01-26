package vikdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// SSTable represents a Sorted String Table on disk
type SSTable struct {
	filePath string
	file     *os.File
	index    map[string]int64 // key -> offset in data section
	mu       sync.RWMutex
}

// SSTableIndexEntry represents an entry in the SSTable index
type SSTableIndexEntry struct {
	Key    []byte
	Offset int64 // Offset in the data section
}

// SSTableFooter contains metadata about the SSTable
type SSTableFooter struct {
	IndexOffset int64 // Offset where index section starts
	IndexSize   int64 // Size of index section in bytes
	DataSize    int64 // Size of data section in bytes
}

// WriteSSTable writes a MemTable to disk as an SSTable
// Returns the file path of the created SSTable
func WriteSSTable(entries []KeyValue, basePath string, sequenceNum int64) (string, error) {
	if len(entries) == 0 {
		return "", errors.New("cannot write empty SSTable")
	}

	// Ensure entries are sorted (they should be from MemTable, but verify)
	if !sort.IsSorted(KeyValueSlice(entries)) {
		sort.Sort(KeyValueSlice(entries))
	}

	// Create SSTable file path
	filePath := filepath.Join(basePath, formatSSTableName(sequenceNum))
	
	// Ensure directory exists
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return "", err
	}

	file, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	var dataOffset int64 = 0
	indexEntries := make([]SSTableIndexEntry, 0, len(entries))

	// Write data section
	for _, entry := range entries {
		// Record index entry (key -> current offset)
		indexEntries = append(indexEntries, SSTableIndexEntry{
			Key:    entry.Key,
			Offset: dataOffset,
		})

		// Write key length (4 bytes)
		if err := binary.Write(file, binary.BigEndian, uint32(len(entry.Key))); err != nil {
			return "", err
		}
		dataOffset += 4

		// Write key
		if _, err := file.Write(entry.Key); err != nil {
			return "", err
		}
		dataOffset += int64(len(entry.Key))

		// Write value length (4 bytes)
		if err := binary.Write(file, binary.BigEndian, uint32(len(entry.Value))); err != nil {
			return "", err
		}
		dataOffset += 4

		// Write value
		if _, err := file.Write(entry.Value); err != nil {
			return "", err
		}
		dataOffset += int64(len(entry.Value))
	}

	dataSize := dataOffset
	indexOffset := dataOffset

	// Write index section
	for _, idxEntry := range indexEntries {
		// Write key length (4 bytes)
		if err := binary.Write(file, binary.BigEndian, uint32(len(idxEntry.Key))); err != nil {
			return "", err
		}

		// Write key
		if _, err := file.Write(idxEntry.Key); err != nil {
			return "", err
		}

		// Write offset (8 bytes)
		if err := binary.Write(file, binary.BigEndian, idxEntry.Offset); err != nil {
			return "", err
		}
	}

	// Get current position (end of index)
	currentPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}
	indexSize := currentPos - indexOffset

	// Write footer
	footer := SSTableFooter{
		IndexOffset: indexOffset,
		IndexSize:   indexSize,
		DataSize:    dataSize,
	}

	if err := binary.Write(file, binary.BigEndian, footer.IndexOffset); err != nil {
		return "", err
	}
	if err := binary.Write(file, binary.BigEndian, footer.IndexSize); err != nil {
		return "", err
	}
	if err := binary.Write(file, binary.BigEndian, footer.DataSize); err != nil {
		return "", err
	}

	// Sync to disk
	if err := file.Sync(); err != nil {
		return "", err
	}

	return filePath, nil
}

// OpenSSTable opens an existing SSTable file and loads its index
func OpenSSTable(filePath string) (*SSTable, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	sst := &SSTable{
		filePath: filePath,
		file:     file,
		index:    make(map[string]int64),
	}

	// Load index
	if err := sst.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}

	return sst, nil
}

// loadIndex reads the footer and loads the index into memory
func (sst *SSTable) loadIndex() error {
	// Seek to footer (last 24 bytes: 3 int64s)
	footerSize := int64(24) // 3 * 8 bytes
	fileInfo, err := sst.file.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size() < footerSize {
		return errors.New("SSTable file too small")
	}

	// Read footer
	footerOffset := fileInfo.Size() - footerSize
	if _, err := sst.file.Seek(footerOffset, io.SeekStart); err != nil {
		return err
	}

	var footer SSTableFooter
	if err := binary.Read(sst.file, binary.BigEndian, &footer.IndexOffset); err != nil {
		return err
	}
	if err := binary.Read(sst.file, binary.BigEndian, &footer.IndexSize); err != nil {
		return err
	}
	if err := binary.Read(sst.file, binary.BigEndian, &footer.DataSize); err != nil {
		return err
	}

	// Read index section
	if _, err := sst.file.Seek(footer.IndexOffset, io.SeekStart); err != nil {
		return err
	}

	indexEnd := footer.IndexOffset + footer.IndexSize
	for {
		currentPos, err := sst.file.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		if currentPos >= indexEnd {
			break
		}

		// Read key length
		var keyLen uint32
		if err := binary.Read(sst.file, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		// Read key
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(sst.file, key); err != nil {
			return err
		}

		// Read offset
		var offset int64
		if err := binary.Read(sst.file, binary.BigEndian, &offset); err != nil {
			return err
		}

		// Store in index
		sst.index[string(key)] = offset
	}

	return nil
}

// Get retrieves a value by key from the SSTable
func (sst *SSTable) Get(key []byte) ([]byte, bool) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	offset, exists := sst.index[string(key)]
	if !exists {
		return nil, false
	}

	// Seek to data offset
	if _, err := sst.file.Seek(offset, io.SeekStart); err != nil {
		return nil, false
	}

	// Read key length (to skip it)
	var keyLen uint32
	if err := binary.Read(sst.file, binary.BigEndian, &keyLen); err != nil {
		return nil, false
	}

	// Skip key
	if _, err := sst.file.Seek(int64(keyLen), io.SeekCurrent); err != nil {
		return nil, false
	}

	// Read value length
	var valueLen uint32
	if err := binary.Read(sst.file, binary.BigEndian, &valueLen); err != nil {
		return nil, false
	}

	// Read value
	value := make([]byte, valueLen)
	if _, err := io.ReadFull(sst.file, value); err != nil {
		return nil, false
	}

	return value, true
}

// GetRange returns all key-value pairs in the specified range
func (sst *SSTable) GetRange(startKey, endKey []byte) []KeyValue {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	// Get all keys in sorted order
	keys := make([]string, 0, len(sst.index))
	for k := range sst.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]KeyValue, 0)
	for _, keyStr := range keys {
		key := []byte(keyStr)

		// Check if key is in range
		if len(startKey) > 0 && string(key) < string(startKey) {
			continue
		}
		if len(endKey) > 0 && string(key) >= string(endKey) {
			break
		}

		// Read value
		offset := sst.index[keyStr]
		if _, err := sst.file.Seek(offset, io.SeekStart); err != nil {
			continue
		}

		// Read key length and skip key
		var keyLen uint32
		if err := binary.Read(sst.file, binary.BigEndian, &keyLen); err != nil {
			continue
		}
		if _, err := sst.file.Seek(int64(keyLen), io.SeekCurrent); err != nil {
			continue
		}

		// Read value
		var valueLen uint32
		if err := binary.Read(sst.file, binary.BigEndian, &valueLen); err != nil {
			continue
		}
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(sst.file, value); err != nil {
			continue
		}

		result = append(result, KeyValue{
			Key:   key,
			Value: value,
		})
	}

	return result
}

// Close closes the SSTable file
func (sst *SSTable) Close() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	if sst.file != nil {
		err := sst.file.Close()
		sst.file = nil
		return err
	}
	return nil
}

// FilePath returns the path to the SSTable file
func (sst *SSTable) FilePath() string {
	return sst.filePath
}

// KeyValueSlice implements sort.Interface for []KeyValue
type KeyValueSlice []KeyValue

func (s KeyValueSlice) Len() int           { return len(s) }
func (s KeyValueSlice) Less(i, j int) bool { return string(s[i].Key) < string(s[j].Key) }
func (s KeyValueSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// formatSSTableName formats an SSTable filename
func formatSSTableName(sequenceNum int64) string {
	return fmt.Sprintf("sstable-%d.sst", sequenceNum)
}
