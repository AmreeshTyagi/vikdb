package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"vikdb/internal/kv"
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

// KeyValueSlice implements sort.Interface for []kv.KeyValue
type KeyValueSlice []kv.KeyValue

func (s KeyValueSlice) Len() int           { return len(s) }
func (s KeyValueSlice) Less(i, j int) bool { return string(s[i].Key) < string(s[j].Key) }
func (s KeyValueSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// WriteSSTable writes a MemTable to disk as an SSTable
// Returns the file path of the created SSTable
func WriteSSTable(entries []kv.KeyValue, basePath string, sequenceNum int64) (string, error) {
	if len(entries) == 0 {
		return "", errors.New("cannot write empty SSTable")
	}

	if !sort.IsSorted(KeyValueSlice(entries)) {
		sort.Sort(KeyValueSlice(entries))
	}

	filePath := filepath.Join(basePath, formatSSTableName(sequenceNum))

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

	for _, entry := range entries {
		indexEntries = append(indexEntries, SSTableIndexEntry{
			Key:    entry.Key,
			Offset: dataOffset,
		})

		if err := binary.Write(file, binary.BigEndian, uint32(len(entry.Key))); err != nil {
			return "", err
		}
		dataOffset += 4

		if _, err := file.Write(entry.Key); err != nil {
			return "", err
		}
		dataOffset += int64(len(entry.Key))

		if err := binary.Write(file, binary.BigEndian, uint32(len(entry.Value))); err != nil {
			return "", err
		}
		dataOffset += 4

		if _, err := file.Write(entry.Value); err != nil {
			return "", err
		}
		dataOffset += int64(len(entry.Value))
	}

	dataSize := dataOffset
	indexOffset := dataOffset

	for _, idxEntry := range indexEntries {
		if err := binary.Write(file, binary.BigEndian, uint32(len(idxEntry.Key))); err != nil {
			return "", err
		}
		if _, err := file.Write(idxEntry.Key); err != nil {
			return "", err
		}
		if err := binary.Write(file, binary.BigEndian, idxEntry.Offset); err != nil {
			return "", err
		}
	}

	currentPos, err := file.Seek(0, io.SeekCurrent)
	if err != nil {
		return "", err
	}
	indexSize := currentPos - indexOffset

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

	if err := sst.loadIndex(); err != nil {
		file.Close()
		return nil, err
	}

	return sst, nil
}

func (sst *SSTable) loadIndex() error {
	footerSize := int64(24)
	fileInfo, err := sst.file.Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size() < footerSize {
		return errors.New("SSTable file too small")
	}

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

		var keyLen uint32
		if err := binary.Read(sst.file, binary.BigEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(sst.file, key); err != nil {
			return err
		}

		var offset int64
		if err := binary.Read(sst.file, binary.BigEndian, &offset); err != nil {
			return err
		}

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

	if _, err := sst.file.Seek(offset, io.SeekStart); err != nil {
		return nil, false
	}

	var keyLen uint32
	if err := binary.Read(sst.file, binary.BigEndian, &keyLen); err != nil {
		return nil, false
	}

	if _, err := sst.file.Seek(int64(keyLen), io.SeekCurrent); err != nil {
		return nil, false
	}

	var valueLen uint32
	if err := binary.Read(sst.file, binary.BigEndian, &valueLen); err != nil {
		return nil, false
	}

	value := make([]byte, valueLen)
	if _, err := io.ReadFull(sst.file, value); err != nil {
		return nil, false
	}

	return value, true
}

// GetRange returns all key-value pairs in the specified range
func (sst *SSTable) GetRange(startKey, endKey []byte) []kv.KeyValue {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	keys := make([]string, 0, len(sst.index))
	for k := range sst.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	result := make([]kv.KeyValue, 0)
	for _, keyStr := range keys {
		key := []byte(keyStr)

		if len(startKey) > 0 && string(key) < string(startKey) {
			continue
		}
		if len(endKey) > 0 && string(key) >= string(endKey) {
			break
		}

		offset := sst.index[keyStr]
		if _, err := sst.file.Seek(offset, io.SeekStart); err != nil {
			continue
		}

		var keyLen uint32
		if err := binary.Read(sst.file, binary.BigEndian, &keyLen); err != nil {
			continue
		}
		if _, err := sst.file.Seek(int64(keyLen), io.SeekCurrent); err != nil {
			continue
		}

		var valueLen uint32
		if err := binary.Read(sst.file, binary.BigEndian, &valueLen); err != nil {
			continue
		}
		value := make([]byte, valueLen)
		if _, err := io.ReadFull(sst.file, value); err != nil {
			continue
		}

		result = append(result, kv.KeyValue{
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

func formatSSTableName(sequenceNum int64) string {
	return fmt.Sprintf("sstable-%d.sst", sequenceNum)
}
