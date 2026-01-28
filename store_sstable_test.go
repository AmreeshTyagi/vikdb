package vikdb

import (
	"path/filepath"
	"testing"
)

func TestStore_FlushToSSTable(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	store, err := NewStore(100, walPath, sstableDir) // Small max size to trigger flush
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Add data that exceeds max size
	store.Put([]byte("key1"), make([]byte, 50))
	store.Put([]byte("key2"), make([]byte, 60))

	// Flush MemTable
	if err := store.FlushMemTable(); err != nil {
		t.Fatalf("Failed to flush MemTable: %v", err)
	}

	// Verify SSTable was created
	if store.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable, got %d", store.GetSSTableCount())
	}

	// Verify MemTable is empty
	if store.GetMemTableSize() != 0 {
		t.Fatal("MemTable should be empty after flush")
	}

	// Verify we can still read from SSTable
	value, exists := store.Read([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist in SSTable")
	}
	if len(value) != 50 {
		t.Fatalf("Expected value length 50, got %d", len(value))
	}
}

func TestStore_GetFromSSTable(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	store, err := NewStore(100, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Add and flush data
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key2"), []byte("value2"))
	store.FlushMemTable()

	// Add new data to MemTable
	store.Put([]byte("key3"), []byte("value3"))

	// Get from MemTable
	value, exists := store.Read([]byte("key3"))
	if !exists || string(value) != "value3" {
		t.Fatal("key3 should be in MemTable")
	}

	// Get from SSTable
	value, exists = store.Read([]byte("key1"))
	if !exists || string(value) != "value1" {
		t.Fatal("key1 should be in SSTable")
	}
}

func TestStore_GetRangeWithSSTables(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	store, err := NewStore(100, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Add and flush first batch
	store.Put([]byte("key1"), []byte("value1"))
	store.Put([]byte("key3"), []byte("value3"))
	store.FlushMemTable()

	// Add and flush second batch
	store.Put([]byte("key2"), []byte("value2"))
	store.Put([]byte("key4"), []byte("value4"))
	store.FlushMemTable()

	// Get range across SSTables
	results := store.ReadKeyRange([]byte("key1"), []byte("key4"))
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
}

func TestCompactor_Compact(t *testing.T) {
	tmpDir := t.TempDir()
	sstableDir := filepath.Join(tmpDir, "sstables")

	// Create multiple SSTables
	entries1 := []KeyValue{
		{Key: []byte("key1"), Value: []byte("value1_old")},
		{Key: []byte("key2"), Value: []byte("value2")},
	}
	path1, _ := WriteSSTable(entries1, sstableDir, 1)
	sst1, _ := OpenSSTable(path1)

	entries2 := []KeyValue{
		{Key: []byte("key1"), Value: []byte("value1_new")}, // Overwrites key1
		{Key: []byte("key3"), Value: []byte("value3")},
	}
	path2, _ := WriteSSTable(entries2, sstableDir, 2)
	sst2, _ := OpenSSTable(path2)

	// Compact
	compactor := NewCompactor(sstableDir, 2)
	mergedPath, err := compactor.Compact([]*SSTable{sst1, sst2}, 3)
	if err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// Verify merged SSTable
	merged, err := OpenSSTable(mergedPath)
	if err != nil {
		t.Fatalf("Failed to open merged SSTable: %v", err)
	}
	defer merged.Close()

	// Verify key1 has new value
	value, exists := merged.Get([]byte("key1"))
	if !exists || string(value) != "value1_new" {
		t.Fatalf("Expected value1_new, got %s", value)
	}

	// Verify all keys exist
	value, exists = merged.Get([]byte("key2"))
	if !exists || string(value) != "value2" {
		t.Fatal("key2 should exist")
	}

	value, exists = merged.Get([]byte("key3"))
	if !exists || string(value) != "value3" {
		t.Fatal("key3 should exist")
	}
}

func TestCompactStore(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	store, err := NewStore(50, walPath, sstableDir) // Small max size
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Create multiple SSTables by flushing multiple times
	store.Put([]byte("key1"), []byte("value1"))
	store.FlushMemTable()

	store.Put([]byte("key2"), []byte("value2"))
	store.FlushMemTable()

	store.Put([]byte("key3"), []byte("value3"))
	store.FlushMemTable()

	// Verify we have 3 SSTables
	if store.GetSSTableCount() != 3 {
		t.Fatalf("Expected 3 SSTables, got %d", store.GetSSTableCount())
	}

	// Compact
	if err := store.Compact(); err != nil {
		t.Fatalf("Failed to compact: %v", err)
	}

	// Verify we now have 1 SSTable
	if store.GetSSTableCount() != 1 {
		t.Fatalf("Expected 1 SSTable after compaction, got %d", store.GetSSTableCount())
	}

	// Verify all keys are still accessible
	value, exists := store.Read([]byte("key1"))
	if !exists || string(value) != "value1" {
		t.Fatal("key1 should exist after compaction")
	}

	value, exists = store.Read([]byte("key2"))
	if !exists || string(value) != "value2" {
		t.Fatal("key2 should exist after compaction")
	}

	value, exists = store.Read([]byte("key3"))
	if !exists || string(value) != "value3" {
		t.Fatal("key3 should exist after compaction")
	}
}
