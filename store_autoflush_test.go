package vikdb

import (
	"path/filepath"
	"testing"
)

func TestStore_AutoFlushOnPut(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	// Create store with small max size to trigger flush easily
	store, err := NewStore(100, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Put data that exceeds max size - should trigger automatic flush
	store.Put([]byte("key1"), make([]byte, 50))
	store.Put([]byte("key2"), make([]byte, 60))

	// Verify MemTable was automatically flushed
	if store.GetMemTableSize() > 0 {
		t.Fatalf("MemTable should be empty after auto-flush, got size %d", store.GetMemTableSize())
	}

	// Verify SSTable was created
	if store.GetSSTableCount() == 0 {
		t.Fatal("SSTable should be created after auto-flush")
	}

	// Verify we can still read the data from SSTable
	value, exists := store.Read([]byte("key1"))
	if !exists {
		t.Fatal("key1 should exist in SSTable after flush")
	}
	if len(value) != 50 {
		t.Fatalf("Expected value length 50, got %d", len(value))
	}
}

func TestStore_AutoFlushOnBatchPut(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	// Create store with small max size
	store, err := NewStore(100, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Batch put data that exceeds max size
	keys := [][]byte{[]byte("key1"), []byte("key2")}
	values := [][]byte{make([]byte, 50), make([]byte, 60)}

	if err := store.BatchPut(keys, values); err != nil {
		t.Fatalf("Failed to batch put: %v", err)
	}

	// Verify MemTable was automatically flushed
	if store.GetMemTableSize() > 0 {
		t.Fatalf("MemTable should be empty after auto-flush, got size %d", store.GetMemTableSize())
	}

	// Verify SSTable was created
	if store.GetSSTableCount() == 0 {
		t.Fatal("SSTable should be created after auto-flush")
	}
}

func TestStore_NoFlushWhenNotFull(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstableDir := filepath.Join(tmpDir, "sstables")

	// Create store with large max size
	store, err := NewStore(1024*1024, walPath, sstableDir)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()

	// Put small amount of data - should NOT trigger flush
	store.Put([]byte("key1"), []byte("value1"))

	// Verify MemTable still has data
	if store.GetMemTableSize() == 0 {
		t.Fatal("MemTable should still have data when not full")
	}

	// Verify no SSTable was created
	if store.GetSSTableCount() != 0 {
		t.Fatal("No SSTable should be created when MemTable is not full")
	}
}
